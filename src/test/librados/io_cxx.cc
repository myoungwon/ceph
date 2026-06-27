// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*
// vim: ts=8 sw=2 sts=2 expandtab

#include <climits>
#include <cstdio>
#include <errno.h>

#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "include/ceph_hash.h"
#include "include/encoding.h"
#include "include/err.h"
#include "include/scope_guard.h"
#include "librados/vector_placement.h"
#include "librados/vector_query_planner.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"

#include "crimson_utils.h"

using namespace librados;
using std::string;

typedef RadosTestPP LibRadosIoPP;
typedef RadosTestECPP LibRadosIoECPP;

static string vector_hex_u32(uint32_t value)
{
  char buf[9];
  snprintf(buf, sizeof(buf), "%08x", value);
  return string(buf);
}

static string vector_hash_string(const string& value)
{
  return vector_hex_u32(ceph_str_hash_rjenkins(
      value.c_str(), static_cast<unsigned>(value.length())));
}

static string vector_test_oid(const string& bucket, const string& index,
                              const string& key,
                              const bufferlist& vector_data)
{
  (void)key;
  const string vector_hash =
    vector_hex_u32(vector_data.crc32c(static_cast<uint32_t>(-1)));
  const string placement_key = vector_hash.substr(0, 4);
  return ".rados.vector/v1/hash-v0/" + vector_hash_string(bucket) + "/" +
    vector_hash_string(index) + "/" + placement_key;
}

static string vector_entry_id(const string& bucket, const string& index,
                              const string& key)
{
  string value;
  value.reserve(bucket.length() + index.length() + key.length() + 2);
  value.append(bucket);
  value.push_back('\0');
  value.append(index);
  value.push_back('\0');
  value.append(key);
  return vector_hex_u32(ceph_str_hash_rjenkins(
      value.c_str(), static_cast<unsigned>(value.length())));
}

static ceph::rados::query_vectors_request_t make_query_planner_request(
    const string& bucket,
    const string& index,
    const bufferlist& query_vector)
{
  ceph::rados::query_vectors_request_t req;
  req.bucket_name = bucket;
  req.index_name = index;
  req.data_type = LIBRADOS_VECTOR_DATA_TYPE_FLOAT32;
  req.distance_metric = LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE;
  req.dimension = 4;
  req.top_k = 10;
  req.return_distance = true;
  req.query_vector = query_vector;
  req.algorithm_id = ceph::rados::vector_query_algorithm_hash;
  req.algorithm_version = ceph::rados::vector_query_algorithm_version_0;
  return req;
}

TEST(VectorQueryPlanner, HashV0DeterministicRouting) {
  float query[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist query_bl;
  query_bl.append(reinterpret_cast<const char *>(query), sizeof(query));

  auto req = make_query_planner_request("bucket", "index", query_bl);
  librados::vector_query::plan_t plan1;
  librados::vector_query::plan_t plan2;
  ASSERT_EQ(0, librados::vector_query::build_plan(req, &plan1));
  ASSERT_EQ(0, librados::vector_query::build_plan(req, &plan2));
  ASSERT_EQ(1u, plan1.probes.size());
  ASSERT_EQ(1u, plan2.probes.size());
  EXPECT_EQ(ceph::rados::vector_query_algorithm_hash, plan1.algorithm_id);
  EXPECT_EQ(ceph::rados::vector_query_algorithm_version_0,
            plan1.algorithm_version);
  EXPECT_EQ(plan1.probes[0].oid.name, plan2.probes[0].oid.name);
  EXPECT_EQ(plan1.probes[0].placement_key,
            plan2.probes[0].placement_key);
  const auto put_placement =
    librados::vector_placement::compute_hash_v0_placement(
        "bucket", "index", query_bl);
  const auto vector_hash =
    librados::vector_placement::hash_v0_vector_hash(query_bl);
  EXPECT_EQ(librados::vector_placement::hash_v0_placement_key(vector_hash),
            plan1.probes[0].placement_key);
  EXPECT_EQ(put_placement.vector_hash, vector_hash);
  EXPECT_EQ(put_placement.placement_key, plan1.probes[0].placement_key);
  EXPECT_EQ(put_placement.oid.name, plan1.probes[0].oid.name);
  EXPECT_EQ(librados::vector_placement::make_hash_v0_oid(
      "bucket", "index", plan1.probes[0].placement_key).name,
      plan1.probes[0].oid.name);

  std::vector<librados::vector_query::routed_request_t> requests;
  ASSERT_EQ(0, librados::vector_query::build_routed_requests(
      req, plan1, &requests));
  ASSERT_EQ(1u, requests.size());
  EXPECT_EQ(plan1.probes[0].oid.name, requests[0].oid.name);
  EXPECT_EQ(plan1.probes[0].placement_key, requests[0].placement_key);
  EXPECT_GT(requests[0].payload.length(), 0u);

  ceph::rados::query_vectors_request_t routed_req;
  auto payload_iter = requests[0].payload.cbegin();
  decode(routed_req, payload_iter);
  ASSERT_EQ(1u, routed_req.probe_prefixes.size());
  EXPECT_EQ(requests[0].placement_key, routed_req.probe_prefixes[0]);
}

TEST(VectorQueryPlanner, HashV0SeparatesBucketAndIndex) {
  float query[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist query_bl;
  query_bl.append(reinterpret_cast<const char *>(query), sizeof(query));

  librados::vector_query::plan_t base;
  librados::vector_query::plan_t other_bucket;
  librados::vector_query::plan_t other_index;
  ASSERT_EQ(0, librados::vector_query::build_plan(
      make_query_planner_request("bucket", "index", query_bl), &base));
  ASSERT_EQ(0, librados::vector_query::build_plan(
      make_query_planner_request("bucket-2", "index", query_bl),
      &other_bucket));
  ASSERT_EQ(0, librados::vector_query::build_plan(
      make_query_planner_request("bucket", "index-2", query_bl),
      &other_index));

  ASSERT_EQ(1u, base.probes.size());
  ASSERT_EQ(1u, other_bucket.probes.size());
  ASSERT_EQ(1u, other_index.probes.size());
  EXPECT_NE(base.probes[0].oid.name, other_bucket.probes[0].oid.name);
  EXPECT_NE(base.probes[0].oid.name, other_index.probes[0].oid.name);
}

TEST(VectorQueryPlanner, PutPlanRespectsWritePolicy) {
  float vector[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector), sizeof(vector));

  ceph::rados::put_vector_request_t req;
  req.bucket_name = "bucket";
  req.index_name = "index";
  req.key = "vec";
  req.data_type = LIBRADOS_VECTOR_DATA_TYPE_FLOAT32;
  req.distance_metric = LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE;
  req.dimension = 4;
  req.vector_data = vector_bl;

  ceph::rados::vector_index_config_t config;
  config.data_type = req.data_type;
  config.distance_metric = req.distance_metric;
  config.dimension = req.dimension;
  config.algorithm_id = ceph::rados::vector_query_algorithm_hash;
  config.algorithm_version = ceph::rados::vector_query_algorithm_version_0;
  config.placement_algorithm = librados::vector_placement::hash_v0_algorithm;
  config.routing_policy.write_pgs = 3;

  librados::vector_query::put_plan_t plan;
  ASSERT_EQ(0, librados::vector_query::build_put_plan(req, config, &plan));
  ASSERT_EQ(3u, plan.targets.size());
  EXPECT_EQ(3u, plan.routing_policy.write_pgs);
  EXPECT_EQ(librados::vector_placement::hash_v0_algorithm,
            plan.placement_algorithm);

  const auto placement =
    librados::vector_placement::compute_hash_v0_placement(
        "bucket", "index", vector_bl);
  EXPECT_EQ(placement.oid.name, plan.targets[0].oid.name);
  EXPECT_EQ(placement.placement_key, plan.targets[0].placement_key);
  EXPECT_EQ(placement.vector_hash, plan.targets[0].vector_hash);
  EXPECT_NE(plan.targets[0].placement_key, plan.targets[1].placement_key);
  EXPECT_NE(plan.targets[1].placement_key, plan.targets[2].placement_key);
}

TEST_F(LibRadosIoPP, TooBigPP) {
  IoCtx ioctx;
  bufferlist bl;
  ASSERT_EQ(-E2BIG, ioctx.write("foo", bl, UINT_MAX, 0));
  ASSERT_EQ(-E2BIG, ioctx.append("foo", bl, UINT_MAX));
  // ioctx.write_full no way to overflow bl.length()
  ASSERT_EQ(-E2BIG, ioctx.writesame("foo", bl, UINT_MAX, 0));
}

TEST_F(LibRadosIoPP, SimpleWritePP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ioctx.set_namespace("nspace");
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
}

TEST_F(LibRadosIoPP, PutVectorPP) {
  float vector[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector), sizeof(vector));
  bufferlist metadata;
  metadata.append("related-object", sizeof("related-object"));
  bufferlist updated_metadata;
  updated_metadata.append("updated-object", sizeof("updated-object"));

  ASSERT_EQ(0, ioctx.put_vector(
      "bucket", "index", "vec-pp",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));
  ASSERT_EQ(0, ioctx.put_vector(
      "bucket", "index", "vec-pp",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), updated_metadata));
  ASSERT_EQ(0, ioctx.put_vector(
      "bucket", "index", "vec-pp-2",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  const string oid = vector_test_oid("bucket", "index", "vec-pp", vector_bl);
  const string entry_id = vector_entry_id("bucket", "index", "vec-pp");
  const string entry_id2 = vector_entry_id("bucket", "index", "vec-pp-2");
  const string vector_hash =
    vector_hex_u32(vector_bl.crc32c(static_cast<uint32_t>(-1)));
  // Vector omap layout example:
  //   _CONTENT_<vector_hash> stores the raw vector payload.
  //   _ENTRY_<entry_id>.* stores metadata for one logical vector entry,
  //   e.g. _ENTRY_abcd1234.user_key and _ENTRY_abcd1234.content_key.
  // The content_key field points back to _CONTENT_<vector_hash>.
  const string content_key = "_CONTENT_" + vector_hash;
  const string prefix = "_ENTRY_" + entry_id + ".";
  const string prefix2 = "_ENTRY_" + entry_id2 + ".";
  std::set<string> keys = {
    content_key,
    prefix + "content_key",
    prefix + "dimension",
    prefix + "metadata",
    prefix + "user_key",
    prefix + "vector_hash",
    prefix2 + "user_key",
  };
  std::map<string, bufferlist> vals;
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(oid, keys, &vals));
  ASSERT_EQ(keys.size(), vals.size());

  auto key_iter = vals[prefix + "user_key"].cbegin();
  string stored_key;
  decode(stored_key, key_iter);
  ASSERT_EQ("vec-pp", stored_key);

  auto content_key_iter = vals[prefix + "content_key"].cbegin();
  string stored_content_key;
  decode(stored_content_key, content_key_iter);
  ASSERT_EQ(content_key, stored_content_key);

  auto key2_iter = vals[prefix2 + "user_key"].cbegin();
  string stored_key2;
  decode(stored_key2, key2_iter);
  ASSERT_EQ("vec-pp-2", stored_key2);

  auto dimension_iter = vals[prefix + "dimension"].cbegin();
  uint32_t stored_dimension = 0;
  decode(stored_dimension, dimension_iter);
  ASSERT_EQ(4u, stored_dimension);

  auto vector_hash_iter = vals[prefix + "vector_hash"].cbegin();
  string stored_vector_hash;
  decode(stored_vector_hash, vector_hash_iter);
  ASSERT_EQ(vector_hash, stored_vector_hash);

  ASSERT_EQ(vector_bl.length(), vals[content_key].length());
  ASSERT_EQ(0, memcmp(vector_bl.c_str(), vals[content_key].c_str(),
                      vector_bl.length()));

  ASSERT_EQ(updated_metadata.length(), vals[prefix + "metadata"].length());
  ASSERT_EQ(0, memcmp(updated_metadata.c_str(),
                      vals[prefix + "metadata"].c_str(),
                      updated_metadata.length()));

  ASSERT_EQ(-EINVAL, ioctx.put_vector(
      "bucket", "index", "bad-dim",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      0, vector, sizeof(vector), metadata));

  ASSERT_EQ(-EINVAL, ioctx.put_vector(
      "bucket", "index", "null-vector",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, nullptr, sizeof(vector), metadata));

  ASSERT_EQ(-EINVAL, ioctx.put_vector(
      "bucket", "", "empty-index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  ASSERT_EQ(-EINVAL, ioctx.put_vector(
      "bucket", "index", "",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  ASSERT_EQ(-EOPNOTSUPP, ioctx.put_vector(
      "bucket", "index", "bad-type",
      static_cast<rados_vector_data_type_t>(999),
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  ASSERT_EQ(-EOPNOTSUPP, ioctx.put_vector(
      "bucket", "index", "bad-metric",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      static_cast<rados_vector_distance_metric_t>(999),
      4, vector, sizeof(vector), metadata));
}

TEST_F(LibRadosIoPP, QueryVectorsValidationPP) {
  float vector[] = {1.0, 2.0, 3.0, 4.0};
  std::vector<query_vectors_result_entry> results;

  ASSERT_EQ(-EOPNOTSUPP, ioctx.query_vectors(
      "bucket", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 10, true, &results));
  ASSERT_TRUE(results.empty());

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "bucket", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      0, vector, sizeof(vector), 10, true, &results));

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "bucket", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 0, true, &results));

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 10, true, &results));

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "bucket", "",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 10, true, &results));

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "bucket", "index",
      static_cast<rados_vector_data_type_t>(999),
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 10, true, &results));

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "bucket", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      static_cast<rados_vector_distance_metric_t>(999),
      4, vector, sizeof(vector), 10, true, &results));

  ASSERT_EQ(-EINVAL, ioctx.query_vectors(
      "bucket", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector) - 1, 10, true, &results));
}

TEST_F(LibRadosIoPP, ReadOpPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));

  {
      bufferlist op_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist op_bl;
      ObjectReadOperation op;
      op.read(0, 0, NULL, NULL); //len=0 mean read the whole object data.
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl, op_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist op_bl;
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, rval);
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl, op_bl;
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, rval);
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl1, read_bl2, op_bl;
      int rval1 = 1000, rval2 = 1002;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl1, &rval1);
      op.read(0, sizeof(buf), &read_bl2, &rval2);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), read_bl1.length());
      ASSERT_EQ(sizeof(buf), read_bl2.length());
      ASSERT_EQ(sizeof(buf) * 2, op_bl.length());
      ASSERT_EQ(0, rval1);
      ASSERT_EQ(0, rval2);
      ASSERT_EQ(0, memcmp(read_bl1.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl2.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(op_bl.c_str() + sizeof(buf), buf, sizeof(buf)));
  }

  {
      bufferlist op_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(0, rval);
  }

  {
      bufferlist read_bl;
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(0, rval);
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl1, read_bl2;
      int rval1 = 1000, rval2 = 1002;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl1, &rval1);
      op.read(0, sizeof(buf), &read_bl2, &rval2);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(sizeof(buf), read_bl1.length());
      ASSERT_EQ(sizeof(buf), read_bl2.length());
      ASSERT_EQ(0, rval1);
      ASSERT_EQ(0, rval2);
      ASSERT_EQ(0, memcmp(read_bl1.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl2.c_str(), buf, sizeof(buf)));
  }

  // read into a preallocated buffer with a cached crc
  {
      bufferlist op_bl;
      op_bl.append(std::string(sizeof(buf), 'x'));
      ASSERT_NE(op_bl.crc32c(0), bl.crc32c(0));  // cache 'x' crc

      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));

      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(op_bl.crc32c(0), bl.crc32c(0));
  }
}

TEST_F(LibRadosIoPP, SparseReadOpPP) {
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));

  {
    std::map<uint64_t, uint64_t> extents;
    bufferlist read_bl;
    int rval = -1;
    ObjectReadOperation op;
    op.sparse_read(0, sizeof(buf), &extents, &read_bl, &rval);
    ASSERT_EQ(0, ioctx.operate("foo", &op, nullptr));
    ASSERT_EQ(0, rval);
    assert_eq_sparse(bl, extents, read_bl);
  }
  {
    bufferlist bl;
    bl.append(buf, sizeof(buf) / 2);

    std::map<uint64_t, uint64_t> extents;
    bufferlist read_bl;
    int rval = -1;
    ObjectReadOperation op;
    op.sparse_read(0, sizeof(buf), &extents, &read_bl, &rval, sizeof(buf) / 2, 1);
    ASSERT_EQ(0, ioctx.operate("foo", &op, nullptr));
    ASSERT_EQ(0, rval);
    assert_eq_sparse(bl, extents, read_bl);
  }
}

TEST_F(LibRadosIoPP, SparseReadExtentArrayOpPP) {
  int buf_len = 32;
  char buf[buf_len], zbuf[buf_len];
  memset(buf, 0xcc, buf_len);
  memset(zbuf, 0, buf_len);
  bufferlist bl;
  int i, len = 1024, skip = 5;
  bl.append(buf, buf_len);
  for (i = 0; i < len; i++) {
    if (!(i % skip) || i == (len - 1)) {
      ASSERT_EQ(0, ioctx.write("sparse-read", bl, bl.length(), i * buf_len));
    }
  }

  bufferlist expect_bl;
  for (i = 0; i < len; i++) {
    if (!(i % skip) || i == (len - 1)) {
      expect_bl.append(buf, buf_len);
    } else {
      expect_bl.append(zbuf, buf_len);
    }
  }

  std::map<uint64_t, uint64_t> extents;
  bufferlist read_bl;
  int rval = -1;
  ObjectReadOperation op;
  op.sparse_read(0, len * buf_len, &extents, &read_bl, &rval);
  ASSERT_EQ(0, ioctx.operate("sparse-read", &op, nullptr));
  ASSERT_EQ(0, rval);
  assert_eq_sparse(expect_bl, extents, read_bl);
}

TEST_F(LibRadosIoPP, RoundTripPP) {
  char buf[128];
  Rados cluster;
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  bufferlist cl;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", cl, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(buf, cl.c_str(), sizeof(buf)));
}

TEST_F(LibRadosIoPP, RoundTripPP2)
{
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write;
  write.write(0, bl);
  write.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ASSERT_EQ(0, ioctx.operate("foo", &write));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  read.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_NOCACHE|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}

TEST_F(LibRadosIoPP, Checksum) {
  char buf[128];
  Rados cluster;
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  bufferlist init_value_bl;
  encode(static_cast<uint32_t>(-1), init_value_bl);
  bufferlist csum_bl;
  ASSERT_EQ(0, ioctx.checksum("foo", LIBRADOS_CHECKSUM_TYPE_CRC32C,
			      init_value_bl, sizeof(buf), 0, 0, &csum_bl));
  auto csum_bl_it = csum_bl.cbegin();
  uint32_t csum_count;
  decode(csum_count, csum_bl_it);
  ASSERT_EQ(1U, csum_count);
  uint32_t csum;
  decode(csum, csum_bl_it);
  ASSERT_EQ(bl.crc32c(-1), csum);
}

TEST_F(LibRadosIoPP, ReadIntoBufferlist) {

  // here we test reading into a non-empty bufferlist referencing existing
  // buffers

  char buf[128];
  Rados cluster;
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  bufferlist bl2;
  char buf2[sizeof(buf)];
  memset(buf2, 0xbb, sizeof(buf2));
  bl2.append(buffer::create_static(sizeof(buf2), buf2));
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl2, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(buf, buf2, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf, bl2.c_str(), sizeof(buf)));
}

TEST_F(LibRadosIoPP, OverlappingWriteRoundTripPP) {
  char buf[128];
  char buf2[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write("foo", bl2, sizeof(buf2), 0));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
  ASSERT_EQ(0, memcmp(bl3.c_str() + sizeof(buf2), buf, sizeof(buf) - sizeof(buf2)));
}

TEST_F(LibRadosIoPP, WriteFullRoundTripPP) {
  char buf[128];
  char buf2[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write_full("foo", bl2));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf2), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
}

TEST_F(LibRadosIoPP, WriteFullRoundTripPP2)
{
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write;
  write.write_full(bl);
  write.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_NOCACHE);
  ASSERT_EQ(0, ioctx.operate("foo", &write));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  read.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}

TEST_F(LibRadosIoPP, AppendRoundTripPP) {
  char buf[64];
  char buf2[64];
  memset(buf, 0xde, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  memset(buf2, 0xad, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.append("foo", bl2, sizeof(buf2)));
  bufferlist bl3;
  ASSERT_EQ((int)(sizeof(buf) + sizeof(buf2)),
	    ioctx.read("foo", bl3, (sizeof(buf) + sizeof(buf2)), 0));
  const char *bl3_str = bl3.c_str();
  ASSERT_EQ(0, memcmp(bl3_str, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(bl3_str + sizeof(buf), buf2, sizeof(buf2)));
}

TEST_F(LibRadosIoPP, TruncTestPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl, sizeof(buf)));
  ASSERT_EQ(0, ioctx.trunc("foo", sizeof(buf) / 2));
  bufferlist bl2;
  ASSERT_EQ((int)(sizeof(buf)/2), ioctx.read("foo", bl2, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)/2));
}

TEST_F(LibRadosIoPP, RemoveTestPP) {
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  ASSERT_EQ(0, ioctx.remove("foo"));
  bufferlist bl2;
  ASSERT_EQ(-ENOENT, ioctx.read("foo", bl2, sizeof(buf), 0));
}

TEST_F(LibRadosIoPP, XattrsRoundTripPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl3));
  bufferlist bl4;
  ASSERT_EQ((int)sizeof(attr1_buf),
      ioctx.getxattr("foo", attr1, bl4));
  ASSERT_EQ(0, memcmp(bl4.c_str(), attr1_buf, sizeof(attr1_buf)));
}

TEST_F(LibRadosIoPP, RmXattrPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl2));
  ASSERT_EQ(0, ioctx.rmxattr("foo", attr1));
  bufferlist bl3;
  ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", attr1, bl3));

  // Test rmxattr on a removed object
  char buf2[128];
  char attr2[] = "attr2";
  char attr2_buf[] = "foo bar baz";
  memset(buf2, 0xbb, sizeof(buf2));
  bufferlist bl21;
  bl21.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo_rmxattr", bl21, sizeof(buf2), 0));
  bufferlist bl22;
  bl22.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo_rmxattr", attr2, bl22));
  ASSERT_EQ(0, ioctx.remove("foo_rmxattr"));
  ASSERT_EQ(-ENOENT, ioctx.rmxattr("foo_rmxattr", attr2));
}

TEST_F(LibRadosIoPP, XattrListPP) {
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr2, bl3));
  std::map<std::string, bufferlist> attrset;
  ASSERT_EQ(0, ioctx.getxattrs("foo", attrset));
  for (std::map<std::string, bufferlist>::iterator i = attrset.begin();
       i != attrset.end(); ++i) {
    if (i->first == string(attr1)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr1_buf, sizeof(attr1_buf)));
    }
    else if (i->first == string(attr2)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr2_buf, sizeof(attr2_buf)));
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
}

TEST_F(LibRadosIoPP, CrcZeroWrite) {
  char buf[128];
  bufferlist bl;

  ASSERT_EQ(0, ioctx.write("foo", bl, 0, 0));
  ASSERT_EQ(0, ioctx.write("foo", bl, 0, sizeof(buf)));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
}

TEST_F(LibRadosIoECPP, SimpleWritePP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  ioctx.set_namespace("nspace");
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
}

TEST_F(LibRadosIoECPP, ReadOpPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));

  {
      bufferlist op_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
    bufferlist op_bl;
    ObjectReadOperation op;
    op.read(0, 0, NULL, NULL); //len=0 mean read the whole object data
    ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
    ASSERT_EQ(sizeof(buf), op_bl.length());
    ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl, op_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist op_bl;
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, rval);
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl, op_bl;
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, rval);
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl1, read_bl2, op_bl;
      int rval1 = 1000, rval2 = 1002;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl1, &rval1);
      op.read(0, sizeof(buf), &read_bl2, &rval2);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), read_bl1.length());
      ASSERT_EQ(sizeof(buf), read_bl2.length());
      ASSERT_EQ(sizeof(buf) * 2, op_bl.length());
      ASSERT_EQ(0, rval1);
      ASSERT_EQ(0, rval2);
      ASSERT_EQ(0, memcmp(read_bl1.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl2.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(op_bl.c_str() + sizeof(buf), buf, sizeof(buf)));
  }

  {
      bufferlist op_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));
      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(0, rval);
  }

  {
      bufferlist read_bl;
      int rval = 1000;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl, &rval);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(sizeof(buf), read_bl.length());
      ASSERT_EQ(0, rval);
      ASSERT_EQ(0, memcmp(read_bl.c_str(), buf, sizeof(buf)));
  }

  {
      bufferlist read_bl1, read_bl2;
      int rval1 = 1000, rval2 = 1002;
      ObjectReadOperation op;
      op.read(0, sizeof(buf), &read_bl1, &rval1);
      op.read(0, sizeof(buf), &read_bl2, &rval2);
      ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
      ASSERT_EQ(sizeof(buf), read_bl1.length());
      ASSERT_EQ(sizeof(buf), read_bl2.length());
      ASSERT_EQ(0, rval1);
      ASSERT_EQ(0, rval2);
      ASSERT_EQ(0, memcmp(read_bl1.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(0, memcmp(read_bl2.c_str(), buf, sizeof(buf)));
  }

  // read into a preallocated buffer with a cached crc
  {
      bufferlist op_bl;
      op_bl.append(std::string(sizeof(buf), 'x'));
      ASSERT_NE(op_bl.crc32c(0), bl.crc32c(0));  // cache 'x' crc

      ObjectReadOperation op;
      op.read(0, sizeof(buf), NULL, NULL);
      ASSERT_EQ(0, ioctx.operate("foo", &op, &op_bl));

      ASSERT_EQ(sizeof(buf), op_bl.length());
      ASSERT_EQ(0, memcmp(op_bl.c_str(), buf, sizeof(buf)));
      ASSERT_EQ(op_bl.crc32c(0), bl.crc32c(0));
  }
}

TEST_F(LibRadosIoECPP, SparseReadOpPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));

  {
    std::map<uint64_t, uint64_t> extents;
    bufferlist read_bl;
    int rval = -1;
    ObjectReadOperation op;
    op.sparse_read(0, sizeof(buf), &extents, &read_bl, &rval);
    ASSERT_EQ(0, ioctx.operate("foo", &op, nullptr));
    ASSERT_EQ(0, rval);
    assert_eq_sparse(bl, extents, read_bl);
  }
}

TEST_F(LibRadosIoECPP, RoundTripPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  Rados cluster;
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl, sizeof(buf), 0));
  bufferlist cl;
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", cl, sizeof(buf) * 3, 0));
  ASSERT_EQ(0, memcmp(buf, cl.c_str(), sizeof(buf)));
}

TEST_F(LibRadosIoECPP, RoundTripPP2)
{
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write;
  write.write(0, bl);
  write.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ASSERT_EQ(0, ioctx.operate("foo", &write));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  read.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}

TEST_F(LibRadosIoECPP, OverlappingWriteRoundTripPP) {
  SKIP_IF_CRIMSON();
  int bsize = alignment;
  int dbsize = bsize * 2;
  char *buf = (char *)new char[dbsize];
  char *buf2 = (char *)new char[bsize];
  auto cleanup = [&] {
    delete[] buf;
    delete[] buf2;
  };
  scope_guard<decltype(cleanup)> sg(std::move(cleanup));
  memset(buf, 0xcc, dbsize);
  bufferlist bl1;
  bl1.append(buf, dbsize);
  ASSERT_EQ(0, ioctx.write("foo", bl1, dbsize, 0));
  memset(buf2, 0xdd, bsize);
  bufferlist bl2;
  bl2.append(buf2, bsize);
  ASSERT_EQ(-EOPNOTSUPP, ioctx.write("foo", bl2, bsize, 0));
  bufferlist bl3;
  ASSERT_EQ(dbsize, ioctx.read("foo", bl3, dbsize, 0));
  // Read the same as first write
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf, dbsize));
}

TEST_F(LibRadosIoECPP, WriteFullRoundTripPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  char buf2[64];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo", bl1, sizeof(buf), 0));
  memset(buf2, 0xdd, sizeof(buf2));
  bufferlist bl2;
  bl2.append(buf2, sizeof(buf2));
  ASSERT_EQ(0, ioctx.write_full("foo", bl2));
  bufferlist bl3;
  ASSERT_EQ((int)sizeof(buf2), ioctx.read("foo", bl3, sizeof(buf), 0));
  ASSERT_EQ(0, memcmp(bl3.c_str(), buf2, sizeof(buf2)));
}

TEST_F(LibRadosIoECPP, WriteFullRoundTripPP2)
{
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write;
  write.write_full(bl);
  write.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  ASSERT_EQ(0, ioctx.operate("foo", &write));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  read.set_op_flags2(LIBRADOS_OP_FLAG_FADVISE_DONTNEED|LIBRADOS_OP_FLAG_FADVISE_RANDOM);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}

TEST_F(LibRadosIoECPP, AppendRoundTripPP) {
  SKIP_IF_CRIMSON();
  char *buf = (char *)new char[alignment];
  char *buf2 = (char *)new char[alignment];
  auto cleanup = [&] {
    delete[] buf;
    delete[] buf2;
  };
  scope_guard<decltype(cleanup)> sg(std::move(cleanup));
  memset(buf, 0xde, alignment);
  bufferlist bl1;
  bl1.append(buf, alignment);
  ASSERT_EQ(0, ioctx.append("foo", bl1, alignment));
  memset(buf2, 0xad, alignment);
  bufferlist bl2;
  bl2.append(buf2, alignment);
  ASSERT_EQ(0, ioctx.append("foo", bl2, alignment));
  bufferlist bl3;
  ASSERT_EQ((int)(alignment * 2),
	    ioctx.read("foo", bl3, (alignment * 4), 0));
  const char *bl3_str = bl3.c_str();
  ASSERT_EQ(0, memcmp(bl3_str, buf, alignment));
  ASSERT_EQ(0, memcmp(bl3_str + alignment, buf2, alignment));
}

TEST_F(LibRadosIoECPP, TruncTestPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl, sizeof(buf)));
  ASSERT_EQ(-EOPNOTSUPP, ioctx.trunc("foo", sizeof(buf) / 2));
  bufferlist bl2;
  // Same size
  ASSERT_EQ((int)sizeof(buf), ioctx.read("foo", bl2, sizeof(buf), 0));
  // No change
  ASSERT_EQ(0, memcmp(bl2.c_str(), buf, sizeof(buf)));
}

TEST_F(LibRadosIoECPP, RemoveTestPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  ASSERT_EQ(0, ioctx.remove("foo"));
  bufferlist bl2;
  ASSERT_EQ(-ENOENT, ioctx.read("foo", bl2, sizeof(buf), 0));
}

TEST_F(LibRadosIoECPP, XattrsRoundTripPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl3));
  bufferlist bl4;
  ASSERT_EQ((int)sizeof(attr1_buf),
      ioctx.getxattr("foo", attr1, bl4));
  ASSERT_EQ(0, memcmp(bl4.c_str(), attr1_buf, sizeof(attr1_buf)));
}

TEST_F(LibRadosIoECPP, RmXattrPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl2));
  ASSERT_EQ(0, ioctx.rmxattr("foo", attr1));
  bufferlist bl3;
  ASSERT_EQ(-ENODATA, ioctx.getxattr("foo", attr1, bl3));

  // Test rmxattr on a removed object
  char buf2[128];
  char attr2[] = "attr2";
  char attr2_buf[] = "foo bar baz";
  memset(buf2, 0xbb, sizeof(buf2));
  bufferlist bl21;
  bl21.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.write("foo_rmxattr", bl21, sizeof(buf2), 0));
  bufferlist bl22;
  bl22.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo_rmxattr", attr2, bl22));
  ASSERT_EQ(0, ioctx.remove("foo_rmxattr"));
  ASSERT_EQ(-ENOENT, ioctx.rmxattr("foo_rmxattr", attr2));
}

TEST_F(LibRadosIoECPP, CrcZeroWrite) {
  SKIP_IF_CRIMSON();
  set_allow_ec_overwrites();
  char buf[128];
  memset(buf, 0xcc, sizeof(buf));
  bufferlist bl;
  bl.append(buf, sizeof(buf));

  ASSERT_EQ(0, ioctx.write("foo", bl, 0, 0));
  ASSERT_EQ(0, ioctx.write("foo", bl, 0, sizeof(buf)));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
}

TEST_F(LibRadosIoECPP, XattrListPP) {
  SKIP_IF_CRIMSON();
  char buf[128];
  char attr1[] = "attr1";
  char attr1_buf[] = "foo bar baz";
  char attr2[] = "attr2";
  char attr2_buf[256];
  for (size_t j = 0; j < sizeof(attr2_buf); ++j) {
    attr2_buf[j] = j % 0xff;
  }
  memset(buf, 0xaa, sizeof(buf));
  bufferlist bl1;
  bl1.append(buf, sizeof(buf));
  ASSERT_EQ(0, ioctx.append("foo", bl1, sizeof(buf)));
  bufferlist bl2;
  bl2.append(attr1_buf, sizeof(attr1_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr1, bl2));
  bufferlist bl3;
  bl3.append(attr2_buf, sizeof(attr2_buf));
  ASSERT_EQ(0, ioctx.setxattr("foo", attr2, bl3));
  std::map<std::string, bufferlist> attrset;
  ASSERT_EQ(0, ioctx.getxattrs("foo", attrset));
  for (std::map<std::string, bufferlist>::iterator i = attrset.begin();
       i != attrset.end(); ++i) {
    if (i->first == string(attr1)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr1_buf, sizeof(attr1_buf)));
    }
    else if (i->first == string(attr2)) {
      ASSERT_EQ(0, memcmp(i->second.c_str(), attr2_buf, sizeof(attr2_buf)));
    }
    else {
      ASSERT_EQ(0, 1);
    }
  }
}

TEST_F(LibRadosIoPP, CmpExtPP) {
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write1));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write2;
  write2.cmpext(0, bl, nullptr);
  write2.write(0, new_bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write2));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "CEPH", 4));
}

TEST_F(LibRadosIoPP, CmpExtDNEPP) {
  bufferlist bl;
  bl.append(std::string(4, '\0'));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write;
  write.cmpext(0, bl, nullptr);
  write.write(0, new_bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "CEPH", 4));
}

TEST_F(LibRadosIoPP, CmpExtMismatchPP) {
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write1));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write2;
  write2.cmpext(0, new_bl, nullptr);
  write2.write(0, new_bl);
  ASSERT_EQ(-MAX_ERRNO, ioctx.operate("foo", &write2));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}

TEST_F(LibRadosIoECPP, CmpExtPP) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write1));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write2;
  write2.cmpext(0, bl, nullptr);
  write2.write_full(new_bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write2));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "CEPH", 4));
}

TEST_F(LibRadosIoECPP, CmpExtDNEPP) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append(std::string(4, '\0'));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write;
  write.cmpext(0, bl, nullptr);
  write.write_full(new_bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "CEPH", 4));
}

TEST_F(LibRadosIoECPP, CmpExtMismatchPP) {
  SKIP_IF_CRIMSON();
  bufferlist bl;
  bl.append("ceph");
  ObjectWriteOperation write1;
  write1.write(0, bl);
  ASSERT_EQ(0, ioctx.operate("foo", &write1));

  bufferlist new_bl;
  new_bl.append("CEPH");
  ObjectWriteOperation write2;
  write2.cmpext(0, new_bl, nullptr);
  write2.write_full(new_bl);
  ASSERT_EQ(-MAX_ERRNO, ioctx.operate("foo", &write2));

  ObjectReadOperation read;
  read.read(0, bl.length(), NULL, NULL);
  ASSERT_EQ(0, ioctx.operate("foo", &read, &bl));
  ASSERT_EQ(0, memcmp(bl.c_str(), "ceph", 4));
}
