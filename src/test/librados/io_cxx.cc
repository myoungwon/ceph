// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*
// vim: ts=8 sw=2 sts=2 expandtab

#include <algorithm>
#include <array>
#include <atomic>
#include <climits>
#include <cstdio>
#include <errno.h>
#include <limits>
#include <memory>
#include <set>
#include <span>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <unistd.h>

#include "gtest/gtest.h"

#include "include/rados/librados.hpp"
#include "include/ceph_hash.h"
#include "include/encoding.h"
#include "include/err.h"
#include "include/scope_guard.h"
#include "common/vector_query_exec.h"
#include "common/vector_omap_scan.h"
#include "json_spirit/json_spirit.h"
#include "librados/vector_pg_lsh.h"
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

static string vector_test_oid(const string& bucket, const string& index,
                              const string& key,
                              const bufferlist& vector_data)
{
  (void)key;
  std::vector<float> values;
  if (librados::vector_placement::copy_float32_vector(
        vector_data, 4, &values) < 0) {
    return string();
  }
  const uint32_t signature =
    librados::vector_placement::lsh_v0_signature(values, 0);
  const string placement_key =
    librados::vector_placement::lsh_v0_placement_key(0, signature);
  return librados::vector_placement::make_lsh_v0_oid(
      bucket, index, placement_key).name;
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

static uint32_t pg_lsh_test_lsh_bucket_id_for_pg(uint32_t table,
                                                 uint32_t pg_num,
                                                 uint32_t seed,
                                                 uint32_t target_pg)
{
  for (uint32_t lsh_bucket_id = 0;
       lsh_bucket_id < (1U << ceph::rados::vector_lsh_v0_max_bits);
       ++lsh_bucket_id) {
    if (librados::vector_placement::pg_lsh_v0_group_to_pg(
          table, lsh_bucket_id, pg_num, seed) == target_pg) {
      return lsh_bucket_id;
    }
  }
  return std::numeric_limits<uint32_t>::max();
}

static uint32_t pg_lsh_test_lsh_bucket_id_not_pg(uint32_t table,
                                                 uint32_t pg_num,
                                                 uint32_t seed,
                                                 uint32_t excluded_pg)
{
  for (uint32_t lsh_bucket_id = 0;
       lsh_bucket_id < (1U << ceph::rados::vector_lsh_v0_max_bits);
       ++lsh_bucket_id) {
    if (librados::vector_placement::pg_lsh_v0_group_to_pg(
          table, lsh_bucket_id, pg_num, seed) != excluded_pg) {
      return lsh_bucket_id;
    }
  }
  return std::numeric_limits<uint32_t>::max();
}

static librados::vector_query::query_request_t make_query_planner_request(
    const string& bucket,
    const string& index,
    const bufferlist& query_vector)
{
  librados::vector_query::query_request_t req;
  req.bucket_name = bucket;
  req.index_name = index;
  req.data_type = LIBRADOS_VECTOR_DATA_TYPE_FLOAT32;
  req.distance_metric = LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE;
  req.dimension = 4;
  req.top_k = 10;
  req.query_vector = query_vector;
  return req;
}

static librados::vector_pg_lsh::index_config_t make_pg_lsh_test_index_config(
    uint32_t dimension,
    const librados::vector_pg_lsh::pool_pg_info_t& pool_info,
    uint32_t k,
    uint32_t l,
    uint32_t seed,
    uint32_t d,
    uint32_t distance_bucket_bits = 0,
    uint32_t residual_bits = 0)
{
  librados::vector_pg_lsh::index_config_t config;
  config.dimension = dimension;
  config.data_type = ceph::rados::vector_data_type_float32;
  config.distance_metric = ceph::rados::vector_distance_metric_euclidean;
  config.k = k;
  config.l = l;
  config.seed = seed;
  config.d = d;
  config.creation_pg_num = pool_info.pg_num;
  config.creation_pgp_num = pool_info.pgp_num;
  config.distance_bucket_bits = distance_bucket_bits;
  config.residual_bits = residual_bits;
  config.anchor_mode = librados::vector_pg_lsh::anchor_mode_random;
  EXPECT_EQ(0, librados::vector_placement::pg_lsh_v0_random_anchor(
      dimension, seed, &config.anchor));
  return config;
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
  EXPECT_EQ(librados::vector_placement::hash_v0_algorithm,
            plan1.placement_algorithm);
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
  ASSERT_TRUE(payload_iter.end());
  EXPECT_EQ("bucket", routed_req.bucket_name);
  EXPECT_EQ("index", routed_req.index_name);
  EXPECT_EQ(4u, routed_req.dimension);
  EXPECT_EQ(10u, routed_req.local_top_k);
  EXPECT_EQ(query_bl.length(), routed_req.query_vector.length());
  ASSERT_EQ(1u, routed_req.probe_prefixes.size());
  EXPECT_EQ(requests[0].placement_key, routed_req.probe_prefixes[0]);
  EXPECT_EQ(0, ceph::rados::vector_query_exec::validate_query_request(
      routed_req));
}

TEST(VectorQueryValidation, RejectsMalformedHashV0WireFields) {
  float query[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist query_bl;
  query_bl.append(reinterpret_cast<const char *>(query), sizeof(query));

  auto req = make_query_planner_request("bucket", "index", query_bl);
  ceph::rados::query_vectors_request_t wire_req;
  wire_req.bucket_name = req.bucket_name;
  wire_req.index_name = req.index_name;
  wire_req.data_type = req.data_type;
  wire_req.distance_metric = req.distance_metric;
  wire_req.dimension = req.dimension;
  wire_req.local_top_k = req.top_k;
  wire_req.query_vector = req.query_vector;
  wire_req.probe_prefixes.push_back("abcd");
  ASSERT_EQ(0, ceph::rados::vector_query_exec::validate_query_request(
      wire_req));

  auto bad_prefix = wire_req;
  bad_prefix.probe_prefixes = {"abc"};
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_query_request(
      bad_prefix));

  auto bad_prefix_case = wire_req;
  bad_prefix_case.probe_prefixes = {"ABCd"};
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_query_request(
      bad_prefix_case));

  auto empty_prefix = wire_req;
  empty_prefix.probe_prefixes = {""};
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_query_request(
      empty_prefix));

  auto empty_local_topk = wire_req;
  empty_local_topk.local_top_k = 0;
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_query_request(
      empty_local_topk));
}

TEST(VectorQueryValidation, PlannerFieldsStayOutOfWireRequest) {
  float query[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist query_bl;
  query_bl.append(reinterpret_cast<const char *>(query), sizeof(query));

  auto req = make_query_planner_request("bucket", "index", query_bl);
  ceph::rados::vector_index_config_t config;
  config.data_type = req.data_type;
  config.distance_metric = req.distance_metric;
  config.dimension = req.dimension;
  config.algorithm_id = ceph::rados::vector_query_algorithm_hash;
  config.algorithm_version = ceph::rados::vector_query_algorithm_version_0;
  config.placement_algorithm = librados::vector_placement::hash_v0_algorithm;
  config.routing_policy.probe_pgs = 1;
  config.routing_policy.write_pgs = 3;
  config.routing_policy.local_topk = 2;

  librados::vector_query::query_plan_t plan;
  ASSERT_EQ(0, librados::vector_query::build_query_plan(req, config, &plan));

  std::vector<librados::vector_query::routed_request_t> requests;
  ASSERT_EQ(0, librados::vector_query::build_routed_requests(
      req, plan, &requests));
  ASSERT_EQ(1u, requests.size());

  ceph::rados::query_vectors_request_t routed_req;
  auto payload_iter = requests[0].payload.cbegin();
  decode(routed_req, payload_iter);
  ASSERT_TRUE(payload_iter.end());
  EXPECT_EQ(2u, routed_req.local_top_k);
  ASSERT_EQ(1u, routed_req.probe_prefixes.size());
  EXPECT_EQ(requests[0].placement_key, routed_req.probe_prefixes[0]);
  EXPECT_EQ(0, ceph::rados::vector_query_exec::validate_query_request(
      routed_req));
}

TEST(VectorPutValidation, RequiresHashV0RoutedFields) {
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
  req.placement_algorithm = librados::vector_placement::hash_v0_algorithm;
  req.placement_key = "abcd";
  req.vector_hash = "1234abcd";
  ASSERT_EQ(0, ceph::rados::vector_query_exec::validate_put_request(
      req, true));

  auto bad_algorithm = req;
  bad_algorithm.placement_algorithm.clear();
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_put_request(
      bad_algorithm, true));

  auto bad_placement_key = req;
  bad_placement_key.placement_key = "abc";
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_put_request(
      bad_placement_key, true));

  auto bad_hash = req;
  bad_hash.vector_hash = "1234abcg";
  EXPECT_EQ(-EINVAL, ceph::rados::vector_query_exec::validate_put_request(
      bad_hash, true));
}

TEST(VectorQueryResults, DedupsByEntryIdKeepingBestDistance) {
  std::vector<ceph::rados::query_vectors_result_entry_t> entries;

  ceph::rados::query_vectors_result_entry_t first;
  first.key = "vec-a";
  first.entry_id = "entry-a";
  first.distance = 10.0;
  ceph::rados::vector_query_exec::merge_result_entry(&entries, first);

  ceph::rados::query_vectors_result_entry_t duplicate;
  duplicate.key = "vec-a";
  duplicate.entry_id = "entry-a";
  duplicate.distance = 2.0;
  ceph::rados::vector_query_exec::merge_result_entry(&entries, duplicate);

  ceph::rados::query_vectors_result_entry_t same_key_other_entry;
  same_key_other_entry.key = "vec-a";
  same_key_other_entry.entry_id = "entry-b";
  same_key_other_entry.distance = 1.0;
  ceph::rados::vector_query_exec::merge_result_entry(
      &entries, same_key_other_entry);

  ASSERT_EQ(2u, entries.size());
  auto entry_a = std::find_if(
      entries.begin(), entries.end(), [](const auto& entry) {
        return entry.entry_id == "entry-a";
      });
  ASSERT_NE(entries.end(), entry_a);
  EXPECT_EQ("vec-a", entry_a->key);
  EXPECT_EQ(2.0, entry_a->distance);

  auto entry_b = std::find_if(
      entries.begin(), entries.end(), [](const auto& entry) {
        return entry.entry_id == "entry-b";
      });
  ASSERT_NE(entries.end(), entry_b);
  EXPECT_EQ("vec-a", entry_b->key);
  EXPECT_EQ(1.0, entry_b->distance);
}

TEST(VectorQueryExecutor, LocalTopKLimitsPartialResults) {
  float query[] = {0.0, 0.0};
  float near[] = {1.0, 0.0};
  float far[] = {3.0, 4.0};
  bufferlist query_bl;
  bufferlist near_bl;
  bufferlist far_bl;
  query_bl.append(reinterpret_cast<const char *>(query), sizeof(query));
  near_bl.append(reinterpret_cast<const char *>(near), sizeof(near));
  far_bl.append(reinterpret_cast<const char *>(far), sizeof(far));

  ceph::rados::query_vectors_request_t req;
  req.bucket_name = "bucket";
  req.index_name = "index";
  req.data_type = LIBRADOS_VECTOR_DATA_TYPE_FLOAT32;
  req.distance_metric = LIBRADOS_VECTOR_DISTANCE_METRIC_EUCLIDEAN;
  req.dimension = 2;
  req.local_top_k = 1;
  req.query_vector = query_bl;
  req.probe_prefixes.push_back("abcd");

  ceph::rados::vector_query_exec::omap_scan_state_t scan;
  ceph::rados::vector_query_exec::omap_entry_t near_entry;
  near_entry.entry_id = "entry-near";
  near_entry.bucket_name = req.bucket_name;
  near_entry.index_name = req.index_name;
  near_entry.user_key = "vec-near";
  near_entry.content_key = "_CONTENT_near";
  near_entry.placement_key = "abcd";
  near_entry.data_type = req.data_type;
  near_entry.distance_metric = req.distance_metric;
  near_entry.dimension = req.dimension;
  near_entry.has_data_type = true;
  near_entry.has_distance_metric = true;
  near_entry.has_dimension = true;
  scan.entries[near_entry.entry_id] = near_entry;
  scan.contents[near_entry.content_key] = near_bl;

  auto far_entry = near_entry;
  far_entry.entry_id = "entry-far";
  far_entry.user_key = "vec-far";
  far_entry.content_key = "_CONTENT_far";
  scan.entries[far_entry.entry_id] = far_entry;
  scan.contents[far_entry.content_key] = far_bl;

  ceph::rados::query_vectors_result_t result;
  ceph::rados::vector_query_exec::local_query_accumulator_t accumulator;
  ASSERT_EQ(0, accumulator.prepare(req));
  for (const auto& [entry_id, entry] : scan.entries) {
    (void)entry_id;
    ASSERT_EQ(0, accumulator.consume(
        ceph::rados::vector_query_exec::make_omap_entry_view(entry, scan)));
  }
  ASSERT_EQ(0, accumulator.finish(&result));
  ASSERT_EQ(1u, result.entries.size());
  EXPECT_EQ("entry-near", result.entries[0].entry_id);
  EXPECT_EQ("vec-near", result.entries[0].key);
  EXPECT_NEAR(1.0f, result.entries[0].distance, 1e-6f);
}

TEST(VectorQueryExecutor, DistanceHandlesFragmentedCandidateWithoutRebuild) {
  const std::vector<float> query_values = {0.0f, 0.0f};
  const float candidate_values[] = {3.0f, 4.0f};
  bufferlist candidate;
  for (const float value : candidate_values) {
    bufferptr segment(sizeof(value));
    std::memcpy(segment.c_str(), &value, sizeof(value));
    candidate.push_back(std::move(segment));
  }
  ASSERT_FALSE(candidate.is_contiguous());
  const unsigned original_buffer_count = candidate.get_num_buffers();

  ceph::rados::query_vectors_request_t req;
  req.data_type = LIBRADOS_VECTOR_DATA_TYPE_FLOAT32;
  req.distance_metric = LIBRADOS_VECTOR_DISTANCE_METRIC_EUCLIDEAN;
  req.dimension = query_values.size();

  float distance = 0;
  ASSERT_EQ(0, ceph::rados::vector_query_exec::compute_float32_distance(
      req, query_values,
      ceph::rados::vector_query_exec::squared_l2_norm(query_values),
      candidate, &distance));
  EXPECT_NEAR(5.0f, distance, 1e-6f);
  EXPECT_FALSE(candidate.is_contiguous());
  EXPECT_EQ(original_buffer_count, candidate.get_num_buffers());
}

TEST(VectorQueryExecutor, LocalTopKKeepsBoundedSortedResults) {
  float query[] = {0.0, 0.0};
  bufferlist query_bl;
  query_bl.append(reinterpret_cast<const char *>(query), sizeof(query));

  ceph::rados::query_vectors_request_t req;
  req.bucket_name = "bucket";
  req.index_name = "index";
  req.data_type = LIBRADOS_VECTOR_DATA_TYPE_FLOAT32;
  req.distance_metric = LIBRADOS_VECTOR_DISTANCE_METRIC_EUCLIDEAN;
  req.dimension = 2;
  req.local_top_k = 2;
  req.query_vector = query_bl;

  ceph::rados::vector_query_exec::omap_scan_state_t scan;
  auto add_entry = [&](const string& entry_id,
                       const string& key,
                       const string& content_key,
                       float x,
                       float y) {
    ceph::rados::vector_query_exec::omap_entry_t entry;
    entry.entry_id = entry_id;
    entry.bucket_name = req.bucket_name;
    entry.index_name = req.index_name;
    entry.user_key = key;
    entry.content_key = content_key;
    entry.data_type = req.data_type;
    entry.distance_metric = req.distance_metric;
    entry.dimension = req.dimension;
    entry.has_data_type = true;
    entry.has_distance_metric = true;
    entry.has_dimension = true;
    scan.entries[entry.entry_id] = entry;

    float vector[] = {x, y};
    bufferlist vector_bl;
    vector_bl.append(reinterpret_cast<const char *>(vector), sizeof(vector));
    scan.contents[entry.content_key] = vector_bl;
  };

  add_entry("entry-a", "vec-a", "_CONTENT_a", 5.0, 0.0);
  add_entry("entry-b", "vec-b", "_CONTENT_b", 1.0, 0.0);
  add_entry("entry-c", "vec-c", "_CONTENT_c", 3.0, 0.0);
  add_entry("entry-d", "vec-d", "_CONTENT_d", 2.0, 0.0);

  ceph::rados::query_vectors_result_t result;
  ceph::rados::vector_query_exec::query_filter_stats_t stats;
  ceph::rados::vector_query_exec::local_query_accumulator_t accumulator;
  ASSERT_EQ(0, accumulator.prepare(req));
  for (const auto& [entry_id, entry] : scan.entries) {
    (void)entry_id;
    ASSERT_EQ(0, accumulator.consume(
        ceph::rados::vector_query_exec::make_omap_entry_view(entry, scan)));
  }
  ASSERT_EQ(0, accumulator.finish(&result, &stats));
  ASSERT_EQ(2u, result.entries.size());
  EXPECT_EQ("entry-b", result.entries[0].entry_id);
  EXPECT_EQ("entry-d", result.entries[1].entry_id);
  EXPECT_LE(result.entries[0].distance, result.entries[1].distance);
  EXPECT_EQ(4u, stats.matched_entries);
  EXPECT_EQ(4u, stats.merged_entries);
  EXPECT_EQ(2u, stats.final_entries);
}

namespace {

struct vector_query_run_t {
  ceph::rados::query_vectors_result_t result;
  ceph::rados::vector_query_exec::query_filter_stats_t stats;
};

bufferlist make_float32_vector(float x, float y)
{
  const float values[] = {x, y};
  bufferlist bl;
  bl.append(
      reinterpret_cast<const char *>(values), sizeof(values));
  return bl;
}

ceph::os::vector_record_t make_query_record(
    const string& entry_id,
    const string& key,
    float x,
    float y,
    uint32_t metric,
    const string& placement_key = "abcd")
{
  ceph::os::vector_record_t record;
  record.entry_id = entry_id;
  record.bucket_name = "bucket";
  record.index_name = "index";
  record.user_key = key;
  record.data_type = ceph::rados::vector_data_type_float32;
  record.distance_metric = metric;
  record.dimension = 2;
  record.placement_algorithm =
    ceph::rados::vector_placement_algorithm_hash_v0;
  record.placement_key = placement_key;
  record.vector_hash = entry_id;
  record.vector_data = make_float32_vector(x, y);
  return record;
}

ceph::rados::query_vectors_request_t make_local_query(
    uint32_t metric,
    uint32_t local_top_k)
{
  ceph::rados::query_vectors_request_t req;
  req.bucket_name = "bucket";
  req.index_name = "index";
  req.data_type = ceph::rados::vector_data_type_float32;
  req.distance_metric = metric;
  req.dimension = 2;
  req.local_top_k = local_top_k;
  req.query_vector = make_float32_vector(1.0, 0.0);
  req.probe_prefixes.push_back("abcd");
  return req;
}

void add_record_to_omap_scan(
    const ceph::os::vector_record_t& record,
    ceph::rados::vector_query_exec::omap_scan_state_t *scan)
{
  auto omap = ceph::os::make_vector_omap(record);
  for (const auto& [key, value] : omap) {
    string bytes(value.length(), '\0');
    auto p = value.cbegin();
    p.copy(value.length(), bytes.data());
    ceph::rados::vector_query_exec::consume_omap_key_value(
        key, bytes, *scan);
  }
}

vector_query_run_t run_omap_query(
    const ceph::rados::query_vectors_request_t& req,
    const std::vector<ceph::os::vector_record_t>& writes)
{
  ceph::rados::vector_query_exec::omap_scan_state_t scan;
  for (const auto& record : writes) {
    add_record_to_omap_scan(record, &scan);
  }

  vector_query_run_t run;
  ceph::rados::vector_query_exec::local_query_accumulator_t accumulator;
  EXPECT_EQ(0, accumulator.prepare(req));
  for (const auto& [entry_id, entry] : scan.entries) {
    (void)entry_id;
    EXPECT_EQ(0, accumulator.consume(
        ceph::rados::vector_query_exec::make_omap_entry_view(entry, scan)));
  }
  EXPECT_EQ(0, accumulator.finish(&run.result, &run.stats));
  return run;
}

vector_query_run_t run_vector_record_query(
    const ceph::rados::query_vectors_request_t& req,
    const std::vector<ceph::os::vector_record_t>& writes)
{
  std::map<string, ceph::os::vector_record_t> records;
  for (const auto& record : writes) {
    records.insert_or_assign(record.entry_id, record);
  }

  ceph::rados::vector_query_exec::local_query_accumulator_t accumulator;
  EXPECT_EQ(0, accumulator.prepare(req));
  for (const auto& [entry_id, record] : records) {
    (void)entry_id;
    EXPECT_EQ(0, accumulator.consume(
        ceph::rados::vector_query_exec::make_vector_entry_view(record)));
  }

  vector_query_run_t run;
  EXPECT_EQ(0, accumulator.finish(&run.result, &run.stats));
  return run;
}

void expect_query_runs_equal(
    const vector_query_run_t& omap,
    const vector_query_run_t& vector_node)
{
  ASSERT_EQ(omap.result.entries.size(), vector_node.result.entries.size());
  for (size_t i = 0; i < omap.result.entries.size(); ++i) {
    EXPECT_EQ(omap.result.entries[i].key,
              vector_node.result.entries[i].key);
    EXPECT_EQ(omap.result.entries[i].entry_id,
              vector_node.result.entries[i].entry_id);
    EXPECT_EQ(omap.result.entries[i].distance,
              vector_node.result.entries[i].distance);
  }

  EXPECT_EQ(omap.stats.total_entries, vector_node.stats.total_entries);
  EXPECT_EQ(omap.stats.incomplete_entries,
            vector_node.stats.incomplete_entries);
  EXPECT_EQ(omap.stats.bucket_mismatch,
            vector_node.stats.bucket_mismatch);
  EXPECT_EQ(omap.stats.index_mismatch,
            vector_node.stats.index_mismatch);
  EXPECT_EQ(omap.stats.data_type_mismatch,
            vector_node.stats.data_type_mismatch);
  EXPECT_EQ(omap.stats.distance_metric_mismatch,
            vector_node.stats.distance_metric_mismatch);
  EXPECT_EQ(omap.stats.dimension_mismatch,
            vector_node.stats.dimension_mismatch);
  EXPECT_EQ(omap.stats.probe_mismatch,
            vector_node.stats.probe_mismatch);
  EXPECT_EQ(omap.stats.missing_content,
            vector_node.stats.missing_content);
  EXPECT_EQ(omap.stats.distance_error,
            vector_node.stats.distance_error);
  EXPECT_EQ(omap.stats.matched_entries,
            vector_node.stats.matched_entries);
  EXPECT_EQ(omap.stats.distance_computations,
            vector_node.stats.distance_computations);
  EXPECT_EQ(omap.stats.merged_entries,
            vector_node.stats.merged_entries);
  EXPECT_EQ(omap.stats.final_entries,
            vector_node.stats.final_entries);
}

} // anonymous namespace

TEST(VectorQueryExecutor, OmapAndVectorRecordAdaptersMatchAllMetrics) {
  const uint32_t metrics[] = {
    ceph::rados::vector_distance_metric_euclidean,
    ceph::rados::vector_distance_metric_cosine,
    ceph::rados::vector_distance_metric_dot,
  };

  for (const auto metric : metrics) {
    SCOPED_TRACE(metric);
    std::vector<ceph::os::vector_record_t> records = {
      make_query_record("00000001", "tie-a", 1.0, 1.0, metric),
      make_query_record("00000002", "tie-b", 1.0, -1.0, metric),
      make_query_record("00000003", "axis", 3.0, 0.0, metric),
      make_query_record(
        "00000004", "other-probe", 1.0, 0.0, metric, "ffff"),
    };

    for (const uint32_t local_top_k : {8u, 2u}) {
      SCOPED_TRACE(local_top_k);
      auto req = make_local_query(metric, local_top_k);
      auto omap = run_omap_query(req, records);
      auto vector_node = run_vector_record_query(req, records);
      expect_query_runs_equal(omap, vector_node);
      EXPECT_EQ(4u, omap.stats.total_entries);
      EXPECT_EQ(3u, omap.stats.matched_entries);
      EXPECT_EQ(3u, omap.stats.distance_computations);
      EXPECT_EQ(1u, omap.stats.probe_mismatch);
      EXPECT_EQ(std::min<size_t>(local_top_k, 3),
                omap.result.entries.size());
    }
  }
}

TEST(VectorQueryExecutor, OmapAndVectorRecordAdaptersMatchTiesAndReplace) {
  const auto metric = ceph::rados::vector_distance_metric_euclidean;
  auto old_record =
    make_query_record("00000001", "old", 10.0, 0.0, metric);
  auto updated_record =
    make_query_record("00000001", "updated", 1.0, 0.0, metric);
  updated_record.metadata.append("metadata", 8);

  std::vector<ceph::os::vector_record_t> writes = {
    old_record,
    make_query_record("00000002", "tie-b", 1.0, 1.0, metric),
    make_query_record("00000003", "tie-a", 1.0, -1.0, metric),
    updated_record,
  };
  auto req = make_local_query(metric, 3);
  auto omap = run_omap_query(req, writes);
  auto vector_node = run_vector_record_query(req, writes);
  expect_query_runs_equal(omap, vector_node);

  ASSERT_EQ(3u, omap.result.entries.size());
  EXPECT_EQ("updated", omap.result.entries[0].key);
  EXPECT_EQ("tie-a", omap.result.entries[1].key);
  EXPECT_EQ("tie-b", omap.result.entries[2].key);
  EXPECT_EQ(3u, omap.stats.total_entries);
  EXPECT_EQ(3u, omap.stats.matched_entries);
}

TEST(VectorQueryExecutor, OmapAndVectorRecordAdaptersMatchMalformedEntries) {
  const auto metric = ceph::rados::vector_distance_metric_euclidean;
  auto req = make_local_query(metric, 2);

  auto incomplete =
    make_query_record("00000001", "", 1.0, 0.0, metric);
  auto omap = run_omap_query(req, {incomplete});
  auto vector_node = run_vector_record_query(req, {incomplete});
  expect_query_runs_equal(omap, vector_node);
  EXPECT_EQ(1u, omap.stats.incomplete_entries);
  EXPECT_TRUE(omap.result.entries.empty());

  auto wrong_length =
    make_query_record("00000002", "wrong-length", 1.0, 0.0, metric);
  const float short_vector = 1.0;
  wrong_length.vector_data.clear();
  wrong_length.vector_data.append(
      reinterpret_cast<const char *>(&short_vector),
      sizeof(short_vector));
  EXPECT_EQ(-EINVAL, ceph::os::validate_vector_record(wrong_length));

  omap = run_omap_query(req, {wrong_length});
  vector_node = run_vector_record_query(req, {wrong_length});
  expect_query_runs_equal(omap, vector_node);
  EXPECT_EQ(1u, omap.stats.distance_computations);
  EXPECT_EQ(1u, omap.stats.distance_error);
  EXPECT_EQ(0u, omap.stats.matched_entries);
}

TEST(VectorQueryExecutor, AccumulatorPrepareAndFinishAreOneShot) {
  const auto metric = ceph::rados::vector_distance_metric_euclidean;
  auto req = make_local_query(metric, 1);
  auto record = make_query_record(
    "00000001", "vector", 1.0, 0.0, metric);

  ceph::rados::vector_query_exec::local_query_accumulator_t accumulator;
  ASSERT_EQ(0, accumulator.prepare(req));
  ASSERT_EQ(0, accumulator.consume(
      ceph::rados::vector_query_exec::make_vector_entry_view(record)));

  auto invalid_req = req;
  invalid_req.local_top_k = 0;
  EXPECT_EQ(-EINVAL, accumulator.prepare(invalid_req));

  ceph::rados::query_vectors_result_t result;
  EXPECT_EQ(-EINVAL, accumulator.finish(&result));

  ASSERT_EQ(0, accumulator.prepare(req));
  ASSERT_EQ(0, accumulator.consume(
      ceph::rados::vector_query_exec::make_vector_entry_view(record)));
  ASSERT_EQ(0, accumulator.finish(&result));
  ASSERT_EQ(1u, result.entries.size());
  EXPECT_EQ("00000001", result.entries.front().entry_id);
  EXPECT_EQ(-EINVAL, accumulator.finish(&result));
}

TEST(VectorQueryExecutor, AccumulatorRejectsEmptyEntryId) {
  const auto metric = ceph::rados::vector_distance_metric_euclidean;
  auto req = make_local_query(metric, 1);
  auto record = make_query_record(
    "00000001", "vector", 1.0, 0.0, metric);
  record.entry_id.clear();

  ceph::rados::vector_query_exec::local_query_accumulator_t accumulator;
  ASSERT_EQ(0, accumulator.prepare(req));
  ASSERT_EQ(0, accumulator.consume(
      ceph::rados::vector_query_exec::make_vector_entry_view(record)));

  ceph::rados::query_vectors_result_t result;
  ceph::rados::vector_query_exec::query_filter_stats_t stats;
  ASSERT_EQ(0, accumulator.finish(&result, &stats));
  EXPECT_TRUE(result.entries.empty());
  EXPECT_EQ(1u, stats.total_entries);
  EXPECT_EQ(1u, stats.incomplete_entries);
  EXPECT_EQ(0u, stats.matched_entries);
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

TEST(VectorPlacement, PgLshV0SelectionUsesLogicalOrderAndBudget) {
  float vector[] = {1.0, -2.0, 3.0, -4.0};
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector), sizeof(vector));

  constexpr uint32_t k = 10;
  constexpr uint32_t l = 16;
  constexpr uint32_t seed = 12345;
  constexpr uint32_t pg_num = 16;

  std::vector<librados::vector_placement::pg_lsh_v0_group_t> exact1;
  std::vector<librados::vector_placement::pg_lsh_v0_group_t> exact2;
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0_exact_groups(
      vector_bl, 4, k, l, seed, &exact1));
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0_exact_groups(
      vector_bl, 4, k, l, seed, &exact2));
  ASSERT_EQ(l, exact1.size());
  ASSERT_EQ(exact1.size(), exact2.size());
  for (size_t i = 0; i < exact1.size(); ++i) {
    EXPECT_EQ(exact1[i].table, exact2[i].table);
    EXPECT_EQ(exact1[i].lsh_bucket_id, exact2[i].lsh_bucket_id);
    EXPECT_EQ(0u, exact1[i].hamming_distance);
  }

  const auto write_pgs =
    librados::vector_placement::pg_lsh_v0_select_write_pgs(
        exact1, pg_num, seed, 4);
  ASSERT_EQ(4u, write_pgs.size());
  std::unordered_set<uint32_t> unique_write_pgs(
      write_pgs.begin(), write_pgs.end());
  EXPECT_EQ(write_pgs.size(), unique_write_pgs.size());

  std::vector<librados::vector_placement::pg_lsh_v0_group_t> query_groups;
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0_query_groups(
      vector_bl, 4, k, l, 1, seed, &query_groups));
  EXPECT_EQ(l * (1 + k), query_groups.size());
  for (uint32_t table = 0; table < l; ++table) {
    EXPECT_EQ(table, query_groups[table].table);
    EXPECT_EQ(0u, query_groups[table].hamming_distance);
  }
  for (uint32_t table = 0; table < l; ++table) {
    const size_t first_radius_one = l + table * k;
    EXPECT_EQ(table, query_groups[first_radius_one].table);
    EXPECT_EQ(1u, query_groups[first_radius_one].hamming_distance);
  }

  librados::vector_pg_lsh::pool_pg_info_t pool_info;
  pool_info.pg_num = pg_num;
  pool_info.pgp_num = pg_num;
  pool_info.osdmap_epoch = 1;

  auto index_config = make_pg_lsh_test_index_config(
      4, pool_info, k, l, seed, 2);
  librados::vector_pg_lsh::query_params_t query_params;
  query_params.hamming_radius = 0;
  query_params.m = 16;

  std::vector<librados::vector_placement::pg_lsh_v0_ranked_pg_t> query_pgs;
  uint64_t generated_group_count = 0;
  ASSERT_EQ(0, librados::vector_pg_lsh::select_query_pgs(
      vector_bl, index_config, query_params, pool_info, &query_pgs,
      &generated_group_count));
  EXPECT_EQ(l, generated_group_count);
  EXPECT_LT(query_pgs.size(), query_params.m);
  EXPECT_EQ(10u, query_pgs.size());
  std::unordered_set<uint32_t> unique_query_pgs;
  for (const auto& pg : query_pgs) {
    EXPECT_TRUE(unique_query_pgs.insert(pg.pg).second);
  }

  std::vector<uint32_t> vector_write_pgs;
  ASSERT_EQ(0, librados::vector_pg_lsh::select_write_pgs(
      vector_bl, index_config, pool_info, &vector_write_pgs));
  ASSERT_EQ(index_config.d, vector_write_pgs.size());

  query_params.m = 8;
  ASSERT_EQ(0, librados::vector_pg_lsh::select_query_pgs(
      vector_bl, index_config, query_params, pool_info, &query_pgs,
      &generated_group_count));
  EXPECT_EQ(l, generated_group_count);
  ASSERT_EQ(query_params.m, query_pgs.size());
  unique_query_pgs.clear();
  for (const auto& pg : query_pgs) {
    unique_query_pgs.insert(pg.pg);
  }
  for (const uint32_t pg : vector_write_pgs) {
    EXPECT_EQ(1u, unique_query_pgs.count(pg));
  }

  query_params.m = 16;
  query_params.hamming_radius = 1;
  ASSERT_EQ(0, librados::vector_pg_lsh::select_query_pgs(
      vector_bl, index_config, query_params, pool_info, &query_pgs,
      &generated_group_count));
  EXPECT_EQ(l * (1 + k), generated_group_count);
  ASSERT_EQ(query_params.m, query_pgs.size());

  auto exhausted_config = make_pg_lsh_test_index_config(
      4, pool_info, 1, 2, seed, 1);
  librados::vector_pg_lsh::query_params_t exhausted_query_params;
  exhausted_query_params.hamming_radius = 0;
  exhausted_query_params.m = 16;
  ASSERT_EQ(0, librados::vector_pg_lsh::select_query_pgs(
      vector_bl, exhausted_config, exhausted_query_params, pool_info,
      &query_pgs,
      &generated_group_count));
  EXPECT_EQ(2u, generated_group_count);
  EXPECT_LE(query_pgs.size(), generated_group_count);
  EXPECT_LT(query_pgs.size(), exhausted_query_params.m);
}

TEST(VectorPlacement, PgLshSubOidNameUsesCoarseDistanceBucket) {
  float vector[] = {1.0, 2.0, 0.0, -1.0};
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector), sizeof(vector));

  std::vector<double> random_anchor;
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0_random_anchor(
      4, 12345, &random_anchor));
  librados::vector_placement::pg_lsh_v0::sub_oid_config_t config = {
    4, 12345, 4, 4, std::span<const double>(random_anchor),
  };
  librados::vector_placement::pg_lsh_v0::sub_oid_t sub_oid;
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0::compute_sub_oid(
      vector_bl, config, &sub_oid));
  EXPECT_EQ(0x4, sub_oid.distance_bucket);
  EXPECT_EQ(0x5, sub_oid.residual_code);
  EXPECT_EQ("g4_r5",
            librados::vector_placement::pg_lsh_v0::format_sub_oid(
                sub_oid, config));

  std::vector<std::string> probe_sub_oids;
  librados::vector_placement::pg_lsh_v0::probe_config_t probe_config = {
    1, 1,
  };
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0::build_probe_sub_oids(
      sub_oid, config, probe_config, &probe_sub_oids));
  ASSERT_EQ(15u, probe_sub_oids.size());
  EXPECT_EQ("g4_r5", probe_sub_oids[0]);

  const std::unordered_set<std::string> names(
      probe_sub_oids.begin(), probe_sub_oids.end());
  EXPECT_EQ(probe_sub_oids.size(), names.size());
  EXPECT_EQ(1u, names.count("g3_r5"));
  EXPECT_EQ(1u, names.count("g5_r5"));
  EXPECT_EQ(1u, names.count("g4_r4"));

  const std::vector<std::string> unchanged_on_error = {"sentinel"};
  std::vector<std::string> invalid_output = unchanged_on_error;
  const librados::vector_placement::pg_lsh_v0::probe_config_t invalid_probe = {
    1, 5,
  };
  EXPECT_EQ(-EINVAL,
            librados::vector_placement::pg_lsh_v0::build_probe_sub_oids(
                sub_oid, config, invalid_probe, &invalid_output));
  EXPECT_EQ(unchanged_on_error, invalid_output);

  float axis_vector[] = {2.0, 0.0, 0.0, 0.0};
  bufferlist axis_bl;
  axis_bl.append(reinterpret_cast<const char *>(axis_vector),
                 sizeof(axis_vector));
  std::vector<double> axis_anchor = {1.0, 0.0, 0.0, 0.0};
  config.anchor = std::span<const double>(axis_anchor);
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0::compute_sub_oid(
      axis_bl, config, &sub_oid));
  EXPECT_EQ(0x0, sub_oid.distance_bucket);
  EXPECT_EQ(0xf, sub_oid.residual_code);
  EXPECT_EQ("g0_rf",
            librados::vector_placement::pg_lsh_v0::format_sub_oid(
                sub_oid, config));
}

TEST(VectorPlacement, PgLshSubOidPreservesDistanceBucketBoundary) {
  std::vector<double> anchor = {1.0, 0.0};
  const librados::vector_placement::pg_lsh_v0::sub_oid_config_t config = {
    2, 12345, 4, 0, std::span<const double>(anchor),
  };

  float below_boundary[] = {0.001f, 1.0f};
  float above_boundary[] = {-0.001f, 1.0f};
  bufferlist below_bl;
  bufferlist above_bl;
  below_bl.append(
      reinterpret_cast<const char *>(below_boundary), sizeof(below_boundary));
  above_bl.append(
      reinterpret_cast<const char *>(above_boundary), sizeof(above_boundary));

  librados::vector_placement::pg_lsh_v0::sub_oid_t below_sub_oid;
  librados::vector_placement::pg_lsh_v0::sub_oid_t above_sub_oid;
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0::compute_sub_oid(
      below_bl, config, &below_sub_oid));
  ASSERT_EQ(0, librados::vector_placement::pg_lsh_v0::compute_sub_oid(
      above_bl, config, &above_sub_oid));

  EXPECT_EQ(0x7, below_sub_oid.distance_bucket);
  EXPECT_EQ(0x8, above_sub_oid.distance_bucket);
  EXPECT_EQ("g7_r0",
            librados::vector_placement::pg_lsh_v0::format_sub_oid(
                below_sub_oid, config));
  EXPECT_EQ("g8_r0",
            librados::vector_placement::pg_lsh_v0::format_sub_oid(
                above_sub_oid, config));
}

TEST(VectorPlacement, PgLshIndexConfigRoundTripsAllPlacementState) {
  librados::vector_pg_lsh::pool_pg_info_t pool_info = {
    16, 16, 1,
  };
  auto original = make_pg_lsh_test_index_config(
      4, pool_info, 10, 16, 12345, 4, 5, 6);
  original.anchor_mode = librados::vector_pg_lsh::anchor_mode_centroid;
  original.anchor = {0.5, -0.5, 0.5, -0.5};

  bufferlist encoded;
  original.encode(encoded);
  librados::vector_pg_lsh::index_config_t decoded;
  auto p = encoded.cbegin();
  decoded.decode(p);
  ASSERT_TRUE(p.end());

  std::string mismatch_field;
  EXPECT_EQ(0, librados::vector_pg_lsh::compare_index_configs(
      original, decoded, &mismatch_field));
  EXPECT_TRUE(mismatch_field.empty());
}

TEST(VectorPlacement, PgLshIndexConfigReportsImmutableMismatches) {
  librados::vector_pg_lsh::pool_pg_info_t pool_info = {
    16, 16, 1,
  };
  const auto stored = make_pg_lsh_test_index_config(
      4, pool_info, 10, 16, 12345, 4, 5, 6);
  std::string mismatch_field;

  auto requested = stored;
  requested.seed++;
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::compare_index_configs(
      requested, stored, &mismatch_field));
  EXPECT_EQ("seed", mismatch_field);

  requested = stored;
  requested.dimension++;
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::compare_index_configs(
      requested, stored, &mismatch_field));
  EXPECT_EQ("dimension", mismatch_field);

  requested = stored;
  requested.anchor[0] = -requested.anchor[0];
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::compare_index_configs(
      requested, stored, &mismatch_field));
  EXPECT_EQ("anchor", mismatch_field);

  requested = stored;
  requested.distance_bucket_bits++;
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::compare_index_configs(
      requested, stored, &mismatch_field));
  EXPECT_EQ("distance_bucket_bits", mismatch_field);

  requested = stored;
  requested.residual_bits++;
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::compare_index_configs(
      requested, stored, &mismatch_field));
  EXPECT_EQ("residual_bits", mismatch_field);
}

TEST(VectorPlacement, PgLshQueryValidatesOnlyExplicitImmutableOverrides) {
  librados::vector_pg_lsh::pool_pg_info_t pool_info = {
    16, 16, 1,
  };
  const auto stored = make_pg_lsh_test_index_config(
      4, pool_info, 10, 16, 12345, 4, 5, 6);
  std::string mismatch_field;

  librados::vector_pg_lsh::index_config_overrides_t no_overrides;
  EXPECT_EQ(0, librados::vector_pg_lsh::validate_index_config_overrides(
      no_overrides, stored, &mismatch_field));
  EXPECT_TRUE(mismatch_field.empty());

  librados::vector_pg_lsh::index_config_overrides_t matching_overrides;
  matching_overrides.k = stored.k;
  matching_overrides.seed = stored.seed;
  EXPECT_EQ(0, librados::vector_pg_lsh::validate_index_config_overrides(
      matching_overrides, stored, &mismatch_field));

  auto wrong_overrides = matching_overrides;
  wrong_overrides.seed = stored.seed + 1;
  EXPECT_EQ(-EINVAL,
            librados::vector_pg_lsh::validate_index_config_overrides(
                wrong_overrides, stored, &mismatch_field));
  EXPECT_EQ("seed", mismatch_field);
}

TEST(VectorPlacement, PgLshRejectsPoolPgConfigurationChanges) {
  const librados::vector_pg_lsh::pool_pg_info_t creation_pool = {
    8, 8, 1,
  };
  const auto config = make_pg_lsh_test_index_config(
      4, creation_pool, 4, 4, 12345, 2, 4, 4);
  std::string mismatch_field;

  auto changed_pool = creation_pool;
  changed_pool.pg_num = 16;
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::validate_index_config(
      config, changed_pool, &mismatch_field));
  EXPECT_EQ("pg_num", mismatch_field);

  changed_pool = creation_pool;
  changed_pool.pgp_num = 16;
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::validate_index_config(
      config, changed_pool, &mismatch_field));
  EXPECT_EQ("pgp_num", mismatch_field);
}

TEST(VectorPlacement, StoredRandomAnchorReproducesSubOidRouting) {
  librados::vector_pg_lsh::pool_pg_info_t pool_info = {
    16, 16, 1,
  };
  const auto original = make_pg_lsh_test_index_config(
      4, pool_info, 10, 16, 12345, 4, 4, 4);
  float vector[] = {1.0, 2.0, 0.0, -1.0};
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector), sizeof(vector));

  librados::vector_placement::pg_lsh_v0::sub_oid_t original_sub_oid;
  ASSERT_EQ(0, librados::vector_pg_lsh::compute_sub_oid(
      vector_bl, original, &original_sub_oid));

  bufferlist encoded;
  original.encode(encoded);
  librados::vector_pg_lsh::index_config_t restored;
  auto p = encoded.cbegin();
  restored.decode(p);
  ASSERT_TRUE(p.end());

  librados::vector_placement::pg_lsh_v0::sub_oid_t restored_sub_oid;
  ASSERT_EQ(0, librados::vector_pg_lsh::compute_sub_oid(
      vector_bl, restored, &restored_sub_oid));
  EXPECT_EQ(original_sub_oid.distance_bucket,
            restored_sub_oid.distance_bucket);
  EXPECT_EQ(original_sub_oid.residual_code, restored_sub_oid.residual_code);
  EXPECT_EQ(
      librados::vector_placement::pg_lsh_v0::format_sub_oid(
          original_sub_oid,
          librados::vector_pg_lsh::sub_oid_config_view(original)),
      librados::vector_placement::pg_lsh_v0::format_sub_oid(
          restored_sub_oid,
          librados::vector_pg_lsh::sub_oid_config_view(restored)));
}

TEST(VectorPlacement, PgLshV0ManualCollisionSelection) {
  constexpr uint32_t seed = 12345;
  constexpr uint32_t pg_num = 16;
  const uint32_t collision_pg =
    librados::vector_placement::pg_lsh_v0_group_to_pg(
        0, 0, pg_num, seed);
  const uint32_t table1_collision_lsh_bucket_id =
    pg_lsh_test_lsh_bucket_id_for_pg(1, pg_num, seed, collision_pg);
  const uint32_t table2_other_lsh_bucket_id =
    pg_lsh_test_lsh_bucket_id_not_pg(2, pg_num, seed, collision_pg);
  ASSERT_NE(std::numeric_limits<uint32_t>::max(),
            table1_collision_lsh_bucket_id);
  ASSERT_NE(std::numeric_limits<uint32_t>::max(),
            table2_other_lsh_bucket_id);

  std::vector<librados::vector_placement::pg_lsh_v0_group_t> exact = {
    {0, 0, 0},
    {1, table1_collision_lsh_bucket_id, 0},
    {2, table2_other_lsh_bucket_id, 0},
  };
  const auto write_pgs =
    librados::vector_placement::pg_lsh_v0_select_write_pgs(
        exact, pg_num, seed, 2);
  ASSERT_EQ(2u, write_pgs.size());
  EXPECT_EQ(collision_pg, write_pgs[0]);
  EXPECT_NE(collision_pg, write_pgs[1]);

  bool found_order_case = false;
  uint32_t high_pg = 0;
  uint32_t low_pg = 0;
  uint32_t high_lsh_bucket_id = 0;
  uint32_t low_lsh_bucket_id = 0;
  for (uint32_t candidate_high = pg_num - 1;
       candidate_high > 0 && !found_order_case;
       --candidate_high) {
    const uint32_t candidate_high_lsh_bucket_id =
      pg_lsh_test_lsh_bucket_id_for_pg(0, pg_num, seed, candidate_high);
    if (candidate_high_lsh_bucket_id ==
        std::numeric_limits<uint32_t>::max()) {
      continue;
    }
    for (uint32_t candidate_low = 0;
         candidate_low < candidate_high;
         ++candidate_low) {
      const uint32_t candidate_low_lsh_bucket_id =
        pg_lsh_test_lsh_bucket_id_for_pg(1, pg_num, seed, candidate_low);
      if (candidate_low_lsh_bucket_id ==
          std::numeric_limits<uint32_t>::max()) {
        continue;
      }
      high_pg = candidate_high;
      low_pg = candidate_low;
      high_lsh_bucket_id = candidate_high_lsh_bucket_id;
      low_lsh_bucket_id = candidate_low_lsh_bucket_id;
      found_order_case = true;
      break;
    }
  }
  ASSERT_TRUE(found_order_case);

  const std::vector<librados::vector_placement::pg_lsh_v0_group_t>
    order_groups = {
      {0, high_lsh_bucket_id, 0},
      {1, low_lsh_bucket_id, 0},
    };
  const auto selected = librados::vector_placement::pg_lsh_v0_select_unique_pgs(
      order_groups, pg_num, seed, 2);
  ASSERT_EQ(2u, selected.size());
  ASSERT_GT(high_pg, low_pg);
  EXPECT_EQ(high_pg, selected[0].pg);
  EXPECT_EQ(low_pg, selected[1].pg);
}

static bool vector_test_json_uint64(const std::string& body,
                                    const std::string& field,
                                    uint64_t *out)
{
  if (out == nullptr) {
    return false;
  }
  json_spirit::mValue root;
  if (!json_spirit::read(body, root) ||
      root.type() != json_spirit::obj_type) {
    return false;
  }
  const auto& obj = root.get_obj();
  const auto it = obj.find(field);
  if (it == obj.end() || it->second.type() != json_spirit::int_type) {
    return false;
  }
  const int64_t value = it->second.get_int64();
  if (value < 0) {
    return false;
  }
  *out = static_cast<uint64_t>(value);
  return true;
}

static int pool_pg_info_for_test(
    Rados& cluster,
    const std::string& pool_name,
    librados::vector_pg_lsh::pool_pg_info_t *info)
{
  if (info == nullptr) {
    return -EINVAL;
  }

  bufferlist outbl;
  uint64_t value = 0;
  std::string cmd = "{\"prefix\":\"osd pool get\",\"pool\":\"" + pool_name +
    "\",\"var\":\"pg_num\",\"format\":\"json\"}";
  int r = cluster.mon_command(std::move(cmd), {}, &outbl, nullptr);
  if (r < 0) {
    return r;
  }
  if (!vector_test_json_uint64(outbl.to_str(), "pg_num", &value) ||
      value == 0 || value > std::numeric_limits<uint32_t>::max()) {
    return -EINVAL;
  }
  info->pg_num = static_cast<uint32_t>(value);

  outbl.clear();
  cmd = "{\"prefix\":\"osd pool get\",\"pool\":\"" + pool_name +
    "\",\"var\":\"pgp_num\",\"format\":\"json\"}";
  r = cluster.mon_command(std::move(cmd), {}, &outbl, nullptr);
  if (r < 0) {
    return r;
  }
  if (!vector_test_json_uint64(outbl.to_str(), "pgp_num", &value) ||
      value == 0 || value > std::numeric_limits<uint32_t>::max()) {
    return -EINVAL;
  }
  info->pgp_num = static_cast<uint32_t>(value);

  outbl.clear();
  cmd = "{\"prefix\":\"osd dump\",\"format\":\"json\"}";
  r = cluster.mon_command(std::move(cmd), {}, &outbl, nullptr);
  if (r < 0) {
    return r;
  }
  if (!vector_test_json_uint64(outbl.to_str(), "epoch", &value)) {
    return -EINVAL;
  }
  info->osdmap_epoch = value;
  return 0;
}

static int ensure_pool_pg_count_for_test(Rados& cluster,
                                         const std::string& pool_name,
                                         uint32_t required_pg_num,
                                         librados::vector_pg_lsh::pool_pg_info_t *info)
{
  if (info == nullptr || required_pg_num == 0) {
    return -EINVAL;
  }

  int r = pool_pg_info_for_test(cluster, pool_name, info);
  if (r < 0) {
    return r;
  }
  if (info->pg_num >= required_pg_num &&
      info->pgp_num >= required_pg_num) {
    return cluster.wait_for_latest_osdmap();
  }

  bufferlist outbl;
  std::string cmd = "{\"prefix\":\"osd pool set\",\"pool\":\"" + pool_name +
    "\",\"var\":\"pg_num\",\"val\":\"" + std::to_string(required_pg_num) +
    "\",\"format\":\"json\"}";
  r = cluster.mon_command(std::move(cmd), {}, &outbl, nullptr);
  if (r < 0) {
    return r;
  }
  outbl.clear();
  cmd = "{\"prefix\":\"osd pool set\",\"pool\":\"" + pool_name +
    "\",\"var\":\"pgp_num\",\"val\":\"" + std::to_string(required_pg_num) +
    "\",\"format\":\"json\"}";
  r = cluster.mon_command(std::move(cmd), {}, &outbl, nullptr);
  if (r < 0) {
    return r;
  }

  r = cluster.wait_for_latest_osdmap();
  if (r < 0) {
    return r;
  }

  for (int attempt = 0; attempt < 30; ++attempt) {
    r = pool_pg_info_for_test(cluster, pool_name, info);
    if (r < 0) {
      return r;
    }
    if (info->pg_num >= required_pg_num &&
        info->pgp_num >= required_pg_num) {
      return 0;
    }
    sleep(1);
  }
  return -ETIMEDOUT;
}

static int acting_primary_for_raw_pg(Rados& cluster,
                                     int64_t pool_id,
                                     uint32_t raw_pg,
                                     int *primary);

static int choose_two_raw_pgs_with_different_primaries(
    Rados& cluster,
    int64_t pool_id,
    uint32_t pg_num,
    uint32_t *pg0,
    uint32_t *pg1,
    int *primary0,
    int *primary1)
{
  if (pg0 == nullptr || pg1 == nullptr ||
      primary0 == nullptr || primary1 == nullptr ||
      pg_num < 2) {
    return -EINVAL;
  }

  bool have_pg0 = false;
  for (uint32_t pg = 0; pg < pg_num; ++pg) {
    int primary = -1;
    int r = acting_primary_for_raw_pg(cluster, pool_id, pg, &primary);
    if (r < 0) {
      return r;
    }
    if (!have_pg0) {
      *pg0 = pg;
      *primary0 = primary;
      have_pg0 = true;
      continue;
    }
    if (primary != *primary0) {
      *pg1 = pg;
      *primary1 = primary;
      return 0;
    }
  }
  return -ENOENT;
}

static int acting_primary_for_raw_pg(Rados& cluster,
                                     int64_t pool_id,
                                     uint32_t raw_pg,
                                     int *primary)
{
  if (primary == nullptr || pool_id < 0) {
    return -EINVAL;
  }

  std::ostringstream pgid;
  pgid << pool_id << "." << std::hex << raw_pg;
  bufferlist outbl;
  std::string cmd = "{\"prefix\":\"pg map\",\"pgid\":\"" + pgid.str() +
    "\",\"format\":\"json\"}";
  int r = cluster.mon_command(std::move(cmd), {}, &outbl, nullptr);
  if (r < 0) {
    return r;
  }

  json_spirit::mValue root;
  if (!json_spirit::read(outbl.to_str(), root)) {
    return -EINVAL;
  }
  const auto& obj = root.get_obj();
  const auto acting_it = obj.find("acting");
  if (acting_it == obj.end()) {
    return -ENOENT;
  }
  const auto& acting = acting_it->second.get_array();
  if (acting.empty()) {
    return -ENOENT;
  }
  *primary = acting.front().get_int();
  return 0;
}

static int put_pg_lsh_test_vector(IoCtx& ioctx,
                                  uint32_t pg,
                                  const string& locator,
                                  const string& key,
                                  const float *vector,
                                  size_t vector_len)
{
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector), vector_len);

  ceph::rados::put_vector_request_t req;
  req.bucket_name = "pg-lsh-bucket";
  req.index_name = "pg-lsh-index";
  req.key = key;
  req.data_type = ceph::rados::vector_data_type_float32;
  req.distance_metric = ceph::rados::vector_distance_metric_euclidean;
  req.dimension = 4;
  req.vector_data = vector_bl;

  librados::vector_pg_lsh::put_target_t target;
  target.pg = pg;
  target.oid = librados::vector_placement::make_pg_lsh_v0_oid(
      req.bucket_name, req.index_name, pg);
  target.locator_key = locator;
  target.placement_key =
    librados::vector_placement::pg_lsh_v0_placement_key(pg);
  target.vector_hash =
    librados::vector_placement::hash_v0_vector_hash(vector_bl);
  return librados::vector_pg_lsh::put_vector(ioctx, target, std::move(req));
}

TEST_F(LibRadosIoPP, PgLshV0SubOidProbeLimitIsPerPg) {
  const std::string sub_pool_name =
    get_temp_pool_name("pg-lsh-sub-oid-probe-limit-");
  bufferlist outbl;
  std::string cmd = "{\"prefix\":\"osd pool create\",\"pool\":\"" +
    sub_pool_name + "\",\"pg_num\":8,\"pgp_num\":8,\"format\":\"json\"}";
  ASSERT_EQ(0, cluster.mon_command(std::move(cmd), {}, &outbl, nullptr));
  auto cleanup_sub_pool = make_scope_guard([&] {
    cluster.pool_delete(sub_pool_name.c_str());
  });

  IoCtx sub_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(sub_pool_name.c_str(), sub_ioctx));
  ASSERT_EQ(0, sub_ioctx.application_enable("rados", true));
  ASSERT_EQ(0, cluster.wait_for_latest_osdmap());

  librados::vector_pg_lsh::pool_pg_info_t pool_info;
  ASSERT_EQ(0, pool_pg_info_for_test(cluster, sub_pool_name, &pool_info));
  ASSERT_GE(pool_info.pg_num, 8u);
  ASSERT_GE(pool_info.pgp_num, 8u);

  auto locator_state =
    std::make_shared<librados::vector_pg_lsh::locator_state_t>();
  librados::vector_pg_lsh::locator_cache_t locator_cache(
      &sub_ioctx, pool_info, sub_pool_name, "pg-lsh-bucket", "pg-lsh-index",
      locator_state);
  ASSERT_EQ(0, locator_cache.precompute_all());

  auto index_config = make_pg_lsh_test_index_config(
      4, pool_info, 4, 4, 12345, 2, 4, 4);
  librados::vector_pg_lsh::query_params_t query_params;
  query_params.hamming_radius = 0;
  query_params.m = 2;
  query_params.distance_bucket_radius = 1;
  query_params.residual_hamming_radius = 0;
  query_params.probe_limit_per_pg = 3;
  ASSERT_EQ(0, librados::vector_pg_lsh::validate_query_params(
      index_config, query_params, pool_info));

  std::array<float, 4> found_vector = {};
  std::vector<librados::vector_pg_lsh::query_probe_t> probes;
  bool found = false;
  for (uint32_t candidate = 0; candidate < 4096; ++candidate) {
    std::array<float, 4> candidate_vector = {};
    for (uint32_t dim = 0; dim < candidate_vector.size(); ++dim) {
      const uint32_t x = candidate * 1103515245u + 12345u +
        dim * 2654435761u;
      int raw = static_cast<int>((x >> 24) & 0xffU) - 128;
      if (raw == 0) {
        raw = static_cast<int>(dim) + 1;
      }
      candidate_vector[dim] = static_cast<float>(raw) / 17.0f;
    }

    bufferlist candidate_bl;
    candidate_bl.append(
        reinterpret_cast<const char *>(candidate_vector.data()),
        candidate_vector.size() * sizeof(float));

    std::vector<librados::vector_pg_lsh::query_probe_t> candidate_probes;
    uint64_t generated_group_count = 0;
    if (librados::vector_pg_lsh::build_query_probes(
          "pg-lsh-bucket", "pg-lsh-index", candidate_bl,
          index_config, query_params, pool_info, &locator_cache,
          &candidate_probes, &generated_group_count) < 0 ||
        candidate_probes.size() != 6 ||
        generated_group_count != index_config.l) {
      continue;
    }

    std::unordered_map<uint32_t, uint32_t> probes_per_pg;
    for (const auto& probe : candidate_probes) {
      ++probes_per_pg[probe.pg];
    }
    if (probes_per_pg.size() != query_params.m) {
      continue;
    }
    bool every_pg_has_limit = true;
    for (const auto& entry : probes_per_pg) {
      if (entry.second != query_params.probe_limit_per_pg) {
        every_pg_has_limit = false;
        break;
      }
    }
    if (!every_pg_has_limit) {
      continue;
    }

    found_vector = candidate_vector;
    probes = std::move(candidate_probes);
    found = true;
    break;
  }

  ASSERT_TRUE(found)
    << "failed to find deterministic M=2 pg-lsh sub-OID route";
  ASSERT_EQ(6u, probes.size());

  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(found_vector.data()),
                   found_vector.size() * sizeof(float));
  librados::vector_placement::pg_lsh_v0::sub_oid_t exact_sub_oid;
  ASSERT_EQ(0, librados::vector_pg_lsh::compute_sub_oid(
      vector_bl, index_config, &exact_sub_oid));
  const std::string exact_sub_oid_name =
    librados::vector_placement::pg_lsh_v0::format_sub_oid(
        exact_sub_oid,
        librados::vector_pg_lsh::sub_oid_config_view(index_config));

  std::unordered_map<uint32_t, uint32_t> probes_per_pg;
  for (const auto& probe : probes) {
    uint32_t actual_pg = 0;
    ASSERT_EQ(0, sub_ioctx.get_object_pg_hash_position2(
        probe.locator_key, &actual_pg));
    EXPECT_EQ(probe.pg, actual_pg);

    auto& pg_probe_count = probes_per_pg[probe.pg];
    if (pg_probe_count == 0) {
      EXPECT_EQ(exact_sub_oid_name, probe.sub_oid_name);
    }
    ++pg_probe_count;
  }

  ASSERT_EQ(2u, probes_per_pg.size());
  for (const auto& entry : probes_per_pg) {
    EXPECT_EQ(3u, entry.second);
  }
}

TEST_F(LibRadosIoPP, PgLshPersistedAnchorRoutesWithoutTrainingFile) {
  librados::vector_pg_lsh::pool_pg_info_t pool_info;
  ASSERT_EQ(0, pool_pg_info_for_test(cluster, pool_name, &pool_info));

  const std::array<float, 4> vector = {1.0f, -2.0f, 3.0f, -4.0f};
  bufferlist vector_bl;
  vector_bl.append(reinterpret_cast<const char *>(vector.data()),
                   vector.size() * sizeof(float));

  const std::array<std::pair<uint32_t, std::vector<double>>, 3> anchors = {{
    {
      librados::vector_pg_lsh::anchor_mode_random,
      {0.5, -0.5, 0.5, -0.5},
    },
    {
      librados::vector_pg_lsh::anchor_mode_centroid,
      {0.25, 0.5, -0.25, -0.5},
    },
    {
      librados::vector_pg_lsh::anchor_mode_representative,
      {1.0, 0.0, 0.0, 0.0},
    },
  }};

  for (size_t mode_index = 0; mode_index < anchors.size(); ++mode_index) {
    const std::string index_name =
      "persisted-anchor-index-" + std::to_string(mode_index);
    auto locator_state =
      std::make_shared<librados::vector_pg_lsh::locator_state_t>();
    librados::vector_pg_lsh::locator_cache_t locator_cache(
        &ioctx, pool_info, pool_name, "persisted-anchor-bucket",
        index_name, locator_state);
    ASSERT_EQ(0, locator_cache.precompute_all());

    auto create_config = make_pg_lsh_test_index_config(
        4, pool_info, 4, 4, 12345, 1, 4, 4);
    create_config.anchor_mode = anchors[mode_index].first;
    create_config.anchor = anchors[mode_index].second;

    std::vector<librados::vector_pg_lsh::put_target_t> create_targets;
    ASSERT_EQ(0, librados::vector_pg_lsh::build_put_targets(
        "persisted-anchor-bucket", index_name, vector_bl, create_config,
        pool_info, &locator_cache, &create_targets));

    std::string mismatch_field;
    ASSERT_EQ(0, librados::vector_pg_lsh::create_index(
        ioctx, "persisted-anchor-bucket", index_name, create_config,
        pool_info, &mismatch_field));

    librados::vector_pg_lsh::index_config_t loaded_config;
    ASSERT_EQ(0, librados::vector_pg_lsh::load_index_config(
        ioctx, "persisted-anchor-bucket", index_name, &loaded_config,
        &mismatch_field));
    ASSERT_EQ(0, librados::vector_pg_lsh::compare_index_configs(
        create_config, loaded_config, &mismatch_field));

    std::vector<librados::vector_pg_lsh::put_target_t> loaded_targets;
    ASSERT_EQ(0, librados::vector_pg_lsh::build_put_targets(
        "persisted-anchor-bucket", index_name, vector_bl, loaded_config,
        pool_info, &locator_cache, &loaded_targets));
    ASSERT_EQ(create_targets.size(), loaded_targets.size());
    for (size_t target = 0; target < create_targets.size(); ++target) {
      EXPECT_EQ(create_targets[target].pg, loaded_targets[target].pg);
      EXPECT_EQ(create_targets[target].oid.name,
                loaded_targets[target].oid.name);
      EXPECT_EQ(create_targets[target].sub_oid_name,
                loaded_targets[target].sub_oid_name);
    }

    librados::vector_pg_lsh::query_params_t query_params;
    query_params.m = 1;
    std::vector<librados::vector_pg_lsh::query_probe_t> probes;
    uint64_t generated_group_count = 0;
    ASSERT_EQ(0, librados::vector_pg_lsh::build_query_probes(
        "persisted-anchor-bucket", index_name, vector_bl, loaded_config,
        query_params, pool_info, &locator_cache, &probes,
        &generated_group_count));
    EXPECT_FALSE(probes.empty());
    EXPECT_EQ(loaded_config.l, generated_group_count);

    ceph::rados::put_vector_request_t put_req;
    put_req.bucket_name = "persisted-anchor-bucket";
    put_req.index_name = index_name;
    put_req.key = "persisted-anchor-vector-" + std::to_string(mode_index);
    put_req.data_type = loaded_config.data_type;
    put_req.distance_metric = loaded_config.distance_metric;
    put_req.dimension = loaded_config.dimension;
    put_req.vector_data = vector_bl;
    ASSERT_EQ(0, librados::vector_pg_lsh::put_vector(
        ioctx, loaded_targets.front(), put_req));

    ceph::rados::query_vectors_request_t query_req;
    query_req.bucket_name = "persisted-anchor-bucket";
    query_req.index_name = index_name;
    query_req.data_type = loaded_config.data_type;
    query_req.distance_metric = loaded_config.distance_metric;
    query_req.dimension = loaded_config.dimension;
    query_req.local_top_k = 1;
    query_req.query_vector = vector_bl;
    bufferlist reply;
    int op_rval = 0;
    ASSERT_EQ(0, librados::vector_pg_lsh::query_sync(
        ioctx, probes.front(), query_req, &reply, &op_rval));
    ASSERT_EQ(0, op_rval);
    ceph::rados::query_vectors_result_t result;
    auto reply_it = reply.cbegin();
    decode(result, reply_it);
    ASSERT_TRUE(reply_it.end());
    ASSERT_EQ(1u, result.entries.size());
    EXPECT_EQ(put_req.key, result.entries.front().key);
  }

  librados::vector_pg_lsh::index_config_t missing_config;
  std::string mismatch_field;
  EXPECT_EQ(-ENOENT, librados::vector_pg_lsh::load_index_config(
      ioctx, "persisted-anchor-bucket", "missing-index", &missing_config,
      &mismatch_field));
  EXPECT_EQ("index_metadata", mismatch_field);

  auto conflicting_config = make_pg_lsh_test_index_config(
      4, pool_info, 4, 4, 54321, 1, 4, 4);
  EXPECT_EQ(-EINVAL, librados::vector_pg_lsh::create_index(
      ioctx, "persisted-anchor-bucket", "persisted-anchor-index-0",
      conflicting_config, pool_info, &mismatch_field));
  EXPECT_EQ("seed", mismatch_field);
}

TEST_F(LibRadosIoPP, PgLshV0RawOpsRouteAndMergeByEntryId) {
  librados::vector_pg_lsh::pool_pg_info_t pool_info;
  ASSERT_EQ(0, ensure_pool_pg_count_for_test(
      cluster, pool_name, 8, &pool_info));
  ioctx.close();
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));
  ASSERT_EQ(0, cluster.wait_for_latest_osdmap());

  const int64_t pool_id = ioctx.get_id();
  ASSERT_GE(pool_id, 0);

  uint32_t pg0 = 0;
  uint32_t pg1 = 0;
  int primary0 = -1;
  int primary1 = -1;
  const int choose_ret = choose_two_raw_pgs_with_different_primaries(
      cluster, pool_id, pool_info.pg_num, &pg0, &pg1,
      &primary0, &primary1);
  if (choose_ret == -ENOENT) {
    GTEST_SKIP() << "test pool does not expose two acting primaries";
  }
  ASSERT_EQ(0, choose_ret);
  EXPECT_NE(primary0, primary1);

  auto locator_state =
    std::make_shared<librados::vector_pg_lsh::locator_state_t>();
  librados::vector_pg_lsh::locator_cache_t locator_cache(
      &ioctx, pool_info, pool_name, "pg-lsh-bucket", "pg-lsh-index",
      locator_state);
  ASSERT_EQ(0, locator_cache.precompute_all());
  IoCtx& test_ioctx = ioctx;
  std::atomic<uint32_t> locator_errors{0};
  std::vector<std::thread> locator_threads;
  for (uint32_t thread_id = 0; thread_id < 4; ++thread_id) {
    locator_threads.emplace_back([&locator_cache, &test_ioctx, &pool_info,
                                  &locator_errors] {
      for (uint32_t pg = 0; pg < pool_info.pg_num; ++pg) {
        string locator;
        uint32_t actual = 0;
        if (locator_cache.locator_for_pg(pg, &locator) < 0 ||
            test_ioctx.get_object_pg_hash_position2(locator, &actual) < 0 ||
            actual != pg) {
          locator_errors.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  for (auto& thread : locator_threads) {
    thread.join();
  }
  ASSERT_EQ(0u, locator_errors.load());

  auto index_config = make_pg_lsh_test_index_config(
      4, pool_info, 4, 2, 12345, 2);
  librados::vector_pg_lsh::query_params_t query_params;
  query_params.hamming_radius = 0;
  query_params.m = 2;
  std::string mismatch_field;
  ASSERT_EQ(0, librados::vector_pg_lsh::create_index(
      ioctx, "pg-lsh-bucket", "pg-lsh-index", index_config, pool_info,
      &mismatch_field));
  librados::vector_pg_lsh::index_config_t stored_index_config;
  ASSERT_EQ(0, librados::vector_pg_lsh::load_index_config(
      ioctx, "pg-lsh-bucket", "pg-lsh-index", &stored_index_config,
      &mismatch_field));
  ASSERT_EQ(0, librados::vector_pg_lsh::compare_index_configs(
      index_config, stored_index_config, &mismatch_field));
  index_config = std::move(stored_index_config);
  ASSERT_EQ(0, librados::vector_pg_lsh::validate_query_params(
      index_config, query_params, pool_info));

  std::array<float, 4> near_vector = {};
  std::vector<librados::vector_pg_lsh::put_target_t> put_targets;
  std::vector<librados::vector_pg_lsh::query_probe_t> probes;
  uint64_t generated_group_count = 0;
  int query_primary0 = -1;
  int query_primary1 = -1;
  bool found_vector = false;
  for (uint32_t candidate = 0; candidate < 4096; ++candidate) {
    std::array<float, 4> candidate_vector = {};
    for (uint32_t dim = 0; dim < candidate_vector.size(); ++dim) {
      const uint32_t x = candidate * 1103515245u + 12345u +
        dim * 2654435761u;
      int raw = static_cast<int>((x >> 24) & 0xffU) - 128;
      if (raw == 0) {
        raw = static_cast<int>(dim) + 1;
      }
      candidate_vector[dim] = static_cast<float>(raw) / 17.0f;
    }

    bufferlist candidate_bl;
    candidate_bl.append(
        reinterpret_cast<const char *>(candidate_vector.data()),
        candidate_vector.size() * sizeof(float));

    std::vector<librados::vector_pg_lsh::put_target_t> candidate_targets;
    if (librados::vector_pg_lsh::build_put_targets(
          "pg-lsh-bucket", "pg-lsh-index", candidate_bl,
          index_config, pool_info, &locator_cache,
          &candidate_targets) < 0 ||
        candidate_targets.size() != index_config.d ||
        candidate_targets[0].pg == candidate_targets[1].pg) {
      continue;
    }

    std::vector<librados::vector_pg_lsh::query_probe_t> candidate_probes;
    uint64_t candidate_generated_groups = 0;
    if (librados::vector_pg_lsh::build_query_probes(
          "pg-lsh-bucket", "pg-lsh-index", candidate_bl,
          index_config, query_params, pool_info, &locator_cache,
          &candidate_probes, &candidate_generated_groups) < 0 ||
        candidate_probes.size() != query_params.m ||
        candidate_generated_groups != index_config.l) {
      continue;
    }

    const std::unordered_set<uint32_t> write_pgs = {
      candidate_targets[0].pg,
      candidate_targets[1].pg,
    };
    const std::unordered_set<uint32_t> query_pgs = {
      candidate_probes[0].pg,
      candidate_probes[1].pg,
    };
    if (write_pgs != query_pgs) {
      continue;
    }

    int p0 = -1;
    int p1 = -1;
    ASSERT_EQ(0, acting_primary_for_raw_pg(
        cluster, pool_id, candidate_probes[0].pg, &p0));
    ASSERT_EQ(0, acting_primary_for_raw_pg(
        cluster, pool_id, candidate_probes[1].pg, &p1));
    if (p0 == p1) {
      continue;
    }

    near_vector = candidate_vector;
    put_targets = std::move(candidate_targets);
    probes = std::move(candidate_probes);
    generated_group_count = candidate_generated_groups;
    query_primary0 = p0;
    query_primary1 = p1;
    found_vector = true;
    break;
  }
  ASSERT_TRUE(found_vector)
    << "failed to find deterministic D=2/M=2 pg-lsh route across acting primaries";
  ASSERT_EQ(index_config.d, put_targets.size());
  ASSERT_EQ(query_params.m, probes.size());
  ASSERT_EQ(index_config.l, generated_group_count);
  EXPECT_NE(query_primary0, query_primary1);

  std::unordered_set<uint32_t> queried_pgs;
  for (const auto& probe : probes) {
    EXPECT_TRUE(queried_pgs.insert(probe.pg).second);
    uint32_t actual = 0;
    ASSERT_EQ(0, ioctx.get_object_pg_hash_position2(
        probe.locator_key, &actual));
    EXPECT_EQ(probe.pg, actual);
  }
  ASSERT_EQ(query_params.m, queried_pgs.size());
  for (const auto& target : put_targets) {
    EXPECT_EQ(1u, queried_pgs.count(target.pg));
    uint32_t actual = 0;
    ASSERT_EQ(0, ioctx.get_object_pg_hash_position2(
        target.locator_key, &actual));
    EXPECT_EQ(target.pg, actual);
  }

  bufferlist near_bl;
  near_bl.append(reinterpret_cast<const char *>(near_vector.data()),
                 near_vector.size() * sizeof(float));
  ceph::rados::put_vector_request_t put_req;
  put_req.bucket_name = "pg-lsh-bucket";
  put_req.index_name = "pg-lsh-index";
  put_req.key = "vec-dup";
  put_req.data_type = ceph::rados::vector_data_type_float32;
  put_req.distance_metric = ceph::rados::vector_distance_metric_euclidean;
  put_req.dimension = near_vector.size();
  put_req.vector_data = near_bl;
  for (const auto& target : put_targets) {
    ASSERT_EQ(0, librados::vector_pg_lsh::put_vector(
        ioctx, target, put_req));
  }

  const float far_vector[] = {10.0, 20.0, 30.0, 40.0};
  ASSERT_EQ(0, put_pg_lsh_test_vector(
      ioctx, probes[1].pg, probes[1].locator_key, "vec-far",
      far_vector, sizeof(far_vector)));

  struct PendingProbe {
    librados::vector_pg_lsh::query_probe_t probe;
    librados::vector_pg_lsh::query_op_state_t op_state;
    std::unique_ptr<AioCompletion> completion;
    bufferlist reply;
    int op_rval = 0;
  };
  std::vector<std::unique_ptr<PendingProbe>> pending;
  pending.reserve(probes.size());
  for (const auto& probe : probes) {
    auto pending_probe = std::make_unique<PendingProbe>();
    pending_probe->probe = probe;
    pending_probe->completion.reset(Rados::aio_create_completion());
    ASSERT_NE(nullptr, pending_probe->completion);

    ceph::rados::query_vectors_request_t req;
    req.bucket_name = "pg-lsh-bucket";
    req.index_name = "pg-lsh-index";
    req.data_type = ceph::rados::vector_data_type_float32;
    req.distance_metric = ceph::rados::vector_distance_metric_euclidean;
    req.dimension = near_vector.size();
    req.local_top_k = 2;
    req.query_vector = near_bl;

    ASSERT_EQ(0, librados::vector_pg_lsh::submit_query(
        ioctx, probe, std::move(req), &pending_probe->op_state,
        pending_probe->completion.get(), &pending_probe->reply,
        &pending_probe->op_rval));
    pending.push_back(std::move(pending_probe));
  }

  std::vector<ceph::rados::query_vectors_result_entry_t> merged;
  uint64_t matching_entries = 0;
  uint64_t distance_computations = 0;
  for (const auto& pending_probe : pending) {
    ASSERT_EQ(0, pending_probe->completion->wait_for_complete());
    ASSERT_EQ(0, pending_probe->completion->get_return_value());
    ASSERT_EQ(0, pending_probe->op_rval);

    ceph::rados::query_vectors_result_t partial;
    auto p = pending_probe->reply.cbegin();
    decode(partial, p);
    ASSERT_TRUE(p.end());
    matching_entries += partial.local_matching_entries;
    distance_computations += partial.local_distance_computations;
    for (const auto& entry : partial.entries) {
      ceph::rados::vector_query_exec::merge_result_entry(&merged, entry);
    }
  }

  EXPECT_EQ(3u, matching_entries);
  EXPECT_EQ(3u, distance_computations);
  ASSERT_EQ(2u, merged.size());
  ceph::rados::vector_query_exec::sort_and_trim_results(&merged, 2);
  EXPECT_EQ("vec-dup", merged[0].key);
  EXPECT_EQ("vec-far", merged[1].key);
  EXPECT_NEAR(0.0f, merged[0].distance, 1e-6f);
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

  std::vector<query_vectors_result_entry> query_results;
  ASSERT_EQ(0, ioctx.query_vectors(
      "bucket", "index",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 2, true, &query_results));
  ASSERT_EQ(2u, query_results.size());
  EXPECT_EQ("vec-pp", query_results[0].key);
  EXPECT_EQ("vec-pp-2", query_results[1].key);
  EXPECT_NEAR(0.0f, query_results[0].distance, 1e-6f);
  EXPECT_NEAR(0.0f, query_results[1].distance, 1e-6f);

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
  if (vals.empty()) {
    string objectstore;
    std::ignore = cluster.conf_get("osd_objectstore", objectstore);
    EXPECT_TRUE(
        is_crimson_cluster() && objectstore == "seastore");
  } else {
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
  }

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
      "bucket", "index", "empty-vector",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, 0, metadata));

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

  ASSERT_EQ(-EINVAL, ioctx.put_vector(
      "bucket", "index", "bad-type",
      static_cast<rados_vector_data_type_t>(999),
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  ASSERT_EQ(-EINVAL, ioctx.put_vector(
      "bucket", "index", "bad-metric",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      static_cast<rados_vector_distance_metric_t>(999),
      4, vector, sizeof(vector), metadata));
}

TEST_F(LibRadosIoPP, PutVectorLegacyOmapFallbackPP) {
  float vector[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist vector_bl;
  vector_bl.append(
      reinterpret_cast<const char *>(vector), sizeof(vector));
  bufferlist metadata;
  metadata.append("legacy-metadata", sizeof("legacy-metadata"));

  const string bucket = "legacy-vector-bucket-pp";
  const string index = "legacy-vector-index-pp";
  const string key = "legacy-vector-key-pp";
  const string oid = vector_test_oid(bucket, index, key, vector_bl);
  ASSERT_FALSE(oid.empty());

  bufferlist marker;
  marker.append("legacy", sizeof("legacy"));
  ASSERT_EQ(0, ioctx.omap_set(oid, {{"legacy-marker", marker}}));
  ASSERT_EQ(0, ioctx.put_vector(
      bucket, index, key,
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  const string entry_id = vector_entry_id(bucket, index, key);
  const string prefix = "_ENTRY_" + entry_id + ".";
  std::map<string, bufferlist> vals;
  ASSERT_EQ(0, ioctx.omap_get_vals_by_keys(
      oid, {"legacy-marker", prefix + "user_key"}, &vals));
  ASSERT_EQ(2u, vals.size());

  auto key_iter = vals[prefix + "user_key"].cbegin();
  string stored_key;
  decode(stored_key, key_iter);
  EXPECT_EQ(key, stored_key);

  std::vector<query_vectors_result_entry> results;
  ASSERT_EQ(0, ioctx.query_vectors(
      bucket, index,
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 1, true, &results));
  ASSERT_EQ(1u, results.size());
  EXPECT_EQ(key, results[0].key);
  EXPECT_NEAR(0.0f, results[0].distance, 1e-6f);
}

TEST_F(LibRadosIoPP, QueryVectorsValidationPP) {
  float vector[] = {1.0, 2.0, 3.0, 4.0};
  std::vector<query_vectors_result_entry> results;

  ASSERT_EQ(0, ioctx.query_vectors(
      "empty-query-bucket-pp", "empty-query-index-pp",
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

TEST_F(LibRadosIoPP, QueryVectorsPP) {
  float vector[] = {1.0, 2.0, 3.0, 4.0};
  bufferlist metadata;

  ASSERT_EQ(0, ioctx.put_vector(
      "query-bucket-pp", "query-index-pp", "vec-a",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));
  ASSERT_EQ(0, ioctx.put_vector(
      "query-bucket-pp", "query-index-pp", "vec-b",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), metadata));

  std::vector<query_vectors_result_entry> results;
  ASSERT_EQ(0, ioctx.query_vectors(
      "query-bucket-pp", "query-index-pp",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 1, true, &results));
  ASSERT_EQ(1u, results.size());
  EXPECT_EQ("vec-a", results[0].key);
  EXPECT_NEAR(0.0f, results[0].distance, 1e-6f);

  ASSERT_EQ(0, ioctx.query_vectors(
      "query-bucket-pp", "query-index-pp",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 10, true, &results));
  ASSERT_EQ(2u, results.size());
  EXPECT_EQ("vec-a", results[0].key);
  EXPECT_EQ("vec-b", results[1].key);
  EXPECT_NEAR(0.0f, results[0].distance, 1e-6f);
  EXPECT_NEAR(0.0f, results[1].distance, 1e-6f);

  ASSERT_EQ(0, ioctx.query_vectors(
      "query-bucket-pp", "query-index-pp",
      LIBRADOS_VECTOR_DATA_TYPE_FLOAT32,
      LIBRADOS_VECTOR_DISTANCE_METRIC_COSINE,
      4, vector, sizeof(vector), 1, false, &results));
  ASSERT_EQ(1u, results.size());
  EXPECT_EQ(0.0f, results[0].distance);
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
