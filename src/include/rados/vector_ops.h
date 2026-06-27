// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_RADOS_VECTOR_OPS_H
#define CEPH_RADOS_VECTOR_OPS_H

#include <cstddef>
#include <cstdint>
#include <errno.h>
#include <string>
#include <utility>
#include <vector>

#include "include/buffer.h"
#include "include/encoding.h"

namespace ceph {
namespace rados {

inline constexpr uint32_t vector_layout_version = 1;
inline constexpr uint32_t vector_max_dimension = 4096;

inline constexpr uint32_t vector_data_type_float32 = 1;

inline constexpr uint32_t vector_distance_metric_euclidean = 1;
inline constexpr uint32_t vector_distance_metric_cosine = 2;
inline constexpr uint32_t vector_distance_metric_dot = 3;

inline constexpr uint32_t vector_query_algorithm_hash = 1;
inline constexpr uint32_t vector_query_algorithm_version_0 = 0;

struct vector_routing_policy_t {
  // Number of placement targets to write for each vector entry.
  uint32_t write_pgs = 1;
  // Number of placement targets to probe for each vector query.
  uint32_t probe_pgs = 1;
  // Per-target result limit before the client merges results; 0 uses top_k.
  uint32_t local_topk = 0;
  // Algorithm-specific candidate limit; 0 means no explicit limit.
  uint32_t max_candidates = 0;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(write_pgs, bl);
    encode(probe_pgs, bl);
    encode(local_topk, bl);
    encode(max_candidates, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(write_pgs, p);
    decode(probe_pgs, p);
    decode(local_topk, p);
    decode(max_candidates, p);
    DECODE_FINISH(p);
  }
};

struct vector_index_config_t {
  // Vector element data type accepted by this index; 0 accepts the request type.
  uint32_t data_type = 0;
  // Distance metric accepted by this index; 0 accepts the request metric.
  uint32_t distance_metric = 0;
  // Number of vector dimensions in this index; 0 accepts the request dimension.
  uint32_t dimension = 0;
  // Vector query algorithm identifier used by the index planner.
  uint32_t algorithm_id = vector_query_algorithm_hash;
  // Version of the query algorithm and algorithm-specific parameters.
  uint32_t algorithm_version = vector_query_algorithm_version_0;
  // Placement algorithm name used to map vectors and probes to RADOS objects.
  std::string placement_algorithm;
  // Routing fanout and per-target query limits for this index.
  vector_routing_policy_t routing_policy;
  // Opaque algorithm-specific configuration parameters.
  ceph::bufferlist algorithm_params;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(data_type, bl);
    encode(distance_metric, bl);
    encode(dimension, bl);
    encode(algorithm_id, bl);
    encode(algorithm_version, bl);
    encode(placement_algorithm, bl);
    routing_policy.encode(bl);
    encode(algorithm_params, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(data_type, p);
    decode(distance_metric, p);
    decode(dimension, p);
    decode(algorithm_id, p);
    decode(algorithm_version, p);
    decode(placement_algorithm, p);
    routing_policy.decode(p);
    decode(algorithm_params, p);
    DECODE_FINISH(p);
  }
};

inline int vector_data_type_size(uint32_t data_type, size_t *size)
{
  switch (data_type) {
  case vector_data_type_float32:
    *size = sizeof(float);
    return 0;
  default:
    return -EOPNOTSUPP;
  }
}

inline bool vector_distance_metric_supported(uint32_t distance_metric)
{
  switch (distance_metric) {
  case vector_distance_metric_euclidean:
  case vector_distance_metric_cosine:
  case vector_distance_metric_dot:
    return true;
  default:
    return false;
  }
}

struct put_vector_request_t {
  // Logical vector bucket name.
  std::string bucket_name;
  // Logical vector index name within the bucket.
  std::string index_name;
  // User-provided vector key.
  std::string key;
  // Vector element data type.
  uint32_t data_type = 0;
  // Distance metric used when comparing this vector.
  uint32_t distance_metric = 0;
  // Number of vector dimensions.
  uint32_t dimension = 0;
  // Raw vector payload bytes.
  ceph::bufferlist vector_data;
  // Optional application-defined metadata stored with the entry.
  ceph::bufferlist metadata;
  // Placement algorithm selected by the client planner.
  std::string placement_algorithm;
  // Placement key for the target object receiving this write.
  std::string placement_key;
  // Hash of vector_data used for placement and content storage keys.
  std::string vector_hash;
  // Opaque algorithm-specific parameters copied from the index config.
  ceph::bufferlist algorithm_params;
  // Effective routing policy copied from the index config.
  vector_routing_policy_t routing_policy;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(bucket_name, bl);
    encode(index_name, bl);
    encode(key, bl);
    encode(data_type, bl);
    encode(distance_metric, bl);
    encode(dimension, bl);
    encode(vector_data, bl);
    encode(metadata, bl);
    encode(placement_algorithm, bl);
    encode(placement_key, bl);
    encode(vector_hash, bl);
    encode(algorithm_params, bl);
    routing_policy.encode(bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(bucket_name, p);
    decode(index_name, p);
    decode(key, p);
    decode(data_type, p);
    decode(distance_metric, p);
    decode(dimension, p);
    decode(vector_data, p);
    decode(metadata, p);
    decode(placement_algorithm, p);
    decode(placement_key, p);
    decode(vector_hash, p);
    decode(algorithm_params, p);
    routing_policy.decode(p);
    DECODE_FINISH(p);
  }
};

struct query_vectors_request_t {
  // Logical vector bucket name.
  std::string bucket_name;
  // Logical vector index name within the bucket.
  std::string index_name;
  // Query vector element data type.
  uint32_t data_type = 0;
  // Distance metric used to rank candidate vectors.
  uint32_t distance_metric = 0;
  // Number of query vector dimensions.
  uint32_t dimension = 0;
  // Number of nearest vectors requested in the merged result.
  uint32_t top_k = 0;
  // Whether distances should be returned to the caller.
  bool return_distance = false;
  // Raw query vector payload bytes.
  ceph::bufferlist query_vector;
  // Vector query algorithm identifier selected for this request.
  uint32_t algorithm_id = 0;
  // Version of the selected query algorithm.
  uint32_t algorithm_version = 0;
  // Placement algorithm used to route query probes.
  std::string placement_algorithm;
  // Effective routing policy used by the query planner and OSD.
  vector_routing_policy_t routing_policy;
  // Opaque algorithm-specific parameters copied from the index config.
  ceph::bufferlist algorithm_params;
  // Placement keys that this routed probe should scan; empty scans all keys.
  std::vector<std::string> probe_prefixes;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(bucket_name, bl);
    encode(index_name, bl);
    encode(data_type, bl);
    encode(distance_metric, bl);
    encode(dimension, bl);
    encode(top_k, bl);
    encode(return_distance, bl);
    encode(query_vector, bl);
    encode(algorithm_id, bl);
    encode(algorithm_version, bl);
    encode(placement_algorithm, bl);
    routing_policy.encode(bl);
    encode(algorithm_params, bl);
    encode(probe_prefixes, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(bucket_name, p);
    decode(index_name, p);
    decode(data_type, p);
    decode(distance_metric, p);
    decode(dimension, p);
    decode(top_k, p);
    decode(return_distance, p);
    decode(query_vector, p);
    decode(algorithm_id, p);
    decode(algorithm_version, p);
    decode(placement_algorithm, p);
    routing_policy.decode(p);
    decode(algorithm_params, p);
    decode(probe_prefixes, p);
    DECODE_FINISH(p);
  }
};

struct query_vectors_result_entry_t {
  std::string key;
  float distance = 0;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(key, bl);
    encode(distance, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(key, p);
    decode(distance, p);
    DECODE_FINISH(p);
  }
};

struct query_vectors_result_t {
  std::vector<query_vectors_result_entry_t> entries;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(static_cast<uint32_t>(entries.size()), bl);
    for (const auto& entry : entries) {
      entry.encode(bl);
    }
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    uint32_t entries_len = 0;
    decode(entries_len, p);
    entries.clear();
    entries.reserve(entries_len);
    for (uint32_t i = 0; i < entries_len; ++i) {
      query_vectors_result_entry_t entry;
      entry.decode(p);
      entries.push_back(std::move(entry));
    }
    DECODE_FINISH(p);
  }
};

} // namespace rados
} // namespace ceph

WRITE_CLASS_ENCODER(ceph::rados::vector_routing_policy_t)
WRITE_CLASS_ENCODER(ceph::rados::vector_index_config_t)
WRITE_CLASS_ENCODER(ceph::rados::put_vector_request_t)
WRITE_CLASS_ENCODER(ceph::rados::query_vectors_request_t)
WRITE_CLASS_ENCODER(ceph::rados::query_vectors_result_entry_t)
WRITE_CLASS_ENCODER(ceph::rados::query_vectors_result_t)

#endif
