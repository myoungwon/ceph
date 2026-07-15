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
inline constexpr uint32_t vector_query_algorithm_lsh = 2;
inline constexpr uint32_t vector_query_algorithm_version_0 = 0;
inline constexpr const char *vector_placement_algorithm_hash_v0 = "hash-v0";
inline constexpr const char *vector_placement_algorithm_lsh_v0 = "lsh-v0";
inline constexpr const char *vector_placement_algorithm_pg_lsh_v0 = "pg-lsh-v0";
inline constexpr uint32_t vector_hash_v0_placement_key_len = 4;
inline constexpr uint32_t vector_hash_v0_vector_hash_len = 8;
inline constexpr uint32_t vector_lsh_v0_bits = 10;
inline constexpr uint32_t vector_lsh_v0_max_bits = 16;
inline constexpr uint32_t vector_lsh_v0_default_table_count = 16;
inline constexpr uint32_t vector_lsh_v0_placement_key_len = 8;

struct vector_routing_policy_t {
  // Number of placement targets to write for each vector entry.
  uint32_t write_pgs = 1;
  // Number of placement targets to probe for each vector query.
  uint32_t probe_pgs = 1;
  // Per-target partial result upper bound; 0 inherits query top_k.
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
  // Planner algorithm family. hash-v0 is a routing baseline; lsh-v0 adds
  // semantic candidate routing while preserving the same OSD local search path.
  uint32_t algorithm_id = vector_query_algorithm_hash;
  // Planner/layout variant within algorithm_id. Version 0 is the hash-v0
  // baseline and is intentionally replaceable by future planners.
  uint32_t algorithm_version = vector_query_algorithm_version_0;
  // Placement algorithm used to map put vectors and query probes to objects.
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
  // User-provided logical vector key. OSDs derive entry_id from
  // bucket_name/index_name/key and use it as the logical vector identity.
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
  // Placement algorithm selected by the client planner. hash-v0 stores vectors
  // under an object name derived from bucket/index and a vector-hash prefix.
  std::string placement_algorithm;
  // Placement key for the target object receiving this write.
  std::string placement_key;
  // Hash of vector_data used for placement and content storage keys.
  std::string vector_hash;

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
  // Maximum number of local results this OSD should return for this routed
  // probe. librados owns global fanout and final top_k trimming.
  uint32_t local_top_k = 0;
  // Raw query vector payload bytes.
  ceph::bufferlist query_vector;
  // Placement keys that this routed probe should scan; empty scans all keys in
  // the target object. Missing prefixes are normal empty-result probes.
  std::vector<std::string> probe_prefixes;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(bucket_name, bl);
    encode(index_name, bl);
    encode(data_type, bl);
    encode(distance_metric, bl);
    encode(dimension, bl);
    encode(local_top_k, bl);
    encode(query_vector, bl);
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
    decode(local_top_k, p);
    decode(query_vector, p);
    decode(probe_prefixes, p);
    DECODE_FINISH(p);
  }
};

struct query_vectors_result_entry_t {
  // User-provided vector key for the matched entry.
  std::string key;
  // Distance between the query vector and this result entry.
  float distance = 0;
  // Logical vector identity. Clients merge duplicate partial results by this
  // field and keep the best distance for each entry_id.
  std::string entry_id;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(key, bl);
    encode(distance, bl);
    encode(entry_id, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(key, p);
    decode(distance, p);
    decode(entry_id, p);
    DECODE_FINISH(p);
  }
};

struct query_vectors_result_t {
  // Sorted vector query result entries.
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
