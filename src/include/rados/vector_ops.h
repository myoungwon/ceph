// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_RADOS_VECTOR_OPS_H
#define CEPH_RADOS_VECTOR_OPS_H

#include <cstddef>
#include <cstdint>
#include <errno.h>
#include <optional>
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

// pg-lsh-v0 parameters the OSD needs to work out which neighboring sub_oid
// ONodes in the same PG could still hold a closer candidate; see
// common/vector_pg_lsh_boundary.h for the math. Kept in its own struct so
// a query that does not use boundary-aware search carries none of these
// bytes. `dimension` comes from query_vectors_request_t and is not
// duplicated here.
struct vector_boundary_search_config_t {
  // Placement anchor (index_config_t::anchor), dimension-sized. Used for
  // Dq = ||q_hat-a_hat||^2 and the query's residual projections.
  std::vector<double> anchor;
  // Hyperplane seed, needed to reproduce the residual signs
  // compute_sub_oid() used at placement time.
  uint32_t seed = 0;
  // sub_oid bit widths, needed to invert the distance_bucket quantization
  // and to know how many residual bits to project.
  uint32_t distance_bucket_bits = 0;
  uint32_t residual_bits = 0;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(anchor, bl);
    encode(seed, bl);
    encode(distance_bucket_bits, bl);
    encode(residual_bits, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(anchor, p);
    decode(seed, p);
    decode(distance_bucket_bits, p);
    decode(residual_bits, p);
    DECODE_FINISH(p);
  }
};

// pg-lsh-v0 parameters for best-first traversal over FLTree subtree
// ranges; see common/vector_pg_lsh_boundary.h and seastore.cc's
// query_vectors(). Separate from vector_boundary_search_config_t above
// because it is a different mechanism -- OSD-side tree descent with a
// global frontier rather than a flat sub_oid range scan -- and the two are
// never combined in one request.
struct vector_tree_boundary_search_config_t {
  // Same meaning as in vector_boundary_search_config_t: Dq, plus decoding
  // a subtree's key range into a distance_bucket range.
  std::vector<double> anchor;
  uint32_t seed = 0;
  uint32_t distance_bucket_bits = 0;
  uint32_t residual_bits = 0;
  // Max additional subtrees, beyond the primary path, the OSD may expand
  // for this query. Traversal stops on this budget or when the next
  // frontier entry can no longer improve the running tau, whichever comes
  // first.
  uint32_t budget = 0;
  // One of common/vector_pg_lsh_placement.h's distance_geometry_*
  // constants. The default, distance_geometry_normalized_angular_v0, does
  // not put distance_bucket and tau in one metric space, so it supports
  // best-first ordering but not exclusion;
  // distance_geometry_raw_euclidean_v1 does.
  uint32_t distance_geometry = 0;
  // Must be > 0 for distance_geometry_raw_euclidean_v1 and 0 otherwise;
  // see index_config_t::raw_distance_scale_max.
  double raw_distance_scale_max = 0;
  // Enables tau-based subtree exclusion rather than reordering alone.
  // Only takes effect under distance_geometry_raw_euclidean_v1; the client
  // already refuses to set it otherwise, but the OSD re-checks
  // distance_geometry rather than trusting the request.
  bool enable_pruning = false;
  // Consecutive expansions that fail to improve the running tau before
  // the OSD stops expanding this probe.
  //
  // 0 stops at the first one, which assumes the frontier is ordered well
  // enough that one miss implies the rest are misses. That does not hold
  // here: most of the ONodes a full-PG scan finds and the traversal misses
  // are still sitting on the frontier, admitted but never popped, with the
  // expansion budget far from exhausted. Raising this trades reads for
  // recall.
  uint32_t no_improve_patience = 0;
  // Max ONodes this probe may open, i.e. read the VectorNode of and scan.
  // 0 is unlimited and is the only setting that stays exact over the
  // candidate space the bound admits.
  //
  // Setting it makes the search approximate: it stops early rather than
  // showing nothing further can qualify. It exists because the exclusion
  // available here, distance_bound_from_tau()'s triangle inequality around
  // the PG anchor, excludes nothing on this workload -- LSH routing picks
  // anchors close to the query, so ||q-anchor|| and tau are the same order
  // of magnitude and the band [(radius-tau)^2, (radius+tau)^2] covers
  // every populated distance_bucket.
  //
  // With distance-ordered traversal (closest child first, closest ONode
  // first within a batch) this makes the recall/candidates trade explicit
  // instead of leaving it to the frontier's stop rule.
  uint32_t onode_budget = 0;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(anchor, bl);
    encode(seed, bl);
    encode(distance_bucket_bits, bl);
    encode(residual_bits, bl);
    encode(budget, bl);
    encode(distance_geometry, bl);
    encode(raw_distance_scale_max, bl);
    encode(enable_pruning, bl);
    encode(no_improve_patience, bl);
    encode(onode_budget, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(anchor, p);
    decode(seed, p);
    decode(distance_bucket_bits, p);
    decode(residual_bits, p);
    decode(budget, p);
    decode(distance_geometry, p);
    decode(raw_distance_scale_max, p);
    decode(enable_pruning, p);
    decode(no_improve_patience, p);
    decode(onode_budget, p);
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
  // Set only for pg-lsh-v0 boundary-aware search. Absent means one ONode
  // and no server-side expansion.
  std::optional<vector_boundary_search_config_t> boundary_search;
  // Set only for tree traversal (see
  // vector_tree_boundary_search_config_t). A client never sets it together
  // with boundary_search, but the two encode independently.
  std::optional<vector_tree_boundary_search_config_t> tree_boundary_search;

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
    const bool has_boundary_search = boundary_search.has_value();
    encode(has_boundary_search, bl);
    if (has_boundary_search) {
      boundary_search->encode(bl);
    }
    const bool has_tree_boundary_search = tree_boundary_search.has_value();
    encode(has_tree_boundary_search, bl);
    if (has_tree_boundary_search) {
      tree_boundary_search->encode(bl);
    }
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
    boundary_search.reset();
    bool has_boundary_search = false;
    decode(has_boundary_search, p);
    if (has_boundary_search) {
      vector_boundary_search_config_t boundary;
      boundary.decode(p);
      boundary_search = std::move(boundary);
    }
    tree_boundary_search.reset();
    bool has_tree_boundary_search = false;
    decode(has_tree_boundary_search, p);
    if (has_tree_boundary_search) {
      vector_tree_boundary_search_config_t tree_boundary;
      tree_boundary.decode(p);
      tree_boundary_search = std::move(tree_boundary);
    }
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

// Per-OSD telemetry for one tree-boundary query, set only when the request
// carried a vector_tree_boundary_search_config_t. Nothing here feeds back
// into query correctness; it exists so a benchmark client can see how much
// of the tree the search actually touched. tau is the current top-k radius,
// absent while the local top-k is still unfilled.
struct query_vectors_tree_stats_t {
  // Candidates returned by the initial PG-local descent.
  uint64_t primary_candidate_count = 0;
  // Frontier expansions performed after that descent.
  uint64_t expanded_subtree_count = 0;
  // Candidates handed to the scan, primary and expanded together.
  uint64_t candidates_seen = 0;
  // Candidates whose VectorNode chain was actually read.
  uint64_t opened_onode_count = 0;
  // Subtrees the tau distance bound excluded before they were expanded.
  uint64_t pruned_distance_count = 0;
  // Candidates the residual wildcard mask excluded before opening them.
  uint64_t pruned_residual_count = 0;
  bool has_tau_initial = false;
  float tau_initial = 0;
  bool has_tau_final = false;
  float tau_final = 0;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(primary_candidate_count, bl);
    encode(expanded_subtree_count, bl);
    encode(candidates_seen, bl);
    encode(opened_onode_count, bl);
    encode(pruned_distance_count, bl);
    encode(pruned_residual_count, bl);
    encode(has_tau_initial, bl);
    encode(tau_initial, bl);
    encode(has_tau_final, bl);
    encode(tau_final, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(primary_candidate_count, p);
    decode(expanded_subtree_count, p);
    decode(candidates_seen, p);
    decode(opened_onode_count, p);
    decode(pruned_distance_count, p);
    decode(pruned_residual_count, p);
    decode(has_tau_initial, p);
    decode(tau_initial, p);
    decode(has_tau_final, p);
    decode(tau_final, p);
    DECODE_FINISH(p);
  }
};

struct query_vectors_result_t {
  // Sorted vector query result entries.
  std::vector<query_vectors_result_entry_t> entries;
  // OSD-local physical scan stats. These are deliberately minimal; benchmark
  // clients derive logical candidate and duplicate counts after global merge.
  uint64_t local_matching_entries = 0;
  uint64_t local_distance_computations = 0;
  // Set only for tree-boundary queries; see query_vectors_tree_stats_t.
  std::optional<query_vectors_tree_stats_t> tree_stats;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(static_cast<uint32_t>(entries.size()), bl);
    for (const auto& entry : entries) {
      entry.encode(bl);
    }
    encode(local_matching_entries, bl);
    encode(local_distance_computations, bl);
    const bool has_tree_stats = tree_stats.has_value();
    encode(has_tree_stats, bl);
    if (has_tree_stats) {
      tree_stats->encode(bl);
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
    decode(local_matching_entries, p);
    decode(local_distance_computations, p);
    tree_stats.reset();
    bool has_tree_stats = false;
    decode(has_tree_stats, p);
    if (has_tree_stats) {
      query_vectors_tree_stats_t stats;
      stats.decode(p);
      tree_stats = std::move(stats);
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
WRITE_CLASS_ENCODER(ceph::rados::query_vectors_tree_stats_t)
WRITE_CLASS_ENCODER(ceph::rados::query_vectors_result_t)

#endif
