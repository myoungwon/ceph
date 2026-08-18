// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_VECTOR_QUERY_EXEC_H
#define CEPH_COMMON_VECTOR_QUERY_EXEC_H

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include <errno.h>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/rados/vector_ops.h"

namespace ceph {
namespace rados {
namespace vector_query_exec {

// Non-owning view. The backing strings and vector data must remain valid
// until consume() returns.
struct vector_entry_view_t {
  std::string_view entry_id;
  std::string_view bucket_name;
  std::string_view index_name;
  std::string_view user_key;
  std::string_view placement_key;
  uint32_t data_type = 0;
  uint32_t distance_metric = 0;
  uint32_t dimension = 0;
  bool has_data_type = false;
  bool has_distance_metric = false;
  bool has_dimension = false;
  // True when metadata references vector content. OMAP may still be missing
  // that payload; VectorNode records always provide it inline.
  bool has_vector_reference = false;
  // Fixed-layout VectorNode records expose a contiguous, non-owning range.
  // It is valid only while the pinned LIST extent remains alive.
  std::string_view contiguous_vector_data;
  // OMAP content can remain fragmented and continues to use bufferlist.
  // At most one of the two payload sources may be populated.
  const ceph::bufferlist* vector_data = nullptr;

  bool has_vector_payload() const {
    return !contiguous_vector_data.empty() || vector_data != nullptr;
  }

  bool has_ambiguous_vector_payload() const {
    return !contiguous_vector_data.empty() && vector_data != nullptr;
  }
};

struct query_filter_stats_t {
  uint64_t total_entries = 0;
  uint64_t incomplete_entries = 0;
  uint64_t bucket_mismatch = 0;
  uint64_t index_mismatch = 0;
  uint64_t data_type_mismatch = 0;
  uint64_t distance_metric_mismatch = 0;
  uint64_t dimension_mismatch = 0;
  uint64_t probe_mismatch = 0;
  uint64_t missing_content = 0;
  uint64_t distance_error = 0;
  uint64_t matched_entries = 0;
  uint64_t distance_computations = 0;
  uint64_t merged_entries = 0;
  uint64_t final_entries = 0;
};

inline bool starts_with(std::string_view value, std::string_view prefix)
{
  return value.size() >= prefix.size() &&
    value.substr(0, prefix.size()) == prefix;
}

inline bool is_lower_hex(char c)
{
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
}

inline bool is_lower_hex_string(std::string_view value, size_t len)
{
  if (value.size() != len) {
    return false;
  }
  for (char c : value) {
    if (!is_lower_hex(c)) {
      return false;
    }
  }
  return true;
}

inline bool is_hash_v0_placement_key(std::string_view placement_key)
{
  return is_lower_hex_string(
      placement_key, vector_hash_v0_placement_key_len);
}

inline bool is_hash_v0_vector_hash(std::string_view vector_hash)
{
  return is_lower_hex_string(vector_hash, vector_hash_v0_vector_hash_len);
}

inline bool is_lsh_v0_placement_key(std::string_view placement_key)
{
  return is_lower_hex_string(
      placement_key, vector_lsh_v0_placement_key_len);
}

inline bool is_pg_lsh_v0_placement_key(std::string_view placement_key)
{
  return is_hash_v0_vector_hash(placement_key);
}

inline bool is_supported_placement_key(std::string_view placement_algorithm,
                                       std::string_view placement_key)
{
  if (placement_algorithm == vector_placement_algorithm_hash_v0) {
    return is_hash_v0_placement_key(placement_key);
  }
  if (placement_algorithm == vector_placement_algorithm_lsh_v0) {
    return is_lsh_v0_placement_key(placement_key);
  }
  if (placement_algorithm == vector_placement_algorithm_pg_lsh_v0) {
    return is_pg_lsh_v0_placement_key(placement_key);
  }
  return false;
}

inline bool is_supported_probe_key(std::string_view placement_key)
{
  return is_hash_v0_placement_key(placement_key) ||
    is_lsh_v0_placement_key(placement_key);
}

inline int validate_vector_payload(uint32_t data_type,
                                   uint32_t distance_metric,
                                   uint32_t dimension,
                                   const ceph::bufferlist& vector_data,
                                   size_t *element_size = nullptr)
{
  if (dimension == 0 || dimension > vector_max_dimension) {
    return -EINVAL;
  }

  size_t type_size = 0;
  int r = vector_data_type_size(data_type, &type_size);
  if (r < 0) {
    return -EINVAL;
  }
  if (!vector_distance_metric_supported(distance_metric)) {
    return -EINVAL;
  }

  const size_t expected_len = static_cast<size_t>(dimension) * type_size;
  if (vector_data.length() != expected_len) {
    return -EINVAL;
  }

  if (element_size != nullptr) {
    *element_size = type_size;
  }
  return 0;
}

inline int validate_put_request(const put_vector_request_t& req,
                                bool require_routed_fields,
                                size_t *element_size = nullptr)
{
  if (req.bucket_name.empty() || req.index_name.empty() || req.key.empty()) {
    return -EINVAL;
  }

  int r = validate_vector_payload(req.data_type, req.distance_metric,
                                  req.dimension, req.vector_data,
                                  element_size);
  if (r < 0) {
    return r;
  }

  if (!require_routed_fields) {
    return 0;
  }

  if (!is_supported_placement_key(req.placement_algorithm,
                                  req.placement_key) ||
      !is_hash_v0_vector_hash(req.vector_hash)) {
    return -EINVAL;
  }
  return 0;
}

template <typename T>
inline bool decode_omap_value(const ceph::bufferlist& bl, T *out)
{
  try {
    T decoded;
    auto p = bl.cbegin();
    decode(decoded, p);
    if (!p.end()) {
      return false;
    }
    *out = decoded;
    return true;
  } catch (const ceph::buffer::error&) {
    return false;
  }
}

inline void decode_omap_string_value(const ceph::bufferlist& bl,
                                     std::string *out)
{
  if (!decode_omap_value(bl, out)) {
    out->clear();
  }
}

inline int validate_query_request(const query_vectors_request_t& req,
                                  size_t *element_size = nullptr)
{
  if (req.bucket_name.empty() || req.index_name.empty() ||
      req.local_top_k == 0) {
    return -EINVAL;
  }

  int r = validate_vector_payload(req.data_type, req.distance_metric,
                                  req.dimension, req.query_vector,
                                  element_size);
  if (r < 0) {
    return r;
  }

  for (const auto& prefix : req.probe_prefixes) {
    if (!is_supported_probe_key(prefix)) {
      return -EINVAL;
    }
  }
  return 0;
}

inline bool probe_matches(const query_vectors_request_t& req,
                          const vector_entry_view_t& entry)
{
  if (req.probe_prefixes.empty()) {
    return true;
  }
  if (entry.placement_key.empty()) {
    return false;
  }
  return std::find(req.probe_prefixes.begin(), req.probe_prefixes.end(),
                   entry.placement_key) != req.probe_prefixes.end();
}

inline float read_float32(const char *data, size_t index)
{
  float value = 0;
  std::memcpy(&value, data + index * sizeof(float), sizeof(float));
  return value;
}

inline int copy_float32_values(const ceph::bufferlist& data,
                               uint32_t dimension,
                               std::vector<float> *out_values)
{
  if (out_values == nullptr || dimension == 0) {
    return -EINVAL;
  }
  const size_t expected_len =
    static_cast<size_t>(dimension) * sizeof(float);
  if (data.length() != expected_len) {
    return -EINVAL;
  }

  out_values->resize(dimension);
  auto p = data.cbegin();
  p.copy(expected_len, reinterpret_cast<char*>(out_values->data()));
  return 0;
}

inline double squared_l2_norm(const std::vector<float>& values)
{
  double norm_squared = 0;
  for (const float value : values) {
    norm_squared += static_cast<double>(value) * value;
  }
  return norm_squared;
}

inline int compute_float32_distance(const query_vectors_request_t& req,
                                    const std::vector<float>& query_values,
                                    double query_norm_squared,
                                    std::string_view candidate,
                                    float *distance)
{
  if (distance == nullptr) {
    return -EINVAL;
  }
  const size_t expected_len =
    static_cast<size_t>(req.dimension) * sizeof(float);
  if (req.data_type != vector_data_type_float32 ||
      query_values.size() != req.dimension ||
      candidate.size() != expected_len) {
    return -EINVAL;
  }

  double dot = 0;
  double candidate_norm = 0;
  double sum_sq = 0;
  for (uint32_t i = 0; i < req.dimension; ++i) {
    const double q = query_values[i];
    const double c = read_float32(candidate.data(), i);
    dot += q * c;
    candidate_norm += c * c;
    const double diff = q - c;
    sum_sq += diff * diff;
  }

  switch (req.distance_metric) {
  case vector_distance_metric_euclidean:
    *distance = static_cast<float>(std::sqrt(sum_sq));
    return 0;
  case vector_distance_metric_cosine:
    if (query_norm_squared == 0 || candidate_norm == 0) {
      *distance = std::numeric_limits<float>::infinity();
    } else {
      *distance = static_cast<float>(
          1.0 - dot /
          (std::sqrt(query_norm_squared) * std::sqrt(candidate_norm)));
    }
    return 0;
  case vector_distance_metric_dot:
    *distance = static_cast<float>(-dot);
    return 0;
  default:
    return -EINVAL;
  }
}

inline int compute_float32_distance(const query_vectors_request_t& req,
                                    const std::vector<float>& query_values,
                                    double query_norm_squared,
                                    const ceph::bufferlist& candidate,
                                    float *distance)
{
  ceph::bufferlist candidate_copy = candidate;
  const char *candidate_data = candidate_copy.c_str();
  return compute_float32_distance(
    req,
    query_values,
    query_norm_squared,
    std::string_view(candidate_data, candidate_copy.length()),
    distance);
}

inline const std::string& result_logical_identity(
    const query_vectors_result_entry_t& entry)
{
  // Partial results being merged belong to one request, which fixes the
  // bucket and index; the user key is therefore the logical identity.
  return entry.key;
}

inline bool result_entry_valid(const query_vectors_result_entry_t& entry)
{
  return !entry.key.empty() && !entry.entry_id.empty();
}

inline bool is_better_query_result(const query_vectors_result_entry_t& lhs,
                                   const query_vectors_result_entry_t& rhs)
{
  if (lhs.distance != rhs.distance) {
    return lhs.distance < rhs.distance;
  }
  return lhs.key < rhs.key;
}

inline void merge_result_entry(
    std::vector<query_vectors_result_entry_t> *retained_results,
    const query_vectors_result_entry_t& new_result)
{
  if (retained_results == nullptr) {
    return;
  }
  const std::string& new_result_id = result_logical_identity(new_result);
  if (new_result_id.empty()) {
    retained_results->push_back(new_result);
    return;
  }
  for (auto& retained_result : *retained_results) {
    if (result_logical_identity(retained_result) == new_result_id) {
      if (is_better_query_result(new_result, retained_result)) {
        retained_result = new_result;
      }
      return;
    }
  }
  retained_results->push_back(new_result);
}

inline void sort_and_trim_results(
    std::vector<query_vectors_result_entry_t> *retained_results,
    uint32_t top_k)
{
  if (retained_results == nullptr) {
    return;
  }
  std::sort(retained_results->begin(), retained_results->end(),
            is_better_query_result);
  if (top_k != 0 && retained_results->size() > top_k) {
    retained_results->resize(top_k);
  }
}

inline void retain_local_topk_result(
    std::vector<query_vectors_result_entry_t> *retained_results,
    const query_vectors_result_entry_t& new_result,
    uint32_t top_k,
    bool *is_heapified)
{
  if (retained_results == nullptr || is_heapified == nullptr) {
    return;
  }
  if (top_k == 0) {
    retained_results->push_back(new_result);
    return;
  }

  const size_t limit = top_k;
  if (retained_results->size() < limit) {
    retained_results->push_back(new_result);
    if (retained_results->size() == limit) {
      std::make_heap(retained_results->begin(), retained_results->end(),
                     is_better_query_result);
      *is_heapified = true;
    }
    return;
  }

  // With the "better" comparator, the heap front is the worst retained
  // result. Replace it only when the newly scanned result ranks better.
  if (is_better_query_result(new_result, retained_results->front())) {
    std::pop_heap(retained_results->begin(), retained_results->end(),
                  is_better_query_result);
    retained_results->back() = new_result;
    std::push_heap(retained_results->begin(), retained_results->end(),
                   is_better_query_result);
  }
}

inline void finalize_local_topk_results(
    std::vector<query_vectors_result_entry_t> *retained_results,
    uint32_t top_k,
    bool is_heapified)
{
  if (retained_results == nullptr) {
    return;
  }
  if (is_heapified) {
    std::sort_heap(retained_results->begin(), retained_results->end(),
                   is_better_query_result);
  } else {
    std::sort(retained_results->begin(), retained_results->end(),
              is_better_query_result);
  }
  if (top_k != 0 && retained_results->size() > top_k) {
    retained_results->resize(top_k);
  }
}

class local_query_accumulator_t {
public:
  int prepare(const query_vectors_request_t& query)
  {
    prepared = false;
    heapified = false;
    entries.clear();
    stats = query_filter_stats_t();
    query_values.clear();
    query_norm = 0;

    int r = validate_query_request(query);
    if (r < 0) {
      return r;
    }

    req = query;
    r = copy_float32_values(
        req.query_vector, req.dimension, &query_values);
    if (r < 0) {
      return r;
    }
    query_norm = squared_l2_norm(query_values);

    prepared = true;
    return 0;
  }

  int consume(const vector_entry_view_t& entry)
  {
    if (!prepared) {
      return -EINVAL;
    }
    stats.total_entries++;
    if (!has_required_fields(entry)) {
      stats.incomplete_entries++;
      return 0;
    }
    if (!matches_request(entry)) {
      return 0;
    }
    accumulate_result(entry);
    return 0;
  }

  int finish(query_vectors_result_t *result,
             query_filter_stats_t *out_stats = nullptr)
  {
    if (!prepared || result == nullptr) {
      return -EINVAL;
    }

    // A local scan visits each physical record once. Logical duplicate
    // merge happens when partial query results are combined.
    stats.merged_entries = stats.matched_entries;
    finalize_local_topk_results(&entries, req.local_top_k, heapified);
    stats.final_entries = entries.size();
    result->entries = std::move(entries);
    if (out_stats != nullptr) {
      *out_stats = stats;
    }
    prepared = false;
    return 0;
  }

private:
  static bool has_required_fields(const vector_entry_view_t& entry)
  {
    return !entry.entry_id.empty() &&
      !entry.bucket_name.empty() &&
      !entry.index_name.empty() &&
      !entry.user_key.empty() &&
      entry.has_vector_reference &&
      entry.has_data_type &&
      entry.has_distance_metric &&
      entry.has_dimension;
  }

  bool matches_request(const vector_entry_view_t& entry)
  {
    if (entry.bucket_name != req.bucket_name) {
      stats.bucket_mismatch++;
      return false;
    }
    if (entry.index_name != req.index_name) {
      stats.index_mismatch++;
      return false;
    }
    if (entry.data_type != req.data_type) {
      stats.data_type_mismatch++;
      return false;
    }
    if (entry.distance_metric != req.distance_metric) {
      stats.distance_metric_mismatch++;
      return false;
    }
    if (entry.dimension != req.dimension) {
      stats.dimension_mismatch++;
      return false;
    }
    if (!probe_matches(req, entry)) {
      stats.probe_mismatch++;
      return false;
    }
    return true;
  }

  void accumulate_result(const vector_entry_view_t& entry)
  {
    if (!entry.has_vector_payload()) {
      stats.missing_content++;
      return;
    }
    if (entry.has_ambiguous_vector_payload()) {
      stats.distance_error++;
      return;
    }

    float distance = 0;
    stats.distance_computations++;
    const int r = !entry.contiguous_vector_data.empty()
      ? compute_float32_distance(
          req,
          query_values,
          query_norm,
          entry.contiguous_vector_data,
          &distance)
      : compute_float32_distance(
          req, query_values, query_norm, *entry.vector_data, &distance);
    if (r < 0) {
      // prepare() has already validated the request and query state. The
      // remaining failure is local to this record's vector payload.
      stats.distance_error++;
      return;
    }
    stats.matched_entries++;

    query_vectors_result_entry_t result_entry;
    result_entry.key = entry.user_key;
    result_entry.distance = distance;
    result_entry.entry_id = entry.entry_id;
    retain_local_topk_result(
        &entries, result_entry, req.local_top_k, &heapified);
  }

  query_vectors_request_t req;
  std::vector<float> query_values;
  double query_norm = 0;
  std::vector<query_vectors_result_entry_t> entries;
  query_filter_stats_t stats;
  bool heapified = false;
  bool prepared = false;
};

} // namespace vector_query_exec
} // namespace rados
} // namespace ceph

#endif
