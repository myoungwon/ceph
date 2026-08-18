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
#include "common/vector_record.h"

#if defined(__x86_64__) || defined(__i386__)
#include <immintrin.h>
#include "arch/probe.h"
#include "arch/intel.h"
#define CEPH_VECTOR_QUERY_EXEC_HAVE_X86_SIMD 1
#endif

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
  // True for a row scanned directly out of a VectorNode LIST page: LIST
  // rows carry only entry_id and vector bytes (see vector_node_layout.h's
  // vector_list_entry_le_t), so bucket_name/index_name/user_key/
  // placement_key are never populated for this source. That is safe to
  // match on anyway: object-name placement already scopes one object to
  // one (bucket_name, index_name) pair (see
  // librados/vector_placement.h's make_*_oid()), and the caller already
  // scoped the scanned LIST chain to this exact (dimension, data_type) via
  // find_group_head() before ever visiting a row, so re-checking those
  // fields per row would be redundant. user_key (and any application
  // metadata) is restored afterwards, by entry_id, from OMAP -- but only
  // for the entries that survive top-k selection, not every scanned row.
  bool is_vector_node_native = false;
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

// Running sums for one query/candidate pair, shared by every kernel.
struct distance_terms_t {
  double dot = 0;
  double candidate_norm_squared = 0;
  double sum_sq = 0;
};

inline void accumulate_distance_terms_scalar(const float *query,
                                             const char *candidate_bytes,
                                             uint32_t dimension,
                                             distance_terms_t *terms)
{
  for (uint32_t i = 0; i < dimension; ++i) {
    const double q = query[i];
    const double c = read_float32(candidate_bytes, i);
    terms->dot += q * c;
    terms->candidate_norm_squared += c * c;
    const double diff = q - c;
    terms->sum_sq += diff * diff;
  }
}

#if defined(CEPH_VECTOR_QUERY_EXEC_HAVE_X86_SIMD)
// Converts each 4-wide float32 group to double before accumulating, using
// a separate multiply and add instead of fused multiply-add, so
// per-element arithmetic matches accumulate_distance_terms_scalar. The
// cross-lane reduction still sums in a different order than the scalar
// loop; the resulting difference is far below float32 precision.
__attribute__((__target__("avx2")))
inline void accumulate_distance_terms_avx2(const float *query,
                                           const char *candidate_bytes,
                                           uint32_t dimension,
                                           distance_terms_t *terms)
{
  __m256d dot_acc = _mm256_setzero_pd();
  __m256d norm_acc = _mm256_setzero_pd();
  __m256d sq_acc = _mm256_setzero_pd();

  uint32_t i = 0;
  for (; i + 4 <= dimension; i += 4) {
    const __m256d q = _mm256_cvtps_pd(_mm_loadu_ps(query + i));
    const __m256d c = _mm256_cvtps_pd(_mm_loadu_ps(
        reinterpret_cast<const float*>(candidate_bytes + i * sizeof(float))));
    dot_acc = _mm256_add_pd(dot_acc, _mm256_mul_pd(q, c));
    norm_acc = _mm256_add_pd(norm_acc, _mm256_mul_pd(c, c));
    const __m256d diff = _mm256_sub_pd(q, c);
    sq_acc = _mm256_add_pd(sq_acc, _mm256_mul_pd(diff, diff));
  }

  alignas(32) double dot_lanes[4];
  alignas(32) double norm_lanes[4];
  alignas(32) double sq_lanes[4];
  _mm256_store_pd(dot_lanes, dot_acc);
  _mm256_store_pd(norm_lanes, norm_acc);
  _mm256_store_pd(sq_lanes, sq_acc);
  for (int lane = 0; lane < 4; ++lane) {
    terms->dot += dot_lanes[lane];
    terms->candidate_norm_squared += norm_lanes[lane];
    terms->sum_sq += sq_lanes[lane];
  }

  for (; i < dimension; ++i) {
    const double q = query[i];
    const double c = read_float32(candidate_bytes, i);
    terms->dot += q * c;
    terms->candidate_norm_squared += c * c;
    const double diff = q - c;
    terms->sum_sq += diff * diff;
  }
}
#endif // CEPH_VECTOR_QUERY_EXEC_HAVE_X86_SIMD

inline void accumulate_distance_terms(const float *query,
                                      const char *candidate_bytes,
                                      uint32_t dimension,
                                      distance_terms_t *terms)
{
#if defined(CEPH_VECTOR_QUERY_EXEC_HAVE_X86_SIMD)
  static const bool has_avx2 = [] {
    ceph_arch_probe();
    return ceph_arch_intel_avx2 != 0;
  }();
  if (has_avx2) {
    accumulate_distance_terms_avx2(query, candidate_bytes, dimension, terms);
    return;
  }
#endif
  accumulate_distance_terms_scalar(query, candidate_bytes, dimension, terms);
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

  distance_terms_t terms;
  accumulate_distance_terms(
      query_values.data(), candidate.data(), req.dimension, &terms);

  switch (req.distance_metric) {
  case vector_distance_metric_euclidean:
    *distance = static_cast<float>(std::sqrt(terms.sum_sq));
    return 0;
  case vector_distance_metric_cosine:
    if (query_norm_squared == 0 || terms.candidate_norm_squared == 0) {
      *distance = std::numeric_limits<float>::infinity();
    } else {
      *distance = static_cast<float>(
          1.0 - terms.dot /
          (std::sqrt(query_norm_squared) *
           std::sqrt(terms.candidate_norm_squared)));
    }
    return 0;
  case vector_distance_metric_dot:
    *distance = static_cast<float>(-terms.dot);
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
    if (entry.entry_id.empty() ||
        !entry.has_vector_reference ||
        !entry.has_data_type ||
        !entry.has_distance_metric ||
        !entry.has_dimension) {
      return false;
    }
    // VectorNode-native rows never carry these (see is_vector_node_native);
    // every other source must have them.
    return entry.is_vector_node_native ||
      (!entry.bucket_name.empty() &&
       !entry.index_name.empty() &&
       !entry.user_key.empty());
  }

  bool matches_request(const vector_entry_view_t& entry)
  {
    // bucket_name/index_name/placement_key are unavailable for
    // VectorNode-native rows and redundant to check for them anyway (see
    // is_vector_node_native's comment).
    if (!entry.is_vector_node_native) {
      if (entry.bucket_name != req.bucket_name) {
        stats.bucket_mismatch++;
        return false;
      }
      if (entry.index_name != req.index_name) {
        stats.index_mismatch++;
        return false;
      }
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
    if (!entry.is_vector_node_native && !probe_matches(req, entry)) {
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

inline vector_entry_view_t make_vector_entry_view(
    const ceph::os::vector_record_t& entry)
{
  vector_entry_view_t view;
  view.entry_id = entry.entry_id;
  view.bucket_name = entry.bucket_name;
  view.index_name = entry.index_name;
  view.user_key = entry.user_key;
  view.placement_key = entry.placement_key;
  view.data_type = entry.data_type;
  view.distance_metric = entry.distance_metric;
  view.dimension = entry.dimension;
  view.has_data_type = true;
  view.has_distance_metric = true;
  view.has_dimension = true;
  view.has_vector_reference = true;
  view.vector_data = &entry.vector_data;
  return view;
}

} // namespace vector_query_exec
} // namespace rados
} // namespace ceph

#endif
