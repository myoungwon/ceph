// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_VECTOR_QUERY_EXEC_H
#define CEPH_COMMON_VECTOR_QUERY_EXEC_H

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <map>
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

struct omap_entry_t {
  std::string entry_id;
  std::string bucket_name;
  std::string index_name;
  std::string user_key;
  std::string content_key;
  std::string placement_key;
  uint32_t data_type = 0;
  uint32_t distance_metric = 0;
  uint32_t dimension = 0;
  bool has_data_type = false;
  bool has_distance_metric = false;
  bool has_dimension = false;
};

struct omap_scan_state_t {
  std::map<std::string, omap_entry_t> entries;
  std::map<std::string, ceph::bufferlist> contents;
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

  if (req.placement_algorithm != vector_placement_algorithm_hash_v0 ||
      !is_hash_v0_placement_key(req.placement_key) ||
      !is_hash_v0_vector_hash(req.vector_hash)) {
    return -EINVAL;
  }
  return 0;
}

inline ceph::bufferlist bufferlist_from_view(std::string_view value)
{
  ceph::bufferlist bl;
  bl.append(value.data(), value.size());
  return bl;
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

inline bool parse_entry_key(std::string_view key,
                            std::string *entry_id,
                            std::string *field)
{
  constexpr std::string_view entry_prefix = "_ENTRY_";
  if (!starts_with(key, entry_prefix)) {
    return false;
  }

  const size_t dot_pos = key.find('.', entry_prefix.size());
  if (dot_pos == std::string_view::npos || dot_pos == entry_prefix.size() ||
      dot_pos + 1 >= key.size()) {
    return false;
  }

  *entry_id = std::string(
      key.substr(entry_prefix.size(), dot_pos - entry_prefix.size()));
  *field = std::string(key.substr(dot_pos + 1));
  return true;
}

inline void consume_omap_key_value(std::string_view key,
                                   std::string_view value,
                                   omap_scan_state_t& state)
{
  constexpr std::string_view content_prefix = "_CONTENT_";
  if (starts_with(key, content_prefix)) {
    state.contents[std::string(key)] = bufferlist_from_view(value);
    return;
  }

  std::string entry_id;
  std::string field;
  if (!parse_entry_key(key, &entry_id, &field)) {
    return;
  }

  ceph::bufferlist bl = bufferlist_from_view(value);
  auto& entry = state.entries[entry_id];
  entry.entry_id = entry_id;

  if (field == "bucket_name") {
    decode_omap_string_value(bl, &entry.bucket_name);
  } else if (field == "index_name") {
    decode_omap_string_value(bl, &entry.index_name);
  } else if (field == "user_key") {
    decode_omap_string_value(bl, &entry.user_key);
  } else if (field == "content_key") {
    decode_omap_string_value(bl, &entry.content_key);
  } else if (field == "placement_key") {
    decode_omap_string_value(bl, &entry.placement_key);
  } else if (field == "data_type") {
    entry.has_data_type = decode_omap_value(bl, &entry.data_type);
  } else if (field == "distance_metric") {
    entry.has_distance_metric =
      decode_omap_value(bl, &entry.distance_metric);
  } else if (field == "dimension") {
    entry.has_dimension = decode_omap_value(bl, &entry.dimension);
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
    if (!is_hash_v0_placement_key(prefix)) {
      return -EINVAL;
    }
  }
  return 0;
}

inline bool probe_matches(const query_vectors_request_t& req,
                          const omap_entry_t& entry)
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

inline bool entry_matches_query(const query_vectors_request_t& req,
                                const omap_entry_t& entry)
{
  return !entry.bucket_name.empty() &&
    !entry.index_name.empty() &&
    !entry.user_key.empty() &&
    !entry.content_key.empty() &&
    entry.has_data_type &&
    entry.has_distance_metric &&
    entry.has_dimension &&
    entry.bucket_name == req.bucket_name &&
    entry.index_name == req.index_name &&
    entry.data_type == req.data_type &&
    entry.distance_metric == req.distance_metric &&
    entry.dimension == req.dimension &&
    probe_matches(req, entry);
}

inline float read_float32(const char *data, size_t index)
{
  float value = 0;
  std::memcpy(&value, data + index * sizeof(float), sizeof(float));
  return value;
}

inline void copy_bufferlist_bytes(const ceph::bufferlist& bl,
                                  char *out,
                                  size_t len)
{
  auto p = bl.cbegin();
  p.copy(len, out);
}

inline int compute_float32_distance(const query_vectors_request_t& req,
                                    const ceph::bufferlist& candidate,
                                    float *distance)
{
  if (distance == nullptr) {
    return -EINVAL;
  }
  const size_t expected_len =
    static_cast<size_t>(req.dimension) * sizeof(float);
  if (req.data_type != vector_data_type_float32 ||
      req.query_vector.length() != expected_len ||
      candidate.length() != expected_len) {
    return -EINVAL;
  }

  std::vector<char> query_data(expected_len);
  std::vector<char> candidate_data(expected_len);
  copy_bufferlist_bytes(req.query_vector, query_data.data(), expected_len);
  copy_bufferlist_bytes(candidate, candidate_data.data(), expected_len);
  double dot = 0;
  double query_norm = 0;
  double candidate_norm = 0;
  double sum_sq = 0;
  for (uint32_t i = 0; i < req.dimension; ++i) {
    const double q = read_float32(query_data.data(), i);
    const double c = read_float32(candidate_data.data(), i);
    dot += q * c;
    query_norm += q * q;
    candidate_norm += c * c;
    const double diff = q - c;
    sum_sq += diff * diff;
  }

  switch (req.distance_metric) {
  case vector_distance_metric_euclidean:
    *distance = static_cast<float>(std::sqrt(sum_sq));
    return 0;
  case vector_distance_metric_cosine:
    if (query_norm == 0 || candidate_norm == 0) {
      *distance = std::numeric_limits<float>::infinity();
    } else {
      *distance = static_cast<float>(
          1.0 - dot / (std::sqrt(query_norm) * std::sqrt(candidate_norm)));
    }
    return 0;
  case vector_distance_metric_dot:
    *distance = static_cast<float>(-dot);
    return 0;
  default:
    return -EINVAL;
  }
}

inline const std::string& result_entry_identity(
    const query_vectors_result_entry_t& entry)
{
  return entry.entry_id.empty() ? entry.key : entry.entry_id;
}

inline bool result_entry_better(const query_vectors_result_entry_t& lhs,
                                const query_vectors_result_entry_t& rhs)
{
  if (lhs.distance != rhs.distance) {
    return lhs.distance < rhs.distance;
  }
  return lhs.key < rhs.key;
}

inline void merge_result_entry(std::vector<query_vectors_result_entry_t> *entries,
                               const query_vectors_result_entry_t& candidate)
{
  if (entries == nullptr) {
    return;
  }
  const std::string& candidate_id = result_entry_identity(candidate);
  for (auto& entry : *entries) {
    if (result_entry_identity(entry) == candidate_id) {
      if (result_entry_better(candidate, entry)) {
        entry = candidate;
      }
      return;
    }
  }
  entries->push_back(candidate);
}

inline void sort_and_trim_results(std::vector<query_vectors_result_entry_t> *entries,
                                  uint32_t top_k)
{
  if (entries == nullptr) {
    return;
  }
  std::sort(entries->begin(), entries->end(), result_entry_better);
  if (top_k != 0 && entries->size() > top_k) {
    entries->resize(top_k);
  }
}

inline int build_local_results(const query_vectors_request_t& req,
                               const omap_scan_state_t& scan,
                               query_vectors_result_t *result)
{
  if (result == nullptr) {
    return -EINVAL;
  }
  result->entries.clear();

  int r = validate_query_request(req);
  if (r < 0) {
    return r;
  }

  for (const auto& [entry_id, entry] : scan.entries) {
    (void)entry_id;
    if (!entry_matches_query(req, entry)) {
      continue;
    }
    const auto content = scan.contents.find(entry.content_key);
    if (content == scan.contents.end()) {
      continue;
    }

    float distance = 0;
    r = compute_float32_distance(req, content->second, &distance);
    if (r < 0) {
      continue;
    }

    query_vectors_result_entry_t result_entry;
    result_entry.key = entry.user_key;
    result_entry.distance = distance;
    result_entry.entry_id = entry.entry_id;
    merge_result_entry(&result->entries, result_entry);
  }

  sort_and_trim_results(&result->entries, req.local_top_k);
  return 0;
}

} // namespace vector_query_exec
} // namespace rados
} // namespace ceph

#endif
