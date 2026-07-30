// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cerrno>
#include <cstdio>
#include <limits>
#include <map>
#include <string>
#include <string_view>

#include "include/ceph_hash.h"
#include "include/encoding.h"
#include "include/rados/vector_ops.h"

namespace ceph::os {

inline constexpr uint32_t vector_record_format_version = 1;

struct vector_record_t {
  uint32_t format_version = vector_record_format_version;
  std::string entry_id;
  std::string bucket_name;
  std::string index_name;
  std::string user_key;
  uint32_t data_type = 0;
  uint32_t distance_metric = 0;
  uint32_t dimension = 0;
  std::string placement_algorithm;
  std::string placement_key;
  std::string vector_hash;
  ceph::bufferlist metadata;
  ceph::bufferlist vector_data;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(format_version, bl);
    encode(entry_id, bl);
    encode(bucket_name, bl);
    encode(index_name, bl);
    encode(user_key, bl);
    encode(data_type, bl);
    encode(distance_metric, bl);
    encode(dimension, bl);
    encode(placement_algorithm, bl);
    encode(placement_key, bl);
    encode(vector_hash, bl);
    encode(metadata, bl);
    encode(vector_data, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(format_version, p);
    decode(entry_id, p);
    decode(bucket_name, p);
    decode(index_name, p);
    decode(user_key, p);
    decode(data_type, p);
    decode(distance_metric, p);
    decode(dimension, p);
    decode(placement_algorithm, p);
    decode(placement_key, p);
    decode(vector_hash, p);
    decode(metadata, p);
    decode(vector_data, p);
    DECODE_FINISH(p);
  }
};

inline bool is_lower_hex_string(std::string_view value, size_t length)
{
  if (value.size() != length) {
    return false;
  }
  for (char c : value) {
    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
      return false;
    }
  }
  return true;
}

inline bool valid_vector_placement(const vector_record_t& record)
{
  using namespace ceph::rados;
  if (record.placement_algorithm == vector_placement_algorithm_hash_v0) {
    return is_lower_hex_string(
        record.placement_key, vector_hash_v0_placement_key_len);
  }
  if (record.placement_algorithm == vector_placement_algorithm_lsh_v0) {
    return is_lower_hex_string(
        record.placement_key, vector_lsh_v0_placement_key_len);
  }
  if (record.placement_algorithm == vector_placement_algorithm_pg_lsh_v0) {
    return is_lower_hex_string(
        record.placement_key, vector_hash_v0_vector_hash_len);
  }
  return false;
}

inline int validate_vector_record(const vector_record_t& record)
{
  if (record.format_version != vector_record_format_version ||
      !is_lower_hex_string(record.entry_id, 8) ||
      record.bucket_name.empty() ||
      record.index_name.empty() ||
      record.user_key.empty() ||
      record.dimension == 0 ||
      record.dimension > ceph::rados::vector_max_dimension ||
      !ceph::rados::vector_distance_metric_supported(
          record.distance_metric) ||
      !valid_vector_placement(record) ||
      !is_lower_hex_string(
          record.vector_hash,
          ceph::rados::vector_hash_v0_vector_hash_len)) {
    return -EINVAL;
  }

  size_t element_size = 0;
  if (ceph::rados::vector_data_type_size(
          record.data_type, &element_size) < 0) {
    return -EINVAL;
  }
  if (record.dimension >
      std::numeric_limits<size_t>::max() / element_size) {
    return -EOVERFLOW;
  }
  const size_t expected_length =
    static_cast<size_t>(record.dimension) * element_size;
  return record.vector_data.length() == expected_length ? 0 : -EINVAL;
}

inline std::string vector_entry_id(
    const ceph::rados::put_vector_request_t& req)
{
  std::string value;
  value.reserve(req.bucket_name.length() + req.index_name.length() +
                req.key.length() + 2);
  value.append(req.bucket_name);
  value.push_back('\0');
  value.append(req.index_name);
  value.push_back('\0');
  value.append(req.key);

  char id[9];
  std::snprintf(
      id, sizeof(id), "%08x",
      ceph_str_hash_rjenkins(
          value.c_str(), static_cast<unsigned>(value.length())));
  return id;
}

inline vector_record_t make_vector_record(
    const ceph::rados::put_vector_request_t& req)
{
  vector_record_t record;
  record.entry_id = vector_entry_id(req);
  record.bucket_name = req.bucket_name;
  record.index_name = req.index_name;
  record.user_key = req.key;
  record.data_type = req.data_type;
  record.distance_metric = req.distance_metric;
  record.dimension = req.dimension;
  record.placement_algorithm = req.placement_algorithm;
  record.placement_key = req.placement_key;
  record.vector_hash = req.vector_hash;
  record.metadata = req.metadata;
  record.vector_data = req.vector_data;
  return record;
}

template <typename T>
inline void set_vector_omap_value(
    std::map<std::string, ceph::bufferlist> *omap,
    const std::string& key,
    const T& value)
{
  ceph::bufferlist bl;
  using ceph::encode;
  encode(value, bl);
  (*omap)[key] = std::move(bl);
}

inline std::map<std::string, ceph::bufferlist> make_vector_omap(
    const vector_record_t& record)
{
  const std::string content_key = "_CONTENT_" + record.vector_hash;
  const std::string entry_prefix = "_ENTRY_" + record.entry_id + ".";
  std::map<std::string, ceph::bufferlist> omap;

  omap[content_key] = record.vector_data;
  set_vector_omap_value(
      &omap, entry_prefix + "bucket_name", record.bucket_name);
  set_vector_omap_value(
      &omap, entry_prefix + "index_name", record.index_name);
  set_vector_omap_value(
      &omap, entry_prefix + "user_key", record.user_key);
  set_vector_omap_value(&omap, entry_prefix + "content_key", content_key);
  set_vector_omap_value(
      &omap, entry_prefix + "data_type", record.data_type);
  set_vector_omap_value(
      &omap, entry_prefix + "distance_metric", record.distance_metric);
  set_vector_omap_value(
      &omap, entry_prefix + "dimension", record.dimension);
  set_vector_omap_value(
      &omap, entry_prefix + "layout_version",
      ceph::rados::vector_layout_version);
  set_vector_omap_value(
      &omap, entry_prefix + "placement_algorithm",
      record.placement_algorithm);
  set_vector_omap_value(
      &omap, entry_prefix + "placement_key", record.placement_key);
  set_vector_omap_value(
      &omap, entry_prefix + "vector_hash", record.vector_hash);
  if (record.metadata.length() > 0) {
    omap[entry_prefix + "metadata"] = record.metadata;
  }
  return omap;
}

} // namespace ceph::os
