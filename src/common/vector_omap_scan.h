// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_VECTOR_OMAP_SCAN_H
#define CEPH_COMMON_VECTOR_OMAP_SCAN_H

#include <map>
#include <string>
#include <string_view>

#include "include/buffer.h"
#include "common/vector_query_exec.h"

// A full-object OMAP scan of the _ENTRY_<entry_id>.<field> /
// _CONTENT_<vector_hash> key layout, for OSD backends that still store
// vector entries directly on the object's OMAP instead of a native
// VectorNode. vector_query_exec.h stays storage-agnostic; this header is
// for the OMAP caller only.

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
  std::string vector_hash;
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

inline ceph::bufferlist bufferlist_from_view(std::string_view value)
{
  ceph::bufferlist bl;
  bl.append(value.data(), value.size());
  return bl;
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
  } else if (field == "vector_hash") {
    decode_omap_string_value(bl, &entry.vector_hash);
  } else if (field == "data_type") {
    entry.has_data_type = decode_omap_value(bl, &entry.data_type);
  } else if (field == "distance_metric") {
    entry.has_distance_metric =
      decode_omap_value(bl, &entry.distance_metric);
  } else if (field == "dimension") {
    entry.has_dimension = decode_omap_value(bl, &entry.dimension);
  }
}

// Builds the storage-agnostic view an OMAP-scanned entry presents to
// local_query_accumulator_t::consume().
inline vector_entry_view_t make_omap_entry_view(
    const omap_entry_t& entry,
    const omap_scan_state_t& scan)
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
  view.has_data_type = entry.has_data_type;
  view.has_distance_metric = entry.has_distance_metric;
  view.has_dimension = entry.has_dimension;
  view.has_vector_reference = !entry.content_key.empty();
  if (view.has_vector_reference) {
    const auto content = scan.contents.find(entry.content_key);
    if (content != scan.contents.end()) {
      view.vector_data = &content->second;
    }
  }
  return view;
}

} // namespace vector_query_exec
} // namespace rados
} // namespace ceph

#endif
