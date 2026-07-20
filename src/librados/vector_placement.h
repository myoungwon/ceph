// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRADOS_VECTOR_PLACEMENT_H
#define CEPH_LIBRADOS_VECTOR_PLACEMENT_H

#include <cstdint>
#include <cstdio>
#include <algorithm>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "include/buffer.h"
#include "include/ceph_hash.h"
#include "include/object.h"
#include "include/rados/vector_ops.h"

namespace librados {
namespace vector_placement {

inline constexpr const char *hash_v0_algorithm =
  ceph::rados::vector_placement_algorithm_hash_v0;
inline constexpr const char *lsh_v0_algorithm =
  ceph::rados::vector_placement_algorithm_lsh_v0;
inline constexpr const char *pg_lsh_v0_algorithm =
  ceph::rados::vector_placement_algorithm_pg_lsh_v0;

struct hash_v0_placement_t {
  object_t oid;
  std::string placement_key;
  std::string vector_hash;
};

inline std::string hex_u32(uint32_t value)
{
  char buf[9];
  std::snprintf(buf, sizeof(buf), "%08x", value);
  return std::string(buf);
}

inline uint32_t mix_u32(uint32_t value)
{
  value ^= value >> 16;
  value *= 0x7feb352dU;
  value ^= value >> 15;
  value *= 0x846ca68bU;
  value ^= value >> 16;
  return value;
}

inline std::string hash_string(const std::string& value)
{
  return hex_u32(ceph_str_hash_rjenkins(
      value.c_str(), static_cast<unsigned>(value.length())));
}

inline uint32_t hash_to_u32(const std::string& value)
{
  return ceph_str_hash_rjenkins(
      value.c_str(), static_cast<unsigned>(value.length()));
}

inline std::string hash_v0_vector_hash(const ceph::bufferlist& vector_data)
{
  return hex_u32(vector_data.crc32c(static_cast<uint32_t>(-1)));
}

inline std::string hash_v0_placement_key(const std::string& vector_hash)
{
  return vector_hash.substr(0, 4);
}

inline object_t make_algorithm_oid(const std::string& placement_algorithm,
                                   const std::string& bucket_name,
                                   const std::string& index_name,
                                   const std::string& placement_key)
{
  return object_t(
      ".rados.vector/v1/" + placement_algorithm + "/" +
      hash_string(bucket_name) + "/" + hash_string(index_name) + "/" +
      placement_key);
}

inline object_t make_hash_v0_oid(const std::string& bucket_name,
                                 const std::string& index_name,
                                 const std::string& placement_key)
{
  return make_algorithm_oid(
      hash_v0_algorithm, bucket_name, index_name, placement_key);
}

inline object_t make_lsh_v0_oid(const std::string& bucket_name,
                                const std::string& index_name,
                                const std::string& placement_key)
{
  return make_algorithm_oid(
      lsh_v0_algorithm, bucket_name, index_name, placement_key);
}

inline object_t make_pg_lsh_v0_oid(const std::string& bucket_name,
                                   const std::string& index_name,
                                   uint32_t pg)
{
  return object_t(
      ".rados.vector/v1/" + std::string(pg_lsh_v0_algorithm) + "/" +
      hash_string(bucket_name) + "/" + hash_string(index_name) + "/pg_" +
      std::to_string(pg));
}

inline hash_v0_placement_t compute_hash_v0_placement(
    const std::string& bucket_name,
    const std::string& index_name,
    const ceph::bufferlist& vector_data)
{
  const std::string vector_hash = hash_v0_vector_hash(vector_data);
  const std::string placement_key = hash_v0_placement_key(vector_hash);
  return {
    make_hash_v0_oid(bucket_name, index_name, placement_key),
    placement_key,
    vector_hash,
  };
}

inline std::string ranked_hash_v0_placement_key(
    const std::string& vector_hash,
    uint32_t rank)
{
  if (rank == 0) {
    return hash_v0_placement_key(vector_hash);
  }
  std::string value = vector_hash;
  value.push_back('\0');
  value.append(std::to_string(rank));
  return hash_v0_placement_key(hex_u32(hash_to_u32(value)));
}

inline int copy_float32_vector(const ceph::bufferlist& vector_data,
                               uint32_t dimension,
                               std::vector<float> *values)
{
  if (values == nullptr || dimension == 0) {
    return -EINVAL;
  }
  const size_t expected_len = static_cast<size_t>(dimension) * sizeof(float);
  if (vector_data.length() != expected_len) {
    return -EINVAL;
  }
  values->resize(dimension);
  auto p = vector_data.cbegin();
  p.copy(expected_len, reinterpret_cast<char*>(values->data()));
  return 0;
}

inline int lsh_v0_hyperplane_sign(uint32_t table,
                                  uint32_t bit,
                                  uint32_t dimension)
{
  const uint32_t seed =
    table * 0x9e3779b9U ^ bit * 0x85ebca6bU ^ dimension * 0xc2b2ae35U;
  return (mix_u32(seed) & 1U) == 0U ? -1 : 1;
}

inline int pg_lsh_v0_hyperplane_sign(uint32_t seed,
                                     uint32_t table,
                                     uint32_t bit,
                                     uint32_t dimension)
{
  const uint32_t mixed_seed =
    seed ^ table * 0x9e3779b9U ^ bit * 0x85ebca6bU ^
    dimension * 0xc2b2ae35U;
  return (mix_u32(mixed_seed) & 1U) == 0U ? -1 : 1;
}

inline uint32_t lsh_v0_signature(
    const std::vector<float>& values,
    uint32_t table,
    uint32_t signature_bits = ceph::rados::vector_lsh_v0_bits)
{
  if (signature_bits == 0) {
    signature_bits = ceph::rados::vector_lsh_v0_bits;
  }
  if (signature_bits > ceph::rados::vector_lsh_v0_max_bits) {
    signature_bits = ceph::rados::vector_lsh_v0_max_bits;
  }

  uint32_t signature = 0;
  for (uint32_t bit = 0; bit < signature_bits; ++bit) {
    double projection = 0;
    for (uint32_t dim = 0; dim < values.size(); ++dim) {
      projection += static_cast<double>(values[dim]) *
        lsh_v0_hyperplane_sign(table, bit, dim);
    }
    if (projection >= 0) {
      signature |= 1U << bit;
    }
  }
  return signature;
}

inline uint32_t pg_lsh_v0_lsh_bucket_id(
    const std::vector<float>& values,
    uint32_t table,
    uint32_t lsh_bucket_id_bits,
    uint32_t seed)
{
  if (lsh_bucket_id_bits == 0) {
    lsh_bucket_id_bits = ceph::rados::vector_lsh_v0_bits;
  }
  if (lsh_bucket_id_bits > ceph::rados::vector_lsh_v0_max_bits) {
    lsh_bucket_id_bits = ceph::rados::vector_lsh_v0_max_bits;
  }

  uint32_t lsh_bucket_id = 0;
  for (uint32_t bit = 0; bit < lsh_bucket_id_bits; ++bit) {
    double projection = 0;
    for (uint32_t dim = 0; dim < values.size(); ++dim) {
      projection += static_cast<double>(values[dim]) *
        pg_lsh_v0_hyperplane_sign(seed, table, bit, dim);
    }
    if (projection >= 0) {
      lsh_bucket_id |= 1U << bit;
    }
  }
  return lsh_bucket_id;
}

inline std::string lsh_v0_placement_key(uint32_t table, uint32_t signature)
{
  return hex_u32(((table & 0xffffU) << 16) | (signature & 0xffffU));
}

inline std::string pg_lsh_v0_placement_key(uint32_t pg)
{
  return hex_u32(pg);
}

inline uint32_t pg_lsh_v0_group_to_pg(uint32_t table,
                                      uint32_t lsh_bucket_id,
                                      uint32_t pg_num,
                                      uint32_t seed)
{
  if (pg_num == 0) {
    return 0;
  }
  const uint32_t h = mix_u32(seed ^ table * 0x9e3779b9U ^
                            lsh_bucket_id * 0x85ebca6bU);
  return h % pg_num;
}

struct pg_lsh_v0_group_t {
  uint32_t table = 0;
  uint32_t lsh_bucket_id = 0;
  uint32_t hamming_distance = 0;
};

inline void pg_lsh_v0_append_hamming_masks(uint32_t lsh_bucket_id_bits,
                                           uint32_t start_bit,
                                           uint32_t remaining,
                                           uint32_t mask,
                                           std::vector<uint32_t> *masks)
{
  if (remaining == 0) {
    masks->push_back(mask);
    return;
  }
  for (uint32_t bit = start_bit; bit < lsh_bucket_id_bits; ++bit) {
    pg_lsh_v0_append_hamming_masks(
        lsh_bucket_id_bits, bit + 1, remaining - 1,
        mask | (1U << bit), masks);
  }
}

inline std::vector<uint32_t> pg_lsh_v0_hamming_masks(
    uint32_t lsh_bucket_id_bits,
    uint32_t radius)
{
  std::vector<uint32_t> masks;
  if (lsh_bucket_id_bits > ceph::rados::vector_lsh_v0_max_bits) {
    lsh_bucket_id_bits = ceph::rados::vector_lsh_v0_max_bits;
  }
  if (radius > lsh_bucket_id_bits) {
    radius = lsh_bucket_id_bits;
  }
  masks.push_back(0);
  for (uint32_t distance = 1; distance <= radius; ++distance) {
    pg_lsh_v0_append_hamming_masks(
        lsh_bucket_id_bits, 0, distance, 0, &masks);
  }
  return masks;
}

inline std::vector<uint32_t> pg_lsh_v0_hamming_masks_at_distance(
    uint32_t lsh_bucket_id_bits,
    uint32_t distance)
{
  std::vector<uint32_t> masks;
  if (lsh_bucket_id_bits > ceph::rados::vector_lsh_v0_max_bits) {
    lsh_bucket_id_bits = ceph::rados::vector_lsh_v0_max_bits;
  }
  if (distance > lsh_bucket_id_bits) {
    return masks;
  }
  if (distance == 0) {
    masks.push_back(0);
    return masks;
  }
  pg_lsh_v0_append_hamming_masks(
      lsh_bucket_id_bits, 0, distance, 0, &masks);
  return masks;
}

inline int pg_lsh_v0_exact_groups(const ceph::bufferlist& vector_data,
                                  uint32_t dimension,
                                  uint32_t lsh_bucket_id_bits,
                                  uint32_t table_count,
                                  uint32_t seed,
                                  std::vector<pg_lsh_v0_group_t> *groups)
{
  if (groups == nullptr || table_count == 0 || table_count > 0xffffU) {
    return -EINVAL;
  }
  std::vector<float> values;
  int r = copy_float32_vector(vector_data, dimension, &values);
  if (r < 0) {
    return r;
  }
  groups->clear();
  groups->reserve(table_count);
  for (uint32_t table = 0; table < table_count; ++table) {
    groups->push_back({
      table,
      pg_lsh_v0_lsh_bucket_id(values, table, lsh_bucket_id_bits, seed),
      0,
    });
  }
  return 0;
}

inline int pg_lsh_v0_query_groups(const ceph::bufferlist& query_vector,
                                  uint32_t dimension,
                                  uint32_t lsh_bucket_id_bits,
                                  uint32_t table_count,
                                  uint32_t hamming_radius,
                                  uint32_t seed,
                                  std::vector<pg_lsh_v0_group_t> *groups)
{
  if (groups == nullptr || table_count == 0 || table_count > 0xffffU) {
    return -EINVAL;
  }
  std::vector<pg_lsh_v0_group_t> exact;
  int r = pg_lsh_v0_exact_groups(
      query_vector, dimension, lsh_bucket_id_bits, table_count, seed, &exact);
  if (r < 0) {
    return r;
  }
  groups->clear();
  groups->reserve(
      exact.size() * pg_lsh_v0_hamming_masks(lsh_bucket_id_bits,
                                            hamming_radius).size());
  if (lsh_bucket_id_bits > ceph::rados::vector_lsh_v0_max_bits) {
    lsh_bucket_id_bits = ceph::rados::vector_lsh_v0_max_bits;
  }
  if (hamming_radius > lsh_bucket_id_bits) {
    hamming_radius = lsh_bucket_id_bits;
  }
  for (uint32_t distance = 0; distance <= hamming_radius; ++distance) {
    const auto masks =
      pg_lsh_v0_hamming_masks_at_distance(lsh_bucket_id_bits, distance);
    for (const auto& group : exact) {
      for (const uint32_t mask : masks) {
        groups->push_back({
          group.table,
          group.lsh_bucket_id ^ mask,
          distance,
        });
      }
    }
  }
  return 0;
}

struct pg_lsh_v0_ranked_pg_t {
  uint32_t pg = 0;
  uint32_t min_hamming_distance = 0;
  uint32_t table_votes = 0;
};

inline std::vector<pg_lsh_v0_ranked_pg_t> pg_lsh_v0_select_unique_pgs(
    const std::vector<pg_lsh_v0_group_t>& groups,
    uint32_t pg_num,
    uint32_t seed,
    uint32_t budget)
{
  std::vector<pg_lsh_v0_ranked_pg_t> out;
  if (pg_num == 0 || budget == 0) {
    return out;
  }

  std::unordered_set<uint32_t> seen;
  out.reserve(std::min<size_t>(groups.size(), budget));
  for (const auto& group : groups) {
    const uint32_t pg = pg_lsh_v0_group_to_pg(
        group.table, group.lsh_bucket_id, pg_num, seed);
    if (!seen.insert(pg).second) {
      continue;
    }
    out.push_back({
      pg,
      group.hamming_distance,
      1,
    });
    if (out.size() == budget) {
      break;
    }
  }
  return out;
}

inline std::vector<uint32_t> pg_lsh_v0_select_write_pgs(
    const std::vector<pg_lsh_v0_group_t>& exact_groups,
    uint32_t pg_num,
    uint32_t seed,
    uint32_t write_pg_count)
{
  std::vector<uint32_t> out;
  out.reserve(write_pg_count);
  const auto selected = pg_lsh_v0_select_unique_pgs(
      exact_groups, pg_num, seed, write_pg_count);
  for (const auto& candidate : selected) {
    out.push_back(candidate.pg);
  }
  return out;
}

} // namespace vector_placement
} // namespace librados

#endif
