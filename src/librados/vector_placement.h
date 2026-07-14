// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRADOS_VECTOR_PLACEMENT_H
#define CEPH_LIBRADOS_VECTOR_PLACEMENT_H

#include <cstdint>
#include <cstdio>
#include <string>
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

inline std::string lsh_v0_placement_key(uint32_t table, uint32_t signature)
{
  return hex_u32(((table & 0xffffU) << 16) | (signature & 0xffffU));
}

} // namespace vector_placement
} // namespace librados

#endif
