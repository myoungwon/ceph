// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_RADOS_VECTOR_OPS_H
#define CEPH_RADOS_VECTOR_OPS_H

#include <cstddef>
#include <cstdint>
#include <errno.h>
#include <string>

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
  std::string bucket_name;
  std::string index_name;
  std::string key;
  uint32_t data_type = 0;
  uint32_t distance_metric = 0;
  uint32_t dimension = 0;
  ceph::bufferlist vector_data;
  ceph::bufferlist metadata;
  std::string placement_algorithm;
  std::string placement_key;
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

} // namespace rados
} // namespace ceph

WRITE_CLASS_ENCODER(ceph::rados::put_vector_request_t)

#endif
