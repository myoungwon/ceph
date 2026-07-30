// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRADOS_VECTOR_PG_LSH_H
#define CEPH_LIBRADOS_VECTOR_PG_LSH_H

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <errno.h>

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/object.h"
#include "include/rados/librados.hpp"
#include "include/rados/vector_ops.h"
#include "librados/vector_put.h"
#include "librados/vector_placement.h"

namespace librados {
inline namespace v14_2_0 {
namespace vector_pg_lsh {

struct pool_pg_info_t {
  uint32_t pg_num = 0;
  uint32_t pgp_num = 0;
  uint64_t osdmap_epoch = 0;
};

inline constexpr uint32_t index_config_format_version = 1;
// "v0" identifies the first immutable PG-LSH placement/routing layout. A
// future incompatible mapping must use a new layout version and algorithm ID.
inline constexpr uint32_t pg_lsh_layout_version = 0;

inline constexpr uint32_t anchor_mode_random = 1;
inline constexpr uint32_t anchor_mode_centroid = 2;
inline constexpr uint32_t anchor_mode_representative = 3;

// Immutable placement state. This object is encoded into the index metadata
// object and owns its anchor so put and query never regenerate routing state.
struct index_config_t {
  uint32_t config_format_version = index_config_format_version;
  std::string placement_algorithm =
    ceph::rados::vector_placement_algorithm_pg_lsh_v0;
  uint32_t placement_layout_version = pg_lsh_layout_version;
  uint32_t dimension = 0;
  uint32_t data_type = 0;
  uint32_t distance_metric = 0;
  uint32_t k = 0;
  uint32_t l = 0;
  uint32_t seed = 0;
  uint32_t d = 0;
  // pg-lsh-v0 maps LSH groups directly into the pool's PG number space.
  // Changing either value is unsupported because it changes existing routes.
  uint32_t creation_pg_num = 0;
  uint32_t creation_pgp_num = 0;
  uint32_t distance_bucket_bits = 0;
  uint32_t residual_bits = 0;
  uint32_t anchor_mode = 0;
  std::vector<double> anchor;

  void encode(ceph::bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    using ceph::encode;
    encode(config_format_version, bl);
    encode(placement_algorithm, bl);
    encode(placement_layout_version, bl);
    encode(dimension, bl);
    encode(data_type, bl);
    encode(distance_metric, bl);
    encode(k, bl);
    encode(l, bl);
    encode(seed, bl);
    encode(d, bl);
    encode(creation_pg_num, bl);
    encode(creation_pgp_num, bl);
    encode(distance_bucket_bits, bl);
    encode(residual_bits, bl);
    encode(anchor_mode, bl);
    encode(anchor, bl);
    ENCODE_FINISH(bl);
  }

  void decode(ceph::bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    using ceph::decode;
    decode(config_format_version, p);
    decode(placement_algorithm, p);
    decode(placement_layout_version, p);
    decode(dimension, p);
    decode(data_type, p);
    decode(distance_metric, p);
    decode(k, p);
    decode(l, p);
    decode(seed, p);
    decode(d, p);
    decode(creation_pg_num, p);
    decode(creation_pgp_num, p);
    decode(distance_bucket_bits, p);
    decode(residual_bits, p);
    decode(anchor_mode, p);
    decode(anchor, p);
    DECODE_FINISH(p);
  }
};

// Query-time tuning changes search volume only; it must not affect placement.
struct query_params_t {
  uint32_t hamming_radius = 0;
  uint32_t m = 0;
  uint32_t distance_bucket_radius = 0;
  uint32_t residual_hamming_radius = 0;
  uint32_t probe_limit_per_pg = 0;
};

// Query callers normally provide no immutable overrides. These optionals are
// only for validating fields that a CLI user explicitly supplied.
struct index_config_overrides_t {
  std::optional<uint32_t> k;
  std::optional<uint32_t> l;
  std::optional<uint32_t> seed;
  std::optional<uint32_t> d;
  std::optional<uint32_t> distance_bucket_bits;
  std::optional<uint32_t> residual_bits;
  std::optional<uint32_t> anchor_mode;
};

struct locator_state_t {
  std::mutex lock;
  std::unordered_map<uint32_t, std::string> cache;
  uint32_t next_salt = 0;
};

struct put_target_t {
  uint32_t pg = 0;
  object_t oid;
  std::string locator_key;
  std::string placement_key;
  std::string vector_hash;
  std::string sub_oid_name;
};

struct query_probe_t {
  uint32_t pg = 0;
  uint32_t min_hamming_distance = 0;
  uint32_t table_vote_count = 0;
  object_t oid;
  std::string locator_key;
  std::string placement_key;
  std::string sub_oid_name;
};

struct query_op_state_t {
  v14_2_0::IoCtx routed_ioctx;
  ::ObjectOperation op;
  ceph::bufferlist payload;
};

using put_op_state_t = vector_internal::put_op_state_t;

inline void set_mismatch_field(std::string *mismatch_field,
                               const char *field)
{
  if (mismatch_field != nullptr) {
    *mismatch_field = field;
  }
}

inline int validate_index_config(const index_config_t& config,
                                 const pool_pg_info_t& pool_info,
                                 std::string *invalid_field = nullptr)
{
  if (invalid_field != nullptr) {
    invalid_field->clear();
  }
  if (config.config_format_version != index_config_format_version) {
    set_mismatch_field(invalid_field, "config_format_version");
    return -EINVAL;
  }
  if (config.placement_algorithm !=
      ceph::rados::vector_placement_algorithm_pg_lsh_v0) {
    set_mismatch_field(invalid_field, "placement_algorithm");
    return -EINVAL;
  }
  if (config.placement_layout_version != pg_lsh_layout_version) {
    set_mismatch_field(invalid_field, "placement_layout_version");
    return -EINVAL;
  }
  if (config.dimension == 0) {
    set_mismatch_field(invalid_field, "dimension");
    return -EINVAL;
  }
  size_t element_size = 0;
  if (ceph::rados::vector_data_type_size(
        config.data_type, &element_size) < 0) {
    set_mismatch_field(invalid_field, "data_type");
    return -EINVAL;
  }
  if (config.distance_metric != ceph::rados::vector_distance_metric_euclidean &&
      config.distance_metric != ceph::rados::vector_distance_metric_cosine &&
      config.distance_metric != ceph::rados::vector_distance_metric_dot) {
    set_mismatch_field(invalid_field, "distance_metric");
    return -EINVAL;
  }
  if (config.k == 0 ||
      config.k > ceph::rados::vector_lsh_v0_max_bits) {
    set_mismatch_field(invalid_field, "k");
    return -EINVAL;
  }
  if (config.l == 0 || config.l > 0xffffU) {
    set_mismatch_field(invalid_field, "l");
    return -EINVAL;
  }
  if (config.d == 0 || config.d > config.l) {
    set_mismatch_field(invalid_field, "d");
    return -EINVAL;
  }
  if (config.creation_pg_num == 0 ||
      config.creation_pg_num != pool_info.pg_num) {
    set_mismatch_field(invalid_field, "pg_num");
    return -EINVAL;
  }
  if (config.creation_pgp_num == 0 ||
      config.creation_pgp_num != pool_info.pgp_num) {
    set_mismatch_field(invalid_field, "pgp_num");
    return -EINVAL;
  }
  if (config.distance_bucket_bits > 16) {
    set_mismatch_field(invalid_field, "distance_bucket_bits");
    return -EINVAL;
  }
  if (config.residual_bits > 16) {
    set_mismatch_field(invalid_field, "residual_bits");
    return -EINVAL;
  }
  if (config.anchor_mode != anchor_mode_random &&
      config.anchor_mode != anchor_mode_centroid &&
      config.anchor_mode != anchor_mode_representative) {
    set_mismatch_field(invalid_field, "anchor_mode");
    return -EINVAL;
  }
  if (config.anchor.size() != config.dimension) {
    set_mismatch_field(invalid_field, "anchor");
    return -EINVAL;
  }
  double anchor_norm_squared = 0;
  for (const double component : config.anchor) {
    if (!std::isfinite(component)) {
      set_mismatch_field(invalid_field, "anchor");
      return -EINVAL;
    }
    anchor_norm_squared += component * component;
  }
  if (anchor_norm_squared <= 0) {
    set_mismatch_field(invalid_field, "anchor");
    return -EINVAL;
  }
  if (config.d > pool_info.pg_num) {
    set_mismatch_field(invalid_field, "d");
    return -EINVAL;
  }
  return 0;
}

inline int validate_query_params(const index_config_t& config,
                                 const query_params_t& query_params,
                                 const pool_pg_info_t& pool_info,
                                 std::string *invalid_field = nullptr)
{
  int ret = validate_index_config(config, pool_info, invalid_field);
  if (ret < 0) {
    return ret;
  }
  if (query_params.hamming_radius > config.k) {
    set_mismatch_field(invalid_field, "hamming_radius");
    return -EINVAL;
  }
  if (query_params.m == 0 || query_params.m > pool_info.pg_num) {
    set_mismatch_field(invalid_field, "m");
    return -EINVAL;
  }
  if (query_params.residual_hamming_radius > config.residual_bits) {
    set_mismatch_field(invalid_field, "residual_hamming_radius");
    return -EINVAL;
  }
  if (config.distance_bucket_bits == 0 &&
      query_params.distance_bucket_radius != 0) {
    set_mismatch_field(invalid_field, "distance_bucket_radius");
    return -EINVAL;
  }
  if (!vector_placement::pg_lsh_v0::sub_oid_enabled(
        config.distance_bucket_bits, config.residual_bits) &&
      query_params.probe_limit_per_pg != 0) {
    set_mismatch_field(invalid_field, "probe_limit_per_pg");
    return -EINVAL;
  }
  return 0;
}

inline int compare_index_configs(const index_config_t& requested,
                                 const index_config_t& stored,
                                 std::string *mismatch_field)
{
#define CHECK_INDEX_CONFIG_FIELD(field) \
  do { \
    if (requested.field != stored.field) { \
      set_mismatch_field(mismatch_field, #field); \
      return -EINVAL; \
    } \
  } while (false)
  CHECK_INDEX_CONFIG_FIELD(config_format_version);
  CHECK_INDEX_CONFIG_FIELD(placement_algorithm);
  CHECK_INDEX_CONFIG_FIELD(placement_layout_version);
  CHECK_INDEX_CONFIG_FIELD(dimension);
  CHECK_INDEX_CONFIG_FIELD(data_type);
  CHECK_INDEX_CONFIG_FIELD(distance_metric);
  CHECK_INDEX_CONFIG_FIELD(k);
  CHECK_INDEX_CONFIG_FIELD(l);
  CHECK_INDEX_CONFIG_FIELD(seed);
  CHECK_INDEX_CONFIG_FIELD(d);
  if (requested.creation_pg_num != stored.creation_pg_num) {
    set_mismatch_field(mismatch_field, "pg_num");
    return -EINVAL;
  }
  if (requested.creation_pgp_num != stored.creation_pgp_num) {
    set_mismatch_field(mismatch_field, "pgp_num");
    return -EINVAL;
  }
  CHECK_INDEX_CONFIG_FIELD(distance_bucket_bits);
  CHECK_INDEX_CONFIG_FIELD(residual_bits);
  CHECK_INDEX_CONFIG_FIELD(anchor_mode);
  CHECK_INDEX_CONFIG_FIELD(anchor);
#undef CHECK_INDEX_CONFIG_FIELD
  if (mismatch_field != nullptr) {
    mismatch_field->clear();
  }
  return 0;
}

inline int validate_index_config_overrides(
    const index_config_overrides_t& overrides,
    const index_config_t& stored,
    std::string *mismatch_field)
{
#define CHECK_INDEX_CONFIG_OVERRIDE(field) \
  do { \
    if (overrides.field && *overrides.field != stored.field) { \
      set_mismatch_field(mismatch_field, #field); \
      return -EINVAL; \
    } \
  } while (false)
  CHECK_INDEX_CONFIG_OVERRIDE(k);
  CHECK_INDEX_CONFIG_OVERRIDE(l);
  CHECK_INDEX_CONFIG_OVERRIDE(seed);
  CHECK_INDEX_CONFIG_OVERRIDE(d);
  CHECK_INDEX_CONFIG_OVERRIDE(distance_bucket_bits);
  CHECK_INDEX_CONFIG_OVERRIDE(residual_bits);
  CHECK_INDEX_CONFIG_OVERRIDE(anchor_mode);
#undef CHECK_INDEX_CONFIG_OVERRIDE
  if (mismatch_field != nullptr) {
    mismatch_field->clear();
  }
  return 0;
}

inline int validate_request_layout(const index_config_t& config,
                                   uint32_t data_type,
                                   uint32_t distance_metric,
                                   uint32_t dimension,
                                   std::string *mismatch_field)
{
  if (data_type != config.data_type) {
    set_mismatch_field(mismatch_field, "data_type");
    return -EINVAL;
  }
  if (distance_metric != config.distance_metric) {
    set_mismatch_field(mismatch_field, "distance_metric");
    return -EINVAL;
  }
  if (dimension != config.dimension) {
    set_mismatch_field(mismatch_field, "dimension");
    return -EINVAL;
  }
  if (mismatch_field != nullptr) {
    mismatch_field->clear();
  }
  return 0;
}

inline vector_placement::pg_lsh_v0::sub_oid_config_t sub_oid_config_view(
    const index_config_t& config)
{
  return {
    config.dimension,
    config.seed,
    config.distance_bucket_bits,
    config.residual_bits,
    std::span<const double>(config.anchor),
  };
}

inline int compute_sub_oid(
    const ceph::bufferlist& vector_data,
    const index_config_t& config,
    vector_placement::pg_lsh_v0::sub_oid_t *out_sub_oid)
{
  return vector_placement::pg_lsh_v0::compute_sub_oid(
      vector_data, sub_oid_config_view(config), out_sub_oid);
}

inline int load_index_config(v14_2_0::IoCtx& ioctx,
                             const std::string& bucket_name,
                             const std::string& index_name,
                             index_config_t *config,
                             std::string *invalid_field = nullptr)
{
  if (config == nullptr || bucket_name.empty() || index_name.empty()) {
    return -EINVAL;
  }
  if (invalid_field != nullptr) {
    invalid_field->clear();
  }

  ceph::bufferlist encoded;
  const object_t metadata_oid =
    vector_placement::make_pg_lsh_index_metadata_oid(
        bucket_name, index_name);
  int ret = ioctx.read(metadata_oid.name, encoded, 0, 0);
  if (ret < 0) {
    if (ret == -ENOENT) {
      set_mismatch_field(invalid_field, "index_metadata");
    }
    return ret;
  }

  try {
    auto p = encoded.cbegin();
    config->decode(p);
    if (!p.end()) {
      set_mismatch_field(invalid_field, "trailing_metadata");
      return -EIO;
    }
  } catch (const ceph::buffer::error&) {
    set_mismatch_field(invalid_field, "encoded_metadata");
    return -EIO;
  }
  return 0;
}

inline int create_index(v14_2_0::IoCtx& ioctx,
                        const std::string& bucket_name,
                        const std::string& index_name,
                        const index_config_t& requested_config,
                        const pool_pg_info_t& pool_info,
                        std::string *mismatch_field = nullptr)
{
  if (bucket_name.empty() || index_name.empty()) {
    return -EINVAL;
  }
  int ret = validate_index_config(
      requested_config, pool_info, mismatch_field);
  if (ret < 0) {
    return ret;
  }

  ceph::bufferlist encoded;
  requested_config.encode(encoded);
  ObjectWriteOperation op;
  op.create(true);
  op.write_full(encoded);
  const object_t metadata_oid =
    vector_placement::make_pg_lsh_index_metadata_oid(
        bucket_name, index_name);
  ret = ioctx.operate(metadata_oid.name, &op);
  if (ret == 0) {
    if (mismatch_field != nullptr) {
      mismatch_field->clear();
    }
    return 0;
  }
  if (ret != -EEXIST) {
    return ret;
  }

  index_config_t stored_config;
  ret = load_index_config(
      ioctx, bucket_name, index_name, &stored_config, mismatch_field);
  if (ret < 0) {
    return ret;
  }
  return compare_index_configs(
      requested_config, stored_config, mismatch_field);
}

class locator_cache_t {
 public:
  locator_cache_t(v14_2_0::IoCtx *ioctx,
                  pool_pg_info_t info,
                  std::string pool_name,
                  std::string bucket_name,
                  std::string index_name,
                  std::shared_ptr<locator_state_t> state = nullptr)
    : ioctx(ioctx),
      info(info),
      pool_name(std::move(pool_name)),
      bucket_hash(vector_placement::hash_string(bucket_name)),
      index_hash(vector_placement::hash_string(index_name)),
      state(state ? std::move(state) : std::make_shared<locator_state_t>()) {}

  int precompute_all()
  {
    if (ioctx == nullptr || info.pg_num == 0) {
      return -EINVAL;
    }

    std::lock_guard locker(state->lock);
    const uint32_t search_limit =
      std::max<uint32_t>(10000000U, info.pg_num * 1024U);
    while (state->cache.size() < info.pg_num &&
           state->next_salt < search_limit) {
      std::string candidate = make_locator_key(state->next_salt++);
      uint32_t actual = 0;
      const int ret =
        ioctx->get_object_pg_hash_position2(candidate, &actual);
      if (ret < 0) {
        return ret;
      }
      if (actual < info.pg_num &&
          state->cache.find(actual) == state->cache.end()) {
        state->cache.emplace(actual, std::move(candidate));
      }
    }

    if (state->cache.size() != info.pg_num) {
      return -ENOENT;
    }

    for (uint32_t pg = 0; pg < info.pg_num; ++pg) {
      const auto found = state->cache.find(pg);
      if (found == state->cache.end()) {
        return -ENOENT;
      }
      uint32_t actual = 0;
      const int ret =
        ioctx->get_object_pg_hash_position2(found->second, &actual);
      if (ret < 0) {
        return ret;
      }
      if (actual != pg) {
        return -ESTALE;
      }
    }
    return 0;
  }

  int locator_for_pg(uint32_t pg, std::string *locator_key)
  {
    if (locator_key == nullptr || ioctx == nullptr || pg >= info.pg_num) {
      return -EINVAL;
    }

    std::string found_locator;
    {
      std::lock_guard locker(state->lock);
      const auto found = state->cache.find(pg);
      if (found == state->cache.end()) {
        return -ENOENT;
      }
      found_locator = found->second;
    }

    const int ret = verify_locator(pg, found_locator);
    if (ret < 0) {
      return ret;
    }
    *locator_key = std::move(found_locator);
    return 0;
  }

  int verify_locator(uint32_t pg, const std::string& locator_key) const
  {
    if (ioctx == nullptr || pg >= info.pg_num || locator_key.empty()) {
      return -EINVAL;
    }
    uint32_t actual = 0;
    const int ret = ioctx->get_object_pg_hash_position2(locator_key, &actual);
    if (ret < 0) {
      return ret;
    }
    return actual == pg ? 0 : -ESTALE;
  }

 private:
  std::string make_locator_key(uint32_t salt) const
  {
    return "pg-lsh-v0:" + pool_name + ":" + bucket_hash + ":" + index_hash +
      ":salt:" + std::to_string(salt);
  }

  v14_2_0::IoCtx *ioctx = nullptr;
  pool_pg_info_t info;
  std::string pool_name;
  std::string bucket_hash;
  std::string index_hash;
  std::shared_ptr<locator_state_t> state;
};

inline int select_write_pgs(const ceph::bufferlist& vector_data,
                            const index_config_t& config,
                            const pool_pg_info_t& pool_info,
                            std::vector<uint32_t> *write_pgs)
{
  if (write_pgs == nullptr) {
    return -EINVAL;
  }
  write_pgs->clear();

  int ret = validate_index_config(config, pool_info);
  if (ret < 0) {
    return ret;
  }

  std::vector<vector_placement::pg_lsh_v0_group_t> exact_groups;
  ret = vector_placement::pg_lsh_v0_exact_groups(
      vector_data, config.dimension, config.k, config.l, config.seed,
      &exact_groups);
  if (ret < 0) {
    return ret;
  }

  *write_pgs = vector_placement::pg_lsh_v0_select_write_pgs(
      exact_groups, pool_info.pg_num, config.seed, config.d);
  return 0;
}

inline int select_query_pgs(
    const ceph::bufferlist& query_vector,
    const index_config_t& config,
    const query_params_t& query_params,
    const pool_pg_info_t& pool_info,
    std::vector<vector_placement::pg_lsh_v0_ranked_pg_t> *query_pgs,
    uint64_t *generated_group_count)
{
  if (query_pgs == nullptr) {
    return -EINVAL;
  }
  query_pgs->clear();
  if (generated_group_count != nullptr) {
    *generated_group_count = 0;
  }

  int ret = validate_query_params(config, query_params, pool_info);
  if (ret < 0) {
    return ret;
  }

  std::vector<vector_placement::pg_lsh_v0_group_t> groups;
  ret = vector_placement::pg_lsh_v0_query_groups(
      query_vector, config.dimension, config.k, config.l,
      query_params.hamming_radius, config.seed, &groups);
  if (ret < 0) {
    return ret;
  }
  if (generated_group_count != nullptr) {
    *generated_group_count = groups.size();
  }

  *query_pgs = vector_placement::pg_lsh_v0_select_unique_pgs(
      groups, pool_info.pg_num, config.seed, query_params.m);
  return 0;
}

inline int build_put_targets(const std::string& bucket_name,
                             const std::string& index_name,
                             const ceph::bufferlist& vector_data,
                             const index_config_t& config,
                             const pool_pg_info_t& pool_info,
                             locator_cache_t *locator_cache,
                             std::vector<put_target_t> *targets)
{
  if (targets == nullptr || locator_cache == nullptr ||
      bucket_name.empty() || index_name.empty()) {
    return -EINVAL;
  }
  targets->clear();

  std::vector<uint32_t> write_pgs;
  int ret = select_write_pgs(
      vector_data, config, pool_info, &write_pgs);
  if (ret < 0) {
    return ret;
  }

  const std::string vector_hash =
    vector_placement::hash_v0_vector_hash(vector_data);
  std::string sub_oid_name;
  if (vector_placement::pg_lsh_v0::sub_oid_enabled(
        config.distance_bucket_bits, config.residual_bits)) {
    vector_placement::pg_lsh_v0::sub_oid_t sub_oid;
    ret = compute_sub_oid(vector_data, config, &sub_oid);
    if (ret < 0) {
      return ret;
    }
    sub_oid_name = vector_placement::pg_lsh_v0::format_sub_oid(
        sub_oid, sub_oid_config_view(config));
  }

  targets->reserve(write_pgs.size());
  for (const uint32_t pg : write_pgs) {
    std::string locator_key;
    ret = locator_cache->locator_for_pg(pg, &locator_key);
    if (ret < 0) {
      return ret;
    }
    targets->push_back({
      pg,
      vector_placement::make_pg_lsh_v0_oid(
          bucket_name, index_name, pg, sub_oid_name),
      std::move(locator_key),
      vector_placement::pg_lsh_v0_placement_key(pg),
      vector_hash,
      sub_oid_name,
    });
  }
  return 0;
}

inline int build_query_probes(const std::string& bucket_name,
                              const std::string& index_name,
                              const ceph::bufferlist& query_vector,
                              const index_config_t& config,
                              const query_params_t& query_params,
                              const pool_pg_info_t& pool_info,
                              locator_cache_t *locator_cache,
                              std::vector<query_probe_t> *probes,
                              uint64_t *generated_group_count)
{
  if (probes == nullptr || locator_cache == nullptr ||
      bucket_name.empty() || index_name.empty()) {
    return -EINVAL;
  }
  probes->clear();
  if (generated_group_count != nullptr) {
    *generated_group_count = 0;
  }

  int ret = validate_query_params(config, query_params, pool_info);
  if (ret < 0) {
    return ret;
  }

  std::vector<vector_placement::pg_lsh_v0_ranked_pg_t> ranked;
  ret = select_query_pgs(
      query_vector, config, query_params, pool_info, &ranked,
      generated_group_count);
  if (ret < 0) {
    return ret;
  }

  std::vector<std::string> probe_sub_oids;
  if (vector_placement::pg_lsh_v0::sub_oid_enabled(
        config.distance_bucket_bits, config.residual_bits)) {
    vector_placement::pg_lsh_v0::sub_oid_t exact_sub_oid;
    ret = compute_sub_oid(query_vector, config, &exact_sub_oid);
    if (ret < 0) {
      return ret;
    }
    const vector_placement::pg_lsh_v0::probe_config_t probe_config = {
      query_params.distance_bucket_radius,
      query_params.residual_hamming_radius,
    };
    ret = vector_placement::pg_lsh_v0::build_probe_sub_oids(
        exact_sub_oid, sub_oid_config_view(config), probe_config,
        &probe_sub_oids);
    if (ret < 0) {
      return ret;
    }
  } else {
    probe_sub_oids.emplace_back();
  }

  probes->reserve(ranked.size() * probe_sub_oids.size());
  std::unordered_set<std::string> seen_oid_names;
  for (const auto& ranked_pg : ranked) {
    std::string locator_key;
    ret = locator_cache->locator_for_pg(ranked_pg.pg, &locator_key);
    if (ret < 0) {
      return ret;
    }
    uint32_t emitted_for_pg = 0;
    for (const auto& probe_sub_oid : probe_sub_oids) {
      object_t oid = vector_placement::make_pg_lsh_v0_oid(
          bucket_name, index_name, ranked_pg.pg, probe_sub_oid);
      if (!seen_oid_names.insert(oid.name).second) {
        continue;
      }
      probes->push_back({
        ranked_pg.pg,
        ranked_pg.min_hamming_distance,
        ranked_pg.table_votes,
        std::move(oid),
        locator_key,
        vector_placement::pg_lsh_v0_placement_key(ranked_pg.pg),
        probe_sub_oid,
      });
      ++emitted_for_pg;
      if (query_params.probe_limit_per_pg != 0 &&
          emitted_for_pg >= query_params.probe_limit_per_pg) {
        break;
      }
    }
  }
  return 0;
}

inline int verify_probe_locator(v14_2_0::IoCtx& ioctx,
                                uint32_t pg,
                                const std::string& locator_key)
{
  uint32_t actual = 0;
  const int ret = ioctx.get_object_pg_hash_position2(locator_key, &actual);
  if (ret < 0) {
    return ret;
  }
  return actual == pg ? 0 : -ESTALE;
}

CEPH_RADOS_API int put_vector(v14_2_0::IoCtx& ioctx,
                              const put_target_t& target,
                              ceph::rados::put_vector_request_t req);

CEPH_RADOS_API int submit_put(v14_2_0::IoCtx& ioctx,
                              const put_target_t& target,
                              ceph::rados::put_vector_request_t req,
                              put_op_state_t *op_state,
                              v14_2_0::AioCompletion *completion);

CEPH_RADOS_API int submit_query(v14_2_0::IoCtx& ioctx,
                                const query_probe_t& probe,
                                ceph::rados::query_vectors_request_t req,
                                query_op_state_t *op_state,
                                v14_2_0::AioCompletion *completion,
                                ceph::bufferlist *reply,
                                int *op_rval);

CEPH_RADOS_API int query_sync(v14_2_0::IoCtx& ioctx,
                              const query_probe_t& probe,
                              ceph::rados::query_vectors_request_t req,
                              ceph::bufferlist *reply,
                              int *op_rval);

} // namespace vector_pg_lsh
} // inline namespace v14_2_0
} // namespace librados

#endif
