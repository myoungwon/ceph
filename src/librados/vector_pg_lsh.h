// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRADOS_VECTOR_PG_LSH_H
#define CEPH_LIBRADOS_VECTOR_PG_LSH_H

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <errno.h>

#include "include/buffer.h"
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

struct params_t {
  uint32_t k = 0;
  uint32_t l = 0;
  uint32_t seed = 0;
  uint32_t hamming_radius = 0;
  uint32_t d = 0;
  uint32_t m = 0;
  uint32_t distance_bucket_bits = 0;
  uint32_t residual_bits = 0;
  uint32_t distance_bucket_radius = 0;
  uint32_t residual_hamming_radius = 0;
  uint32_t probe_limit_per_pg = 0;
  std::vector<double> anchor;
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

inline int validate_params(const params_t& params,
                           const pool_pg_info_t& pool_info)
{
  if (params.k == 0 ||
      params.k > ceph::rados::vector_lsh_v0_max_bits ||
      params.l == 0 ||
      params.l > 0xffffU ||
      params.hamming_radius > params.k ||
      params.d == 0 ||
      params.m == 0 ||
      params.distance_bucket_bits > 16 ||
      params.residual_bits > 16 ||
      params.residual_hamming_radius > params.residual_bits ||
      pool_info.pg_num == 0 ||
      pool_info.pgp_num == 0) {
    return -EINVAL;
  }
  if (params.d > pool_info.pg_num || params.m > pool_info.pg_num) {
    return -EINVAL;
  }
  if (params.d > params.l) {
    return -EINVAL;
  }
  if (params.distance_bucket_bits == 0 &&
      params.distance_bucket_radius != 0) {
    return -EINVAL;
  }
  if (!vector_placement::pg_lsh_v0_sub_oid_enabled(
        params.distance_bucket_bits, params.residual_bits) &&
      params.probe_limit_per_pg != 0) {
    return -EINVAL;
  }
  return 0;
}

inline vector_placement::pg_lsh_v0::sub_oid_config_t sub_oid_config_view(
    uint32_t dimension,
    const params_t& params)
{
  return {
    dimension,
    params.seed,
    params.distance_bucket_bits,
    params.residual_bits,
    std::span<const double>(params.anchor),
  };
}

inline int compute_sub_oid(
    const ceph::bufferlist& vector_data,
    uint32_t dimension,
    const params_t& params,
    vector_placement::pg_lsh_v0::sub_oid_t *out_sub_oid)
{
  if (!params.anchor.empty()) {
    return vector_placement::pg_lsh_v0::compute_sub_oid(
        vector_data, sub_oid_config_view(dimension, params), out_sub_oid);
  }

  std::vector<double> default_anchor;
  int ret = vector_placement::pg_lsh_v0_random_anchor(
      dimension, params.seed, &default_anchor);
  if (ret < 0) {
    return ret;
  }
  const vector_placement::pg_lsh_v0::sub_oid_config_t config = {
    dimension,
    params.seed,
    params.distance_bucket_bits,
    params.residual_bits,
    std::span<const double>(default_anchor),
  };
  return vector_placement::pg_lsh_v0::compute_sub_oid(
      vector_data, config, out_sub_oid);
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
                            uint32_t dimension,
                            const params_t& params,
                            const pool_pg_info_t& pool_info,
                            std::vector<uint32_t> *write_pgs)
{
  if (write_pgs == nullptr) {
    return -EINVAL;
  }
  write_pgs->clear();

  int ret = validate_params(params, pool_info);
  if (ret < 0) {
    return ret;
  }

  std::vector<vector_placement::pg_lsh_v0_group_t> exact_groups;
  ret = vector_placement::pg_lsh_v0_exact_groups(
      vector_data, dimension, params.k, params.l, params.seed,
      &exact_groups);
  if (ret < 0) {
    return ret;
  }

  *write_pgs = vector_placement::pg_lsh_v0_select_write_pgs(
      exact_groups, pool_info.pg_num, params.seed, params.d);
  return 0;
}

inline int select_query_pgs(
    const ceph::bufferlist& query_vector,
    uint32_t dimension,
    const params_t& params,
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

  int ret = validate_params(params, pool_info);
  if (ret < 0) {
    return ret;
  }

  std::vector<vector_placement::pg_lsh_v0_group_t> groups;
  ret = vector_placement::pg_lsh_v0_query_groups(
      query_vector, dimension, params.k, params.l, params.hamming_radius,
      params.seed, &groups);
  if (ret < 0) {
    return ret;
  }
  if (generated_group_count != nullptr) {
    *generated_group_count = groups.size();
  }

  *query_pgs = vector_placement::pg_lsh_v0_select_unique_pgs(
      groups, pool_info.pg_num, params.seed, params.m);
  return 0;
}

inline int build_put_targets(const std::string& bucket_name,
                             const std::string& index_name,
                             const ceph::bufferlist& vector_data,
                             uint32_t dimension,
                             const params_t& params,
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
      vector_data, dimension, params, pool_info, &write_pgs);
  if (ret < 0) {
    return ret;
  }

  const std::string vector_hash =
    vector_placement::hash_v0_vector_hash(vector_data);
  std::string sub_oid_name;
  if (vector_placement::pg_lsh_v0::sub_oid_enabled(
        params.distance_bucket_bits, params.residual_bits)) {
    vector_placement::pg_lsh_v0::sub_oid_t sub_oid;
    ret = compute_sub_oid(
        vector_data, dimension, params, &sub_oid);
    if (ret < 0) {
      return ret;
    }
    sub_oid_name = vector_placement::pg_lsh_v0::format_sub_oid(
        sub_oid, sub_oid_config_view(dimension, params));
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
                              uint32_t dimension,
                              const params_t& params,
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

  int ret = validate_params(params, pool_info);
  if (ret < 0) {
    return ret;
  }

  std::vector<vector_placement::pg_lsh_v0_ranked_pg_t> ranked;
  ret = select_query_pgs(
      query_vector, dimension, params, pool_info, &ranked,
      generated_group_count);
  if (ret < 0) {
    return ret;
  }

  std::vector<std::string> probe_sub_oids;
  if (vector_placement::pg_lsh_v0::sub_oid_enabled(
        params.distance_bucket_bits, params.residual_bits)) {
    vector_placement::pg_lsh_v0::sub_oid_t exact_sub_oid;
    ret = compute_sub_oid(
        query_vector, dimension, params, &exact_sub_oid);
    if (ret < 0) {
      return ret;
    }
    const vector_placement::pg_lsh_v0::probe_config_t probe_config = {
      params.distance_bucket_radius,
      params.residual_hamming_radius,
    };
    ret = vector_placement::pg_lsh_v0::build_probe_sub_oids(
        exact_sub_oid, sub_oid_config_view(dimension, params), probe_config,
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
      if (params.probe_limit_per_pg != 0 &&
          emitted_for_pg >= params.probe_limit_per_pg) {
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
