// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRADOS_VECTOR_QUERY_PLANNER_H
#define CEPH_LIBRADOS_VECTOR_QUERY_PLANNER_H

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <errno.h>

#include "include/buffer.h"
#include "include/object.h"
#include "include/rados/vector_ops.h"
#include "librados/vector_placement.h"

namespace librados {
namespace vector_query {

struct put_target_t {
  object_t oid;
  std::string placement_key;
  std::string vector_hash;
};

struct probe_t {
  object_t oid;
  std::string placement_key;
};

struct put_plan_t {
  uint32_t algorithm_id = 0;
  uint32_t algorithm_version = 0;
  std::string placement_algorithm;
  ceph::rados::vector_routing_policy_t routing_policy;
  ceph::bufferlist algorithm_params;
  std::vector<put_target_t> targets;
};

struct query_plan_t {
  uint32_t algorithm_id = 0;
  uint32_t algorithm_version = 0;
  std::string placement_algorithm;
  ceph::rados::vector_routing_policy_t routing_policy;
  ceph::bufferlist algorithm_params;
  std::vector<probe_t> probes;
};

using plan_t = query_plan_t;

struct routed_request_t {
  object_t oid;
  std::string placement_key;
  ceph::bufferlist payload;
};

inline ceph::rados::vector_routing_policy_t normalize_routing_policy(
    ceph::rados::vector_routing_policy_t policy)
{
  if (policy.write_pgs == 0) {
    policy.write_pgs = 1;
  }
  if (policy.probe_pgs == 0) {
    policy.probe_pgs = 1;
  }
  return policy;
}

inline std::string default_placement_algorithm(uint32_t algorithm_id)
{
  switch (algorithm_id) {
  case ceph::rados::vector_query_algorithm_hash:
    return vector_placement::hash_v0_algorithm;
  default:
    return std::string();
  }
}

inline std::string effective_placement_algorithm(
    const ceph::rados::vector_index_config_t& config,
    const std::string& request_algorithm = std::string())
{
  if (!config.placement_algorithm.empty()) {
    return config.placement_algorithm;
  }
  if (!request_algorithm.empty()) {
    return request_algorithm;
  }
  return default_placement_algorithm(config.algorithm_id);
}

inline int validate_index_config(
    uint32_t req_data_type,
    uint32_t req_distance_metric,
    uint32_t req_dimension,
    const ceph::rados::vector_index_config_t& config)
{
  if (config.algorithm_version !=
      ceph::rados::vector_query_algorithm_version_0) {
    return -EOPNOTSUPP;
  }
  if (config.algorithm_id != ceph::rados::vector_query_algorithm_hash) {
    return -EOPNOTSUPP;
  }
  if (config.data_type != 0 && config.data_type != req_data_type) {
    return -EINVAL;
  }
  if (config.distance_metric != 0 &&
      config.distance_metric != req_distance_metric) {
    return -EINVAL;
  }
  if (config.dimension != 0 && config.dimension != req_dimension) {
    return -EINVAL;
  }
  return 0;
}

inline int append_hash_v0_targets(
    const std::string& bucket_name,
    const std::string& index_name,
    const ceph::bufferlist& vector_data,
    uint32_t count,
    std::vector<put_target_t> *targets)
{
  if (targets == nullptr || count == 0) {
    return -EINVAL;
  }
  const std::string vector_hash =
    vector_placement::hash_v0_vector_hash(vector_data);
  for (uint32_t rank = 0; rank < count; ++rank) {
    const std::string placement_key =
      vector_placement::ranked_hash_v0_placement_key(vector_hash, rank);
    targets->push_back({
      vector_placement::make_hash_v0_oid(
          bucket_name, index_name, placement_key),
      placement_key,
      vector_hash,
    });
  }
  return 0;
}

inline int build_put_plan(
    const ceph::rados::put_vector_request_t& req,
    const ceph::rados::vector_index_config_t& config,
    put_plan_t *plan)
{
  if (plan == nullptr || req.bucket_name.empty() || req.index_name.empty() ||
      req.key.empty() || req.vector_data.length() == 0) {
    return -EINVAL;
  }

  plan->algorithm_id = 0;
  plan->algorithm_version = 0;
  plan->placement_algorithm.clear();
  plan->routing_policy = ceph::rados::vector_routing_policy_t();
  plan->algorithm_params.clear();
  plan->targets.clear();

  int r = validate_index_config(
      req.data_type, req.distance_metric, req.dimension, config);
  if (r < 0) {
    return r;
  }

  const std::string placement_algorithm =
    effective_placement_algorithm(config, req.placement_algorithm);
  if (placement_algorithm.empty()) {
    return -EOPNOTSUPP;
  }

  plan->algorithm_id = config.algorithm_id;
  plan->algorithm_version = config.algorithm_version;
  plan->placement_algorithm = placement_algorithm;
  plan->routing_policy = normalize_routing_policy(config.routing_policy);
  plan->algorithm_params = config.algorithm_params;

  if (placement_algorithm == vector_placement::hash_v0_algorithm) {
    return append_hash_v0_targets(
        req.bucket_name, req.index_name, req.vector_data,
        plan->routing_policy.write_pgs, &plan->targets);
  }
  return -EOPNOTSUPP;
}

inline int build_query_plan(
    const ceph::rados::query_vectors_request_t& req,
    const ceph::rados::vector_index_config_t& config,
    query_plan_t *plan)
{
  if (plan == nullptr || req.bucket_name.empty() || req.index_name.empty() ||
      req.query_vector.length() == 0) {
    return -EINVAL;
  }

  plan->algorithm_id = 0;
  plan->algorithm_version = 0;
  plan->placement_algorithm.clear();
  plan->routing_policy = ceph::rados::vector_routing_policy_t();
  plan->algorithm_params.clear();
  plan->probes.clear();

  int r = validate_index_config(
      req.data_type, req.distance_metric, req.dimension, config);
  if (r < 0) {
    return r;
  }

  const std::string placement_algorithm =
    effective_placement_algorithm(config, req.placement_algorithm);
  if (placement_algorithm.empty()) {
    return -EOPNOTSUPP;
  }

  plan->algorithm_id = config.algorithm_id;
  plan->algorithm_version = config.algorithm_version;
  plan->placement_algorithm = placement_algorithm;
  plan->routing_policy = normalize_routing_policy(config.routing_policy);
  plan->algorithm_params = config.algorithm_params;

  std::vector<put_target_t> targets;
  if (placement_algorithm == vector_placement::hash_v0_algorithm) {
    r = append_hash_v0_targets(
        req.bucket_name, req.index_name, req.query_vector,
        plan->routing_policy.probe_pgs, &targets);
  } else {
    return -EOPNOTSUPP;
  }
  if (r < 0) {
    return r;
  }

  plan->probes.reserve(targets.size());
  for (const auto& target : targets) {
    plan->probes.push_back({
      target.oid,
      target.placement_key,
    });
  }
  return 0;
}

inline int build_query_plan(
    const ceph::rados::query_vectors_request_t& req,
    query_plan_t *plan)
{
  ceph::rados::vector_index_config_t config;
  config.data_type = req.data_type;
  config.distance_metric = req.distance_metric;
  config.dimension = req.dimension;
  config.algorithm_id = req.algorithm_id;
  config.algorithm_version = req.algorithm_version;
  config.placement_algorithm = req.placement_algorithm;
  config.routing_policy = req.routing_policy;
  config.algorithm_params = req.algorithm_params;
  return build_query_plan(req, config, plan);
}

inline int build_plan(const ceph::rados::query_vectors_request_t& req,
                      plan_t *plan)
{
  return build_query_plan(req, plan);
}

inline int build_routed_requests(
    const ceph::rados::query_vectors_request_t& req,
    const query_plan_t& plan,
    std::vector<routed_request_t> *requests)
{
  if (requests == nullptr) {
    return -EINVAL;
  }
  requests->clear();
  if (plan.probes.empty()) {
    return -EINVAL;
  }

  for (const auto& probe : plan.probes) {
    ceph::rados::query_vectors_request_t probe_req = req;
    probe_req.algorithm_id = plan.algorithm_id;
    probe_req.algorithm_version = plan.algorithm_version;
    probe_req.placement_algorithm = plan.placement_algorithm;
    probe_req.routing_policy = plan.routing_policy;
    probe_req.algorithm_params = plan.algorithm_params;
    probe_req.probe_prefixes.clear();
    probe_req.probe_prefixes.push_back(probe.placement_key);

    ceph::bufferlist payload;
    encode(probe_req, payload);
    requests->push_back({
      probe.oid,
      probe.placement_key,
      std::move(payload),
    });
  }
  return 0;
}

} // namespace vector_query
} // namespace librados

#endif
