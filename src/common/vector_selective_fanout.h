// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_COMMON_VECTOR_SELECTIVE_FANOUT_H
#define CEPH_COMMON_VECTOR_SELECTIVE_FANOUT_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <errno.h>

// Selective PG fan-out: instead of dispatching all m routed PGs for a query
// at once, dispatch them in increasing cumulative batches (8, 16, 32, ...)
// and re-examine the merged global top-k between batches.
//
// This header holds the schedule, cross-batch top-k accumulator, and stopping
// policy. Probe construction and dispatch remain with the caller.

namespace ceph {
namespace rados {
namespace vector_selective_fanout {

// Cumulative PG counts. Each entry is a total, not a delta: {8, 16, 32}
// dispatches PGs [0,8), then [8,16), then [16,32).
using schedule_t = std::vector<uint32_t>;

inline const schedule_t& default_schedule()
{
  static const schedule_t schedule{8, 16, 32, 64, 96};
  return schedule;
}

// A schedule must be strictly increasing and start above zero, so that
// stage boundaries name a non-empty half-open PG range and cumulative
// counts never move backwards.
inline int validate_schedule(const schedule_t& schedule)
{
  if (schedule.empty() || schedule.front() == 0) {
    return -EINVAL;
  }
  for (size_t i = 1; i < schedule.size(); ++i) {
    if (schedule[i] <= schedule[i - 1]) {
      return -EINVAL;
    }
  }
  return 0;
}

// Parses "8,16,32,64,96". Rejects anything validate_schedule() rejects.
inline int parse_schedule(std::string_view spec, schedule_t *out)
{
  if (out == nullptr) {
    return -EINVAL;
  }
  out->clear();
  size_t pos = 0;
  while (pos <= spec.size()) {
    const size_t comma = spec.find(',', pos);
    const std::string_view field = spec.substr(
        pos, comma == std::string_view::npos ? std::string_view::npos
                                             : comma - pos);
    if (field.empty()) {
      return -EINVAL;
    }
    uint64_t value = 0;
    for (const char c : field) {
      if (c < '0' || c > '9') {
        return -EINVAL;
      }
      value = value * 10 + static_cast<uint64_t>(c - '0');
      if (value > 0xffffffffULL) {
        return -EINVAL;
      }
    }
    out->push_back(static_cast<uint32_t>(value));
    if (comma == std::string_view::npos) {
      break;
    }
    pos = comma + 1;
  }
  const int r = validate_schedule(*out);
  if (r < 0) {
    out->clear();
  }
  return r;
}

// The half-open ranked-PG range [begin, end) a stage dispatches. Ranked PG
// order comes from librados/vector_placement.h's
// pg_lsh_v0_select_unique_pgs(), which walks query groups in ascending
// hamming distance and appends each newly seen PG until the budget is hit.
// That makes it prefix-stable: the first N entries of a budget-M selection
// are exactly the entries of a budget-N selection, so a single
// build_query_probes(m = schedule.back()) call can be sliced per stage with
// no re-routing.
struct stage_range_t {
  uint32_t begin = 0;
  uint32_t end = 0;

  bool empty() const { return begin >= end; }
  uint32_t count() const { return empty() ? 0 : end - begin; }
};

// `available_pgs` clamps the schedule to what routing actually produced --
// pg_lsh_v0_select_unique_pgs() can return fewer than the requested budget
// (distinct-PG collisions), so a schedule tail past that point yields empty
// stages the caller skips.
inline stage_range_t stage_range(const schedule_t& schedule,
                                 size_t stage,
                                 uint32_t available_pgs)
{
  if (stage >= schedule.size()) {
    return {};
  }
  const uint32_t begin = stage == 0 ? 0 : schedule[stage - 1];
  const uint32_t end = schedule[stage];
  return {
    std::min(begin, available_pgs),
    std::min(end, available_pgs),
  };
}

// Everything one (query, stage) row records. Distances are whatever metric
// the index ranks by; ratios below are unitless and therefore comparable
// across queries with different distance scales.
struct stage_record_t {
  size_t stage = 0;
  // Cumulative PG count after this stage, and the PGs this stage added.
  uint32_t cum_m = 0;
  uint32_t batch_pg_count = 0;

  // d1: best distance held after this stage. tau: the top_k-th distance,
  // i.e. the bound a stopping rule compares against. tau is unset until
  // top_k results have been retained, meaning no bound exists yet rather
  // than the worst possible one; see tree_boundary_tau_improved() in
  // vector_query_exec.h.
  std::optional<float> d1;
  std::optional<float> tau;
  // tau / d1. Divides out the per-query distance scale, so a threshold on
  // it means the same thing for a query whose neighbours sit at distance 30
  // and one whose neighbours sit at 300.
  //
  // The two ratios below are double even though the distances they come
  // from are float, because they are compared against double thresholds:
  // 0.02f is 0.0199999995... as a double, so a stage that improved by
  // exactly the configured minimum would read as below it.
  std::optional<double> tau_over_d1;
  // tau_prev - tau (in distance units, so float), and that over tau_prev
  // (unitless, so double). Both unset unless tau existed at the previous
  // stage too.
  std::optional<float> tau_improvement_abs;
  std::optional<double> tau_improvement_rel;

  // How many of the retained top-k entry_ids are ones this stage brought
  // in. Independent of tau: a stage can leave tau untouched while still
  // churning ranks 1..k-1, which says the result set has not settled even
  // though the bound has.
  uint32_t topk_replacements = 0;
  uint32_t topk_size = 0;

  // Server-reported work, for this stage and cumulatively.
  uint64_t batch_candidates = 0;
  uint64_t cum_candidates = 0;
  uint64_t batch_distance_computations = 0;
  uint64_t cum_distance_computations = 0;

  double batch_latency_us = 0;
  double cum_latency_us = 0;

  // Ground-truth recall of the retained top-k after this stage. Only set
  // when the caller supplied ground truth.
  std::optional<double> recall;

};

// Cross-batch global top-k. Entries survive between stages: a stage merges
// in its own new rows and the retained set is re-sorted and re-capped
// rather than rebuilt, so the state after stage i matches what a one-shot
// query at that stage's cumulative m would produce. The schedule includes
// the one-shot baseline m so a sweep can check that directly.
//
// EntryT must expose `.distance` (float) and `.entry_id` (string-like).
template <typename EntryT>
class topk_accumulator_t {
 public:
  explicit topk_accumulator_t(uint32_t top_k) : top_k(top_k) {}

  // Merges one stage's rows. Deduplicates by entry_id against everything
  // seen so far -- the same vector is reachable from several routed PGs, and
  // counting it twice would let one vector occupy two top-k slots.
  // Returns how many of the retained top-k are new as of this call.
  template <typename EntryRange>
  uint32_t merge_stage(EntryRange&& stage_entries)
  {
    std::unordered_set<std::string> previous_topk;
    previous_topk.reserve(entries.size());
    for (const auto& entry : entries) {
      previous_topk.insert(std::string(entry.entry_id));
    }

    for (auto&& entry : stage_entries) {
      if (!seen_entry_ids.insert(std::string(entry.entry_id)).second) {
        ++duplicates_dropped;
        continue;
      }
      entries.push_back(std::forward<decltype(entry)>(entry));
    }

    std::sort(entries.begin(), entries.end(),
              [](const EntryT& a, const EntryT& b) {
                return a.distance < b.distance;
              });
    if (top_k != 0 && entries.size() > top_k) {
      entries.resize(top_k);
    }

    uint32_t replacements = 0;
    for (const auto& entry : entries) {
      if (previous_topk.find(std::string(entry.entry_id)) ==
          previous_topk.end()) {
        ++replacements;
      }
    }
    return replacements;
  }

  std::optional<float> current_d1() const
  {
    if (entries.empty()) {
      return std::nullopt;
    }
    return entries.front().distance;
  }

  // Unset until top_k entries have actually been retained -- see
  // stage_record_t::tau.
  std::optional<float> current_tau() const
  {
    if (top_k == 0 || entries.size() < top_k) {
      return std::nullopt;
    }
    return entries[top_k - 1].distance;
  }

  const std::vector<EntryT>& results() const { return entries; }
  uint64_t dropped_duplicates() const { return duplicates_dropped; }

 private:
  uint32_t top_k;
  std::vector<EntryT> entries;
  // Kept separately from `entries` so a vector that was merged and then
  // pushed out of the top-k is still not re-merged by a later stage.
  std::unordered_set<std::string> seen_entry_ids;
  uint64_t duplicates_dropped = 0;
};

// Fills the derived ratio/improvement fields of `record` from the raw d1/tau
// it already carries plus the previous stage's tau. Split out from the
// accumulator so it can be unit-tested on its own and so a caller replaying
// a CSV offline computes them identically.
inline void finalize_stage_record(std::optional<float> tau_prev,
                                  stage_record_t *record)
{
  if (record == nullptr) {
    return;
  }
  if (record->tau && record->d1 && *record->d1 > 0) {
    record->tau_over_d1 =
      static_cast<double>(*record->tau) / static_cast<double>(*record->d1);
  }
  if (record->tau && tau_prev) {
    const float improvement = *tau_prev - *record->tau;
    record->tau_improvement_abs = improvement;
    if (*tau_prev > 0) {
      record->tau_improvement_rel =
        static_cast<double>(improvement) / static_cast<double>(*tau_prev);
    }
  }
}

enum class stop_mode_t {
  // Always run the schedule to its end.
  never,
  // Stop once tau exists and the last stage improved it by less than
  // `min_relative_improvement`. A candidate rule, not a chosen one; kept
  // here so a rule picked from the offline data can be replayed live.
  tau_relative_improvement,
};

struct policy_t {
  schedule_t schedule = default_schedule();
  stop_mode_t stop_mode = stop_mode_t::never;
  double min_relative_improvement = 0.02;
};

// Whether to dispatch another stage after `record`.
//
// Under tau_relative_improvement the two "no bound yet" cases both continue,
// for different reasons: no tau at all means top-k is not even filled, and a
// tau that has just appeared for the first time has nothing to be compared
// against yet.
// Header row matching write_stage_csv_row() below.
inline const char *stage_csv_header()
{
  return "query_index,stage,cum_m,batch_pg_count,"
         "d1,tau,tau_over_d1,tau_impr_abs,tau_impr_rel,"
         "topk_replacements,topk_size,"
         "batch_candidates,cum_candidates,"
         "batch_distcomp,cum_distcomp,"
         "batch_latency_us,cum_latency_us,"
         "recall_at_stage";
}

// One CSV row. An unset optional is written as an empty field, never as 0 or
// -1: "top-k was not filled, so no bound exists" and "the bound is 0" are
// different facts, and an offline rule that cannot tell them apart repeats
// the trap this file's tau comments warn about.
inline void write_stage_csv_row(std::ostream& out,
                                uint64_t query_index,
                                const stage_record_t& record)
{
  const auto field = [&out](const auto& value) {
    if (value) {
      out << *value;
    }
    out << ',';
  };
  out << query_index << ','
      << record.stage << ','
      << record.cum_m << ','
      << record.batch_pg_count << ',';
  field(record.d1);
  field(record.tau);
  field(record.tau_over_d1);
  field(record.tau_improvement_abs);
  field(record.tau_improvement_rel);
  out << record.topk_replacements << ','
      << record.topk_size << ','
      << record.batch_candidates << ','
      << record.cum_candidates << ','
      << record.batch_distance_computations << ','
      << record.cum_distance_computations << ','
      << record.batch_latency_us << ','
      << record.cum_latency_us << ',';
  if (record.recall) {
    out << *record.recall;
  }
  out << '\n';
}

inline bool should_expand(const policy_t& policy,
                          const stage_record_t& record)
{
  switch (policy.stop_mode) {
    case stop_mode_t::never:
      return true;
    case stop_mode_t::tau_relative_improvement:
      if (!record.tau) {
        return true;
      }
      if (!record.tau_improvement_rel) {
        return true;
      }
      return *record.tau_improvement_rel >= policy.min_relative_improvement;
  }
  return true;
}

} // namespace vector_selective_fanout
} // namespace rados
} // namespace ceph

#endif
