// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "common/vector_selective_fanout.h"

#include <gtest/gtest.h>

namespace sf = ceph::rados::vector_selective_fanout;

namespace {

struct test_entry_t {
  float distance = 0;
  std::string entry_id;
};

test_entry_t entry(float distance, std::string entry_id)
{
  return {distance, std::move(entry_id)};
}

} // namespace

TEST(VectorSelectiveFanout, ValidateSchedule)
{
  EXPECT_EQ(0, sf::validate_schedule({8, 16, 32, 64, 96}));
  EXPECT_EQ(0, sf::validate_schedule({1}));
  EXPECT_EQ(0, sf::validate_schedule(sf::default_schedule()));

  EXPECT_EQ(-EINVAL, sf::validate_schedule({}));
  // A zero first stage would dispatch an empty PG range.
  EXPECT_EQ(-EINVAL, sf::validate_schedule({0, 8}));
  // Cumulative counts must strictly increase; equal entries would make a
  // stage dispatch nothing, and a decrease has no meaning at all.
  EXPECT_EQ(-EINVAL, sf::validate_schedule({8, 8}));
  EXPECT_EQ(-EINVAL, sf::validate_schedule({8, 4}));
}

TEST(VectorSelectiveFanout, ParseSchedule)
{
  sf::schedule_t parsed;
  ASSERT_EQ(0, sf::parse_schedule("8,16,32,64,96", &parsed));
  EXPECT_EQ((sf::schedule_t{8, 16, 32, 64, 96}), parsed);

  ASSERT_EQ(0, sf::parse_schedule("4", &parsed));
  EXPECT_EQ((sf::schedule_t{4}), parsed);

  EXPECT_EQ(-EINVAL, sf::parse_schedule("", &parsed));
  EXPECT_EQ(-EINVAL, sf::parse_schedule("8,,16", &parsed));
  EXPECT_EQ(-EINVAL, sf::parse_schedule("8,16,", &parsed));
  EXPECT_EQ(-EINVAL, sf::parse_schedule("8,x", &parsed));
  EXPECT_EQ(-EINVAL, sf::parse_schedule("-8", &parsed));
  // Rejected by validate_schedule() rather than by the lexer.
  EXPECT_EQ(-EINVAL, sf::parse_schedule("16,8", &parsed));
  EXPECT_EQ(-EINVAL, sf::parse_schedule("0,8", &parsed));
  EXPECT_EQ(-EINVAL, sf::parse_schedule("8", nullptr));
}

TEST(VectorSelectiveFanout, StageRangesArePartitionAndClamped)
{
  const sf::schedule_t schedule{8, 16, 32};

  // With every PG available the stages exactly partition [0, 32).
  EXPECT_EQ(0u, sf::stage_range(schedule, 0, 32).begin);
  EXPECT_EQ(8u, sf::stage_range(schedule, 0, 32).end);
  EXPECT_EQ(8u, sf::stage_range(schedule, 1, 32).begin);
  EXPECT_EQ(16u, sf::stage_range(schedule, 1, 32).end);
  EXPECT_EQ(16u, sf::stage_range(schedule, 2, 32).begin);
  EXPECT_EQ(32u, sf::stage_range(schedule, 2, 32).end);

  // Routing returning fewer distinct PGs than the schedule asks for must
  // clamp, not run off the end of the probe vector.
  EXPECT_EQ(8u, sf::stage_range(schedule, 1, 12).begin);
  EXPECT_EQ(12u, sf::stage_range(schedule, 1, 12).end);
  EXPECT_TRUE(sf::stage_range(schedule, 2, 12).empty());
  EXPECT_EQ(0u, sf::stage_range(schedule, 2, 12).count());

  // Out-of-range stage index is empty, not undefined.
  EXPECT_TRUE(sf::stage_range(schedule, 3, 32).empty());
}

TEST(VectorSelectiveFanout, AccumulatorTauUnsetUntilTopKFilled)
{
  sf::topk_accumulator_t<test_entry_t> acc(3);

  std::vector<test_entry_t> stage0{entry(10.0f, "a"), entry(20.0f, "b")};
  acc.merge_stage(stage0);
  EXPECT_TRUE(acc.current_d1().has_value());
  EXPECT_FLOAT_EQ(10.0f, *acc.current_d1());
  // Two of three slots filled: there is no bound yet, which must be
  // distinguishable from "the bound is 20".
  EXPECT_FALSE(acc.current_tau().has_value());

  std::vector<test_entry_t> stage1{entry(15.0f, "c")};
  acc.merge_stage(stage1);
  ASSERT_TRUE(acc.current_tau().has_value());
  EXPECT_FLOAT_EQ(20.0f, *acc.current_tau());
}

TEST(VectorSelectiveFanout, AccumulatorDedupesAcrossStages)
{
  sf::topk_accumulator_t<test_entry_t> acc(3);

  std::vector<test_entry_t> stage0{entry(10.0f, "a"), entry(20.0f, "b")};
  acc.merge_stage(stage0);

  // The same vector routed to a second PG must not take a second top-k slot.
  std::vector<test_entry_t> stage1{entry(10.0f, "a"), entry(30.0f, "d")};
  acc.merge_stage(stage1);
  EXPECT_EQ(1u, acc.dropped_duplicates());
  ASSERT_EQ(3u, acc.results().size());
  EXPECT_EQ("a", acc.results()[0].entry_id);
  EXPECT_EQ("b", acc.results()[1].entry_id);
  EXPECT_EQ("d", acc.results()[2].entry_id);

  // An entry already pushed out of the retained top-k must still not be
  // re-merged later, or it could displace a genuinely better result.
  sf::topk_accumulator_t<test_entry_t> small(1);
  std::vector<test_entry_t> first{entry(10.0f, "a"), entry(99.0f, "z")};
  small.merge_stage(first);
  ASSERT_EQ(1u, small.results().size());
  std::vector<test_entry_t> again{entry(99.0f, "z")};
  small.merge_stage(again);
  EXPECT_EQ(1u, small.dropped_duplicates());
  EXPECT_EQ(1u, small.results().size());
  EXPECT_EQ("a", small.results()[0].entry_id);
}

TEST(VectorSelectiveFanout, IncrementalMergeEqualsOneShot)
{
  // The property the sweep relies on: merging stage by stage must land on
  // exactly the top-k a single query over all the same PGs would produce.
  const std::vector<std::vector<test_entry_t>> stages{
    {entry(50.0f, "e"), entry(10.0f, "a")},
    {entry(70.0f, "g"), entry(30.0f, "c")},
    {entry(20.0f, "b"), entry(60.0f, "f"), entry(40.0f, "d")},
  };

  sf::topk_accumulator_t<test_entry_t> incremental(4);
  for (const auto& stage : stages) {
    incremental.merge_stage(stage);
  }

  sf::topk_accumulator_t<test_entry_t> one_shot(4);
  std::vector<test_entry_t> all;
  for (const auto& stage : stages) {
    all.insert(all.end(), stage.begin(), stage.end());
  }
  one_shot.merge_stage(all);

  ASSERT_EQ(one_shot.results().size(), incremental.results().size());
  for (size_t i = 0; i < one_shot.results().size(); ++i) {
    EXPECT_EQ(one_shot.results()[i].entry_id, incremental.results()[i].entry_id)
        << "rank " << i;
    EXPECT_FLOAT_EQ(one_shot.results()[i].distance,
                    incremental.results()[i].distance) << "rank " << i;
  }
  ASSERT_TRUE(incremental.current_tau().has_value());
  EXPECT_FLOAT_EQ(40.0f, *incremental.current_tau());
}

TEST(VectorSelectiveFanout, TopkReplacementsCountsChurn)
{
  sf::topk_accumulator_t<test_entry_t> acc(2);

  std::vector<test_entry_t> stage0{entry(10.0f, "a"), entry(20.0f, "b")};
  EXPECT_EQ(2u, acc.merge_stage(stage0));

  // Nothing better arrives: tau does not move and neither does membership.
  std::vector<test_entry_t> stage1{entry(30.0f, "c")};
  EXPECT_EQ(0u, acc.merge_stage(stage1));
  EXPECT_FLOAT_EQ(20.0f, *acc.current_tau());

  // A better result displaces the incumbent: churn without the bound having
  // been the thing that told us so at rank 1.
  std::vector<test_entry_t> stage2{entry(5.0f, "d")};
  EXPECT_EQ(1u, acc.merge_stage(stage2));
  EXPECT_FLOAT_EQ(10.0f, *acc.current_tau());
}

TEST(VectorSelectiveFanout, FinalizeStageRecordDerivedFields)
{
  sf::stage_record_t record;
  record.d1 = 10.0f;
  record.tau = 20.0f;
  sf::finalize_stage_record(std::nullopt, &record);
  ASSERT_TRUE(record.tau_over_d1.has_value());
  EXPECT_DOUBLE_EQ(2.0, *record.tau_over_d1);
  // No previous bound to improve on -- not "zero improvement".
  EXPECT_FALSE(record.tau_improvement_abs.has_value());
  EXPECT_FALSE(record.tau_improvement_rel.has_value());

  sf::stage_record_t next;
  next.d1 = 10.0f;
  next.tau = 16.0f;
  sf::finalize_stage_record(20.0f, &next);
  ASSERT_TRUE(next.tau_improvement_abs.has_value());
  EXPECT_FLOAT_EQ(4.0f, *next.tau_improvement_abs);
  ASSERT_TRUE(next.tau_improvement_rel.has_value());
  EXPECT_DOUBLE_EQ(0.2, *next.tau_improvement_rel);

  // tau still unfilled: no derived field may be invented.
  sf::stage_record_t unfilled;
  unfilled.d1 = 10.0f;
  sf::finalize_stage_record(20.0f, &unfilled);
  EXPECT_FALSE(unfilled.tau_over_d1.has_value());
  EXPECT_FALSE(unfilled.tau_improvement_abs.has_value());
}

TEST(VectorSelectiveFanout, ShouldExpandNeverModeAlwaysContinues)
{
  sf::policy_t policy;
  ASSERT_EQ(sf::stop_mode_t::never, policy.stop_mode);

  sf::stage_record_t settled;
  settled.tau = 20.0f;
  settled.tau_improvement_rel = 0.0;
  EXPECT_TRUE(sf::should_expand(policy, settled));
}

TEST(VectorSelectiveFanout, ShouldExpandTauRelativeImprovementCandidateRule)
{
  sf::policy_t policy;
  policy.stop_mode = sf::stop_mode_t::tau_relative_improvement;
  policy.min_relative_improvement = 0.02;

  // top-k not filled: no bound exists, so continue regardless.
  sf::stage_record_t unfilled;
  EXPECT_TRUE(sf::should_expand(policy, unfilled));

  // Bound established for the first time: nothing to compare against yet.
  sf::stage_record_t first_bound;
  first_bound.tau = 20.0f;
  EXPECT_TRUE(sf::should_expand(policy, first_bound));

  sf::stage_record_t improved;
  improved.tau = 16.0f;
  improved.tau_improvement_rel = 0.2;
  EXPECT_TRUE(sf::should_expand(policy, improved));

  sf::stage_record_t plateau;
  plateau.tau = 16.0f;
  plateau.tau_improvement_rel = 0.005;
  EXPECT_FALSE(sf::should_expand(policy, plateau));

  // Exactly at the threshold still continues. This is why
  // tau_improvement_rel is double: as a float it would land just below the
  // double 0.02 and silently stop a stage early.
  sf::stage_record_t at_threshold;
  at_threshold.tau = 16.0f;
  at_threshold.tau_improvement_rel = 0.02;
  EXPECT_TRUE(sf::should_expand(policy, at_threshold));
}

TEST(VectorSelectiveFanout, CsvRowLeavesUnsetOptionalsEmpty)
{
  sf::stage_record_t record;
  record.stage = 1;
  record.cum_m = 16;
  record.batch_pg_count = 8;
  record.d1 = 10.0f;
  // tau deliberately unset: top-k was not filled at this stage.
  record.topk_replacements = 2;
  record.topk_size = 3;
  record.batch_candidates = 100;
  record.cum_candidates = 250;

  std::ostringstream out;
  sf::write_stage_csv_row(out, 42, record);
  const std::string row = out.str();

  // "no bound" must serialize as an empty field, never as a number an
  // offline rule could mistake for a real bound of 0.
  EXPECT_NE(std::string::npos, row.find("42,1,16,8,10,,,,,2,3,100,250,"));
  EXPECT_EQ('\n', row.back());

  // Header column count must match the row's field count.
  const auto count_fields = [](const std::string& s) {
    size_t n = 1;
    for (const char c : s) {
      if (c == ',') ++n;
    }
    return n;
  };
  std::string trimmed = row;
  trimmed.pop_back();
  EXPECT_EQ(count_fields(sf::stage_csv_header()), count_fields(trimmed));
}
