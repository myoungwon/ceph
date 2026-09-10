// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <getopt.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/vector_record.h"
#include "common/vector_selective_fanout.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/rados/vector_ops.h"
#include "librados/vector_pg_lsh.h"

namespace {

using clock_type = std::chrono::steady_clock;
namespace pg_lsh = librados::vector_pg_lsh;
namespace sf = ceph::rados::vector_selective_fanout;

struct options_t {
  std::string base_path;
  std::string query_path;
  std::string groundtruth_path;
  std::string pool;
  std::string bucket = "siftsmall";
  std::string index = "fixed-layout";
  std::string key_prefix = "siftsmall-";
  std::string client_name = "client.admin";
  std::string cluster_name = "ceph";
  std::string conf_path;
  std::string operation = "both";
  std::string routing_mode;
  uint32_t pg_num = 8;
  uint32_t k = 10;
  uint32_t l = 16;
  uint32_t seed = 1315423911U;
  uint32_t d = 1;
  // Load path only, and a post-filter on the already-built target list
  // rather than a change to any routing function. Both routings build
  // their write targets as a prefix of a d-independent ranking, so
  // targets(d=D) is targets(d=D-1) plus one more entry. Writing only ranks
  // [begin, d) therefore reproduces exactly the store state a full --d D
  // load would have produced, given ranks [0, begin) are already present
  // from an earlier load -- which is how a d=1 index is grown to d=2/d=3
  // without re-PUTting the copies it already holds.
  uint32_t write_rank_begin = 0;
  uint32_t m = 1;
  uint32_t hamming_radius = 0;
  uint32_t distance_bucket_bits = 0;
  uint32_t residual_bits = 0;
  uint32_t distance_bucket_radius = 0;
  uint32_t residual_hamming_radius = 0;
  uint32_t probe_limit_per_pg = 0;
  // pg-suboid only: disables the fixed client-side distance_bucket/
  // residual Hamming-ball probe expansion (distance_bucket_radius/
  // residual_hamming_radius must both be 0 in this mode) and lets the OSD
  // expand neighboring sub_oid ONodes itself -- see
  // librados::vector_pg_lsh::query_params_t::server_side_boundary_search.
  bool server_side_boundary_search = false;
  // pg-suboid only: vector-aware multi-path *tree* traversal (see
  // librados::vector_pg_lsh::query_params_t::tree_boundary_search_budget).
  // A distinct, independent mechanism from server_side_boundary_search
  // above -- never both set. 0 (the default) disables it.
  uint32_t tree_search_budget = 0;
  // Uses index layout pg_lsh_layout_version_raw_euclidean_v1: distance
  // buckets are raw Euclidean distances from a data centroid, the same
  // metric space as top-k tau. Required by --tree-prune, whose subtree
  // exclusion is only safe under this layout.
  bool raw_euclidean_geometry = false;
  // Tree-search-budget only: opt-in real tau-based subtree EXCLUSION
  // (librados::vector_pg_lsh::query_params_t::tree_boundary_search_prune)
  // instead of the existing best-first-reordering-only behavior. Requires
  // --raw-euclidean-geometry and --tree-search-budget > 0.
  bool tree_prune = false;
  // Tree-search-budget only: how many consecutive expansions that fail to
  // narrow tau a probe may make before it stops
  // (query_params_t::tree_boundary_search_no_improve_patience). 0 keeps the
  // existing stop-on-first-non-improving-expansion behaviour.
  uint32_t tree_no_improve_patience = 0;
  // Tree-search-budget only: cap on ONodes the OSD may open per probe
  // (query_params_t::tree_boundary_search_onode_budget). 0 is unlimited.
  uint32_t tree_onode_budget = 0;
  // routing-mode=pivot only: see build_pivot_model()/pivot_rank_pgs() below.
  // write_top_pgs/probe_pgs reuse --d/--m (same "how many PGs" meaning as
  // LSH's write/query fanout).
  uint32_t pivot_build_sample = 16384;
  uint32_t pivot_probe_budget = 12;
  uint32_t top_k = 10;
  uint32_t query_concurrency = 1;
  uint32_t load_concurrency = 1;
  uint32_t warmup_rounds = 1;
  uint32_t rounds = 1;
  uint64_t min_queries = 0;
  uint32_t min_seconds = 0;
  size_t base_limit = 0;
  size_t query_limit = 0;
  std::string query_index_list_path;
  std::string destination_pg_log_path;
  // fanout-query only: "sequential" or "parallel" dispatch of the m probes
  // a single logical query resolves to.
  std::string fanout_mode = "parallel";
  // fanout-query only: RADOS ops kept outstanding per --query-concurrency
  // worker. 0 submits a whole stage and then waits on it, one logical query
  // at a time; N > 0 keeps a rolling window of N probe slots refilled as
  // each completes, filled across several in-flight logical queries. A
  // client-side queue depth, unrelated to seastore's io_concurrency.
  uint32_t io_depth = 0;
  std::string fanout_log_path;
  uint64_t fanout_log_count = 10;
  // fanout-query only: dispatch the m routed PGs in growing cumulative
  // batches instead of all at once, re-examining the merged global top-k
  // between batches (see common/vector_selective_fanout.h).
  // Query-side probe ordering (see librados/vector_placement.h's
  // pg_lsh_v0_query_routing_t). Neither changes placement.
  uint32_t query_table_count = 0;
  bool margin_ordered_probes = false;
  bool selective = false;
  std::string selective_schedule = "8,16,32,64,96";
  // "never" (default) runs every stage of the schedule for every query,
  // which is what a telemetry run wants: one CSV row per (query, stage) so
  // stopping rules can be compared offline against per-stage recall.
  // "tau-relative" replays the candidate rule live.
  std::string selective_stop = "never";
  double selective_min_improvement = 0.02;
  std::string selective_csv_path;
};

struct float_dataset_t {
  uint32_t dimension = 0;
  std::vector<std::vector<float>> rows;
};

struct ivec_dataset_t {
  std::vector<std::vector<uint32_t>> rows;
};

struct load_result_t {
  uint64_t completed = 0;
  uint64_t failures = 0;
  uint64_t put_targets = 0;
  // Rows whose target list --write-rank-begin consumed whole: the routing
  // produced fewer than begin+1 unique PGs for this vector, so a full load
  // would also have written it fewer times and there is no rank-`begin`
  // copy to add. Skipping it is what keeps the two equivalent.
  uint64_t rank_skipped = 0;
  uint64_t target_violations = 0;
  int first_error = 0;
  double seconds = 0;
  std::set<std::string> vector_oids;
  std::string first_oid;
};

struct query_result_t {
  uint64_t completed = 0;
  uint64_t failures = 0;
  uint64_t missing_objects = 0;
  uint64_t empty_results = 0;
  uint64_t unsorted_results = 0;
  uint64_t zero_counter_results = 0;
  uint64_t pg_probes = 0;
  uint64_t object_probes = 0;
  uint64_t matching_records = 0;
  uint64_t distance_computations = 0;
  uint64_t returned_entries = 0;
  uint64_t recall_hits = 0;
  uint64_t recall_denominator = 0;
  int first_error = 0;
  double seconds = 0;
  std::vector<double> latency_us;
  std::string first_oid;
};

void usage(std::ostream& out, const char *program)
{
  out << "Usage: " << program << " --routing-mode MODE --pool POOL [options]\n"
      << "\n"
      << "Native VectorNode PG-LSH benchmark. MODE is pg-container or pg-suboid.\n"
      << "\n"
      << "Dataset: --base FILE --query FILE --groundtruth FILE\n"
      << "Operation: --operation load|query|both|fanout-query"
      << " --base-limit N --query-limit N\n"
      << "  --query-index-list FILE: use exactly these query-file row indices,\n"
      << "    in this order, instead of the first --query-limit rows.\n"
      << "  --destination-pg-log FILE: during the measured query run, record\n"
      << "    ticket,query_index,pg for the first requests (debug/verification).\n"
      << "  fanout-query: one logical query resolves (via --m) to m distinct\n"
      << "    candidate PGs, all queried for the same query vector; results are\n"
      << "    merged (dedup by key + global top-k) into one logical result.\n"
      << "    --fanout-mode sequential|parallel (default parallel)\n"
      << "    --io-depth N: keep N RADOS ops outstanding per worker,\n"
      << "      refilling each slot as it completes, instead of submitting\n"
      << "      a whole stage and waiting on it (default 0).\n"
      << "    --fanout-log FILE --fanout-log-count N: dump the first N logical\n"
      << "      queries' per-subrequest pg/sub_oid/submit_us/complete_us.\n"
      << "    --selective: dispatch the m routed PGs in growing cumulative\n"
      << "      batches, re-examining the merged global top-k between them,\n"
      << "      instead of sending all m at once. Slices ONE routing result\n"
      << "      per query -- no PG is re-routed or re-queried.\n"
      << "      --selective-schedule 8,16,32,64,96 (cumulative; last must\n"
      << "        equal --m)\n"
      << "      --selective-stop never|tau-relative (default never: run every\n"
      << "        stage for every query, which is what --selective-csv wants)\n"
      << "      --selective-min-improvement F (tau-relative only, default 0.02)\n"
      << "      --selective-csv FILE: one row per (query, stage) with d1, tau,\n"
      << "        tau/d1, tau improvement, top-k churn, candidates, latency and\n"
      << "        ground-truth recall, for comparing stopping rules offline.\n"
      << "Routing: --pg-num N --k N --l N --seed N --d N --m N\n"
      << "         --hamming-radius N --distance-bucket-bits N --residual-bits N\n"
      << "         --distance-bucket-radius N --residual-hamming-radius N\n"
      << "         --probe-limit-per-pg N\n"
      << "  --query-table-count N: probe only the first N of the L hash\n"
      << "    tables (0 = all, the default). Query-side only -- placement is\n"
      << "    untouched, so this is safe on an already-loaded index. Data\n"
      << "    written with --d D only ever lands in PGs derived from tables\n"
      << "    [0,D), so probing beyond that spends budget on PGs that cannot\n"
      << "    hold a related vector. Set this to --d.\n"
      << "  --margin-ordered-probes: order perturbations by ascending\n"
      << "    sum-of-|projection| over the bits they flip (query-directed\n"
      << "    multi-probe LSH) instead of by hamming distance then bit index.\n"
      << "  --raw-euclidean-geometry: pg-suboid only, requires\n"
      << "    --distance-bucket-bits > 0. Places sub_oid distance_bucket by\n"
      << "    RAW (non-unit-normalized) Euclidean distance from a\n"
      << "    data-centroid anchor instead of v0's normalized angular\n"
      << "    distance, in the same metric space as top-k tau -- see\n"
      << "    librados/vector_pg_lsh.h's pg_lsh_layout_version_raw_\n"
      << "    euclidean_v1. Required for --tree-prune.\n"
      << "  --tree-search-budget N --tree-prune: real tau-based subtree\n"
      << "    EXCLUSION (not just best-first reordering) within the tree\n"
      << "    search; requires --raw-euclidean-geometry.\n"
      << "  --tree-search-budget N --tree-no-improve-patience N: let a probe\n"
      << "    survive N consecutive expansions that do not narrow tau\n"
      << "    (default 0: stop at the first one).\n"
      << "  --tree-search-budget N --tree-onode-budget N: cap the ONodes the\n"
      << "    OSD opens per probe (default 0: unlimited).\n"
      << "Load: --load-concurrency N --write-rank-begin R\n"
      << "    --write-rank-begin R writes only write-target ranks\n"
      << "    [R, --d), growing an index loaded at --d R to --d D\n"
      << "    without re-PUTting the copies it already holds.\n"
      << "Query: --top-k N --query-concurrency N --warmup-rounds N --rounds N\n"
      << "       --min-queries N --min-seconds N\n"
      << "Ceph: --conf FILE --client NAME --cluster NAME\n";
}

template <typename T>
bool parse_unsigned(std::string_view text, T *value)
{
  static_assert(std::is_unsigned_v<T>);
  if (value == nullptr || text.empty()) {
    return false;
  }
  T parsed = 0;
  const auto result = std::from_chars(
      text.data(), text.data() + text.size(), parsed);
  if (result.ec != std::errc() || result.ptr != text.data() + text.size()) {
    return false;
  }
  *value = parsed;
  return true;
}

bool parse_options(int argc, char **argv, options_t *options)
{
  enum : int {
    OPT_BASE = 1000, OPT_QUERY, OPT_GROUNDTRUTH, OPT_POOL, OPT_BUCKET,
    OPT_INDEX, OPT_KEY_PREFIX, OPT_CLIENT, OPT_CLUSTER, OPT_CONF,
    OPT_OPERATION, OPT_ROUTING_MODE, OPT_PG_NUM, OPT_K, OPT_L, OPT_SEED,
    OPT_D, OPT_M, OPT_HAMMING_RADIUS, OPT_DISTANCE_BUCKET_BITS,
    OPT_RESIDUAL_BITS, OPT_DISTANCE_BUCKET_RADIUS,
    OPT_RESIDUAL_HAMMING_RADIUS, OPT_PROBE_LIMIT_PER_PG,
    OPT_PIVOT_BUILD_SAMPLE, OPT_PIVOT_PROBE_BUDGET, OPT_TOP_K,
    OPT_QUERY_CONCURRENCY, OPT_LOAD_CONCURRENCY, OPT_WRITE_RANK_BEGIN,
    OPT_WARMUP_ROUNDS, OPT_ROUNDS, OPT_MIN_QUERIES,
    OPT_MIN_SECONDS, OPT_BASE_LIMIT, OPT_QUERY_LIMIT,
    OPT_QUERY_INDEX_LIST, OPT_DESTINATION_PG_LOG,
    OPT_FANOUT_MODE, OPT_IO_DEPTH, OPT_FANOUT_LOG, OPT_FANOUT_LOG_COUNT,
    OPT_QUERY_TABLE_COUNT, OPT_MARGIN_ORDERED_PROBES,
    OPT_SELECTIVE, OPT_SELECTIVE_SCHEDULE, OPT_SELECTIVE_STOP,
    OPT_SELECTIVE_MIN_IMPROVEMENT, OPT_SELECTIVE_CSV,
    OPT_SERVER_SIDE_BOUNDARY_SEARCH, OPT_TREE_SEARCH_BUDGET,
    OPT_RAW_EUCLIDEAN_GEOMETRY, OPT_TREE_PRUNE,
    OPT_TREE_NO_IMPROVE_PATIENCE, OPT_TREE_ONODE_BUDGET, OPT_HELP,
  };
  const option long_options[] = {
    {"base", required_argument, nullptr, OPT_BASE},
    {"query", required_argument, nullptr, OPT_QUERY},
    {"groundtruth", required_argument, nullptr, OPT_GROUNDTRUTH},
    {"pool", required_argument, nullptr, OPT_POOL},
    {"bucket", required_argument, nullptr, OPT_BUCKET},
    {"index", required_argument, nullptr, OPT_INDEX},
    {"key-prefix", required_argument, nullptr, OPT_KEY_PREFIX},
    {"client", required_argument, nullptr, OPT_CLIENT},
    {"cluster", required_argument, nullptr, OPT_CLUSTER},
    {"conf", required_argument, nullptr, OPT_CONF},
    {"operation", required_argument, nullptr, OPT_OPERATION},
    {"routing-mode", required_argument, nullptr, OPT_ROUTING_MODE},
    {"pg-num", required_argument, nullptr, OPT_PG_NUM},
    {"k", required_argument, nullptr, OPT_K},
    {"l", required_argument, nullptr, OPT_L},
    {"seed", required_argument, nullptr, OPT_SEED},
    {"d", required_argument, nullptr, OPT_D},
    {"m", required_argument, nullptr, OPT_M},
    {"hamming-radius", required_argument, nullptr, OPT_HAMMING_RADIUS},
    {"distance-bucket-bits", required_argument, nullptr, OPT_DISTANCE_BUCKET_BITS},
    {"residual-bits", required_argument, nullptr, OPT_RESIDUAL_BITS},
    {"distance-bucket-radius", required_argument, nullptr, OPT_DISTANCE_BUCKET_RADIUS},
    {"residual-hamming-radius", required_argument, nullptr, OPT_RESIDUAL_HAMMING_RADIUS},
    {"probe-limit-per-pg", required_argument, nullptr, OPT_PROBE_LIMIT_PER_PG},
    {"pivot-build-sample", required_argument, nullptr, OPT_PIVOT_BUILD_SAMPLE},
    {"pivot-probe-budget", required_argument, nullptr, OPT_PIVOT_PROBE_BUDGET},
    {"top-k", required_argument, nullptr, OPT_TOP_K},
    {"query-concurrency", required_argument, nullptr, OPT_QUERY_CONCURRENCY},
    {"load-concurrency", required_argument, nullptr, OPT_LOAD_CONCURRENCY},
    {"write-rank-begin", required_argument, nullptr, OPT_WRITE_RANK_BEGIN},
    {"warmup-rounds", required_argument, nullptr, OPT_WARMUP_ROUNDS},
    {"rounds", required_argument, nullptr, OPT_ROUNDS},
    {"min-queries", required_argument, nullptr, OPT_MIN_QUERIES},
    {"min-seconds", required_argument, nullptr, OPT_MIN_SECONDS},
    {"base-limit", required_argument, nullptr, OPT_BASE_LIMIT},
    {"query-limit", required_argument, nullptr, OPT_QUERY_LIMIT},
    {"query-index-list", required_argument, nullptr, OPT_QUERY_INDEX_LIST},
    {"destination-pg-log", required_argument, nullptr, OPT_DESTINATION_PG_LOG},
    {"fanout-mode", required_argument, nullptr, OPT_FANOUT_MODE},
    {"io-depth", required_argument, nullptr, OPT_IO_DEPTH},
    {"fanout-log", required_argument, nullptr, OPT_FANOUT_LOG},
    {"fanout-log-count", required_argument, nullptr, OPT_FANOUT_LOG_COUNT},
    {"query-table-count", required_argument, nullptr, OPT_QUERY_TABLE_COUNT},
    {"margin-ordered-probes", no_argument, nullptr, OPT_MARGIN_ORDERED_PROBES},
    {"selective", no_argument, nullptr, OPT_SELECTIVE},
    {"selective-schedule", required_argument, nullptr, OPT_SELECTIVE_SCHEDULE},
    {"selective-stop", required_argument, nullptr, OPT_SELECTIVE_STOP},
    {"selective-min-improvement", required_argument, nullptr,
     OPT_SELECTIVE_MIN_IMPROVEMENT},
    {"selective-csv", required_argument, nullptr, OPT_SELECTIVE_CSV},
    {"server-side-boundary-search", no_argument, nullptr,
     OPT_SERVER_SIDE_BOUNDARY_SEARCH},
    {"tree-search-budget", required_argument, nullptr,
     OPT_TREE_SEARCH_BUDGET},
    {"raw-euclidean-geometry", no_argument, nullptr,
     OPT_RAW_EUCLIDEAN_GEOMETRY},
    {"tree-prune", no_argument, nullptr, OPT_TREE_PRUNE},
    {"tree-no-improve-patience", required_argument, nullptr,
     OPT_TREE_NO_IMPROVE_PATIENCE},
    {"tree-onode-budget", required_argument, nullptr, OPT_TREE_ONODE_BUDGET},
    {"help", no_argument, nullptr, OPT_HELP},
    {nullptr, 0, nullptr, 0},
  };

  for (;;) {
    const int c = getopt_long(argc, argv, "", long_options, nullptr);
    if (c == -1) break;
    switch (c) {
    case OPT_BASE: options->base_path = optarg; break;
    case OPT_QUERY: options->query_path = optarg; break;
    case OPT_GROUNDTRUTH: options->groundtruth_path = optarg; break;
    case OPT_POOL: options->pool = optarg; break;
    case OPT_BUCKET: options->bucket = optarg; break;
    case OPT_INDEX: options->index = optarg; break;
    case OPT_KEY_PREFIX: options->key_prefix = optarg; break;
    case OPT_CLIENT: options->client_name = optarg; break;
    case OPT_CLUSTER: options->cluster_name = optarg; break;
    case OPT_CONF: options->conf_path = optarg; break;
    case OPT_OPERATION: options->operation = optarg; break;
    case OPT_ROUTING_MODE: options->routing_mode = optarg; break;
#define PARSE_OPT(field) \
    if (!parse_unsigned(std::string_view(optarg), &options->field)) return false
    case OPT_PG_NUM: PARSE_OPT(pg_num); break;
    case OPT_K: PARSE_OPT(k); break;
    case OPT_L: PARSE_OPT(l); break;
    case OPT_SEED: PARSE_OPT(seed); break;
    case OPT_D: PARSE_OPT(d); break;
    case OPT_M: PARSE_OPT(m); break;
    case OPT_HAMMING_RADIUS: PARSE_OPT(hamming_radius); break;
    case OPT_DISTANCE_BUCKET_BITS: PARSE_OPT(distance_bucket_bits); break;
    case OPT_RESIDUAL_BITS: PARSE_OPT(residual_bits); break;
    case OPT_DISTANCE_BUCKET_RADIUS: PARSE_OPT(distance_bucket_radius); break;
    case OPT_RESIDUAL_HAMMING_RADIUS: PARSE_OPT(residual_hamming_radius); break;
    case OPT_PROBE_LIMIT_PER_PG: PARSE_OPT(probe_limit_per_pg); break;
    case OPT_PIVOT_BUILD_SAMPLE: PARSE_OPT(pivot_build_sample); break;
    case OPT_PIVOT_PROBE_BUDGET: PARSE_OPT(pivot_probe_budget); break;
    case OPT_TOP_K: PARSE_OPT(top_k); break;
    case OPT_QUERY_CONCURRENCY: PARSE_OPT(query_concurrency); break;
    case OPT_LOAD_CONCURRENCY: PARSE_OPT(load_concurrency); break;
    case OPT_WRITE_RANK_BEGIN: PARSE_OPT(write_rank_begin); break;
    case OPT_WARMUP_ROUNDS: PARSE_OPT(warmup_rounds); break;
    case OPT_ROUNDS: PARSE_OPT(rounds); break;
    case OPT_QUERY_TABLE_COUNT: PARSE_OPT(query_table_count); break;
    case OPT_MIN_QUERIES: PARSE_OPT(min_queries); break;
    case OPT_MIN_SECONDS: PARSE_OPT(min_seconds); break;
    case OPT_BASE_LIMIT: PARSE_OPT(base_limit); break;
    case OPT_QUERY_LIMIT: PARSE_OPT(query_limit); break;
    case OPT_TREE_SEARCH_BUDGET: PARSE_OPT(tree_search_budget); break;
    case OPT_IO_DEPTH: PARSE_OPT(io_depth); break;
    case OPT_TREE_NO_IMPROVE_PATIENCE:
      PARSE_OPT(tree_no_improve_patience); break;
    case OPT_TREE_ONODE_BUDGET: PARSE_OPT(tree_onode_budget); break;
#undef PARSE_OPT
    case OPT_QUERY_INDEX_LIST: options->query_index_list_path = optarg; break;
    case OPT_DESTINATION_PG_LOG: options->destination_pg_log_path = optarg; break;
    case OPT_FANOUT_MODE: options->fanout_mode = optarg; break;
    case OPT_FANOUT_LOG: options->fanout_log_path = optarg; break;
    case OPT_FANOUT_LOG_COUNT:
      if (!parse_unsigned(std::string_view(optarg), &options->fanout_log_count)) {
        return false;
      }
      break;
    case OPT_MARGIN_ORDERED_PROBES:
      options->margin_ordered_probes = true;
      break;
    case OPT_SELECTIVE: options->selective = true; break;
    case OPT_SELECTIVE_SCHEDULE: options->selective_schedule = optarg; break;
    case OPT_SELECTIVE_STOP: options->selective_stop = optarg; break;
    case OPT_SELECTIVE_MIN_IMPROVEMENT:
      options->selective_min_improvement = std::atof(optarg);
      break;
    case OPT_SELECTIVE_CSV: options->selective_csv_path = optarg; break;
    case OPT_SERVER_SIDE_BOUNDARY_SEARCH:
      options->server_side_boundary_search = true;
      break;
    case OPT_RAW_EUCLIDEAN_GEOMETRY:
      options->raw_euclidean_geometry = true;
      break;
    case OPT_TREE_PRUNE:
      options->tree_prune = true;
      break;
    case OPT_HELP: usage(std::cout, argv[0]); std::exit(0);
    default: return false;
    }
  }

  if (optind != argc || options->pool.empty() || options->base_path.empty()) {
    return false;
  }
  if (options->operation != "load" && options->operation != "query" &&
      options->operation != "both" && options->operation != "fanout-query") {
    return false;
  }
  if (options->operation == "fanout-query" &&
      options->fanout_mode != "sequential" && options->fanout_mode != "parallel") {
    return false;
  }
  if (options->routing_mode != "pg-container" &&
      options->routing_mode != "pg-suboid" &&
      options->routing_mode != "pivot") {
    return false;
  }
  if (options->operation != "load" && options->query_path.empty()) {
    return false;
  }
  // m>1 fans one logical query out to m distinct candidate PGs
  // (multi-probe LSH); every other operation keeps today's single-probe
  // (m=1) contract unchanged.
  if (options->pg_num == 0 ||
      (options->pg_num & (options->pg_num - 1)) != 0 ||
      options->d == 0 || options->d > options->pg_num ||
      (options->operation != "fanout-query" && options->m != 1) ||
      (options->operation == "fanout-query" &&
       (options->m == 0 || options->m > options->pg_num)) ||
      options->hamming_radius > options->k) {
    return false;
  }
  if (options->routing_mode == "pg-container" ||
      options->routing_mode == "pivot") {
    // Both are one-object-per-PG (no sub-OID filtering): pivot routing
    // only changes which PG a vector lands in, not the in-PG storage
    // layout.
    if (options->residual_bits != 0 || options->probe_limit_per_pg != 0 ||
        options->distance_bucket_bits != 0 ||
        options->distance_bucket_radius != 0 ||
        options->residual_hamming_radius != 0) {
      return false;
    }
  } else if (options->residual_bits != 4 && options->residual_bits != 0) {
    // residual_bits==0 is legal: the residual wildcard-mask and Hamming
    // radius logic is skipped entirely, which isolates distance_bucket
    // effects in a microbenchmark. 4 stays the default.
    return false;
  }
  if (options->routing_mode == "pivot" &&
      options->pivot_probe_budget == 0) {
    return false;
  }
  // sequential issues one probe at a time through the blocking
  // query_sync() path, so a queue depth means nothing there.
  if (options->io_depth > 0 &&
      (options->operation != "fanout-query" ||
       options->fanout_mode != "parallel")) {
    std::cerr << "error: --io-depth needs --operation fanout-query"
                 " --fanout-mode parallel\n";
    return false;
  }
  // [begin, d) has to be a non-empty range, and any other operation would
  // silently ignore the option.
  if (options->write_rank_begin > 0) {
    if (options->write_rank_begin >= options->d) {
      std::cerr << "error: --write-rank-begin must be < --d\n";
      return false;
    }
    if (options->operation != "load") {
      std::cerr << "error: --write-rank-begin is only valid with"
                   " --operation load\n";
      return false;
    }
  }
  // Boundary-aware search only exists for pg-suboid's sub_oid key space
  // (see common/vector_pg_lsh_boundary.h).
  if (options->server_side_boundary_search &&
      options->routing_mode != "pg-suboid") {
    return false;
  }
  // raw_euclidean_geometry needs a sub_oid distance_bucket axis to place
  // anything into (see librados/vector_pg_lsh.h's
  // pg_lsh_layout_version_raw_euclidean_v1), same precondition as
  // server_side_boundary_search above.
  if (options->raw_euclidean_geometry &&
      (options->routing_mode != "pg-suboid" ||
       options->distance_bucket_bits == 0)) {
    return false;
  }
  // Real tau-based exclusion is only provably safe under raw_euclidean_v1
  // -- see librados/vector_pg_lsh.h's query_params_t::
  // tree_boundary_search_prune / validate_query_params().
  if (options->tree_prune &&
      (!options->raw_euclidean_geometry ||
       options->tree_search_budget == 0)) {
    return false;
  }
  if (options->query_table_count > options->l) {
    return false;
  }
  if (options->selective) {
    if (options->operation != "fanout-query") {
      return false;
    }
    if (options->selective_stop != "never" &&
        options->selective_stop != "tau-relative") {
      return false;
    }
    ceph::rados::vector_selective_fanout::schedule_t schedule;
    if (ceph::rados::vector_selective_fanout::parse_schedule(
          options->selective_schedule, &schedule) < 0) {
      return false;
    }
    // The last stage must be exactly --m. The schedule slices one
    // build_query_probes(m) result, so a tail past m would name PGs that
    // were never routed, and a tail short of m would silently make --m
    // (which labels every number this run reports) a lie.
    if (schedule.back() != options->m) {
      return false;
    }
    if (!(options->selective_min_improvement >= 0.0) ||
        !std::isfinite(options->selective_min_improvement)) {
      return false;
    }
  }
  if (options->routing_mode == "pivot" &&
      options->pivot_build_sample == 0) {
    return false;
  }
  return options->top_k > 0 && options->k > 0 && options->l > 0 &&
    options->query_concurrency > 0 && options->load_concurrency > 0 &&
    options->rounds > 0;
}

uint32_t decode_le32(const unsigned char bytes[4])
{
  return static_cast<uint32_t>(bytes[0]) |
    (static_cast<uint32_t>(bytes[1]) << 8) |
    (static_cast<uint32_t>(bytes[2]) << 16) |
    (static_cast<uint32_t>(bytes[3]) << 24);
}

bool read_exact(std::ifstream& input, void *buffer, size_t length)
{
  input.read(static_cast<char *>(buffer), length);
  return input.gcount() == static_cast<std::streamsize>(length);
}

std::optional<float_dataset_t> read_fvecs(
    const std::string& path, size_t limit, std::string *error)
{
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    *error = "cannot open " + path;
    return std::nullopt;
  }
  float_dataset_t data;
  while (limit == 0 || data.rows.size() < limit) {
    unsigned char encoded[4];
    input.read(reinterpret_cast<char *>(encoded), sizeof(encoded));
    if (input.gcount() == 0 && input.eof()) break;
    if (input.gcount() != static_cast<std::streamsize>(sizeof(encoded))) {
      *error = "truncated dimension in " + path;
      return std::nullopt;
    }
    const uint32_t dimension = decode_le32(encoded);
    if (dimension == 0 || dimension > LIBRADOS_VECTOR_MAX_DIMENSION ||
        (data.dimension != 0 && data.dimension != dimension)) {
      *error = "invalid or inconsistent dimension in " + path;
      return std::nullopt;
    }
    data.dimension = dimension;
    std::vector<float> row(dimension);
    if (!read_exact(input, row.data(), row.size() * sizeof(float))) {
      *error = "truncated vector in " + path;
      return std::nullopt;
    }
    data.rows.push_back(std::move(row));
  }
  if (data.rows.empty()) {
    *error = "empty dataset " + path;
    return std::nullopt;
  }
  return data;
}

std::optional<ivec_dataset_t> read_ivecs(
    const std::string& path, size_t limit, std::string *error)
{
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    *error = "cannot open " + path;
    return std::nullopt;
  }
  ivec_dataset_t data;
  while (limit == 0 || data.rows.size() < limit) {
    unsigned char encoded[4];
    input.read(reinterpret_cast<char *>(encoded), sizeof(encoded));
    if (input.gcount() == 0 && input.eof()) break;
    if (input.gcount() != static_cast<std::streamsize>(sizeof(encoded))) {
      *error = "truncated ivecs dimension in " + path;
      return std::nullopt;
    }
    const uint32_t count = decode_le32(encoded);
    if (count == 0 || count > (1U << 20)) {
      *error = "invalid ivecs dimension in " + path;
      return std::nullopt;
    }
    std::vector<uint32_t> row(count);
    for (uint32_t& value : row) {
      if (!read_exact(input, encoded, sizeof(encoded))) {
        *error = "truncated ivecs row in " + path;
        return std::nullopt;
      }
      value = decode_le32(encoded);
    }
    data.rows.push_back(std::move(row));
  }
  if (data.rows.empty()) {
    *error = "empty ground truth " + path;
    return std::nullopt;
  }
  return data;
}

// Newline-separated 0-based row indices into a query fvecs file. Used to
// build a curated workload (e.g. "every one of these rows routes to PG X")
// instead of run_queries()'s default "first --query-limit rows, cycled".
std::optional<std::vector<size_t>> read_index_list(
    const std::string& path, std::string *error)
{
  std::ifstream input(path);
  if (!input) {
    *error = "cannot open " + path;
    return std::nullopt;
  }
  std::vector<size_t> indices;
  size_t value = 0;
  while (input >> value) indices.push_back(value);
  if (!input.eof()) {
    *error = "malformed index in " + path;
    return std::nullopt;
  }
  if (indices.empty()) {
    *error = "empty index list " + path;
    return std::nullopt;
  }
  return indices;
}

// Selects specific rows (by index, in the given order) out of an
// already-loaded dataset, for --query-index-list.
std::optional<float_dataset_t> select_rows(
    const float_dataset_t& source,
    const std::vector<size_t>& indices,
    std::string *error)
{
  float_dataset_t out;
  out.dimension = source.dimension;
  out.rows.reserve(indices.size());
  for (const size_t index : indices) {
    if (index >= source.rows.size()) {
      *error = "index out of range: " + std::to_string(index);
      return std::nullopt;
    }
    out.rows.push_back(source.rows[index]);
  }
  return out;
}

std::string error_text(int r)
{
  return r < 0 ? std::string(std::strerror(-r)) : "success";
}

std::string vector_key(const options_t& options, size_t index)
{
  std::ostringstream out;
  out << options.key_prefix << std::setw(8) << std::setfill('0') << index;
  return out.str();
}

// entry_id is a deterministic function of (bucket_name, index_name, key)
// computed the same way at PUT time (common/vector_record.h's
// vector_entry_id()) and returned verbatim by every query. Recall compares
// returned entry_ids against the entry_id expected for each groundtruth
// row, rather than parsing entry.key, whose server-side restoration from
// OMAP is unreliable.
std::string expected_entry_id(const options_t& options, uint32_t row)
{
  ceph::rados::put_vector_request_t req;
  req.bucket_name = options.bucket;
  req.index_name = options.index;
  req.key = vector_key(options, row);
  return ceph::os::vector_entry_id(req);
}

ceph::bufferlist vector_buffer(const std::vector<float>& vector)
{
  ceph::bufferlist out;
  out.append(reinterpret_cast<const char *>(vector.data()),
             vector.size() * sizeof(float));
  return out;
}

pg_lsh::pool_pg_info_t pool_info(const options_t& options)
{
  return {options.pg_num, options.pg_num, 0};
}

// Coordinate-wise mean (anchor) and a max-squared-distance scale for
// pg_lsh_layout_version_raw_euclidean_v1 placement, from a strided --base
// sample. A centroid anchor keeps distance_bucket spread: with a
// near-origin anchor, ||v-anchor||^2 is dominated by ||v||^2 alone on
// large-magnitude data such as SIFT1M. scale_max is 1.1x the largest
// sampled ||v-centroid||^2 for bucket spread only; any positive value is
// safe, since overflow clamps into the top bucket.
struct raw_euclidean_anchor_t {
  std::vector<double> anchor;
  double scale_max = 0;
};

raw_euclidean_anchor_t build_raw_euclidean_anchor(
    const float_dataset_t& base, uint32_t build_sample)
{
  raw_euclidean_anchor_t out;
  if (base.rows.empty() || base.dimension == 0) {
    return out;
  }
  const size_t vector_count = base.rows.size();
  const size_t sample_limit = std::min<size_t>(
      vector_count, std::max<size_t>(1, build_sample));
  const size_t stride = std::max<size_t>(1, vector_count / sample_limit);
  std::vector<const std::vector<float> *> samples;
  samples.reserve(sample_limit);
  for (size_t i = 0; i < vector_count && samples.size() < sample_limit;
       i += stride) {
    samples.push_back(&base.rows[i]);
  }
  if (samples.empty()) {
    return out;
  }

  out.anchor.assign(base.dimension, 0.0);
  for (const auto *sample : samples) {
    for (uint32_t d = 0; d < base.dimension; ++d) {
      out.anchor[d] += static_cast<double>((*sample)[d]);
    }
  }
  for (uint32_t d = 0; d < base.dimension; ++d) {
    out.anchor[d] /= static_cast<double>(samples.size());
  }

  double max_sq_dist = 0.0;
  for (const auto *sample : samples) {
    double sq_dist = 0.0;
    for (uint32_t d = 0; d < base.dimension; ++d) {
      const double diff = static_cast<double>((*sample)[d]) - out.anchor[d];
      sq_dist += diff * diff;
    }
    max_sq_dist = std::max(max_sq_dist, sq_dist);
  }
  out.scale_max = max_sq_dist > 0 ? max_sq_dist * 1.1 : 1.0;
  return out;
}

pg_lsh::index_config_t requested_config(const options_t& options,
                                        const float_dataset_t& base)
{
  const uint32_t dimension = base.dimension;
  pg_lsh::index_config_t config;
  config.dimension = dimension;
  config.data_type = ceph::rados::vector_data_type_float32;
  config.distance_metric = ceph::rados::vector_distance_metric_euclidean;
  config.k = options.k;
  config.l = options.l;
  config.seed = options.seed;
  config.d = options.d;
  config.creation_pg_num = options.pg_num;
  config.creation_pgp_num = options.pg_num;
  config.distance_bucket_bits = options.distance_bucket_bits;
  config.residual_bits = options.residual_bits;
  if (options.raw_euclidean_geometry) {
    config.placement_layout_version =
      pg_lsh::pg_lsh_layout_version_raw_euclidean_v1;
    config.anchor_mode = pg_lsh::anchor_mode_centroid;
    const auto built = build_raw_euclidean_anchor(
        base, options.pivot_build_sample);
    config.anchor = built.anchor;
    config.raw_distance_scale_max = built.scale_max;
  } else {
    config.anchor_mode = pg_lsh::anchor_mode_random;
    const int r = librados::vector_placement::pg_lsh_v0_random_anchor(
        dimension, options.seed, &config.anchor);
    if (r < 0) config.anchor.clear();
  }
  return config;
}

pg_lsh::query_params_t query_params(const options_t& options)
{
  pg_lsh::query_params_t params;
  params.hamming_radius = options.hamming_radius;
  params.m = options.m;
  params.distance_bucket_radius = options.distance_bucket_radius;
  params.residual_hamming_radius = options.residual_hamming_radius;
  params.probe_limit_per_pg = options.probe_limit_per_pg;
  params.server_side_boundary_search = options.server_side_boundary_search;
  params.tree_boundary_search_budget = options.tree_search_budget;
  params.tree_boundary_search_prune = options.tree_prune;
  params.tree_boundary_search_no_improve_patience =
      options.tree_no_improve_patience;
  params.tree_boundary_search_onode_budget = options.tree_onode_budget;
  params.query_routing.table_count = options.query_table_count;
  params.query_routing.margin_ordered_probes = options.margin_ordered_probes;
  return params;
}

bool validate_oid(const options_t& options,
                  const std::string& oid,
                  const std::string& sub_oid)
{
  const auto pg_pos = oid.rfind("/pg_");
  if (pg_pos == std::string::npos) return false;
  const auto after_pg = oid.find('/', pg_pos + 1);
  if (options.routing_mode == "pg-container" ||
      options.routing_mode == "pivot") {
    return sub_oid.empty() && after_pg == std::string::npos;
  }
  return !sub_oid.empty() && after_pg != std::string::npos &&
    oid.substr(after_pg + 1) == sub_oid &&
    oid.find('/', after_pg + 1) == std::string::npos;
}

int validate_locator(librados::IoCtx& ioctx, uint32_t expected_pg,
                     const std::string& locator)
{
  uint32_t actual_pg = std::numeric_limits<uint32_t>::max();
  const int r = ioctx.get_object_pg_hash_position2(locator, &actual_pg);
  if (r < 0) return r;
  return actual_pg == expected_pg ? 0 : -ESTALE;
}

// routing-mode=pivot: PG routing by nearest representative (squared L2)
// instead of LSH hashing. Everything downstream of the PG choice is
// unchanged pg-lsh-v0 plumbing; submit_put()/submit_query() only require
// that the locator hashes to the chosen PG.
//
// reps[pg] is the representative for PG `pg`, built by build_pivot_model()
// below and fixed after construction.
struct PivotModel {
  int dim = 0;
  size_t sample_count = 0;
  std::vector<std::vector<float>> reps;
};

inline float l2_dist_sq(const float *a, const float *b, int dim)
{
  float sum = 0.0f;
  for (int i = 0; i < dim; ++i) {
    const float diff = a[i] - b[i];
    sum += diff * diff;
  }
  return sum;
}

// Deterministic stride sampling plus farthest-first traversal: the first
// sample is the first representative, and each next representative is the
// sample farthest from those already chosen, ties going to the smallest
// index.
PivotModel build_pivot_model(const float_dataset_t& base,
                             uint32_t pg_num,
                             uint32_t pivot_build_sample)
{
  PivotModel model;
  model.dim = static_cast<int>(base.dimension);
  if (base.rows.empty() || pg_num == 0 || base.dimension == 0) {
    return model;
  }

  const size_t vector_count = base.rows.size();
  const size_t sample_limit = std::min<size_t>(
      vector_count, std::max<size_t>(1, pivot_build_sample));
  const size_t stride = std::max<size_t>(1, vector_count / sample_limit);
  std::vector<const std::vector<float> *> samples;
  samples.reserve(sample_limit);
  for (size_t i = 0; i < vector_count && samples.size() < sample_limit;
       i += stride) {
    samples.push_back(&base.rows[i]);
  }
  if (samples.empty()) {
    return model;
  }
  model.sample_count = samples.size();

  const size_t pivot_count = std::min<size_t>(pg_num, samples.size());
  model.reps.reserve(pivot_count);

  std::vector<float> min_dist(samples.size(), std::numeric_limits<float>::max());
  model.reps.push_back(*samples[0]);
  for (size_t i = 0; i < samples.size(); ++i) {
    min_dist[i] = l2_dist_sq(samples[i]->data(), samples[0]->data(), model.dim);
  }
  min_dist[0] = 0.0f;

  while (model.reps.size() < pivot_count) {
    size_t best_idx = 0;
    float best_dist = -1.0f;
    for (size_t i = 0; i < samples.size(); ++i) {
      if (min_dist[i] > best_dist) {
        best_dist = min_dist[i];
        best_idx = i;
      }
    }
    if (best_dist <= 0.0f) {
      break;
    }
    model.reps.push_back(*samples[best_idx]);
    min_dist[best_idx] = 0.0f;
    for (size_t i = 0; i < samples.size(); ++i) {
      const float d2 = l2_dist_sq(
          samples[i]->data(), samples[best_idx]->data(), model.dim);
      if (d2 < min_dist[i]) {
        min_dist[i] = d2;
      }
    }
  }

  return model;
}

// Ranks every representative by squared L2 distance to `vec` and returns up
// to `keep_n` PG ids, nearest first (keep_n is pivot_probe_budget; callers
// further slice the first --d (write) or --m (probe) of the result). Ties
// broken by smaller PG id for determinism.
std::vector<uint32_t> pivot_rank_pgs(const float *vec, int dim,
                                     const PivotModel& model, uint32_t keep_n)
{
  struct dist_pg_t { float dist; uint32_t pg; };
  std::vector<dist_pg_t> ranked;
  ranked.reserve(model.reps.size());
  for (size_t pg = 0; pg < model.reps.size(); ++pg) {
    ranked.push_back(
        {l2_dist_sq(vec, model.reps[pg].data(), dim), static_cast<uint32_t>(pg)});
  }
  std::sort(ranked.begin(), ranked.end(),
      [](const dist_pg_t& a, const dist_pg_t& b) {
        if (a.dist != b.dist) return a.dist < b.dist;
        return a.pg < b.pg;
      });
  const size_t keep = std::min<size_t>(keep_n, ranked.size());
  std::vector<uint32_t> out(keep);
  for (size_t i = 0; i < keep; ++i) {
    out[i] = ranked[i].pg;
  }
  return out;
}

// Builds put_target_t/query_probe_t entries for pivot-selected PGs by
// reusing the exact same OID/locator/placement_key construction as
// pg_lsh::build_put_targets()/build_query_probes() (vector_placement.h) --
// only the PG selection differs. One-object-per-PG (empty sub_oid), same as
// routing-mode=pg-container.
int pivot_put_targets(const options_t& options, const ceph::bufferlist& vector_bl,
                      const std::vector<uint32_t>& pgs,
                      pg_lsh::locator_cache_t *locator_cache,
                      std::vector<pg_lsh::put_target_t> *targets)
{
  targets->clear();
  targets->reserve(pgs.size());
  const std::string vector_hash =
    librados::vector_placement::hash_v0_vector_hash(vector_bl);
  for (const uint32_t pg : pgs) {
    std::string locator_key;
    const int r = locator_cache->locator_for_pg(pg, &locator_key);
    if (r < 0) return r;
    targets->push_back({
      pg,
      librados::vector_placement::make_pg_lsh_v0_oid(
          options.bucket, options.index, pg, ""),
      std::move(locator_key),
      librados::vector_placement::pg_lsh_v0_placement_key(pg),
      vector_hash,
      "",
    });
  }
  return 0;
}

int pivot_query_probes(const options_t& options,
                       const std::vector<uint32_t>& pgs,
                       pg_lsh::locator_cache_t *locator_cache,
                       std::vector<pg_lsh::query_probe_t> *probes)
{
  probes->clear();
  probes->reserve(pgs.size());
  for (const uint32_t pg : pgs) {
    std::string locator_key;
    const int r = locator_cache->locator_for_pg(pg, &locator_key);
    if (r < 0) return r;
    probes->push_back({
      pg,
      0,
      0,
      librados::vector_placement::make_pg_lsh_v0_oid(
          options.bucket, options.index, pg, ""),
      std::move(locator_key),
      librados::vector_placement::pg_lsh_v0_placement_key(pg),
      "",
    });
  }
  return 0;
}

// Sequential at load_concurrency=1. Each worker owns its IoCtx and
// locator_cache and shares the precomputed locator_state; rows are handed
// out by an atomic ticket counter, so above concurrency 1 the row that
// "first_oid" reports is whichever finished first. Any failure raises
// "stop" and every worker winds down.
load_result_t load_vectors(const options_t& options,
                           const float_dataset_t& base,
                           librados::Rados& cluster,
                           std::shared_ptr<pg_lsh::locator_state_t> locator_state,
                           const pg_lsh::index_config_t& config,
                           const PivotModel *pivot_model = nullptr)
{
  load_result_t result;
  const auto begin = clock_type::now();
  std::atomic<uint64_t> next{0};
  std::atomic<bool> stop{false};
  std::atomic<int> first_error{0};

  struct worker_result_t {
    load_result_t result;
  };
  std::vector<worker_result_t> worker_results(options.load_concurrency);
  std::vector<std::thread> workers;
  workers.reserve(options.load_concurrency);

  for (uint32_t worker_id = 0; worker_id < options.load_concurrency; ++worker_id) {
    workers.emplace_back([&, worker_id] {
      auto& local = worker_results[worker_id].result;
      librados::IoCtx ioctx;
      int r = cluster.ioctx_create(options.pool.c_str(), ioctx);
      if (r < 0) {
        ++local.failures;
        local.first_error = r;
        int expected = 0;
        first_error.compare_exchange_strong(expected, r);
        stop.store(true);
        return;
      }
      pg_lsh::locator_cache_t locator_cache(
          &ioctx, pool_info(options), options.pool, options.bucket,
          options.index, locator_state);
      r = locator_cache.precompute_all();
      if (r < 0) {
        ++local.failures;
        local.first_error = r;
        int expected = 0;
        first_error.compare_exchange_strong(expected, r);
        stop.store(true);
        return;
      }

      ceph::bufferlist metadata;
      while (!stop.load(std::memory_order_relaxed)) {
        const uint64_t row = next.fetch_add(1, std::memory_order_relaxed);
        if (row >= base.rows.size()) break;

        const auto vector_bl = vector_buffer(base.rows[row]);
        std::vector<pg_lsh::put_target_t> targets;
        int pr;
        if (options.routing_mode == "pivot") {
          const auto ranked = pivot_rank_pgs(
              base.rows[row].data(), pivot_model->dim, *pivot_model,
              options.pivot_probe_budget);
          std::vector<uint32_t> write_pgs(
              ranked.begin(),
              ranked.begin() + std::min<size_t>(options.d, ranked.size()));
          pr = pivot_put_targets(
              options, vector_bl, write_pgs, &locator_cache, &targets);
        } else {
          pr = pg_lsh::build_put_targets(
              options.bucket, options.index, vector_bl, config,
              pool_info(options), &locator_cache, &targets);
        }
        if (options.write_rank_begin > 0 && pr >= 0) {
          if (options.write_rank_begin >= targets.size()) {
            targets.clear();
            ++local.rank_skipped;
            ++local.completed;
            continue;
          }
          targets.erase(targets.begin(),
                        targets.begin() + options.write_rank_begin);
        }
        local.put_targets += targets.size();
        // d>1 write fanout: build_put_targets() already dedups to at most
        // config.d unique PGs, so fewer targets than requested (PG
        // collisions between LSH tables) is expected and reported via
        // actual_put_targets_per_vector, not treated as a failure -- only
        // an empty set or a malformed individual target is.
        bool targets_ok = pr >= 0 && !targets.empty();
        if (targets_ok) {
          for (const auto& target : targets) {
            if (!validate_oid(options, target.oid.name, target.sub_oid_name) ||
                validate_locator(ioctx, target.pg, target.locator_key) < 0) {
              targets_ok = false;
              break;
            }
          }
        }
        if (!targets_ok) {
          ++local.failures;
          ++local.target_violations;
          local.first_error = pr < 0 ? pr : -EINVAL;
          int expected = 0;
          first_error.compare_exchange_strong(expected, local.first_error);
          stop.store(true);
          break;
        }
        if (local.first_oid.empty()) local.first_oid = targets.front().oid.name;
        for (const auto& target : targets) {
          local.vector_oids.insert(target.oid.name);
        }

        bool put_failed = false;
        int put_error = 0;
        for (const auto& target : targets) {
          ceph::rados::put_vector_request_t req;
          req.bucket_name = options.bucket;
          req.index_name = options.index;
          req.key = vector_key(options, row);
          req.data_type = config.data_type;
          req.distance_metric = config.distance_metric;
          req.dimension = config.dimension;
          req.vector_data = vector_bl;
          req.metadata = metadata;
          pr = pg_lsh::put_vector(ioctx, target, std::move(req));
          if (pr < 0) {
            put_failed = true;
            put_error = pr;
            break;
          }
        }
        if (put_failed) {
          ++local.failures;
          local.first_error = put_error;
          int expected = 0;
          first_error.compare_exchange_strong(expected, put_error);
          stop.store(true);
          break;
        }
        ++local.completed;
      }
    });
  }
  for (auto& worker : workers) worker.join();
  result.seconds = std::chrono::duration<double>(clock_type::now() - begin).count();
  result.first_error = first_error.load();
  for (auto& worker : worker_results) {
    auto& local = worker.result;
    result.completed += local.completed;
    result.failures += local.failures;
    result.put_targets += local.put_targets;
    result.rank_skipped += local.rank_skipped;
    result.target_violations += local.target_violations;
    result.vector_oids.insert(local.vector_oids.begin(), local.vector_oids.end());
    if (result.first_oid.empty()) result.first_oid = local.first_oid;
  }
  return result;
}

double percentile(std::vector<double> values, double fraction)
{
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const size_t index = std::min(
      values.size() - 1,
      static_cast<size_t>(std::ceil(fraction * values.size())) - 1);
  return values[index];
}

bool sorted_distances(const std::vector<ceph::rados::query_vectors_result_entry_t>& entries)
{
  for (size_t i = 0; i < entries.size(); ++i) {
    if (!std::isfinite(entries[i].distance) ||
        (i != 0 && entries[i - 1].distance > entries[i].distance)) {
      return false;
    }
  }
  return true;
}

// --destination-pg-log verification sink: records which real PG each of
// the first `cap` requests actually resolved to, so a curated
// --query-index-list workload's PG-fanout can be proven from the live run.
struct dest_pg_log_t {
  std::mutex lock;
  std::ofstream out;
  uint64_t cap = 500;
  uint64_t written = 0;

  void record(uint64_t ticket, size_t query_index, uint32_t pg)
  {
    std::lock_guard<std::mutex> locker(lock);
    if (written >= cap) return;
    out << ticket << ',' << query_index << ',' << pg << '\n';
    ++written;
  }
};

query_result_t run_queries(
    const options_t& options,
    const float_dataset_t& queries,
    const ivec_dataset_t *groundtruth,
    const pg_lsh::index_config_t& config,
    librados::Rados& cluster,
    std::shared_ptr<pg_lsh::locator_state_t> locator_state,
    uint32_t rounds,
    uint64_t min_queries,
    uint32_t min_seconds,
    bool measure,
    dest_pg_log_t *dest_log = nullptr)
{
  query_result_t result;
  const uint64_t fixed_total =
    static_cast<uint64_t>(queries.rows.size()) * rounds;
  std::atomic<uint64_t> next{0};
  std::atomic<bool> stop{false};
  std::atomic<int> first_error{0};
  std::mutex first_oid_lock;
  const auto begin = clock_type::now();

  struct worker_result_t {
    query_result_t result;
  };
  std::vector<worker_result_t> worker_results(options.query_concurrency);
  std::vector<std::thread> workers;
  workers.reserve(options.query_concurrency);

  for (uint32_t worker_id = 0; worker_id < options.query_concurrency; ++worker_id) {
    workers.emplace_back([&, worker_id] {
      auto& local = worker_results[worker_id].result;
      librados::IoCtx ioctx;
      int r = cluster.ioctx_create(options.pool.c_str(), ioctx);
      if (r < 0) {
        local.failures = 1;
        local.first_error = r;
        int expected = 0;
        first_error.compare_exchange_strong(expected, r);
        stop.store(true);
        return;
      }
      pg_lsh::locator_cache_t cache(
          &ioctx, pool_info(options), options.pool, options.bucket,
          options.index, locator_state);
      r = cache.precompute_all();
      if (r < 0) {
        local.failures = 1;
        local.first_error = r;
        int expected = 0;
        first_error.compare_exchange_strong(expected, r);
        stop.store(true);
        return;
      }

      while (!stop.load(std::memory_order_relaxed)) {
        const uint64_t ticket = next.fetch_add(1, std::memory_order_relaxed);
        if (min_queries == 0 && min_seconds == 0) {
          if (ticket >= fixed_total) break;
        } else if (ticket >= min_queries) {
          const double elapsed = std::chrono::duration<double>(
              clock_type::now() - begin).count();
          if (elapsed >= min_seconds) break;
        }
        const size_t query_index = ticket % queries.rows.size();
        const auto query_bl = vector_buffer(queries.rows[query_index]);
        std::vector<pg_lsh::query_probe_t> probes;
        uint64_t generated_groups = 0;
        r = pg_lsh::build_query_probes(
            options.bucket, options.index, query_bl, config,
            query_params(options), pool_info(options), &cache, &probes,
            &generated_groups);
        std::unordered_set<uint32_t> unique_pgs;
        for (const auto& probe : probes) unique_pgs.insert(probe.pg);
        if (r < 0 || generated_groups != config.l || probes.size() != 1 ||
            unique_pgs.size() != 1 ||
            !validate_oid(options, probes.front().oid.name,
                          probes.front().sub_oid_name) ||
            validate_locator(ioctx, probes.front().pg,
                             probes.front().locator_key) < 0) {
          ++local.failures;
          local.first_error = r < 0 ? r : -EINVAL;
          int expected = 0;
          first_error.compare_exchange_strong(expected, local.first_error);
          stop.store(true);
          break;
        }
        ++local.pg_probes;
        ++local.object_probes;
        if (local.first_oid.empty()) local.first_oid = probes.front().oid.name;
        if (dest_log != nullptr) {
          dest_log->record(ticket, query_index, probes.front().pg);
        }

        ceph::rados::query_vectors_request_t req;
        req.bucket_name = options.bucket;
        req.index_name = options.index;
        req.data_type = config.data_type;
        req.distance_metric = config.distance_metric;
        req.dimension = config.dimension;
        req.local_top_k = options.top_k;
        req.query_vector = query_bl;
        pg_lsh::apply_boundary_search_config(config, query_params(options), &req);
        pg_lsh::apply_tree_boundary_search_config(config, query_params(options), &req);
        ceph::bufferlist reply;
        int op_rval = 0;
        const auto query_begin = clock_type::now();
        r = pg_lsh::query_sync(
            ioctx, probes.front(), std::move(req), &reply, &op_rval);
        const auto query_end = clock_type::now();
        if (r == -ENOENT || op_rval == -ENOENT) {
          ++local.missing_objects;
          ++local.empty_results;
          ++local.completed;
          if (measure) {
            local.latency_us.push_back(std::chrono::duration<double, std::micro>(
                query_end - query_begin).count());
          }
          continue;
        }
        if (r < 0 || op_rval < 0 || reply.length() == 0) {
          ++local.failures;
          local.first_error = r < 0 ? r : (op_rval < 0 ? op_rval : -EIO);
          int expected = 0;
          first_error.compare_exchange_strong(expected, local.first_error);
          stop.store(true);
          break;
        }

        ceph::rados::query_vectors_result_t partial;
        try {
          auto p = reply.cbegin();
          decode(partial, p);
          if (!p.end()) throw ceph::buffer::malformed_input("trailing reply");
        } catch (const ceph::buffer::error&) {
          ++local.failures;
          local.first_error = -EIO;
          int expected = 0;
          first_error.compare_exchange_strong(expected, -EIO);
          stop.store(true);
          break;
        }

        if (partial.entries.empty()) ++local.empty_results;
        if (!sorted_distances(partial.entries)) ++local.unsorted_results;
        if (partial.local_matching_entries == 0 ||
            partial.local_distance_computations == 0) {
          ++local.zero_counter_results;
        }
        local.returned_entries += partial.entries.size();
        local.matching_records += partial.local_matching_entries;
        local.distance_computations += partial.local_distance_computations;
        if (groundtruth != nullptr && query_index < groundtruth->rows.size()) {
          const auto& truth = groundtruth->rows[query_index];
          const size_t truth_k = std::min<size_t>(options.top_k, truth.size());
          std::set<std::string> expected;
          for (size_t i = 0; i < truth_k; ++i) {
            expected.insert(expected_entry_id(options, truth[i]));
          }
          for (const auto& entry : partial.entries) {
            if (expected.erase(entry.entry_id) != 0) ++local.recall_hits;
          }
          local.recall_denominator += truth_k;
        }
        ++local.completed;
        if (measure) {
          local.latency_us.push_back(std::chrono::duration<double, std::micro>(
              query_end - query_begin).count());
        }
      }
    });
  }
  for (auto& worker : workers) worker.join();
  result.seconds = std::chrono::duration<double>(clock_type::now() - begin).count();
  result.first_error = first_error.load();
  for (auto& worker : worker_results) {
    auto& local = worker.result;
    result.completed += local.completed;
    result.failures += local.failures;
    result.missing_objects += local.missing_objects;
    result.empty_results += local.empty_results;
    result.unsorted_results += local.unsorted_results;
    result.zero_counter_results += local.zero_counter_results;
    result.pg_probes += local.pg_probes;
    result.object_probes += local.object_probes;
    result.matching_records += local.matching_records;
    result.distance_computations += local.distance_computations;
    result.returned_entries += local.returned_entries;
    result.recall_hits += local.recall_hits;
    result.recall_denominator += local.recall_denominator;
    result.latency_us.insert(result.latency_us.end(),
                             local.latency_us.begin(), local.latency_us.end());
    if (result.first_oid.empty() && !local.first_oid.empty()) {
      result.first_oid = local.first_oid;
    }
  }
  return result;
}

double average(uint64_t value, uint64_t denominator)
{
  return denominator == 0 ? 0.0 :
    static_cast<double>(value) / static_cast<double>(denominator);
}

struct fanout_query_result_t {
  uint64_t completed = 0;
  uint64_t failures = 0;
  uint64_t missing_objects = 0;
  // Entries whose logical key (query_vectors_result_entry_t::key) appeared
  // in more than one probe's reply for the same logical query. --d 1 means
  // every stored vector lives in exactly one physical object, so this is
  // expected to be 0 under these conditions -- reported, not assumed.
  uint64_t duplicate_entries = 0;
  uint64_t returned_entries = 0;
  // Summed across every probe of every completed logical query (i.e. the
  // per-logical-query TOTAL, summed again across queries) -- divide by
  // completed*probe_count for the per-PG average, or by completed alone
  // for the per-logical-query-total average. Never treat this as a
  // per-request count the way single-probe query_result_t's fields are.
  uint64_t probe_matching_records = 0;
  uint64_t probe_distance_computations = 0;
  uint64_t recall_hits = 0;
  uint64_t recall_denominator = 0;
  int first_error = 0;
  std::string first_oid;
  double seconds = 0;
  std::vector<double> latency_us;
  // Pipeline occupancy: without these --io-depth is a knob with no way to
  // tell whether the window ever filled. queue_wait is time a query spent
  // with an issuable probe and no free slot; active is the rest of its
  // end-to-end. inflight_integral/pipeline_wall give the average depth.
  uint64_t queue_wait_us_sum = 0;
  uint64_t active_us_sum = 0;
  double inflight_integral_us = 0;
  double pipeline_wall_us = 0;
  size_t max_inflight = 0;
  // Summed across every probe of every completed logical query that came
  // back with a query_vectors_tree_stats_t, i.e. every tree-search probe.
  uint64_t tree_stats_probes = 0;
  uint64_t tree_primary_candidate_count = 0;
  uint64_t tree_expanded_subtree_count = 0;
  uint64_t tree_pruned_distance_count = 0;
  uint64_t tree_pruned_residual_count = 0;
  uint64_t tree_containment_violations = 0;
  uint64_t tree_opened_onode_count = 0;
  uint64_t tree_candidates_seen = 0;
  // Selective fan-out (--selective). final_m is the cumulative PG count the
  // query stopped at, which under the default stop_mode_t::never is always
  // the schedule's last stage. stages_sum over completed is the average
  // number of sequential round-trips per query, the latency this trades
  // candidates for.
  uint64_t selective_final_m_sum = 0;
  uint64_t selective_stages_sum = 0;
  std::map<uint32_t, uint64_t> selective_final_m_histogram;
};

struct fanout_probe_outcome_t {
  uint32_t pg = 0;
  std::string sub_oid;
  int r = 0;
  int op_rval = 0;
  ceph::bufferlist reply;
  // When this probe became issuable, versus when a slot was actually free
  // for it: submit_us - ready_us is its wait for a slot.
  double ready_us = 0;
  double submit_us = 0;
  double complete_us = 0;
  uint64_t matching_records = 0;
  uint64_t distance_computations = 0;
};

// One logical query resolves through config.m to m distinct candidate PGs,
// i.e. multi-probe LSH fan-out rather than m independent queries.
// fanout_mode picks the dispatch: "sequential" issues and waits one at a
// time, "parallel" submits all m before waiting on any. Both resolve the
// same probe set, since build_query_probes() runs once up front.
fanout_query_result_t run_fanout_queries(
    const options_t& options,
    const float_dataset_t& queries,
    const ivec_dataset_t *groundtruth,
    const pg_lsh::index_config_t& config,
    librados::Rados& cluster,
    std::shared_ptr<pg_lsh::locator_state_t> locator_state,
    uint32_t rounds,
    uint64_t min_queries,
    uint32_t min_seconds,
    bool measure,
    std::ofstream *fanout_log,
    const sf::policy_t& policy,
    std::ofstream *selective_csv = nullptr,
    const PivotModel *pivot_model = nullptr)
{
  fanout_query_result_t result;
  const uint64_t fixed_total =
    static_cast<uint64_t>(queries.rows.size()) * rounds;
  std::atomic<uint64_t> next{0};
  std::atomic<bool> stop{false};
  std::atomic<int> first_error{0};
  std::mutex log_lock;
  const auto begin = clock_type::now();

  struct worker_result_t { fanout_query_result_t result; };
  std::vector<worker_result_t> worker_results(options.query_concurrency);
  std::vector<std::thread> workers;
  workers.reserve(options.query_concurrency);

  for (uint32_t worker_id = 0; worker_id < options.query_concurrency; ++worker_id) {
    workers.emplace_back([&, worker_id] {
      auto& local = worker_results[worker_id].result;
      librados::IoCtx ioctx;
      int r = cluster.ioctx_create(options.pool.c_str(), ioctx);
      if (r < 0) {
        local.failures = 1;
        local.first_error = r;
        int expected = 0;
        first_error.compare_exchange_strong(expected, r);
        stop.store(true);
        return;
      }
      pg_lsh::locator_cache_t cache(
          &ioctx, pool_info(options), options.pool, options.bucket,
          options.index, locator_state);
      r = cache.precompute_all();
      if (r < 0) {
        local.failures = 1;
        local.first_error = r;
        int expected = 0;
        first_error.compare_exchange_strong(expected, r);
        stop.store(true);
        return;
      }

      // Slots are refilled across several in-flight logical queries, not
      // within one: the m probes of a single query all target distinct PGs,
      // so a per-query submit-all/wait-all barrier holds every PG at depth 1
      // and leaves the OSD idle during that query's routing and merge.
      //
      // At --io-depth 0 claim_slot() grows slots without a bound, so the
      // loop submits a whole stage before waiting on it. That is the old
      // behaviour, which is why there is only one dispatch path here.
      const bool sequential_mode = options.fanout_mode == "sequential";
      const bool bounded_depth = options.io_depth > 0;
      const size_t query_slot_count = bounded_depth ? options.io_depth : 1;

      struct probe_slot_t {
        size_t query_slot = 0;
        size_t probe_index = 0;
        size_t outcome_index = 0;
        // Recreated per op: ObjectOperation accumulates, and routed_ioctx
        // must outlive the op, so a slot never reuses one.
        std::unique_ptr<pg_lsh::query_op_state_t> op_state;
        librados::AioCompletion *completion = nullptr;
        // Stable storage for the completion callback's argument.
        void *queue = nullptr;
        size_t self = 0;
      };
      struct query_slot_t {
        bool active = false;
        uint64_t ticket = 0;
        size_t query_index = 0;
        clock_type::time_point query_begin{};
        ceph::bufferlist query_bl;
        std::vector<pg_lsh::query_probe_t> probes;
        std::vector<std::pair<size_t, size_t>> pg_probe_ranges;
        size_t distinct_pgs = 0;
        uint32_t available_pgs = 0;
        std::optional<sf::topk_accumulator_t<
            ceph::rados::query_vectors_result_entry_t>> accumulator;
        std::vector<fanout_probe_outcome_t> outcomes;
        std::vector<fanout_probe_outcome_t> stage_outcomes;
        std::vector<sf::stage_record_t> stage_records;
        std::optional<float> tau_prev;
        size_t stage = 0;
        uint32_t stage_range_begin = 0;
        uint32_t stage_range_end = 0;
        size_t stage_probe_begin = 0;
        size_t next_probe = 0;
        size_t probe_end = 0;
        size_t outstanding = 0;
        double stage_begin_us = 0;
        double stage_end_us = 0;
        bool no_stages = false;
        // Time this query spent with an issuable probe but no free slot.
        double queue_wait_us = 0;
        std::optional<clock_type::time_point> blocked_since;
        uint64_t query_matching = 0;
        uint64_t query_distance = 0;
        uint32_t final_cum_m = 0;
        bool any_missing = false;
        bool decode_error = false;
        bool probe_failed = false;
      };
      // Completions fire on a librados finisher thread: the callback only
      // pushes the slot index and signals. Everything else (get_return_
      // value(), release(), resubmit) happens on this worker thread.
      struct done_queue_t {
        std::mutex lock;
        std::condition_variable cv;
        std::vector<size_t> slots;
        void push(size_t slot) {
          {
            std::lock_guard<std::mutex> locker(lock);
            slots.push_back(slot);
          }
          cv.notify_one();
        }
      };
      done_queue_t done;
      const librados::callback_t probe_complete_cb =
        [](librados::completion_t, void *arg) {
          auto *slot = static_cast<probe_slot_t*>(arg);
          static_cast<done_queue_t*>(slot->queue)->push(slot->self);
        };

      std::vector<std::unique_ptr<query_slot_t>> query_slots;
      query_slots.reserve(query_slot_count);
      for (size_t i = 0; i < query_slot_count; ++i) {
        query_slots.emplace_back(new query_slot_t());
      }
      // Admission order, oldest first: a query that is already in flight
      // always outbids a newer one for a free slot, so head-of-line queries
      // drain instead of every query advancing in lockstep.
      std::vector<size_t> active_order;
      std::vector<std::unique_ptr<probe_slot_t>> slots;
      std::vector<size_t> free_slots;
      size_t outstanding_total = 0;

      // Integral of the outstanding-op count over wall time, so avg_inflight
      // is a time-average rather than a per-completion sample.
      const auto pipeline_begin = clock_type::now();
      auto depth_sampled_at = pipeline_begin;
      double inflight_integral_us = 0;
      size_t max_inflight = 0;
      const auto note_depth = [&]() {
        const auto now = clock_type::now();
        inflight_integral_us +=
          std::chrono::duration<double, std::micro>(now - depth_sampled_at)
            .count() * static_cast<double>(outstanding_total);
        depth_sampled_at = now;
      };

      const auto elapsed_us = [](const query_slot_t& qs,
                                 clock_type::time_point tp) {
        return std::chrono::duration<double, std::micro>(
            tp - qs.query_begin).count();
      };
      const auto build_request = [&](const ceph::bufferlist& query_bl) {
        ceph::rados::query_vectors_request_t req;
        req.bucket_name = options.bucket;
        req.index_name = options.index;
        req.data_type = config.data_type;
        req.distance_metric = config.distance_metric;
        req.dimension = config.dimension;
        req.local_top_k = options.top_k;
        req.query_vector = query_bl;
        pg_lsh::apply_boundary_search_config(
            config, query_params(options), &req);
        pg_lsh::apply_tree_boundary_search_config(
            config, query_params(options), &req);
        return req;
      };

      // Ground-truth recall of a candidate result set, without touching
      // the run-level counters -- the final recall accounting below still
      // owns those. Only called per stage when a CSV is being written,
      // since it rebuilds the expected-id set each time.
      const auto measure_recall =
        [&](size_t query_index,
            const std::vector<ceph::rados::query_vectors_result_entry_t>&
              results) -> std::optional<double> {
        if (groundtruth == nullptr ||
            query_index >= groundtruth->rows.size()) {
          return std::nullopt;
        }
        const auto& truth = groundtruth->rows[query_index];
        const size_t truth_k = std::min<size_t>(options.top_k, truth.size());
        if (truth_k == 0) {
          return std::nullopt;
        }
        std::set<std::string> expected;
        for (size_t i = 0; i < truth_k; ++i) {
          expected.insert(expected_entry_id(options, truth[i]));
        }
        uint64_t hits = 0;
        for (const auto& entry : results) {
          if (expected.erase(entry.entry_id) != 0) ++hits;
        }
        return static_cast<double>(hits) / static_cast<double>(truth_k);
      };

      // Points a query slot at its next non-empty schedule stage. Stages
      // slice by ranked-PG index, never by flat probe index -- see the
      // pg_probe_ranges comment in admit_query() below.
      const auto begin_stage = [&](query_slot_t& qs) -> bool {
        while (qs.stage < policy.schedule.size()) {
          const auto range =
            sf::stage_range(policy.schedule, qs.stage, qs.available_pgs);
          if (range.empty()) {
            ++qs.stage;
            continue;
          }
          qs.stage_range_begin = range.begin;
          qs.stage_range_end = range.end;
          qs.stage_probe_begin = qs.pg_probe_ranges[range.begin].first;
          qs.probe_end = qs.pg_probe_ranges[range.end - 1].second;
          qs.next_probe = qs.stage_probe_begin;
          // assign() once, never resized while the stage is outstanding:
          // submit_query() holds &oc.reply and &oc.op_rval for the life of
          // the op.
          qs.stage_outcomes.assign(qs.probe_end - qs.stage_probe_begin,
                                   fanout_probe_outcome_t{});
          qs.stage_begin_us = elapsed_us(qs, clock_type::now());
          qs.stage_end_us = 0;
          return true;
        }
        return false;
      };

      bool admitting = true;
      const auto admit_query = [&](query_slot_t& qs, size_t slot_index) -> bool {
        if (stop.load(std::memory_order_relaxed)) return false;
        const uint64_t ticket = next.fetch_add(1, std::memory_order_relaxed);
        if (min_queries == 0 && min_seconds == 0) {
          if (ticket >= fixed_total) return false;
        } else if (ticket >= min_queries) {
          const double elapsed = std::chrono::duration<double>(
              clock_type::now() - begin).count();
          if (elapsed >= min_seconds) return false;
        }
        const size_t query_index = ticket % queries.rows.size();
        // Starts before build_query_probes() so routing/LSH computation is
        // included in logical latency, per spec.
        qs.query_begin = clock_type::now();
        qs.ticket = ticket;
        qs.query_index = query_index;
        qs.query_bl = vector_buffer(queries.rows[query_index]);

        qs.probes.clear();
        uint64_t generated_groups = 0;
        int rr = 0;
        if (options.routing_mode == "pivot") {
          const auto ranked = pivot_rank_pgs(
              queries.rows[query_index].data(), pivot_model->dim,
              *pivot_model, options.pivot_probe_budget);
          std::vector<uint32_t> probe_pgs(
              ranked.begin(),
              ranked.begin() + std::min<size_t>(options.m, ranked.size()));
          generated_groups = probe_pgs.size();
          rr = pivot_query_probes(options, probe_pgs, &cache, &qs.probes);
        } else {
          rr = pg_lsh::build_query_probes(
              options.bucket, options.index, qs.query_bl, config,
              query_params(options), pool_info(options), &cache, &qs.probes,
              &generated_groups);
        }
        (void)generated_groups;
        std::unordered_set<uint32_t> unique_pgs;
        for (const auto& probe : qs.probes) unique_pgs.insert(probe.pg);
        qs.distinct_pgs = unique_pgs.size();
        // Checked on every query, not just the logged sample.
        // probe_limit_per_pg (and distance_bucket_radius/residual_hamming_
        // radius) can make probes.size() exceed options.m -- several sub_oid
        // probes per PG -- so only the PG count is checked, not a 1:1 ratio.
        if (rr < 0 || unique_pgs.size() != options.m || qs.probes.empty() ||
            !validate_oid(options, qs.probes.front().oid.name,
                          qs.probes.front().sub_oid_name)) {
          ++local.failures;
          local.first_error = rr < 0 ? rr : -EINVAL;
          int expected = 0;
          first_error.compare_exchange_strong(expected, local.first_error);
          stop.store(true);
          return false;
        }
        if (local.first_oid.empty()) {
          local.first_oid = qs.probes.front().oid.name;
        }

        // build_query_probes() emits probes PG-major: the outer loop walks
        // the ranked PGs in order and the inner loop emits that PG's
        // sub_oid probes, so one ranked PG's probes are always adjacent.
        // Stages therefore slice by ranked-PG index, never by flat probe
        // index -- the latter would cut a PG in half the moment
        // distance_bucket_radius/residual_hamming_radius puts more than one
        // sub_oid probe on it.
        qs.pg_probe_ranges.clear();
        for (size_t i = 0; i < qs.probes.size(); ++i) {
          if (!qs.pg_probe_ranges.empty() &&
              qs.probes[i].pg == qs.probes[qs.pg_probe_ranges.back().first].pg) {
            qs.pg_probe_ranges.back().second = i + 1;
          } else {
            qs.pg_probe_ranges.emplace_back(i, i + 1);
          }
        }
        qs.available_pgs = static_cast<uint32_t>(qs.pg_probe_ranges.size());

        qs.accumulator.emplace(options.top_k);
        qs.outcomes.clear();
        qs.stage_outcomes.clear();
        qs.stage_records.clear();
        qs.tau_prev.reset();
        qs.stage = 0;
        qs.next_probe = 0;
        qs.probe_end = 0;
        qs.outstanding = 0;
        qs.queue_wait_us = 0;
        qs.blocked_since.reset();
        qs.query_matching = 0;
        qs.query_distance = 0;
        qs.final_cum_m = 0;
        qs.any_missing = false;
        qs.decode_error = false;
        qs.probe_failed = false;

        qs.no_stages = !begin_stage(qs);
        if (qs.no_stages) {
          // Empty schedule against the routed PGs: nothing to dispatch.
          qs.next_probe = 0;
          qs.probe_end = 0;
        }
        qs.active = true;
        active_order.push_back(slot_index);
        return true;
      };

      // A free slot, growing the pool when --io-depth is unbounded.
      const auto claim_slot = [&]() -> probe_slot_t* {
        if (free_slots.empty()) {
          if (bounded_depth && slots.size() >= options.io_depth) {
            return nullptr;
          }
          slots.emplace_back(new probe_slot_t());
          slots.back()->queue = &done;
          slots.back()->self = slots.size() - 1;
          free_slots.push_back(slots.size() - 1);
        }
        const size_t index = free_slots.back();
        free_slots.pop_back();
        return slots[index].get();
      };

      // Issues every probe that can be issued right now, oldest query
      // first. Called before any decode/merge work so a slot is never idle
      // while this thread is busy on the CPU.
      const auto submit_ready = [&]() {
        for (const size_t qi : active_order) {
          auto& qs = *query_slots[qi];
          if (!qs.active) continue;
          while (qs.next_probe < qs.probe_end) {
            if (sequential_mode) {
              const size_t pi = qs.next_probe++;
              auto& oc = qs.stage_outcomes[pi - qs.stage_probe_begin];
              oc.pg = qs.probes[pi].pg;
              oc.sub_oid = qs.probes[pi].sub_oid_name;
              oc.ready_us = qs.stage_begin_us;
              oc.submit_us = elapsed_us(qs, clock_type::now());
              oc.r = pg_lsh::query_sync(
                  ioctx, qs.probes[pi], build_request(qs.query_bl),
                  &oc.reply, &oc.op_rval);
              oc.complete_us = elapsed_us(qs, clock_type::now());
              if (oc.r < 0 && oc.r != -ENOENT) {
                qs.probe_failed = true;
                qs.next_probe = qs.probe_end;
              }
              continue;
            }
            probe_slot_t *slot = claim_slot();
            if (slot == nullptr) break;
            const size_t pi = qs.next_probe++;
            slot->query_slot = qi;
            slot->probe_index = pi;
            slot->outcome_index = pi - qs.stage_probe_begin;
            slot->op_state.reset(new pg_lsh::query_op_state_t());
            slot->completion = librados::Rados::aio_create_completion(
                slot, probe_complete_cb);
            auto& oc = qs.stage_outcomes[slot->outcome_index];
            oc.pg = qs.probes[pi].pg;
            oc.sub_oid = qs.probes[pi].sub_oid_name;
            oc.ready_us = qs.stage_begin_us;
            oc.submit_us = elapsed_us(qs, clock_type::now());
            note_depth();
            ++outstanding_total;
            ++qs.outstanding;
            if (outstanding_total > max_inflight) {
              max_inflight = outstanding_total;
            }
            oc.r = pg_lsh::submit_query(
                ioctx, qs.probes[pi], build_request(qs.query_bl),
                slot->op_state.get(), slot->completion, &oc.reply,
                &oc.op_rval);
            if (oc.r != 0) {
              // Never reached the wire, so no callback will ever fire for
              // this slot: retire it here.
              note_depth();
              --outstanding_total;
              --qs.outstanding;
              oc.complete_us = elapsed_us(qs, clock_type::now());
              slot->completion->release();
              slot->completion = nullptr;
              free_slots.push_back(slot->self);
              if (oc.r != -ENOENT) qs.probe_failed = true;
            }
          }
          if (qs.stage_end_us == 0 && qs.outstanding == 0 &&
              qs.next_probe >= qs.probe_end) {
            qs.stage_end_us = elapsed_us(qs, clock_type::now());
          }
          // Queue wait is only the time this query was starved for a slot
          // -- not the time its own ops were on the wire.
          const bool blocked = qs.next_probe < qs.probe_end;
          const auto now = clock_type::now();
          if (blocked && !qs.blocked_since) {
            qs.blocked_since = now;
          } else if (!blocked && qs.blocked_since) {
            qs.queue_wait_us += std::chrono::duration<double, std::micro>(
                now - *qs.blocked_since).count();
            qs.blocked_since.reset();
          }
        }
      };

      const auto reap_done = [&]() {
        std::vector<size_t> reaped;
        {
          std::unique_lock<std::mutex> locker(done.lock);
          done.cv.wait(locker, [&] { return !done.slots.empty(); });
          reaped.swap(done.slots);
        }
        for (const size_t slot_index : reaped) {
          auto& slot = *slots[slot_index];
          auto& qs = *query_slots[slot.query_slot];
          auto& oc = qs.stage_outcomes[slot.outcome_index];
          oc.r = slot.completion->get_return_value();
          oc.complete_us = elapsed_us(qs, clock_type::now());
          slot.completion->release();
          slot.completion = nullptr;
          note_depth();
          --outstanding_total;
          --qs.outstanding;
          if (qs.outstanding == 0 && qs.next_probe >= qs.probe_end) {
            qs.stage_end_us = oc.complete_us;
          }
          if (oc.r < 0 && oc.r != -ENOENT) qs.probe_failed = true;
          free_slots.push_back(slot_index);
        }
      };

      // Everything that happens once a stage's probes have all landed:
      // decode, per-probe stats, global top-k merge, stage record, and the
      // selective-fan-out stop decision. Returns true when the logical
      // query is finished (schedule exhausted, stop rule fired, or the
      // stage failed). Stage decisions stay strictly sequential WITHIN a
      // query -- only distinct logical queries ever overlap -- so the
      // candidate set is identical to the pre-pipeline dispatcher's.
      const auto finish_stage = [&](query_slot_t& qs) -> bool {
        auto& accumulator = *qs.accumulator;
        auto& stage_outcomes = qs.stage_outcomes;
        auto& outcomes = qs.outcomes;
        auto& stage_records = qs.stage_records;
        auto& tau_prev = qs.tau_prev;
        auto& any_missing = qs.any_missing;
        auto& decode_error = qs.decode_error;
        auto& query_matching = qs.query_matching;
        auto& query_distance = qs.query_distance;
        auto& final_cum_m = qs.final_cum_m;
        const size_t stage = qs.stage;
        const sf::stage_range_t range{qs.stage_range_begin, qs.stage_range_end};
        const double stage_begin_us = qs.stage_begin_us;
        const double stage_end_us = qs.stage_end_us;
        if (qs.no_stages) {
          return true;
        }
        if (qs.probe_failed) {
          return true;
        }
        std::vector<ceph::rados::query_vectors_result_entry_t> stage_entries;
        uint64_t stage_matching = 0;
        uint64_t stage_distance = 0;
        for (auto& oc : stage_outcomes) {
          if (oc.r == -ENOENT || oc.op_rval == -ENOENT) {
            any_missing = true;
            continue;
          }
          if (oc.reply.length() == 0) { decode_error = true; break; }
          ceph::rados::query_vectors_result_t partial;
          try {
            auto p = oc.reply.cbegin();
            decode(partial, p);
            if (!p.end()) throw ceph::buffer::malformed_input("trailing reply");
          } catch (const ceph::buffer::error&) { decode_error = true; break; }
          oc.matching_records = partial.local_matching_entries;
          oc.distance_computations = partial.local_distance_computations;
          stage_matching += partial.local_matching_entries;
          stage_distance += partial.local_distance_computations;
          if (partial.tree_stats) {
            const auto& ts = *partial.tree_stats;
            ++local.tree_stats_probes;
            local.tree_primary_candidate_count += ts.primary_candidate_count;
            local.tree_expanded_subtree_count += ts.expanded_subtree_count;
            local.tree_pruned_distance_count += ts.pruned_distance_count;
            local.tree_pruned_residual_count += ts.pruned_residual_count;
            local.tree_opened_onode_count += ts.opened_onode_count;
            local.tree_candidates_seen += ts.candidates_seen;
            local.tree_containment_violations += ts.containment_violations;
          }
          for (auto& entry : partial.entries) {
            stage_entries.push_back(std::move(entry));
          }
        }
        if (decode_error) {
          return true;
        }

        // Global merge dedups by entry_id, not entry.key. entry.key is
        // restored server-side from OMAP per entry_id (pg_backend.cc's
        // query_vectors handler) and that restoration is currently
        // unreliable (separate, undiagnosed issue -- see
        // expected_entry_id()'s comment above); entry_id itself is
        // returned verbatim from the LIST scan and does not depend on it.
        const uint64_t dupes_before = accumulator.dropped_duplicates();
        const auto merge_begin = clock_type::now();
        const uint32_t replacements = accumulator.merge_stage(stage_entries);
        local.duplicate_entries +=
          accumulator.dropped_duplicates() - dupes_before;

        query_matching += stage_matching;
        query_distance += stage_distance;
        final_cum_m = range.end;

        sf::stage_record_t record;
        record.stage = stage;
        record.cum_m = range.end;
        record.batch_pg_count = range.count();
        record.d1 = accumulator.current_d1();
        record.tau = accumulator.current_tau();
        record.topk_replacements = replacements;
        record.topk_size =
          static_cast<uint32_t>(accumulator.results().size());
        record.batch_candidates = stage_matching;
        record.cum_candidates = query_matching;
        record.batch_distance_computations = stage_distance;
        record.cum_distance_computations = query_distance;
        record.batch_latency_us = stage_end_us - stage_begin_us;
        record.cum_latency_us = stage_end_us;
        if (selective_csv != nullptr) {
          record.recall =
            measure_recall(qs.query_index, accumulator.results());
        }
        sf::finalize_stage_record(tau_prev, &record);
        tau_prev = record.tau;

        outcomes.insert(outcomes.end(),
                        std::make_move_iterator(stage_outcomes.begin()),
                        std::make_move_iterator(stage_outcomes.end()));
        stage_records.push_back(std::move(record));

        if (!sf::should_expand(policy, stage_records.back())) {
          return true;
        }
        ++qs.stage;
        return !begin_stage(qs);
      };

      // Retires a finished logical query. The end-to-end latency keeps its
      // old meaning exactly -- query_begin to query_end, slot queueing
      // included -- and the queue/active split is reported alongside it so
      // a depth sweep stays comparable with pre-pipeline runs.
      const auto complete_query = [&](query_slot_t& qs) {
        auto& accumulator = *qs.accumulator;
        const auto& probes = qs.probes;
        const auto& outcomes = qs.outcomes;
        const auto& stage_records = qs.stage_records;
        const size_t query_index = qs.query_index;
        const uint64_t ticket = qs.ticket;
        const auto& query_bl = qs.query_bl;
        const uint32_t final_cum_m = qs.final_cum_m;
        if (qs.blocked_since) {
          qs.queue_wait_us += std::chrono::duration<double, std::micro>(
              clock_type::now() - *qs.blocked_since).count();
          qs.blocked_since.reset();
        }
        if (qs.probe_failed || qs.decode_error) {
          ++local.failures;
          local.first_error = -EIO;
          int expected = 0;
          first_error.compare_exchange_strong(expected, -EIO);
          stop.store(true);
          return;
        }

        const auto& merged = accumulator.results();
        if (qs.any_missing && merged.empty()) ++local.missing_objects;
        local.returned_entries += merged.size();
        local.probe_matching_records += qs.query_matching;
        local.probe_distance_computations += qs.query_distance;
        local.selective_final_m_sum += final_cum_m;
        local.selective_stages_sum += stage_records.size();
        ++local.selective_final_m_histogram[final_cum_m];
        if (groundtruth != nullptr && query_index < groundtruth->rows.size()) {
          const auto& truth = groundtruth->rows[query_index];
          const size_t truth_k = std::min<size_t>(options.top_k, truth.size());
          std::set<std::string> expected;
          for (size_t i = 0; i < truth_k; ++i) {
            expected.insert(expected_entry_id(options, truth[i]));
          }
          for (const auto& entry : merged) {
            if (expected.erase(entry.entry_id) != 0) ++local.recall_hits;
          }
          local.recall_denominator += truth_k;
        }

        const auto query_end = clock_type::now();
        const double end_to_end_us = std::chrono::duration<double, std::micro>(
            query_end - qs.query_begin).count();
        ++local.completed;
        // queue_wait: time this query had an issuable probe but no free
        // slot. active = end-to-end minus that -- the span where its ops
        // were actually on the wire, server-side queueing included.
        const double queue_wait_us = std::min(qs.queue_wait_us, end_to_end_us);
        local.queue_wait_us_sum += static_cast<uint64_t>(queue_wait_us);
        local.active_us_sum +=
            static_cast<uint64_t>(end_to_end_us - queue_wait_us);
        if (measure) {
          local.latency_us.push_back(end_to_end_us);
        }
        if (selective_csv != nullptr) {
          std::ostringstream rows;
          for (const auto& record : stage_records) {
            sf::write_stage_csv_row(rows, query_index, record);
          }
          std::lock_guard<std::mutex> locker(log_lock);
          *selective_csv << rows.str();
        }

        if (fanout_log != nullptr && ticket < options.fanout_log_count) {
          std::ostringstream block;
          block << "logical_query_id=" << ticket << '\n'
                << "query_index=" << query_index << '\n'
                << "query_vector_hash="
                << librados::vector_placement::hash_v0_vector_hash(query_bl) << '\n'
                << "mode=" << options.fanout_mode << '\n'
                << "target_pg_count=" << probes.size() << '\n'
                << "target_pgs=[";
          for (size_t i = 0; i < probes.size(); ++i) {
            if (i != 0) block << ',';
            block << probes[i].pg;
          }
          block << "]\n"
                << "runtime_distinct_pgs=" << qs.distinct_pgs << '\n'
                << "stages_run=" << stage_records.size() << '\n'
                << "final_cum_m=" << final_cum_m << '\n';
          for (const auto& record : stage_records) {
            block << "stage " << record.stage
                  << " -> cum_m=" << record.cum_m
                  << " batch_pgs=" << record.batch_pg_count
                  << " tau=";
            if (record.tau) block << *record.tau; else block << "unfilled";
            block << " topk_replacements=" << record.topk_replacements
                  << " batch_candidates=" << record.batch_candidates
                  << " batch_latency_us=" << record.batch_latency_us << '\n';
          }
          for (size_t i = 0; i < outcomes.size(); ++i) {
            block << "subrequest " << i << " -> pg=" << outcomes[i].pg
                  << " sub_oid=" << outcomes[i].sub_oid
                  << " min_hamming_distance=" << probes[i].min_hamming_distance
                  // table_vote_count is currently always 1 (pg_lsh_v0_select_
                  // unique_pgs() never aggregates votes across tables despite
                  // the field name) -- logged for reference only, not a real
                  // confidence signal.
                  << " table_vote_count_ref_only=" << probes[i].table_vote_count
                  << " matching_records=" << outcomes[i].matching_records
                  << " distance_computations=" << outcomes[i].distance_computations
                  << " r=" << outcomes[i].r
                  << " op_rval=" << outcomes[i].op_rval
                  << " ready_us=" << outcomes[i].ready_us
                  << " submit_us=" << outcomes[i].submit_us
                  << " complete_us=" << outcomes[i].complete_us;
            block << '\n';
          }
          std::lock_guard<std::mutex> locker(log_lock);
          *fanout_log << block.str() << "---\n";
          fanout_log->flush();
        }
      };

      // Finishes every query whose current stage has fully landed. Runs
      // after submit_ready(), never before it.
      const auto finish_ready_stages = [&]() {
        for (size_t idx = 0; idx < active_order.size(); ) {
          auto& qs = *query_slots[active_order[idx]];
          if (qs.outstanding != 0 || qs.next_probe < qs.probe_end) {
            ++idx;
            continue;
          }
          if (finish_stage(qs)) {
            complete_query(qs);
            qs.active = false;
            qs.probes.clear();
            qs.outcomes.clear();
            qs.stage_outcomes.clear();
            qs.accumulator.reset();
            active_order.erase(active_order.begin() + idx);
          } else {
            ++idx;
          }
        }
      };

      for (;;) {
        submit_ready();
        while (admitting) {
          size_t idle_slot = query_slots.size();
          for (size_t i = 0; i < query_slots.size(); ++i) {
            if (!query_slots[i]->active) { idle_slot = i; break; }
          }
          if (idle_slot == query_slots.size()) break;
          // Only admit when a probe slot is actually available: admitting
          // into a saturated pipeline would grow the backlog (and the
          // reported end-to-end latency) without raising queue depth.
          if (!sequential_mode && free_slots.empty() && bounded_depth &&
              slots.size() >= options.io_depth) {
            break;
          }
          if (!admit_query(*query_slots[idle_slot], idle_slot)) {
            admitting = false;
            break;
          }
          submit_ready();
        }
        if (active_order.empty() && outstanding_total == 0) {
          break;
        }
        if (outstanding_total != 0) {
          reap_done();
          // Refill before any decode/merge work so no slot idles while
          // this thread is on the CPU -- this ordering is what actually
          // holds the queue depth.
          submit_ready();
        }
        finish_ready_stages();
      }
      // Callbacks reference worker-local slots, so nothing may go out of
      // scope while an op is still outstanding. The loop above only exits
      // at zero, but a future early return must drain here too.
      while (outstanding_total != 0) {
        reap_done();
      }
      note_depth();
      local.inflight_integral_us += inflight_integral_us;
      local.pipeline_wall_us += std::chrono::duration<double, std::micro>(
          clock_type::now() - pipeline_begin).count();
      if (max_inflight > local.max_inflight) local.max_inflight = max_inflight;
    });
  }
  for (auto& worker : workers) worker.join();
  result.seconds = std::chrono::duration<double>(clock_type::now() - begin).count();
  result.first_error = first_error.load();
  for (auto& worker : worker_results) {
    auto& local = worker.result;
    result.completed += local.completed;
    result.failures += local.failures;
    result.missing_objects += local.missing_objects;
    result.duplicate_entries += local.duplicate_entries;
    result.returned_entries += local.returned_entries;
    result.probe_matching_records += local.probe_matching_records;
    result.probe_distance_computations += local.probe_distance_computations;
    result.recall_hits += local.recall_hits;
    result.recall_denominator += local.recall_denominator;
    result.latency_us.insert(result.latency_us.end(),
                             local.latency_us.begin(), local.latency_us.end());
    if (result.first_oid.empty() && !local.first_oid.empty()) {
      result.first_oid = local.first_oid;
    }
    result.tree_stats_probes += local.tree_stats_probes;
    result.tree_primary_candidate_count += local.tree_primary_candidate_count;
    result.tree_expanded_subtree_count += local.tree_expanded_subtree_count;
    result.tree_pruned_distance_count += local.tree_pruned_distance_count;
    result.tree_pruned_residual_count += local.tree_pruned_residual_count;
    result.tree_opened_onode_count += local.tree_opened_onode_count;
    result.tree_candidates_seen += local.tree_candidates_seen;
    result.tree_containment_violations += local.tree_containment_violations;
    result.selective_final_m_sum += local.selective_final_m_sum;
    result.selective_stages_sum += local.selective_stages_sum;
    for (const auto& [final_m, count] : local.selective_final_m_histogram) {
      result.selective_final_m_histogram[final_m] += count;
    }
    result.queue_wait_us_sum += local.queue_wait_us_sum;
    result.active_us_sum += local.active_us_sum;
    result.inflight_integral_us += local.inflight_integral_us;
    result.pipeline_wall_us += local.pipeline_wall_us;
    result.max_inflight = std::max(result.max_inflight, local.max_inflight);
  }
  return result;
}

void print_fanout_result(const options_t& options, const fanout_query_result_t& result)
{
  const double recall = result.recall_denominator == 0 ? 0.0 :
    static_cast<double>(result.recall_hits) /
      static_cast<double>(result.recall_denominator);
  std::cout << std::fixed << std::setprecision(6)
            << "FANOUT_QUERY mode=" << options.fanout_mode
            << " probe_pg_count=" << options.m
            << " subrequests_per_logical_query=" << options.m
            << " completed=" << result.completed
            << " failures=" << result.failures
            << " missing_objects=" << result.missing_objects
            << " duplicate_entries=" << result.duplicate_entries
            << " seconds=" << result.seconds
            << " logical_qps=" << static_cast<double>(result.completed) /
                 (result.seconds > 0 ? result.seconds : 1)
            << " logical_latency_us_avg=" << (result.latency_us.empty() ? 0.0 :
                 std::accumulate(result.latency_us.begin(), result.latency_us.end(), 0.0) /
                   result.latency_us.size())
            << " logical_latency_us_p50=" << percentile(result.latency_us, 0.50)
            << " logical_latency_us_p95=" << percentile(result.latency_us, 0.95)
            << " logical_latency_us_p99=" << percentile(result.latency_us, 0.99)
            << " avg_matching_records_per_pg="
            << average(result.probe_matching_records, result.completed * options.m)
            << " avg_distance_computations_per_pg="
            << average(result.probe_distance_computations, result.completed * options.m)
            << " avg_total_matching_records_per_query="
            << average(result.probe_matching_records, result.completed)
            << " avg_total_distance_computations_per_query="
            << average(result.probe_distance_computations, result.completed)
            << " recall_at_top_k=" << recall
            << " avg_returned_results=" << average(result.returned_entries, result.completed);
  if (options.selective) {
    // Selective fan-out wins when recall holds near the one-shot baseline
    // while avg_final_m sits well below --m. avg_stages is the cost: each
    // stage is one more round-trip, so p50 rises even as candidates fall.
    // Under the default --selective-stop never both follow the schedule.
    std::cout << " selective_schedule=" << options.selective_schedule
              << " selective_stop=" << options.selective_stop
              << " avg_final_m="
              << average(result.selective_final_m_sum, result.completed)
              << " avg_stages="
              << average(result.selective_stages_sum, result.completed)
              << " final_m_histogram=[";
    bool first = true;
    for (const auto& [final_m, count] : result.selective_final_m_histogram) {
      if (!first) std::cout << ',';
      first = false;
      std::cout << final_m << ':' << count;
    }
    std::cout << ']';
  }
  if (result.tree_stats_probes > 0) {
    // Averages are per logical query (i.e. summed across the --m probes
    // of one query, matching avg_total_*_per_query's own convention
    // above), not per-probe -- see fanout_query_result_t's tree_* field
    // comments.
    std::cout
        << " tree_primary_candidates_per_query="
        << average(result.tree_primary_candidate_count, result.completed)
        << " tree_expanded_subtrees_per_query="
        << average(result.tree_expanded_subtree_count, result.completed)
        << " tree_pruned_distance_per_query="
        << average(result.tree_pruned_distance_count, result.completed)
        << " tree_pruned_residual_per_query="
        << average(result.tree_pruned_residual_count, result.completed)
        << " tree_containment_violations="
        << result.tree_containment_violations
        << " tree_opened_onodes_per_query="
        << average(result.tree_opened_onode_count, result.completed)
        << " tree_candidates_seen_per_query="
        << average(result.tree_candidates_seen, result.completed);
  }
  // avg_inflight is the time-average outstanding-op count per worker: it is
  // what shows the depth was actually held, and slot_idle_ratio how much of
  // the configured depth went unused. avg_queue_wait_us is the part of
  // end-to-end latency a query spent waiting for a slot; avg_active_us is
  // the rest, which still contains server-side queueing.
  {
    const double avg_inflight = result.pipeline_wall_us > 0 ?
        result.inflight_integral_us / result.pipeline_wall_us : 0.0;
    std::cout
        << " io_depth=" << options.io_depth
        << " avg_queue_wait_us="
        << average(result.queue_wait_us_sum, result.completed)
        << " avg_active_us="
        << average(result.active_us_sum, result.completed)
        << " avg_inflight=" << avg_inflight
        << " max_inflight=" << result.max_inflight;
    if (options.io_depth > 0) {
      std::cout << " slot_idle_ratio="
                << (1.0 - avg_inflight / static_cast<double>(options.io_depth));
    }
  }
  std::cout << '\n';
}

void print_load_result(const load_result_t& result)
{
  std::cout << std::fixed << std::setprecision(6)
            << "LOAD completed=" << result.completed
            << " failures=" << result.failures
            << " seconds=" << result.seconds
            << " actual_put_targets_per_vector="
            << average(result.put_targets, result.completed + result.failures)
            << " rank_skipped=" << result.rank_skipped
            << " target_violations=" << result.target_violations
            << " vector_object_count=" << result.vector_oids.size() << '\n'
            << "PUT_OID_EXAMPLE " << result.first_oid << '\n';
}

void print_query_result(const options_t& options,
                        const query_result_t& result)
{
  const double qps = result.seconds == 0 ? 0 : result.completed / result.seconds;
  const double latency_avg = result.latency_us.empty() ? 0 :
    std::accumulate(result.latency_us.begin(), result.latency_us.end(), 0.0) /
    result.latency_us.size();
  const double recall = result.recall_denominator == 0 ? 0 :
    static_cast<double>(result.recall_hits) / result.recall_denominator;
  const std::string name = options.routing_mode == "pg-container" ?
    "PG-level one-PG-container exact scan" :
    "Object-level one-object exact scan (B_total=7)";

  std::cout << std::fixed << std::setprecision(6)
            << "RESULT_NAME " << name << '\n'
            << "QUERY completed=" << result.completed
            << " failures=" << result.failures
            << " missing_objects=" << result.missing_objects
            << " empty_results=" << result.empty_results
            << " unsorted_results=" << result.unsorted_results
            << " zero_counter_results=" << result.zero_counter_results
            << " seconds=" << result.seconds
            << " logical_qps=" << qps
            << " latency_us_avg=" << latency_avg
            << " latency_us_p50=" << percentile(result.latency_us, 0.50)
            << " latency_us_p95=" << percentile(result.latency_us, 0.95)
            << " latency_us_p99=" << percentile(result.latency_us, 0.99)
            << " recall_at_" << options.top_k << "=" << recall
            << " avg_returned_results=" << average(result.returned_entries, result.completed)
            << " pg_probes_per_query=" << average(result.pg_probes, result.completed)
            << " object_probes_per_query=" << average(result.object_probes, result.completed)
            << " avg_matching_records=" << average(result.matching_records, result.completed)
            << " avg_distance_computations=" << average(result.distance_computations, result.completed)
            << '\n'
            << "QUERY_OID_EXAMPLE " << result.first_oid << '\n'
            << "CSV," << options.routing_mode << ',' << options.pool << ','
            << options.query_concurrency << ',' << result.completed << ','
            << result.seconds << ',' << qps << ',' << latency_avg << ','
            << percentile(result.latency_us, 0.50) << ','
            << percentile(result.latency_us, 0.95) << ','
            << percentile(result.latency_us, 0.99) << ',' << recall << ','
            << average(result.returned_entries, result.completed) << ','
            << average(result.pg_probes, result.completed) << ','
            << average(result.object_probes, result.completed) << ','
            << average(result.matching_records, result.completed) << ','
            << average(result.distance_computations, result.completed) << ','
            << result.failures << ',' << result.missing_objects << '\n';
}

} // anonymous namespace

int main(int argc, char **argv)
{
  options_t options;
  if (!parse_options(argc, argv, &options)) {
    usage(std::cerr, argv[0]);
    return 2;
  }

  std::string error;
  auto base = read_fvecs(options.base_path, options.base_limit, &error);
  if (!base) {
    std::cerr << "error: " << error << '\n';
    return 2;
  }
  std::optional<float_dataset_t> queries;
  std::optional<ivec_dataset_t> groundtruth;
  std::vector<size_t> query_indices;
  if (options.operation != "load") {
    const bool use_index_list = !options.query_index_list_path.empty();
    // limit=0 reads the whole file: --query-index-list picks specific rows
    // out of the full set rather than "the first --query-limit rows", and
    // groundtruth (if requested) must be looked up by those same original
    // row indices, not by position in the subset.
    queries = read_fvecs(
        options.query_path, use_index_list ? 0 : options.query_limit,
        &error);
    if (!queries || queries->dimension != base->dimension) {
      std::cerr << "error: " << (queries ? "dimension mismatch" : error) << '\n';
      return 2;
    }
    if (use_index_list) {
      auto indices = read_index_list(options.query_index_list_path, &error);
      if (!indices) {
        std::cerr << "error: " << error << '\n';
        return 2;
      }
      query_indices = *indices;
      auto selected = select_rows(*queries, query_indices, &error);
      if (!selected) {
        std::cerr << "error: " << error << '\n';
        return 2;
      }
      queries = std::move(selected);
    }
    if (!options.groundtruth_path.empty()) {
      groundtruth = read_ivecs(
          options.groundtruth_path,
          use_index_list ? 0 : queries->rows.size(), &error);
      if (!groundtruth ||
          (!use_index_list && groundtruth->rows.size() < queries->rows.size())) {
        std::cerr << "error: " << (groundtruth ? "short ground truth" : error) << '\n';
        return 2;
      }
      if (use_index_list) {
        ivec_dataset_t selected_truth;
        selected_truth.rows.reserve(query_indices.size());
        for (const size_t index : query_indices) {
          if (index >= groundtruth->rows.size()) {
            std::cerr << "error: ground truth index out of range: "
                      << index << '\n';
            return 2;
          }
          selected_truth.rows.push_back(groundtruth->rows[index]);
        }
        groundtruth = std::move(selected_truth);
      }
    }
  }

  librados::Rados cluster;
  int r = cluster.init2(options.client_name.c_str(),
                        options.cluster_name.c_str(), 0);
  if (r == 0) r = cluster.conf_read_file(
      options.conf_path.empty() ? nullptr : options.conf_path.c_str());
  if (r == 0) r = cluster.connect();
  if (r < 0) {
    std::cerr << "error: cluster connection: " << r << " " << error_text(r) << '\n';
    return 1;
  }

  librados::IoCtx ioctx;
  r = cluster.ioctx_create(options.pool.c_str(), ioctx);
  if (r < 0) {
    std::cerr << "error: ioctx: " << r << " " << error_text(r) << '\n';
    return 1;
  }

  auto config = requested_config(options, *base);
  std::string invalid_field;
  // routing-mode=pivot keeps no server-side index config: the model is
  // determined by --base, --pivot-build-sample, --pg-num and --seed, so
  // every invocation recomputes the same one.
  if (options.routing_mode != "pivot") {
    if (options.operation == "load" || options.operation == "both") {
      r = pg_lsh::create_index(ioctx, options.bucket, options.index,
                               config, pool_info(options), &invalid_field);
    } else {
      r = pg_lsh::load_index_config(ioctx, options.bucket, options.index,
                                    &config, &invalid_field);
      if (r == 0) {
        const auto requested = requested_config(options, *base);
        r = pg_lsh::compare_index_configs(requested, config, &invalid_field);
      }
    }
    if (r < 0 || pg_lsh::validate_query_params(
          config, query_params(options), pool_info(options), &invalid_field) < 0) {
      std::cerr << "error: pg-lsh config field=" << invalid_field
                << " code=" << r << '\n';
      return 1;
    }
  }

  std::optional<PivotModel> pivot_model;
  if (options.routing_mode == "pivot") {
    pivot_model = build_pivot_model(*base, options.pg_num, options.pivot_build_sample);
  }
  if (pivot_model) {
    std::cout << "PIVOT_MODEL mode=" << options.routing_mode
              << " seed=" << options.seed
              << " sample_count=" << pivot_model->sample_count
              << " representative_count=" << pivot_model->reps.size()
              << " dim=" << pivot_model->dim << '\n';
    if (pivot_model->reps.size() != options.pg_num) {
      std::cerr << "error: pivot model built " << pivot_model->reps.size()
                << " representatives, expected pg_num=" << options.pg_num << '\n';
      return 1;
    }
  }
  const PivotModel *pivot_model_ptr = pivot_model ? &*pivot_model : nullptr;

  auto locator_state = std::make_shared<pg_lsh::locator_state_t>();
  pg_lsh::locator_cache_t locator_cache(
      &ioctx, pool_info(options), options.pool, options.bucket,
      options.index, locator_state);
  r = locator_cache.precompute_all();
  if (r < 0) {
    std::cerr << "error: locator precompute: " << r << ' ' << error_text(r) << '\n';
    return 1;
  }

  std::cout << "CONFIG routing_mode=" << options.routing_mode
            << " placement_algorithm=pg-lsh-v0 pg_num=" << options.pg_num
            << " d=" << options.d << " m=" << options.m
            << " hamming_radius=" << options.hamming_radius
            << " distance_bucket_bits=" << options.distance_bucket_bits
            << " residual_bits=" << options.residual_bits
            << " distance_bucket_radius=" << options.distance_bucket_radius
            << " residual_hamming_radius=" << options.residual_hamming_radius
            << " probe_limit_per_pg=" << options.probe_limit_per_pg
            << " top_k=" << options.top_k << '\n';

  if (options.operation == "load" || options.operation == "both") {
    const auto loaded = load_vectors(
        options, *base, cluster, locator_state, config, pivot_model_ptr);
    print_load_result(loaded);
    if (loaded.failures != 0 || loaded.completed != base->rows.size()) return 1;
  }
  if (options.operation == "query" || options.operation == "both") {
    if (options.warmup_rounds != 0) {
      const auto warmup = run_queries(
          options, *queries, nullptr, config, cluster, locator_state,
          options.warmup_rounds, 0, 0, false);
      std::cout << "WARMUP completed=" << warmup.completed
                << " failures=" << warmup.failures
                << " missing_objects=" << warmup.missing_objects
                << " empty_results=" << warmup.empty_results << '\n';
      if (warmup.failures != 0) return 1;
    }
    dest_pg_log_t dest_log;
    dest_pg_log_t *dest_log_ptr = nullptr;
    if (!options.destination_pg_log_path.empty()) {
      dest_log.out.open(options.destination_pg_log_path);
      if (!dest_log.out) {
        std::cerr << "error: cannot open "
                  << options.destination_pg_log_path << '\n';
        return 2;
      }
      dest_log.out << "ticket,query_index,pg\n";
      dest_log_ptr = &dest_log;
    }
    const auto measured = run_queries(
        options, *queries, groundtruth ? &*groundtruth : nullptr,
        config, cluster, locator_state, options.rounds,
        options.min_queries, options.min_seconds, true, dest_log_ptr);
    print_query_result(options, measured);
    if (measured.failures != 0) return 1;
  }
  if (options.operation == "fanout-query") {
    std::ofstream fanout_log;
    std::ofstream *fanout_log_ptr = nullptr;
    if (!options.fanout_log_path.empty()) {
      fanout_log.open(options.fanout_log_path);
      if (!fanout_log) {
        std::cerr << "error: cannot open " << options.fanout_log_path << '\n';
        return 2;
      }
      fanout_log_ptr = &fanout_log;
    }
    // A non-selective run is the single-stage case of the same loop: one
    // stage covering all m routed PGs, dispatched exactly as before.
    sf::policy_t policy;
    if (options.selective) {
      if (sf::parse_schedule(options.selective_schedule, &policy.schedule) < 0) {
        std::cerr << "error: bad --selective-schedule "
                  << options.selective_schedule << '\n';
        return 2;
      }
      policy.stop_mode = options.selective_stop == "tau-relative"
        ? sf::stop_mode_t::tau_relative_improvement
        : sf::stop_mode_t::never;
      policy.min_relative_improvement = options.selective_min_improvement;
    } else {
      policy.schedule = {options.m};
      policy.stop_mode = sf::stop_mode_t::never;
    }
    std::ofstream selective_csv;
    std::ofstream *selective_csv_ptr = nullptr;
    if (!options.selective_csv_path.empty()) {
      selective_csv.open(options.selective_csv_path);
      if (!selective_csv) {
        std::cerr << "error: cannot open " << options.selective_csv_path << '\n';
        return 2;
      }
      selective_csv << sf::stage_csv_header() << '\n';
      selective_csv_ptr = &selective_csv;
    }
    if (options.warmup_rounds != 0) {
      // Warmup writes no CSV: its rows would be indistinguishable from the
      // measured run's in the same file, and its latencies are cold.
      const auto warmup = run_fanout_queries(
          options, *queries, nullptr, config, cluster, locator_state,
          options.warmup_rounds, 0, 0, false, nullptr, policy, nullptr,
          pivot_model_ptr);
      std::cout << "WARMUP completed=" << warmup.completed
                << " failures=" << warmup.failures
                << " missing_objects=" << warmup.missing_objects << '\n';
      if (warmup.failures != 0) return 1;
    }
    const auto measured = run_fanout_queries(
        options, *queries, groundtruth ? &*groundtruth : nullptr,
        config, cluster, locator_state, options.rounds,
        options.min_queries, options.min_seconds, true, fanout_log_ptr,
        policy, selective_csv_ptr, pivot_model_ptr);
    print_fanout_result(options, measured);
    if (measured.failures != 0) return 1;
  }
  return 0;
}
