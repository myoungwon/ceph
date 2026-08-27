// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <functional>
#include <optional>
#include <string_view>

#include "common/vector_record.h"
#include "include/buffer.h"

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/vector_node_layout.h"

namespace crimson::os::seastore {

// Compiled-in default LIST block size; VectorNodeManager's constructor
// accepts an override.
static constexpr extent_len_t VECTOR_NODE_LIST_DEFAULT_BYTES = 16 << 10;
// Fixed INDEX page size. Independent of, and never derived from, the
// configured LIST block size -- see vector_node_index_entry_capacity().
static constexpr extent_len_t VECTOR_NODE_INDEX_BYTES = laddr_t::UNIT_SIZE;

struct vector_scan_stats_t {
  uint64_t logical_entries = 0;
  uint64_t list_extents = 0;
  // Logical sizes of visited cached extents, not physical device I/O bytes.
  uint64_t extent_bytes_visited = 0;
  uint64_t visitor_ns = 0;
};

class VectorNode : public LogicalChildNode {
public:
  using Ref = TCachedExtentRef<VectorNode>;
  static constexpr extent_types_t TYPE = extent_types_t::VECTOR_NODE;

  explicit VectorNode(ceph::bufferptr &&ptr);
  explicit VectorNode(extent_len_t length);
  VectorNode(const VectorNode &rhs);

  CachedExtentRef duplicate_for_write(Transaction &t) final;

  extent_types_t get_type() const final {
    return TYPE;
  }

  void on_clean_read() final;
  void on_initial_write() final;
  void prepare_commit(Transaction &t) final;
  ceph::bufferlist get_delta() final;
  void clear_delta() final;

  std::optional<VectorNodeIndexLayout> index_layout() const;
  std::optional<VectorNodeListLayout> list_layout() const;

private:
  int initialize_index();
  int initialize_list();
  std::optional<VectorNodeIndexLayout> mutable_index_layout();
  std::optional<VectorNodeListLayout> mutable_list_layout();
  int replace_page(std::string_view page);
  bool validate_page() const;

  void apply_delta(const ceph::bufferlist &bl) final;
  std::ostream &print_detail_l(std::ostream &out) const final;

  friend class VectorNodeManager;
  friend class VectorNodeTestPeer;
};
using VectorNodeRef = VectorNode::Ref;

class VectorNodeManager {
public:
  // The view and all of its string_views are valid only for the duration of
  // the visitor call; the backing LIST extent remains pinned by scan().
  using scan_visitor_t =
    std::function<void(const VectorNodeRecordView&)>;

  explicit VectorNodeManager(
    TransactionManager &tm,
    extent_len_t list_block_bytes = VECTOR_NODE_LIST_DEFAULT_BYTES)
    : tm(tm), list_block_bytes(list_block_bytes) {}

  using create_iertr = TransactionManager::alloc_extent_iertr;
  using create_ret = create_iertr::future<VectorNodeRef>;
  create_ret create_index_table(Transaction &t);

  using read_iertr = TransactionManager::read_extent_iertr;
  using read_ret = read_iertr::future<VectorNodeRef>;
  read_ret read_vector_node(Transaction &t, laddr_t addr);

  using find_iertr = read_iertr;
  using find_ret = find_iertr::future<std::optional<laddr_t>>;
  // Exact (dimension, data_type) lookup only -- never nearest/range. A
  // miss returns nullopt; the query path treats that as an empty result
  // without ever reading a LIST block. Purely in-memory over the already
  // read `index`, so this needs no Transaction.
  find_ret find_group_head(
    VectorNodeRef index,
    uint32_t dimension,
    uint32_t data_type);

  using append_iertr = TransactionManager::alloc_extent_iertr::extend<
    crimson::ct_error::enoent>;
  using append_ret = append_iertr::future<>;
  // Appends `entry` to its (dimension, data_type) group's tail LIST block,
  // creating the group (and its first LIST block) if it does not already
  // exist. No duplicate/entry_id lookup is ever performed: a repeated
  // bucket/index/key PUT always appends a second physical record. The
  // common case (room in the current tail) writes only the tail LIST
  // extent; the INDEX extent is written only when the group is created or
  // the tail block rolls over.
  append_ret append_vector_entry(
    Transaction &t,
    VectorNodeRef index,
    const ceph::os::vector_record_t &entry);

  using scan_ret = read_iertr::future<vector_scan_stats_t>;
  // Walks the LIST chain starting at head_laddr to its end, visiting every
  // record in append order. entry_id plays no role. `dimension`/`data_type`
  // must be the same pair used to resolve head_laddr via find_group_head():
  // every row in the chain shares that group's row byte length
  // (dimension * element_size(data_type)), which LIST pages no longer
  // store per row (see VectorNodeListLayout), so the caller supplies it.
  // measure_visitor times the visitor calls and accumulates them into
  // vector_scan_stats_t::visitor_ns; off by default since it costs a
  // clock read per LIST block even when unused.
  scan_ret scan_vector_group(
    Transaction &t,
    laddr_t head_laddr,
    uint32_t dimension,
    uint32_t data_type,
    scan_visitor_t visitor,
    bool measure_visitor = false);

  using remove_iertr = TransactionManager::ref_iertr;
  using remove_ret = remove_iertr::future<>;
  // Frees every LIST extent reachable from every index entry's head_laddr,
  // then the INDEX extent itself.
  remove_ret remove_index_table(Transaction &t, VectorNodeRef index);

private:
  TransactionManager &tm;
  extent_len_t list_block_bytes;

  bool is_valid_list_extent_size(extent_len_t length) const;
  create_ret create_empty_vector_list(Transaction &t);
};

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::VectorNode>
  : fmt::ostream_formatter {};
#endif
