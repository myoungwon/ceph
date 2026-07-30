// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <functional>
#include <string>
#include <vector>

#include "common/vector_record.h"
#include "include/buffer.h"
#include "include/ceph_assert.h"

#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore {

static constexpr uint32_t VECTOR_NODE_FORMAT_VERSION = 1;
static constexpr extent_len_t VECTOR_NODE_MAX_BYTES = 64 << 10;

enum class vector_node_kind_t : uint8_t {
  ROOT = 1,
  LEAF = 2,
};

using vector_node_entry_t = ceph::os::vector_record_t;

struct vector_leaf_descriptor_t {
  std::string first_entry_id;
  std::string last_entry_id;
  laddr_t laddr = L_ADDR_NULL;
  uint32_t entry_count = 0;
  uint32_t encoded_bytes = 0;
};

struct vector_node_t {
  uint32_t format_version = VECTOR_NODE_FORMAT_VERSION;
  vector_node_kind_t kind = vector_node_kind_t::ROOT;
  uint64_t logical_entry_count = 0;
  std::vector<vector_leaf_descriptor_t> leaves;
  std::vector<vector_node_entry_t> entries;
};

struct vector_scan_stats_t {
  uint64_t logical_entries = 0;
  uint64_t leaf_extents = 0;
  uint64_t extent_bytes_read = 0;
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

  void initialize_root();
  void initialize_leaf(std::vector<vector_node_entry_t> entries);

  const vector_node_t &get_contents() const {
    ceph_assert(decoded);
    return contents;
  }

  size_t get_encoded_length() const;

  static ceph::bufferlist encode_contents(const vector_node_t &node);
  static vector_node_t decode_contents(const ceph::bufferlist &bl);
  static bool is_sorted(const std::vector<vector_node_entry_t> &entries);
  static bool is_sorted(
    const std::vector<vector_leaf_descriptor_t> &leaves);

private:
  vector_node_t contents;
  bool decoded = false;
  bool dirty = false;

  void decode_from_buffer();
  void materialize();
  void apply_delta(const ceph::bufferlist &bl) final;
  void logical_on_delta_write() final;
  std::ostream &print_detail_l(std::ostream &out) const final;

  friend class VectorNodeManager;
};
using VectorNodeRef = VectorNode::Ref;

class VectorNodeManager {
public:
  using scan_visitor_t =
    std::function<void(const vector_node_entry_t&)>;

  explicit VectorNodeManager(
    TransactionManager &tm,
    extent_len_t node_bytes = VECTOR_NODE_MAX_BYTES)
    : tm(tm), node_bytes(node_bytes) {}

  using create_iertr = TransactionManager::alloc_extent_iertr;
  using create_ret = create_iertr::future<VectorNodeRef>;
  create_ret create_vector_root(Transaction &t);

  using read_iertr = TransactionManager::read_extent_iertr;
  using read_ret = read_iertr::future<VectorNodeRef>;
  read_ret read_vector_node(
    Transaction &t,
    laddr_t addr);

  using mutate_iertr = TransactionManager::alloc_extent_iertr::extend<
    crimson::ct_error::enoent>;
  using upsert_ret = mutate_iertr::future<VectorNodeRef>;
  upsert_ret upsert_vector_entry(
    Transaction &t,
    VectorNodeRef root,
    const vector_node_entry_t &entry);

  using scan_ret = read_iertr::future<vector_scan_stats_t>;
  scan_ret scan_vector_entries(
    Transaction &t,
    VectorNodeRef root,
    scan_visitor_t visitor,
    bool measure_visitor = false);

  using remove_iertr = TransactionManager::ref_iertr;
  using remove_ret = remove_iertr::future<>;
  remove_ret remove_vector_tree(
    Transaction &t,
    VectorNodeRef root);

private:
  TransactionManager &tm;
  extent_len_t node_bytes;

  bool is_valid_extent_size(extent_len_t length) const;
  create_ret create_vector_leaf(
    Transaction &t,
    std::vector<vector_node_entry_t> entries);
  upsert_ret replace_contents(
    Transaction &t,
    VectorNodeRef node,
    vector_node_t contents);
};

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::VectorNode>
  : fmt::ostream_formatter {};
#endif
