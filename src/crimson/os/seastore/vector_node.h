// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <string>
#include <vector>

#include "include/buffer.h"
#include "include/ceph_assert.h"

#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore {

static constexpr uint32_t VECTOR_NODE_FORMAT_VERSION = 1;
// Upper bound for future resize/split support. PR1 allocates one
// tm.get_block_size() extent and returns enospc when that extent is full.
static constexpr extent_len_t VECTOR_NODE_MAX_BYTES = 64 << 10;

struct vector_node_entry_t {
  std::string entry_id;
  ceph::bufferlist vector_data;
};

struct vector_node_t {
  uint32_t format_version = VECTOR_NODE_FORMAT_VERSION;
  uint32_t data_type = 0;
  uint32_t dimension = 0;
  std::vector<vector_node_entry_t> entries;
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

  void initialize(uint32_t data_type, uint32_t dimension);

  const vector_node_t &get_contents() const {
    ceph_assert(decoded);
    return contents;
  }

  static ceph::bufferlist encode_contents(const vector_node_t &node);
  static vector_node_t decode_contents(const ceph::bufferlist &bl);
  static bool is_sorted(const std::vector<vector_node_entry_t> &entries);

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
  explicit VectorNodeManager(TransactionManager &tm) : tm(tm) {}

  using create_iertr = TransactionManager::alloc_extent_iertr;
  using create_ret = create_iertr::future<VectorNodeRef>;
  create_ret create_vector_node(
    Transaction &t,
    uint32_t data_type,
    uint32_t dimension);

  using read_iertr = TransactionManager::read_extent_iertr;
  using read_ret = read_iertr::future<VectorNodeRef>;
  read_ret read_vector_node(
    Transaction &t,
    laddr_t addr);

  using mutate_iertr = TransactionManager::alloc_extent_iertr;
  using upsert_ret = mutate_iertr::future<VectorNodeRef>;
  upsert_ret upsert_vector_entry(
    Transaction &t,
    VectorNodeRef node,
    const std::string &entry_id,
    const ceph::bufferlist &vector_data);

  using remove_ret = mutate_iertr::future<std::pair<VectorNodeRef, bool>>;
  remove_ret remove_vector_entry(
    Transaction &t,
    VectorNodeRef node,
    const std::string &entry_id);

private:
  TransactionManager &tm;

  bool is_valid_extent_size(extent_len_t length) const;
};

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::VectorNode>
  : fmt::ostream_formatter {};
#endif
