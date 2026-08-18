// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <type_traits>

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

inline constexpr uint32_t VECTOR_NODE_LAYOUT_MAGIC = 0x564e5047;
// Physical versions are independent contracts for each extent kind.
inline constexpr uint16_t VECTOR_NODE_INDEX_LAYOUT_VERSION = 1;

// ONode reaches VectorNode data through exactly two page kinds:
//   INDEX: one per object, exact (dimension, data_type) -> {head,tail} table
//   LIST:  forward-linked, append-only chain of vector records for one
//          (dimension, data_type) group
// There is no ROOT/LEAF entry_id-sorted tree; neither page kind sorts or
// searches records by entry_id.
enum class vector_node_layout_kind_t : uint8_t {
  INDEX = 1,
  LIST = 2,
};

struct __attribute__((packed)) vector_node_header_le_t {
  ceph_le32 magic{0};
  ceph_le16 layout_version{0};
  uint8_t kind = 0;
  uint8_t flags = 0;
  ceph_le32 header_size{0};
  ceph_le32 item_count{0};
  ceph_le64 logical_entry_count{0};
  ceph_le32 free_begin{0};
  ceph_le32 free_end{0};
  // INDEX: header plus entries. LIST: header, entries, and vector rows.
  ceph_le32 used_bytes{0};
  uint8_t reserved[28]{};
};

// INDEX carries no header extension: a single fixed-size INDEX page holds
// every (dimension, data_type) group an object can realistically have (see
// vector_node_index_entry_capacity() below), so no chaining metadata is
// needed.
struct __attribute__((packed)) vector_index_entry_le_t {
  // Exact-match lookup key. Not a nearest/range-search key: a query only
  // ever matches the literal (dimension, data_type) pair it asks for.
  ceph_le32 dimension{0};
  ceph_le32 data_type{0};
  // First LIST block in the chain; query walks head -> ... via
  // next_page_laddr. Set once at group creation, never rewritten.
  laddr_le_t head_laddr;
  // Last LIST block in the chain; PUT reads this directly so it never
  // walks the chain to find where to append. Rewritten only when the
  // current tail block is full and a new block is linked.
  laddr_le_t tail_laddr;
};

static_assert(sizeof(vector_node_header_le_t) == 64);
static_assert(std::is_trivially_copyable_v<vector_node_header_le_t>);
static_assert(std::is_standard_layout_v<vector_node_header_le_t>);
static_assert(offsetof(vector_node_header_le_t, item_count) == 12);
static_assert(offsetof(vector_node_header_le_t, free_begin) == 24);
static_assert(offsetof(vector_node_header_le_t, used_bytes) == 32);
static_assert(sizeof(vector_index_entry_le_t) == 40);
static_assert(std::is_trivially_copyable_v<vector_index_entry_le_t>);
static_assert(std::is_standard_layout_v<vector_index_entry_le_t>);
static_assert(offsetof(vector_index_entry_le_t, head_laddr) == 8);
static_assert(offsetof(vector_index_entry_le_t, tail_laddr) == 24);

inline constexpr uint32_t VECTOR_NODE_INDEX_HEADER_SIZE =
  sizeof(vector_node_header_le_t);

constexpr uint32_t vector_node_index_entry_capacity(uint32_t page_bytes)
{
  return page_bytes < VECTOR_NODE_INDEX_HEADER_SIZE
    ? 0
    : (page_bytes - VECTOR_NODE_INDEX_HEADER_SIZE) /
      sizeof(vector_index_entry_le_t);
}

// A fixed-size INDEX page, sized to SeaStore's standard minimum extent
// unit, holds 100 entries -- far more than the realistic number of
// distinct (dimension, data_type) pairs one object will carry.
static_assert(vector_node_index_entry_capacity(laddr_t::UNIT_SIZE) == 100);

class VectorNodeIndexEntryView {
public:
  uint32_t dimension() const;
  uint32_t data_type() const;
  laddr_t head_laddr() const;
  laddr_t tail_laddr() const;

private:
  explicit VectorNodeIndexEntryView(const vector_index_entry_le_t *entry)
    : entry(entry) {}

  const vector_index_entry_le_t *entry = nullptr;

  friend class VectorNodeIndexLayout;
};

// INDEX page: a small array of exact-match (dimension, data_type) entries,
// kept sorted by that key so lookup is O(log n) binary search. There is no
// "closest" or "nearest" notion here; a miss simply means the group does
// not exist.
class VectorNodeIndexLayout {
public:
  static std::optional<VectorNodeIndexLayout> initialize(
    char *data,
    size_t length);
  static std::optional<VectorNodeIndexLayout> open_checked(
    char *data,
    size_t length);
  static std::optional<VectorNodeIndexLayout> open_checked(
    const char *data,
    size_t length);

  int validate() const;

  uint32_t item_count() const;

  std::optional<VectorNodeIndexEntryView> entry_at(uint32_t index) const;
  // Exact match only. Returns the entry's index, or nullopt on a miss.
  std::optional<uint32_t> find_entry(
    uint32_t dimension, uint32_t data_type) const;
  // Fails with -EEXIST if an entry for (dimension, data_type) already
  // exists; the caller is expected to have already checked via
  // find_entry() before deciding to create a new group.
  int insert_entry(
    uint32_t dimension,
    uint32_t data_type,
    laddr_t head_laddr,
    laddr_t tail_laddr);
  // Rewrites only entry_at(index)'s tail_laddr; used on tail-block
  // rollover. head_laddr, dimension, and data_type are never rewritten.
  int set_tail_laddr(uint32_t index, laddr_t tail_laddr);

private:
  VectorNodeIndexLayout(char *data, size_t length)
    : data(data), mutable_data(data), length(length) {}
  VectorNodeIndexLayout(const char *data, size_t length)
    : data(data), length(length) {}

  int initialize_page();
  // Position of (dimension, data_type) in sorted order: the entry there iff
  // found is true, else the correct insertion position.
  struct search_result_t {
    uint32_t index = 0;
    bool found = false;
  };
  search_result_t search(uint32_t dimension, uint32_t data_type) const;
  const vector_node_header_le_t *header() const;
  vector_node_header_le_t *mutable_header();
  const vector_index_entry_le_t *entries() const;
  vector_index_entry_le_t *mutable_entries();

  const char *data = nullptr;
  char *mutable_data = nullptr;
  size_t length = 0;
};

} // namespace crimson::os::seastore
