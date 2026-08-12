// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string_view>
#include <type_traits>

#include "common/vector_record.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

inline constexpr uint32_t VECTOR_NODE_LAYOUT_MAGIC = 0x564e5047;
// Physical versions are independent contracts for each extent kind.
inline constexpr uint16_t VECTOR_NODE_INDEX_LAYOUT_VERSION = 1;
inline constexpr uint16_t VECTOR_NODE_LIST_LAYOUT_VERSION = 1;
inline constexpr uint32_t VECTOR_NODE_VARIABLE_ALIGNMENT = 8;
inline constexpr uint32_t VECTOR_NODE_VECTOR_ALIGNMENT = 32;

static_assert(
  VECTOR_NODE_VARIABLE_ALIGNMENT != 0 &&
  (VECTOR_NODE_VARIABLE_ALIGNMENT &
    (VECTOR_NODE_VARIABLE_ALIGNMENT - 1)) == 0);
static_assert(
  VECTOR_NODE_VECTOR_ALIGNMENT != 0 &&
  (VECTOR_NODE_VECTOR_ALIGNMENT &
    (VECTOR_NODE_VECTOR_ALIGNMENT - 1)) == 0);
static_assert(
  VECTOR_NODE_VECTOR_ALIGNMENT % VECTOR_NODE_VARIABLE_ALIGNMENT == 0);

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

// LIST page header extension: a single forward link. LIST pages form a
// plain singly-linked chain, not a balanced/split tree.
struct __attribute__((packed)) vector_list_header_extension_le_t {
  laddr_le_t next_page_laddr;
};

struct __attribute__((packed)) vector_list_page_header_le_t {
  vector_node_header_le_t common;
  vector_list_header_extension_le_t list;
};

// Vector-row identity tag. This is the *only* per-row LIST field: bucket
// name, index name, user_key, placement info, and any application metadata
// live in OMAP (see ceph::os::make_vector_omap()/make_vector_omap_identity()
// in common/vector_record.h) under the same entry_id, keyed as
// _ENTRY_<entry_id>.*. A query joins a scanned row back to that OMAP record
// by entry_id; the LIST page itself never stores or interprets those
// fields.
struct __attribute__((packed)) vector_list_entry_le_t {
  // 8-digit lowercase-hex identity value. Append order is insertion order:
  // nothing reads this field back to sort, search, or deduplicate records.
  char entry_id[8]{};
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
static_assert(sizeof(vector_list_header_extension_le_t) == 16);
static_assert(std::is_trivially_copyable_v<
  vector_list_header_extension_le_t>);
static_assert(std::is_standard_layout_v<
  vector_list_header_extension_le_t>);
static_assert(sizeof(vector_list_page_header_le_t) == 80);
static_assert(std::is_trivially_copyable_v<vector_list_page_header_le_t>);
static_assert(std::is_standard_layout_v<vector_list_page_header_le_t>);
static_assert(offsetof(vector_list_page_header_le_t, common) == 0);
static_assert(offsetof(vector_list_page_header_le_t, list) == 64);
static_assert(
  offsetof(vector_list_page_header_le_t, list) +
    offsetof(vector_list_header_extension_le_t, next_page_laddr) == 64);
static_assert(sizeof(vector_list_entry_le_t) == 8);
static_assert(std::is_trivially_copyable_v<vector_list_entry_le_t>);
static_assert(std::is_standard_layout_v<vector_list_entry_le_t>);

inline constexpr uint32_t VECTOR_NODE_INDEX_HEADER_SIZE =
  sizeof(vector_node_header_le_t);
inline constexpr uint32_t VECTOR_NODE_LIST_HEADER_SIZE =
  sizeof(vector_list_page_header_le_t);
// The vector plane must start VECTOR_NODE_VECTOR_ALIGNMENT-aligned (each
// row's page offset is checked against that alignment), so the plane's
// base is the raw header size rounded up -- not the raw header size
// itself, which is not a multiple of VECTOR_NODE_VECTOR_ALIGNMENT.
inline constexpr uint32_t VECTOR_NODE_LIST_VECTOR_PLANE_BASE =
  (VECTOR_NODE_LIST_HEADER_SIZE + VECTOR_NODE_VECTOR_ALIGNMENT - 1) &
    ~(VECTOR_NODE_VECTOR_ALIGNMENT - 1);

constexpr uint32_t vector_node_index_entry_capacity(uint32_t page_bytes)
{
  return page_bytes < VECTOR_NODE_INDEX_HEADER_SIZE
    ? 0
    : (page_bytes - VECTOR_NODE_INDEX_HEADER_SIZE) /
      sizeof(vector_index_entry_le_t);
}

// A fixed 4KiB INDEX page (SeaStore's standard minimum extent unit, see
// laddr_t::UNIT_SIZE) holds 100 entries -- far more than the realistic
// number of distinct (dimension, data_type) pairs one object will carry.
static_assert(vector_node_index_entry_capacity(4 * 1024) == 100);

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

// Non-owning view over one LIST row: its identity tag and its vector
// payload bytes only. Everything else a query needs to return to the
// client (user_key, any application metadata) lives in OMAP under
// _ENTRY_<entry_id>.* and is resolved by the caller, by entry_id, only for
// the rows that survive top-k selection -- this view never carries it.
class VectorNodeRecordView {
public:
  std::string_view entry_id() const { return entry_id_view; }
  std::string_view vector_data() const { return vector_data_view; }

private:
  VectorNodeRecordView(
    std::string_view entry_id,
    std::string_view vector_data)
    : entry_id_view(entry_id), vector_data_view(vector_data) {}

  std::string_view entry_id_view;
  std::string_view vector_data_view;

  friend class VectorNodeListLayout;
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

// LIST page layout (low to high address):
//   [header][alignment padding][contiguous vector plane][entry_id array]
//   [free space][page end]
//
// The vector plane and entry_id array grow together at their shared
// boundary (header().free_begin): appending a row shifts the (tiny,
// fixed-stride) entry_id array forward by one aligned row and drops the
// new row into the space it vacated, so the plane never needs a separate
// moved-bytes step of its own. There is no metadata region: bucket_name,
// index_name, user_key, placement info, and application metadata are not
// LIST data at all -- see the header comment on vector_list_entry_le_t.
//
// Every row in one LIST chain shares its group's (dimension, data_type)
// (see VectorNodeManager::append_vector_entry(), which only ever appends
// to the chain reached by looking that pair up in the INDEX page), so
// every row also has the same aligned byte length. Callers that already
// know that length (derived once from (dimension, data_type), not stored
// per row) pass it into vector_offset_at()/record_at()/scan_records()/
// vector_matrix_view() so this class can index rows in O(1) instead of
// carrying or re-deriving a per-row length.
//
// Invariant: slot order is append order, nothing else -- there is no
// entry_id sort invariant and no search-for-existing-entry_id step. Every
// insert is an unconditional back-append: both the entry_id array and the
// vector plane grow at their shared boundary with no existing vector
// bytes moved. A page that cannot fit the next record signals "full"; the
// caller (see VectorNodeManager) allocates a new LIST page and links it
// via next_page_laddr rather than rebuilding or splitting this one.
class VectorNodeListLayout {
public:
  static std::optional<VectorNodeListLayout> initialize(
    char *data,
    size_t length);
  static std::optional<VectorNodeListLayout> open_checked(
    char *data,
    size_t length);
  static std::optional<VectorNodeListLayout> open_checked(
    const char *data,
    size_t length);

  int validate() const;

  uint32_t item_count() const;
  uint64_t logical_entry_count() const;
  laddr_t next_page_laddr() const;
  int set_next_page_laddr(laddr_t next_page_laddr);
  // Bytes available for the next append; the single pool an append draws
  // from. With no metadata region left to fragment, this is the page's
  // only notion of free space.
  uint32_t contiguous_free_space() const;
  uint32_t used_space() const;

  std::string_view entry_id_at(uint32_t index) const;
  // Start of the vector plane; a pure function of item_count.
  uint32_t vector_plane_base() const;
  // Row `index`'s page offset, given the group's per-row logical (unaligned)
  // byte length `row_length` (== dimension * element_size(data_type),
  // known by the caller from (dimension, data_type) -- see the class
  // comment). Directly computed, O(1): vector_plane_base() +
  // index * aligned_vector_bytes(row_length).
  uint32_t vector_offset_at(uint32_t index, uint32_t row_length) const;

  // Bounds-checked; the factory-created layout has completed full
  // validation. O(1) -- see vector_offset_at(). `row_length` as above.
  std::optional<VectorNodeRecordView> record_at(
    uint32_t index, uint32_t row_length) const;

  // Visits every record in slot (append) order in O(item_count) total.
  // `row_length` as above; this is the query hot path's entry point.
  using record_visitor_t = std::function<void(const VectorNodeRecordView&)>;
  int scan_records(
    uint32_t row_length, const record_visitor_t &visitor) const;

  // Non-owning view over vector rows [begin, end), directly usable as a
  // row_count x row_stride matrix. `row_length` as above -- every row in
  // this LIST chain shares it by construction, so there is no per-row
  // uniformity check left to make: a mismatched `row_length` (wrong
  // group) is caught by append_record()'s own check, not re-verified here
  // on every read.
  struct matrix_view_t {
    const char *data = nullptr;
    uint32_t row_count = 0;
    // Bytes per row, including any alignment padding after the payload.
    uint32_t row_stride = 0;
    // Logical (unaligned) vector-payload bytes at the front of each row.
    uint32_t row_length = 0;
  };
  std::optional<matrix_view_t> vector_matrix_view(
    uint32_t begin, uint32_t end, uint32_t row_length) const;

  // Unconditional back-append: no duplicate check, no dedup, no sort
  // position search. Returns -ENOSPC when the record does not fit in
  // contiguous_free_space(); the caller allocates a new LIST page.
  // Returns -EINVAL if `record`'s aligned vector length disagrees with an
  // already-present row's -- every row in a chain must share one length
  // (see the class comment); the caller is expected to never trigger this
  // by only ever appending records already routed to this chain by their
  // own (dimension, data_type) matching the group's.
  int append_record(const ceph::os::vector_record_t &record);

private:
  VectorNodeListLayout(char *data, size_t length)
    : data(data), mutable_data(data), length(length) {}
  VectorNodeListLayout(const char *data, size_t length)
    : data(data), length(length) {}

  int initialize_page();
  const vector_node_header_le_t *header() const;
  vector_node_header_le_t *mutable_header();
  // Page offset of entries()[0]: header().free_begin minus the fixed-stride
  // entry_id array's own size. Equivalently, the vector plane's current
  // end.
  uint32_t entry_array_base() const;
  const vector_list_entry_le_t *entries() const;
  std::optional<VectorNodeRecordView> record_view_at(
    uint32_t index, uint32_t vector_offset, uint32_t row_length) const;

  const char *data = nullptr;
  char *mutable_data = nullptr;
  size_t length = 0;
};

} // namespace crimson::os::seastore
