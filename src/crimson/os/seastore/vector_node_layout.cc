// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/vector_node_layout.h"

#include <cerrno>
#include <cstring>
#include <limits>

namespace crimson::os::seastore {

namespace {

bool checked_align_up(uint64_t value, uint32_t alignment, uint64_t *result)
{
  if (alignment == 0 || result == nullptr ||
      value > std::numeric_limits<uint64_t>::max() - (alignment - 1)) {
    return false;
  }
  *result = (value + alignment - 1) & ~(uint64_t(alignment) - 1);
  return true;
}

// vector_max_dimension bounds vector_length well under 32-bit overflow
// range for VECTOR_NODE_VECTOR_ALIGNMENT, so this never needs to report
// failure the way the general checked_align_up() does.
uint32_t aligned_vector_length(uint32_t vector_length)
{
  uint64_t result = 0;
  checked_align_up(vector_length, VECTOR_NODE_VECTOR_ALIGNMENT, &result);
  return static_cast<uint32_t>(result);
}

int validate_common_header(
  const char *data,
  size_t length,
  vector_node_layout_kind_t expected_kind,
  uint32_t item_size,
  uint16_t expected_layout_version,
  uint32_t expected_header_size)
{
  if (data == nullptr || length < expected_header_size ||
      expected_header_size < sizeof(vector_node_header_le_t)) {
    return -EINVAL;
  }
  if (length > std::numeric_limits<uint32_t>::max()) {
    return -EOVERFLOW;
  }
  const auto *header =
    reinterpret_cast<const vector_node_header_le_t *>(data);
  if (uint32_t(header->magic) != VECTOR_NODE_LAYOUT_MAGIC ||
      uint16_t(header->layout_version) != expected_layout_version ||
      header->kind != static_cast<uint8_t>(expected_kind) ||
      header->flags != 0 ||
      uint32_t(header->header_size) != expected_header_size) {
    return -EINVAL;
  }
  for (const auto byte : header->reserved) {
    if (byte != 0) {
      return -EINVAL;
    }
  }

  // free_begin must at least cover the fixed-stride item array; kind-
  // specific validate() callers check the exact formula, since LIST's
  // free_begin also covers a variable-size vector plane that INDEX has no
  // equivalent of.
  const uint64_t item_bytes =
    uint64_t(uint32_t(header->item_count)) * item_size;
  if (item_bytes > std::numeric_limits<uint32_t>::max()) {
    return -EOVERFLOW;
  }
  const uint64_t min_free_begin = expected_header_size + item_bytes;
  if (min_free_begin > length ||
      uint32_t(header->free_begin) < min_free_begin) {
    return -EINVAL;
  }

  const uint32_t free_begin = header->free_begin;
  const uint32_t free_end = header->free_end;
  if (free_begin > free_end || free_end > length) {
    return -EINVAL;
  }
  return 0;
}

} // anonymous namespace

uint32_t VectorNodeIndexEntryView::dimension() const
{
  return entry == nullptr ? 0 : uint32_t(entry->dimension);
}

uint32_t VectorNodeIndexEntryView::data_type() const
{
  return entry == nullptr ? 0 : uint32_t(entry->data_type);
}

laddr_t VectorNodeIndexEntryView::head_laddr() const
{
  if (entry == nullptr) {
    return L_ADDR_NULL;
  }
  laddr_le_t stored;
  std::memcpy(&stored, &entry->head_laddr, sizeof(stored));
  return stored;
}

laddr_t VectorNodeIndexEntryView::tail_laddr() const
{
  if (entry == nullptr) {
    return L_ADDR_NULL;
  }
  laddr_le_t stored;
  std::memcpy(&stored, &entry->tail_laddr, sizeof(stored));
  return stored;
}

const vector_node_header_le_t *VectorNodeIndexLayout::header() const
{
  return data == nullptr || length < sizeof(vector_node_header_le_t)
    ? nullptr
    : reinterpret_cast<const vector_node_header_le_t *>(data);
}

vector_node_header_le_t *VectorNodeIndexLayout::mutable_header()
{
  return mutable_data == nullptr || length < sizeof(vector_node_header_le_t)
    ? nullptr
    : reinterpret_cast<vector_node_header_le_t *>(mutable_data);
}

const vector_index_entry_le_t *VectorNodeIndexLayout::entries() const
{
  return reinterpret_cast<const vector_index_entry_le_t *>(
    data + VECTOR_NODE_INDEX_HEADER_SIZE);
}

vector_index_entry_le_t *VectorNodeIndexLayout::mutable_entries()
{
  return reinterpret_cast<vector_index_entry_le_t *>(
    mutable_data + VECTOR_NODE_INDEX_HEADER_SIZE);
}

std::optional<VectorNodeIndexLayout> VectorNodeIndexLayout::initialize(
  char *data,
  size_t length)
{
  VectorNodeIndexLayout layout(data, length);
  if (layout.initialize_page() < 0 || layout.validate() < 0) {
    return std::nullopt;
  }
  return layout;
}

std::optional<VectorNodeIndexLayout> VectorNodeIndexLayout::open_checked(
  char *data,
  size_t length)
{
  VectorNodeIndexLayout layout(data, length);
  return layout.validate() == 0
    ? std::optional<VectorNodeIndexLayout>(layout)
    : std::nullopt;
}

std::optional<VectorNodeIndexLayout> VectorNodeIndexLayout::open_checked(
  const char *data,
  size_t length)
{
  VectorNodeIndexLayout layout(data, length);
  return layout.validate() == 0
    ? std::optional<VectorNodeIndexLayout>(layout)
    : std::nullopt;
}

int VectorNodeIndexLayout::initialize_page()
{
  if (mutable_data == nullptr ||
      length < VECTOR_NODE_INDEX_HEADER_SIZE ||
      length > std::numeric_limits<uint32_t>::max()) {
    return -EINVAL;
  }
  std::memset(mutable_data, 0, length);
  auto *stored = mutable_header();
  stored->magic = VECTOR_NODE_LAYOUT_MAGIC;
  stored->layout_version = VECTOR_NODE_INDEX_LAYOUT_VERSION;
  stored->kind = static_cast<uint8_t>(vector_node_layout_kind_t::INDEX);
  stored->header_size = VECTOR_NODE_INDEX_HEADER_SIZE;
  stored->free_begin = VECTOR_NODE_INDEX_HEADER_SIZE;
  stored->free_end = static_cast<uint32_t>(length);
  stored->used_bytes = VECTOR_NODE_INDEX_HEADER_SIZE;
  return 0;
}

int VectorNodeIndexLayout::validate() const
{
  const int common = validate_common_header(
    data,
    length,
    vector_node_layout_kind_t::INDEX,
    sizeof(vector_index_entry_le_t),
    VECTOR_NODE_INDEX_LAYOUT_VERSION,
    VECTOR_NODE_INDEX_HEADER_SIZE);
  if (common < 0) {
    return common;
  }
  const auto *stored = header();
  if (uint32_t(stored->free_end) != length ||
      uint32_t(stored->used_bytes) != uint32_t(stored->free_begin) ||
      uint32_t(stored->free_begin) != VECTOR_NODE_INDEX_HEADER_SIZE +
        uint64_t(uint32_t(stored->item_count)) *
          sizeof(vector_index_entry_le_t) ||
      uint64_t(stored->logical_entry_count) !=
        uint32_t(stored->item_count)) {
    return -EINVAL;
  }

  bool have_previous = false;
  uint32_t previous_dimension = 0;
  uint32_t previous_data_type = 0;
  for (uint32_t i = 0; i < uint32_t(stored->item_count); ++i) {
    const auto entry = entry_at(i);
    if (!entry) {
      return -EINVAL;
    }
    const bool strictly_after = !have_previous ||
      previous_dimension < entry->dimension() ||
      (previous_dimension == entry->dimension() &&
       previous_data_type < entry->data_type());
    if (!strictly_after ||
        entry->head_laddr() == L_ADDR_NULL ||
        entry->tail_laddr() == L_ADDR_NULL) {
      return -EINVAL;
    }
    previous_dimension = entry->dimension();
    previous_data_type = entry->data_type();
    have_previous = true;
  }
  return 0;
}

uint32_t VectorNodeIndexLayout::item_count() const
{
  return header() == nullptr ? 0 : uint32_t(header()->item_count);
}

std::optional<VectorNodeIndexEntryView> VectorNodeIndexLayout::entry_at(
  uint32_t index) const
{
  const auto *stored = header();
  if (stored == nullptr || index >= uint32_t(stored->item_count)) {
    return std::nullopt;
  }
  const uint64_t item_end = VECTOR_NODE_INDEX_HEADER_SIZE +
    (uint64_t(index) + 1) * sizeof(vector_index_entry_le_t);
  if (item_end > length || item_end > uint32_t(stored->free_begin)) {
    return std::nullopt;
  }
  return VectorNodeIndexEntryView(entries() + index);
}

VectorNodeIndexLayout::search_result_t VectorNodeIndexLayout::search(
  uint32_t dimension, uint32_t data_type) const
{
  uint32_t first = 0;
  uint32_t count = item_count();
  while (count != 0) {
    const uint32_t step = count / 2;
    const uint32_t middle = first + step;
    const auto entry = entry_at(middle);
    const bool middle_before = entry->dimension() < dimension ||
      (entry->dimension() == dimension && entry->data_type() < data_type);
    if (middle_before) {
      first = middle + 1;
      count -= step + 1;
    } else {
      count = step;
    }
  }
  const auto entry = entry_at(first);
  const bool found = entry.has_value() &&
    entry->dimension() == dimension && entry->data_type() == data_type;
  return search_result_t{first, found};
}

std::optional<uint32_t> VectorNodeIndexLayout::find_entry(
  uint32_t dimension, uint32_t data_type) const
{
  const auto result = search(dimension, data_type);
  if (!result.found) {
    return std::nullopt;
  }
  return result.index;
}

int VectorNodeIndexLayout::insert_entry(
  uint32_t dimension,
  uint32_t data_type,
  laddr_t head_laddr,
  laddr_t tail_laddr)
{
  if (mutable_data == nullptr) {
    return -EROFS;
  }
  const int current_validation = validate();
  if (current_validation < 0) {
    return current_validation;
  }
  if (head_laddr == L_ADDR_NULL || tail_laddr == L_ADDR_NULL) {
    return -EINVAL;
  }
  const auto position = search(dimension, data_type);
  if (position.found) {
    return -EEXIST;
  }
  const auto *current_header = header();
  const uint32_t free_bytes = uint32_t(current_header->free_end) -
    uint32_t(current_header->free_begin);
  if (free_bytes < sizeof(vector_index_entry_le_t)) {
    return -ENOSPC;
  }

  const uint32_t count = item_count();
  auto *items = mutable_entries();
  std::memmove(
    items + position.index + 1,
    items + position.index,
    (count - position.index) * sizeof(*items));
  vector_index_entry_le_t entry;
  entry.dimension = dimension;
  entry.data_type = data_type;
  entry.head_laddr = head_laddr;
  entry.tail_laddr = tail_laddr;
  std::memcpy(items + position.index, &entry, sizeof(entry));

  auto *stored = mutable_header();
  stored->item_count = count + 1;
  stored->logical_entry_count = count + 1;
  stored->free_begin = VECTOR_NODE_INDEX_HEADER_SIZE +
    (count + 1) * sizeof(vector_index_entry_le_t);
  stored->used_bytes = stored->free_begin;
  return validate();
}

int VectorNodeIndexLayout::set_tail_laddr(uint32_t index, laddr_t tail_laddr)
{
  if (mutable_data == nullptr) {
    return -EROFS;
  }
  const int current_validation = validate();
  if (current_validation < 0) {
    return current_validation;
  }
  if (index >= item_count() || tail_laddr == L_ADDR_NULL) {
    return -EINVAL;
  }
  mutable_entries()[index].tail_laddr = tail_laddr;
  return validate();
}

const vector_node_header_le_t *VectorNodeListLayout::header() const
{
  return data == nullptr || length < sizeof(vector_node_header_le_t)
    ? nullptr
    : reinterpret_cast<const vector_node_header_le_t *>(data);
}

vector_node_header_le_t *VectorNodeListLayout::mutable_header()
{
  return mutable_data == nullptr || length < sizeof(vector_node_header_le_t)
    ? nullptr
    : reinterpret_cast<vector_node_header_le_t *>(mutable_data);
}

uint32_t VectorNodeListLayout::entry_array_base() const
{
  const auto *stored = header();
  if (stored == nullptr) {
    return VECTOR_NODE_LIST_VECTOR_PLANE_BASE;
  }
  const uint64_t slot_bytes =
    uint64_t(uint32_t(stored->item_count)) * sizeof(vector_list_entry_le_t);
  return slot_bytes <= uint32_t(stored->free_begin)
    ? uint32_t(stored->free_begin) - static_cast<uint32_t>(slot_bytes)
    : VECTOR_NODE_LIST_VECTOR_PLANE_BASE;
}

const vector_list_entry_le_t *VectorNodeListLayout::entries() const
{
  return reinterpret_cast<const vector_list_entry_le_t *>(
    data + entry_array_base());
}

std::optional<VectorNodeListLayout> VectorNodeListLayout::initialize(
  char *data,
  size_t length)
{
  VectorNodeListLayout layout(data, length);
  if (layout.initialize_page() < 0 || layout.validate() < 0) {
    return std::nullopt;
  }
  return layout;
}

std::optional<VectorNodeListLayout> VectorNodeListLayout::open_checked(
  char *data,
  size_t length)
{
  VectorNodeListLayout layout(data, length);
  return layout.validate() == 0
    ? std::optional<VectorNodeListLayout>(layout)
    : std::nullopt;
}

std::optional<VectorNodeListLayout> VectorNodeListLayout::open_checked(
  const char *data,
  size_t length)
{
  VectorNodeListLayout layout(data, length);
  return layout.validate() == 0
    ? std::optional<VectorNodeListLayout>(layout)
    : std::nullopt;
}

int VectorNodeListLayout::initialize_page()
{
  if (mutable_data == nullptr ||
      length < VECTOR_NODE_LIST_HEADER_SIZE ||
      length > std::numeric_limits<uint32_t>::max() ||
      length % VECTOR_NODE_VECTOR_ALIGNMENT != 0) {
    return -EINVAL;
  }
  std::memset(mutable_data, 0, length);
  auto *stored = mutable_header();
  stored->magic = VECTOR_NODE_LAYOUT_MAGIC;
  stored->layout_version = VECTOR_NODE_LIST_LAYOUT_VERSION;
  stored->kind = static_cast<uint8_t>(vector_node_layout_kind_t::LIST);
  stored->header_size = VECTOR_NODE_LIST_HEADER_SIZE;
  stored->free_begin = VECTOR_NODE_LIST_VECTOR_PLANE_BASE;
  stored->free_end = static_cast<uint32_t>(length);
  stored->used_bytes = VECTOR_NODE_LIST_VECTOR_PLANE_BASE;
  const vector_list_header_extension_le_t extension{};
  std::memcpy(
    mutable_data + sizeof(vector_node_header_le_t),
    &extension,
    sizeof(extension));
  return 0;
}

int VectorNodeListLayout::validate() const
{
  const int common = validate_common_header(
    data,
    length,
    vector_node_layout_kind_t::LIST,
    sizeof(vector_list_entry_le_t),
    VECTOR_NODE_LIST_LAYOUT_VERSION,
    VECTOR_NODE_LIST_HEADER_SIZE);
  if (common < 0) {
    return common;
  }
  const auto *stored = header();
  // No metadata region is ever bump-allocated from the tail any more, so
  // free_end is a fixed page-length marker, not a moving cursor.
  if (length % VECTOR_NODE_VECTOR_ALIGNMENT != 0 ||
      uint32_t(stored->free_end) != length ||
      uint32_t(stored->used_bytes) != uint32_t(stored->free_begin) ||
      uint64_t(stored->logical_entry_count) !=
        uint32_t(stored->item_count)) {
    return -EINVAL;
  }

  const uint32_t count = stored->item_count;
  for (uint32_t i = 0; i < count; ++i) {
    if (!ceph::os::is_lower_hex_string(entry_id_at(i), 8)) {
      return -EINVAL;
    }
  }

  // The vector plane fills [vector_plane_base(), entry_array_base())
  // exactly with `count` equal-stride rows: slot order defines vector-row
  // order, with no separately tracked vector-index state, so any gap,
  // overlap, or uneven split here is corruption. The page cannot know the
  // *logical* (unaligned) row length -- that comes from the group's
  // (dimension, data_type), which only the caller has -- so this only
  // checks that the plane divides evenly into `count` aligned rows.
  const uint32_t plane_bytes = entry_array_base() - vector_plane_base();
  if (count == 0) {
    if (plane_bytes != 0) {
      return -EINVAL;
    }
  } else if (plane_bytes % count != 0 ||
             (plane_bytes / count) % VECTOR_NODE_VECTOR_ALIGNMENT != 0) {
    return -EINVAL;
  }
  return 0;
}

uint32_t VectorNodeListLayout::item_count() const
{
  return header() == nullptr ? 0 : uint32_t(header()->item_count);
}

uint64_t VectorNodeListLayout::logical_entry_count() const
{
  return header() == nullptr ? 0 : uint64_t(header()->logical_entry_count);
}

laddr_t VectorNodeListLayout::next_page_laddr() const
{
  if (header() == nullptr || length < VECTOR_NODE_LIST_HEADER_SIZE) {
    return L_ADDR_NULL;
  }
  vector_list_header_extension_le_t extension;
  std::memcpy(
    &extension,
    data + sizeof(vector_node_header_le_t),
    sizeof(extension));
  return extension.next_page_laddr;
}

int VectorNodeListLayout::set_next_page_laddr(laddr_t next_page_laddr)
{
  if (mutable_data == nullptr) {
    return -EROFS;
  }
  const int current_validation = validate();
  if (current_validation < 0) {
    return current_validation;
  }
  vector_list_header_extension_le_t extension;
  std::memcpy(
    &extension,
    mutable_data + sizeof(vector_node_header_le_t),
    sizeof(extension));
  extension.next_page_laddr = next_page_laddr;
  std::memcpy(
    mutable_data + sizeof(vector_node_header_le_t),
    &extension,
    sizeof(extension));
  return validate();
}

uint32_t VectorNodeListLayout::contiguous_free_space() const
{
  if (header() == nullptr ||
      uint32_t(header()->free_begin) > length) {
    return 0;
  }
  return static_cast<uint32_t>(length) - uint32_t(header()->free_begin);
}

uint32_t VectorNodeListLayout::used_space() const
{
  return header() == nullptr ? 0 : uint32_t(header()->used_bytes);
}

namespace {
bool entry_in_bounds(
  const vector_node_header_le_t *stored, uint32_t base, size_t length,
  uint32_t index)
{
  if (stored == nullptr || index >= uint32_t(stored->item_count)) {
    return false;
  }
  const uint64_t entry_end =
    uint64_t(base) + (uint64_t(index) + 1) * sizeof(vector_list_entry_le_t);
  return entry_end <= length && entry_end <= uint32_t(stored->free_begin);
}
} // anonymous namespace

std::string_view VectorNodeListLayout::entry_id_at(uint32_t index) const
{
  const auto *stored = header();
  if (!entry_in_bounds(stored, entry_array_base(), length, index)) {
    return {};
  }
  return std::string_view(entries()[index].entry_id, 8);
}

uint32_t VectorNodeListLayout::vector_plane_base() const
{
  return VECTOR_NODE_LIST_VECTOR_PLANE_BASE;
}

uint32_t VectorNodeListLayout::vector_offset_at(
  uint32_t index, uint32_t row_length) const
{
  return vector_plane_base() +
    index * aligned_vector_length(row_length);
}

std::optional<VectorNodeRecordView> VectorNodeListLayout::record_at(
  uint32_t index, uint32_t row_length) const
{
  if (index >= item_count()) {
    return std::nullopt;
  }
  return record_view_at(index, vector_offset_at(index, row_length),
                         row_length);
}

std::optional<VectorNodeRecordView> VectorNodeListLayout::record_view_at(
  uint32_t index, uint32_t vector_offset, uint32_t row_length) const
{
  const auto *stored = header();
  if (!entry_in_bounds(stored, entry_array_base(), length, index) ||
      vector_offset > length || row_length > length - vector_offset) {
    return std::nullopt;
  }
  return VectorNodeRecordView(
    std::string_view(entries()[index].entry_id, 8),
    std::string_view(data + vector_offset, row_length));
}

int VectorNodeListLayout::scan_records(
  uint32_t row_length, const record_visitor_t &visitor) const
{
  if (header() == nullptr) {
    return -EINVAL;
  }
  const uint32_t count = item_count();
  const uint32_t stride = aligned_vector_length(row_length);
  uint32_t offset = vector_plane_base();
  for (uint32_t i = 0; i < count; ++i) {
    const auto view = record_view_at(i, offset, row_length);
    if (!view) {
      return -EINVAL;
    }
    visitor(*view);
    offset += stride;
  }
  return 0;
}

std::optional<VectorNodeListLayout::matrix_view_t>
VectorNodeListLayout::vector_matrix_view(
  uint32_t begin, uint32_t end, uint32_t row_length) const
{
  const uint32_t count = item_count();
  if (data == nullptr || begin >= end || end > count || row_length == 0) {
    return std::nullopt;
  }
  const uint32_t offset = vector_offset_at(begin, row_length);
  const uint32_t stride = aligned_vector_length(row_length);
  const uint64_t span = uint64_t(end - begin - 1) * stride + row_length;
  if (offset > length || span > length - offset) {
    return std::nullopt;
  }
  matrix_view_t view;
  view.data = data + offset;
  view.row_count = end - begin;
  view.row_stride = stride;
  view.row_length = row_length;
  return view;
}

int VectorNodeListLayout::append_record(
  const ceph::os::vector_record_t &record)
{
  if (mutable_data == nullptr) {
    return -EROFS;
  }
  const int current_validation = validate();
  if (current_validation < 0) {
    return current_validation;
  }
  const int record_validation = ceph::os::validate_vector_record(record);
  if (record_validation < 0) {
    return record_validation;
  }
  // validate_vector_record() has already checked
  // vector_data.length() == dimension * element_size(data_type).
  if (record.vector_data.length() > std::numeric_limits<uint32_t>::max()) {
    return -EOVERFLOW;
  }
  const uint32_t row_length =
    static_cast<uint32_t>(record.vector_data.length());
  const uint32_t aligned_vector = aligned_vector_length(row_length);
  const uint32_t count = item_count();

  // Every row in this chain must share one aligned length (see the class
  // comment): a page that already holds rows locks in the stride the
  // first row established. The caller is expected to never trigger this,
  // since it only ever appends records already routed to this chain by
  // their own (dimension, data_type) matching the group's.
  if (count != 0) {
    const uint32_t existing_stride =
      (entry_array_base() - vector_plane_base()) / count;
    if (existing_stride != aligned_vector) {
      return -EINVAL;
    }
  }

  const uint64_t required =
    uint64_t(sizeof(vector_list_entry_le_t)) + aligned_vector;
  if (required > contiguous_free_space()) {
    return -ENOSPC;
  }

  // Grow the vector plane's tail: shift the (tiny, fixed-stride) entry_id
  // array forward by the new row's aligned size, then drop the row into
  // the space it vacated. This is cheaper than shifting the vector plane
  // itself, since a slot is a few bytes and a row can be kilobytes.
  const uint32_t old_slots_base = entry_array_base();
  const uint32_t new_slots_base = old_slots_base + aligned_vector;
  std::memmove(
    mutable_data + new_slots_base,
    mutable_data + old_slots_base,
    uint64_t(count) * sizeof(vector_list_entry_le_t));

  char *vector_dest = mutable_data + old_slots_base;
  std::memset(vector_dest, 0, aligned_vector);
  auto vector_it = record.vector_data.cbegin();
  vector_it.copy(row_length, vector_dest);

  vector_list_entry_le_t slot;
  std::memcpy(slot.entry_id, record.entry_id.data(), 8);
  std::memcpy(
    mutable_data + new_slots_base + uint64_t(count) *
      sizeof(vector_list_entry_le_t),
    &slot, sizeof(slot));

  auto *stored = mutable_header();
  stored->item_count = count + 1;
  stored->logical_entry_count = count + 1;
  stored->free_begin = new_slots_base +
    (count + 1) * sizeof(vector_list_entry_le_t);
  stored->used_bytes = stored->free_begin;
  return validate();
}

} // namespace crimson::os::seastore
