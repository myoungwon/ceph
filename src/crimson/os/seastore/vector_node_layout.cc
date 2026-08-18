// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/vector_node_layout.h"

#include <cerrno>
#include <cstring>
#include <limits>

namespace crimson::os::seastore {

namespace {

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

} // namespace crimson::os::seastore
