// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/vector_node.h"

#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <limits>
#include <optional>

#include <boost/range/irange.hpp>

#include "common/error_code.h"
#include "include/ceph_assert.h"

namespace crimson::os::seastore {

namespace {

bool valid_page_image(std::string_view page)
{
  return VectorNodeIndexLayout::open_checked(page.data(), page.size()) ||
    VectorNodeListLayout::open_checked(page.data(), page.size());
}

} // anonymous namespace

VectorNode::VectorNode(ceph::bufferptr &&ptr)
  : LogicalChildNode(std::move(ptr)) {}

VectorNode::VectorNode(extent_len_t length)
  : LogicalChildNode(length) {}

VectorNode::VectorNode(const VectorNode &rhs)
  : LogicalChildNode(rhs) {}

CachedExtentRef VectorNode::duplicate_for_write(Transaction&)
{
  return CachedExtentRef(new VectorNode(*this));
}

std::optional<VectorNodeIndexLayout> VectorNode::index_layout() const
{
  return VectorNodeIndexLayout::open_checked(
    get_bptr().c_str(), get_length());
}

std::optional<VectorNodeListLayout> VectorNode::list_layout() const
{
  return VectorNodeListLayout::open_checked(
    get_bptr().c_str(), get_length());
}

std::optional<VectorNodeIndexLayout> VectorNode::mutable_index_layout()
{
  return VectorNodeIndexLayout::open_checked(
    get_bptr().c_str(), get_length());
}

std::optional<VectorNodeListLayout> VectorNode::mutable_list_layout()
{
  return VectorNodeListLayout::open_checked(
    get_bptr().c_str(), get_length());
}

bool VectorNode::validate_page() const
{
  return index_layout().has_value() || list_layout().has_value();
}

void VectorNode::on_clean_read()
{
  if (!validate_page()) {
    throw ceph::buffer::malformed_input("invalid VectorNode page");
  }
}

void VectorNode::on_initial_write()
{}

void VectorNode::prepare_commit(Transaction&)
{
  ceph_assert(validate_page());
}

ceph::bufferlist VectorNode::get_delta()
{
  // VectorNode journal deltas are full-page images.
  ceph::bufferlist bl;
  ceph::bufferptr bptr(get_bptr(), 0, get_length());
  bl.append(bptr);
  return bl;
}

void VectorNode::clear_delta()
{}

int VectorNode::initialize_index()
{
  return VectorNodeIndexLayout::initialize(
    get_bptr().c_str(), get_length()).has_value() ? 0 : -EINVAL;
}

int VectorNode::initialize_list()
{
  return VectorNodeListLayout::initialize(
    get_bptr().c_str(), get_length()).has_value() ? 0 : -EINVAL;
}

int VectorNode::replace_page(std::string_view page)
{
  if (page.size() != get_length() || !valid_page_image(page)) {
    return -EINVAL;
  }
  get_bptr().copy_in(0, page.size(), page.data());
  return 0;
}

void VectorNode::apply_delta(const ceph::bufferlist &_bl)
{
  if (_bl.length() != get_length()) {
    throw ceph::buffer::malformed_input("invalid VectorNode replay image");
  }
  ceph::bufferlist bl = _bl;
  bl.rebuild();
  const std::string_view page(bl.front().c_str(), bl.length());
  if (!valid_page_image(page)) {
    throw ceph::buffer::malformed_input("invalid VectorNode replay image");
  }
  get_bptr().copy_in(0, page.size(), page.data());
}

std::ostream &VectorNode::print_detail_l(std::ostream &out) const
{
  if (const auto index = index_layout(); index) {
    return out << "index entries=" << index->item_count();
  }
  if (const auto list = list_layout(); list) {
    return out << "list entries=" << list->item_count();
  }
  return out << "invalid";
}

// A false result is surfaced by callers as ct_error::enospc, even though a
// misconfigured block size is a configuration error rather than exhausted
// space: create_iertr (TransactionManager::alloc_extent_iertr) has no other
// error in its set to report this with.
bool VectorNodeManager::is_valid_list_extent_size(extent_len_t length) const
{
  return (length == (16u << 10) || length == (32u << 10)) &&
         length % VECTOR_NODE_VECTOR_ALIGNMENT == 0 &&
         is_aligned(length, tm.get_block_size());
}

VectorNodeManager::create_ret VectorNodeManager::create_index_table(
  Transaction &t)
{
  // See is_valid_list_extent_size(): enospc here likewise stands in for a
  // block-size/alignment configuration error, not exhausted space.
  if (!is_aligned(VECTOR_NODE_INDEX_BYTES, tm.get_block_size())) {
    return crimson::ct_error::enospc::make();
  }
  return tm.alloc_non_data_extent<VectorNode>(
    t,
    laddr_hint_t::create_global_md_hint(),
    VECTOR_NODE_INDEX_BYTES
  ).si_then([](auto index) -> create_ret {
    if (index->initialize_index() < 0) {
      return crimson::ct_error::input_output_error::make();
    }
    return create_iertr::make_ready_future<VectorNodeRef>(std::move(index));
  });
}

VectorNodeManager::create_ret VectorNodeManager::create_empty_vector_list(
  Transaction &t)
{
  if (!is_valid_list_extent_size(list_block_bytes)) {
    return crimson::ct_error::enospc::make();
  }
  return tm.alloc_non_data_extent<VectorNode>(
    t,
    laddr_hint_t::create_global_md_hint(),
    list_block_bytes
  ).si_then([](auto list) -> create_ret {
    if (list->initialize_list() < 0) {
      return crimson::ct_error::input_output_error::make();
    }
    return create_iertr::make_ready_future<VectorNodeRef>(std::move(list));
  });
}

VectorNodeManager::read_ret VectorNodeManager::read_vector_node(
  Transaction &t,
  laddr_t addr)
{
  return tm.read_extent<VectorNode>(t, addr
  ).si_then([](auto ret) -> read_ret {
    // Clean reads and replay are validated by VectorNode lifecycle hooks;
    // layout accessors still checked-open the requested INDEX or LIST kind.
    return read_iertr::make_ready_future<VectorNodeRef>(
      std::move(ret.extent));
  });
}

VectorNodeManager::find_ret VectorNodeManager::find_group_head(
  VectorNodeRef index,
  uint32_t dimension,
  uint32_t data_type)
{
  if (!index) {
    return crimson::ct_error::input_output_error::make();
  }
  const auto layout = index->index_layout();
  if (!layout) {
    return crimson::ct_error::input_output_error::make();
  }
  const auto position = layout->find_entry(dimension, data_type);
  if (!position) {
    return find_iertr::make_ready_future<std::optional<laddr_t>>(
      std::nullopt);
  }
  const auto entry = layout->entry_at(*position);
  if (!entry) {
    return crimson::ct_error::input_output_error::make();
  }
  return find_iertr::make_ready_future<std::optional<laddr_t>>(
    entry->head_laddr());
}

VectorNodeManager::append_ret VectorNodeManager::append_vector_entry(
  Transaction &t,
  VectorNodeRef index,
  const ceph::os::vector_record_t &entry)
{
  if (!index || ceph::os::validate_vector_record(entry) < 0) {
    return crimson::ct_error::input_output_error::make();
  }
  const auto index_layout = index->index_layout();
  if (!index_layout) {
    return crimson::ct_error::input_output_error::make();
  }
  const auto position =
    index_layout->find_entry(entry.dimension, entry.data_type);

  if (!position) {
    // Miss: create the group's first (and, for now, only) LIST block, then
    // insert the index entry with head == tail == that block. No scan, no
    // duplicate check.
    return create_empty_vector_list(t
    ).si_then([this, &t, index=std::move(index), entry](auto list) mutable
        -> append_ret {
      auto mutable_list = list->mutable_list_layout();
      if (!mutable_list || mutable_list->append_record(entry) < 0) {
        return crimson::ct_error::input_output_error::make();
      }
      auto mutable_index = tm.get_mutable_extent(
        t, index->cast<LogicalChildNode>())->cast<VectorNode>();
      auto mutable_index_layout = mutable_index->mutable_index_layout();
      if (!mutable_index_layout) {
        return crimson::ct_error::input_output_error::make();
      }
      const int result = mutable_index_layout->insert_entry(
        entry.dimension, entry.data_type,
        list->get_laddr(), list->get_laddr());
      if (result < 0) {
        return result == -ENOSPC
          ? append_ret(crimson::ct_error::enospc::make())
          : append_ret(crimson::ct_error::input_output_error::make());
      }
      return append_iertr::now();
    });
  }

  // Hit: go straight to the tail block via the index entry -- this never
  // walks the chain to find where to append.
  const auto current_entry = index_layout->entry_at(*position);
  if (!current_entry) {
    return crimson::ct_error::input_output_error::make();
  }
  const laddr_t tail_laddr = current_entry->tail_laddr();
  const uint32_t entry_index = *position;

  return read_vector_node(t, tail_laddr
  ).si_then([this, &t, index=std::move(index), entry, entry_index](
      auto tail) mutable -> append_ret {
    auto mutable_tail = tm.get_mutable_extent(
      t, tail->template cast<LogicalChildNode>())->template cast<VectorNode>();
    auto mutable_tail_layout = mutable_tail->mutable_list_layout();
    if (!mutable_tail_layout) {
      return crimson::ct_error::input_output_error::make();
    }
    const int append_result = mutable_tail_layout->append_record(entry);
    if (append_result == 0) {
      // Common case: room in the current tail. The INDEX extent is never
      // touched here.
      return append_iertr::now();
    }
    if (append_result != -ENOSPC) {
      return crimson::ct_error::input_output_error::make();
    }

    // Tail full: allocate a new block, link the old tail to it, and
    // rewrite only this entry's tail_laddr in the INDEX extent.
    return create_empty_vector_list(t
    ).si_then([this, &t, index=std::move(index), entry, entry_index,
               mutable_tail=std::move(mutable_tail)](auto new_tail) mutable
        -> append_ret {
      auto new_list_layout = new_tail->mutable_list_layout();
      if (!new_list_layout || new_list_layout->append_record(entry) < 0) {
        return crimson::ct_error::input_output_error::make();
      }
      auto old_list_layout = mutable_tail->mutable_list_layout();
      if (!old_list_layout ||
          old_list_layout->set_next_page_laddr(new_tail->get_laddr()) < 0) {
        return crimson::ct_error::input_output_error::make();
      }
      auto mutable_index = tm.get_mutable_extent(
        t, index->cast<LogicalChildNode>())->cast<VectorNode>();
      auto mutable_index_layout = mutable_index->mutable_index_layout();
      if (!mutable_index_layout ||
          mutable_index_layout->set_tail_laddr(
            entry_index, new_tail->get_laddr()) < 0) {
        return crimson::ct_error::input_output_error::make();
      }
      return append_iertr::now();
    });
  });
}

VectorNodeManager::scan_ret VectorNodeManager::scan_vector_group(
  Transaction &t,
  laddr_t head_laddr,
  uint32_t dimension,
  uint32_t data_type,
  scan_visitor_t visitor,
  bool measure_visitor)
{
  if (head_laddr == L_ADDR_NULL) {
    return crimson::ct_error::input_output_error::make();
  }
  size_t element_size = 0;
  if (ceph::rados::vector_data_type_size(data_type, &element_size) < 0 ||
      dimension == 0 || dimension > ceph::rados::vector_max_dimension) {
    return crimson::ct_error::input_output_error::make();
  }
  const uint32_t row_length =
    static_cast<uint32_t>(uint64_t(dimension) * element_size);
  return seastar::do_with(
    head_laddr,
    std::move(visitor),
    vector_scan_stats_t(),
    [this, &t, measure_visitor, row_length](
        auto &next_laddr, auto &visitor, auto &stats) {
      return trans_intr::repeat(
        [this, &t, &next_laddr, &visitor, &stats, measure_visitor,
         row_length]()
            -> read_iertr::future<seastar::stop_iteration> {
          if (next_laddr == L_ADDR_NULL) {
            return read_iertr::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::yes);
          }
          return read_vector_node(t, next_laddr
          ).si_then([&next_laddr, &visitor, &stats, measure_visitor,
                     row_length](
              auto list) -> read_iertr::future<seastar::stop_iteration> {
            const auto layout = list->list_layout();
            if (!layout) {
              return crimson::ct_error::input_output_error::make();
            }
            stats.list_extents++;
            stats.extent_bytes_visited += list->get_length();
            const auto visitor_begin = measure_visitor
              ? std::chrono::steady_clock::now()
              : std::chrono::steady_clock::time_point();
            uint64_t block_entries = 0;
            const int scan_result = layout->scan_records(
              row_length,
              [&visitor, &block_entries](const VectorNodeRecordView &record) {
                visitor(record);
                ++block_entries;
              });
            if (scan_result < 0) {
              return crimson::ct_error::input_output_error::make();
            }
            stats.logical_entries += block_entries;
            if (measure_visitor) {
              stats.visitor_ns += std::chrono::duration_cast<
                std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - visitor_begin).count();
            }
            next_laddr = layout->next_page_laddr();
            return read_iertr::make_ready_future<seastar::stop_iteration>(
              seastar::stop_iteration::no);
          });
        }
      ).si_then([&stats] {
        return read_iertr::make_ready_future<vector_scan_stats_t>(stats);
      });
    });
}

VectorNodeManager::remove_ret VectorNodeManager::remove_index_table(
  Transaction &t,
  VectorNodeRef index)
{
  if (!index) {
    return crimson::ct_error::input_output_error::make();
  }
  auto layout = index->index_layout();
  if (!layout) {
    return crimson::ct_error::input_output_error::make();
  }

  return seastar::do_with(
    std::move(index),
    std::move(*layout),
    [this, &t](auto &index, auto &layout) {
      const auto indices = boost::irange<uint32_t>(0, layout.item_count());
      return trans_intr::do_for_each(
        indices.begin(), indices.end(),
        [this, &t, &layout](uint32_t entry_index) -> remove_iertr::future<> {
          const auto entry = layout.entry_at(entry_index);
          if (!entry) {
            return crimson::ct_error::input_output_error::make();
          }
          return seastar::do_with(
            entry->head_laddr(),
            [this, &t](auto &next_laddr) {
              return trans_intr::repeat(
                [this, &t, &next_laddr]()
                    -> remove_iertr::future<seastar::stop_iteration> {
                  if (next_laddr == L_ADDR_NULL) {
                    return remove_iertr::make_ready_future<
                      seastar::stop_iteration>(seastar::stop_iteration::yes);
                  }
                  return read_vector_node(t, next_laddr
                  ).si_then([this, &t, &next_laddr](auto list)
                      -> remove_iertr::future<seastar::stop_iteration> {
                    const auto list_layout = list->list_layout();
                    if (!list_layout) {
                      return crimson::ct_error::input_output_error::make();
                    }
                    const laddr_t current = next_laddr;
                    next_laddr = list_layout->next_page_laddr();
                    return tm.remove(t, current
                    ).si_then([](auto) {
                      return remove_iertr::make_ready_future<
                        seastar::stop_iteration>(seastar::stop_iteration::no);
                    });
                  });
                });
            });
        }
      ).si_then([this, &t, &index] {
        return tm.remove(t, index->template cast<LogicalChildNode>()
        ).si_then([](auto) {
          return remove_iertr::now();
        });
      });
    });
}

} // namespace crimson::os::seastore
