// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/vector_node.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>

#include "common/error_code.h"
#include "include/ceph_assert.h"
#include "include/encoding.h"

namespace crimson::os::seastore {

namespace {

constexpr uint32_t MAX_VECTOR_NODE_ITEMS =
  VECTOR_NODE_MAX_BYTES / sizeof(uint32_t);

void encode_laddr(laddr_t addr, ceph::bufferlist &bl)
{
  using ceph::encode;
  const laddr_le_t encoded(addr);
  encode(encoded.low64, bl);
  encode(encoded.high64, bl);
}

laddr_t decode_laddr(ceph::bufferlist::const_iterator &p)
{
  using ceph::decode;
  laddr_le_t encoded;
  decode(encoded.low64, p);
  decode(encoded.high64, p);
  return encoded;
}

void encode_descriptor(
  const vector_leaf_descriptor_t &descriptor,
  ceph::bufferlist &bl)
{
  using ceph::encode;
  encode(descriptor.first_entry_id, bl);
  encode(descriptor.last_entry_id, bl);
  encode_laddr(descriptor.laddr, bl);
  encode(descriptor.entry_count, bl);
  encode(descriptor.encoded_bytes, bl);
}

vector_leaf_descriptor_t decode_descriptor(
  ceph::bufferlist::const_iterator &p)
{
  using ceph::decode;
  vector_leaf_descriptor_t descriptor;
  decode(descriptor.first_entry_id, p);
  decode(descriptor.last_entry_id, p);
  descriptor.laddr = decode_laddr(p);
  decode(descriptor.entry_count, p);
  decode(descriptor.encoded_bytes, p);
  return descriptor;
}

void encode_entry(
  const vector_node_entry_t &entry,
  ceph::bufferlist &bl)
{
  entry.encode(bl);
}

vector_node_entry_t decode_entry(
  ceph::bufferlist::const_iterator &p)
{
  vector_node_entry_t entry;
  entry.decode(p);
  if (ceph::os::validate_vector_record(entry) < 0) {
    throw ceph::buffer::malformed_input("invalid VectorNode record");
  }
  return entry;
}

vector_leaf_descriptor_t make_descriptor(
  const VectorNodeRef &leaf)
{
  const auto &contents = leaf->get_contents();
  ceph_assert(contents.kind == vector_node_kind_t::LEAF);
  ceph_assert(!contents.entries.empty());
  return vector_leaf_descriptor_t{
    contents.entries.front().entry_id,
    contents.entries.back().entry_id,
    leaf->get_laddr(),
    static_cast<uint32_t>(contents.entries.size()),
    static_cast<uint32_t>(VectorNode::encode_contents(contents).length())
  };
}

bool valid_descriptor(const vector_leaf_descriptor_t &descriptor)
{
  return ceph::os::is_lower_hex_string(descriptor.first_entry_id, 8) &&
    ceph::os::is_lower_hex_string(descriptor.last_entry_id, 8) &&
    descriptor.first_entry_id <= descriptor.last_entry_id &&
    descriptor.laddr != L_ADDR_NULL &&
    descriptor.entry_count != 0 &&
    descriptor.encoded_bytes != 0 &&
    descriptor.encoded_bytes <= VECTOR_NODE_MAX_BYTES;
}

bool valid_node_contents(const vector_node_t &node)
{
  if (node.format_version != VECTOR_NODE_FORMAT_VERSION) {
    return false;
  }
  if (node.kind == vector_node_kind_t::ROOT) {
    if (!node.entries.empty() || !VectorNode::is_sorted(node.leaves)) {
      return false;
    }
    uint64_t entry_count = 0;
    for (const auto &descriptor : node.leaves) {
      if (!valid_descriptor(descriptor) ||
          entry_count > std::numeric_limits<uint64_t>::max() -
            descriptor.entry_count) {
        return false;
      }
      entry_count += descriptor.entry_count;
    }
    return entry_count == node.logical_entry_count;
  }
  if (node.kind == vector_node_kind_t::LEAF) {
    return node.leaves.empty() &&
      !node.entries.empty() &&
      node.logical_entry_count == node.entries.size() &&
      VectorNode::is_sorted(node.entries) &&
      std::all_of(
        node.entries.begin(), node.entries.end(), [](const auto &entry) {
          return ceph::os::validate_vector_record(entry) == 0;
        });
  }
  return false;
}

bool leaf_matches_descriptor(
  const vector_node_t &leaf,
  const vector_leaf_descriptor_t &descriptor)
{
  return leaf.kind == vector_node_kind_t::LEAF &&
    !leaf.entries.empty() &&
    leaf.entries.front().entry_id == descriptor.first_entry_id &&
    leaf.entries.back().entry_id == descriptor.last_entry_id &&
    leaf.entries.size() == descriptor.entry_count;
}

std::optional<size_t> find_split_position(
  const std::vector<vector_node_entry_t> &entries,
  extent_len_t node_bytes)
{
  if (entries.size() < 2) {
    return std::nullopt;
  }

  const size_t midpoint = entries.size() / 2;
  for (size_t distance = 0; distance < entries.size(); ++distance) {
    const size_t candidates[] = {
      midpoint >= distance ? midpoint - distance : 0,
      midpoint + distance,
    };
    for (size_t position : candidates) {
      if (position == 0 || position >= entries.size()) {
        continue;
      }
      vector_node_t left;
      left.kind = vector_node_kind_t::LEAF;
      left.entries.assign(entries.begin(), entries.begin() + position);
      left.logical_entry_count = left.entries.size();
      vector_node_t right;
      right.kind = vector_node_kind_t::LEAF;
      right.entries.assign(entries.begin() + position, entries.end());
      right.logical_entry_count = right.entries.size();
      if (VectorNode::encode_contents(left).length() <= node_bytes &&
          VectorNode::encode_contents(right).length() <= node_bytes) {
        return position;
      }
    }
  }
  return std::nullopt;
}

}

ceph::bufferlist VectorNode::encode_contents(const vector_node_t &node)
{
  using ceph::encode;
  ceph::bufferlist bl;
  encode(node.format_version, bl);
  encode(static_cast<uint8_t>(node.kind), bl);
  encode(node.logical_entry_count, bl);

  if (node.kind == vector_node_kind_t::ROOT) {
    encode(static_cast<uint32_t>(node.leaves.size()), bl);
    for (const auto &leaf : node.leaves) {
      encode_descriptor(leaf, bl);
    }
  } else {
    encode(static_cast<uint32_t>(node.entries.size()), bl);
    for (const auto &entry : node.entries) {
      encode_entry(entry, bl);
    }
  }
  return bl;
}

vector_node_t VectorNode::decode_contents(const ceph::bufferlist &bl)
{
  using ceph::decode;
  auto p = bl.cbegin();
  vector_node_t node;
  decode(node.format_version, p);
  if (node.format_version != VECTOR_NODE_FORMAT_VERSION) {
    throw ceph::buffer::malformed_input("unsupported VectorNode format");
  }

  uint8_t kind = 0;
  decode(kind, p);
  if (kind != static_cast<uint8_t>(vector_node_kind_t::ROOT) &&
      kind != static_cast<uint8_t>(vector_node_kind_t::LEAF)) {
    throw ceph::buffer::malformed_input("invalid VectorNode kind");
  }
  node.kind = static_cast<vector_node_kind_t>(kind);
  decode(node.logical_entry_count, p);

  uint32_t item_count = 0;
  decode(item_count, p);
  if (item_count > MAX_VECTOR_NODE_ITEMS) {
    throw ceph::buffer::malformed_input("invalid VectorNode item count");
  }

  if (node.kind == vector_node_kind_t::ROOT) {
    node.leaves.reserve(item_count);
    uint64_t entry_count = 0;
    for (uint32_t i = 0; i < item_count; ++i) {
      auto descriptor = decode_descriptor(p);
      if (!valid_descriptor(descriptor)) {
        throw ceph::buffer::malformed_input(
            "invalid VectorNode leaf descriptor");
      }
      if (entry_count >
          std::numeric_limits<uint64_t>::max() - descriptor.entry_count) {
        throw ceph::buffer::malformed_input(
            "VectorNode entry count overflow");
      }
      entry_count += descriptor.entry_count;
      node.leaves.push_back(std::move(descriptor));
    }
    if (entry_count != node.logical_entry_count ||
        !VectorNode::is_sorted(node.leaves)) {
      throw ceph::buffer::malformed_input("invalid VectorNode root");
    }
  } else {
    if (node.logical_entry_count != item_count) {
      throw ceph::buffer::malformed_input(
          "invalid VectorNode leaf entry count");
    }
    node.entries.reserve(item_count);
    for (uint32_t i = 0; i < item_count; ++i) {
      node.entries.push_back(decode_entry(p));
    }
    if (node.entries.empty() || !VectorNode::is_sorted(node.entries)) {
      throw ceph::buffer::malformed_input("invalid VectorNode leaf");
    }
  }
  return node;
}

VectorNode::VectorNode(ceph::bufferptr &&ptr)
  : LogicalChildNode(std::move(ptr)) {}

VectorNode::VectorNode(extent_len_t length)
  : LogicalChildNode(length) {}

VectorNode::VectorNode(const VectorNode &rhs)
  : LogicalChildNode(rhs),
    contents(rhs.contents),
    decoded(rhs.decoded)
{
  ceph_assert(!rhs.dirty);
}

CachedExtentRef VectorNode::duplicate_for_write(Transaction&)
{
  return CachedExtentRef(new VectorNode(*this));
}

void VectorNode::on_clean_read()
{
  decode_from_buffer();
  dirty = false;
}

void VectorNode::on_initial_write()
{
  dirty = false;
}

void VectorNode::prepare_commit(Transaction&)
{
  if (dirty) {
    materialize();
  }
}

ceph::bufferlist VectorNode::get_delta()
{
  if (dirty) {
    materialize();
  }

  ceph::bufferlist bl;
  ceph::bufferptr bptr(get_bptr(), 0, get_length());
  bl.append(bptr);
  return bl;
}

void VectorNode::clear_delta()
{
  dirty = false;
}

void VectorNode::initialize_root()
{
  contents = vector_node_t();
  contents.kind = vector_node_kind_t::ROOT;
  decoded = true;
  dirty = true;
  materialize();
}

void VectorNode::initialize_leaf(
  std::vector<vector_node_entry_t> entries)
{
  contents = vector_node_t();
  contents.kind = vector_node_kind_t::LEAF;
  contents.logical_entry_count = entries.size();
  contents.entries = std::move(entries);
  decoded = true;
  dirty = true;
  materialize();
}

bool VectorNode::is_sorted(
  const std::vector<vector_node_entry_t> &entries)
{
  return std::adjacent_find(
    entries.begin(),
    entries.end(),
    [](const auto &lhs, const auto &rhs) {
      return !(lhs.entry_id < rhs.entry_id);
    }) == entries.end();
}

bool VectorNode::is_sorted(
  const std::vector<vector_leaf_descriptor_t> &leaves)
{
  return std::adjacent_find(
    leaves.begin(),
    leaves.end(),
    [](const auto &lhs, const auto &rhs) {
      return !(lhs.last_entry_id < rhs.first_entry_id);
    }) == leaves.end();
}

void VectorNode::decode_from_buffer()
{
  ceph::bufferlist bl;
  bl.append(get_bptr());
  contents = decode_contents(bl);
  decoded = true;
}

void VectorNode::materialize()
{
  ceph_assert(decoded);
  auto bl = encode_contents(contents);
  ceph_assert(bl.length() <= get_length());
  ceph_assert(bl.length() <= VECTOR_NODE_MAX_BYTES);

  bl.rebuild();
  get_bptr().zero();
  if (bl.length()) {
    get_bptr().copy_in(0, bl.length(), bl.front().c_str());
  }
}

void VectorNode::apply_delta(const ceph::bufferlist &_bl)
{
  ceph_assert(_bl.length() == get_length());

  ceph::bufferlist bl = _bl;
  bl.rebuild();
  get_bptr().copy_in(0, bl.length(), bl.front().c_str());
  decode_from_buffer();
  dirty = false;
}

void VectorNode::logical_on_delta_write()
{
  dirty = false;
}

std::ostream &VectorNode::print_detail_l(std::ostream &out) const
{
  if (!decoded) {
    return out << "undecoded";
  }
  if (contents.kind == vector_node_kind_t::ROOT) {
    return out << "root entries=" << contents.logical_entry_count
               << ", leaves=" << contents.leaves.size();
  }
  return out << "leaf entries=" << contents.entries.size();
}

bool VectorNodeManager::is_valid_extent_size(extent_len_t length) const
{
  return length != 0 &&
         length <= VECTOR_NODE_MAX_BYTES &&
         is_aligned(length, tm.get_block_size()) &&
         is_aligned(VECTOR_NODE_MAX_BYTES, tm.get_block_size());
}

VectorNodeManager::create_ret VectorNodeManager::create_vector_root(
  Transaction &t)
{
  if (!is_valid_extent_size(node_bytes)) {
    return crimson::ct_error::enospc::make();
  }

  return tm.alloc_non_data_extent<VectorNode>(
    t,
    laddr_hint_t::create_global_md_hint(),
    node_bytes
  ).si_then([](auto root) {
    root->initialize_root();
    return create_iertr::make_ready_future<VectorNodeRef>(std::move(root));
  });
}

VectorNodeManager::create_ret VectorNodeManager::create_vector_leaf(
  Transaction &t,
  std::vector<vector_node_entry_t> entries)
{
  vector_node_t candidate;
  candidate.kind = vector_node_kind_t::LEAF;
  candidate.logical_entry_count = entries.size();
  candidate.entries = entries;
  if (!valid_node_contents(candidate)) {
    return crimson::ct_error::input_output_error::make();
  }
  if (!is_valid_extent_size(node_bytes) ||
      VectorNode::encode_contents(candidate).length() > node_bytes) {
    return crimson::ct_error::enospc::make();
  }

  return tm.alloc_non_data_extent<VectorNode>(
    t,
    laddr_hint_t::create_global_md_hint(),
    node_bytes
  ).si_then([entries=std::move(entries)](auto leaf) mutable {
    leaf->initialize_leaf(std::move(entries));
    return create_iertr::make_ready_future<VectorNodeRef>(std::move(leaf));
  });
}

VectorNodeManager::read_ret VectorNodeManager::read_vector_node(
  Transaction &t,
  laddr_t addr)
{
  return tm.read_extent<VectorNode>(t, addr
  ).si_then([](auto ret) {
    return read_iertr::make_ready_future<VectorNodeRef>(
      std::move(ret.extent));
  });
}

VectorNodeManager::upsert_ret VectorNodeManager::replace_contents(
  Transaction &t,
  VectorNodeRef node,
  vector_node_t contents)
{
  if (!valid_node_contents(contents)) {
    return crimson::ct_error::input_output_error::make();
  }
  const auto encoded_length = VectorNode::encode_contents(contents).length();
  if (encoded_length > node->get_length() ||
      encoded_length > VECTOR_NODE_MAX_BYTES) {
    return crimson::ct_error::enospc::make();
  }

  auto mutable_node = tm.get_mutable_extent(
    t,
    node->cast<LogicalChildNode>())->cast<VectorNode>();
  mutable_node->contents = std::move(contents);
  mutable_node->decoded = true;
  mutable_node->dirty = true;
  mutable_node->materialize();
  return mutate_iertr::make_ready_future<VectorNodeRef>(
    std::move(mutable_node));
}

VectorNodeManager::upsert_ret VectorNodeManager::upsert_vector_entry(
  Transaction &t,
  VectorNodeRef root,
  const vector_node_entry_t &entry)
{
  if (!root ||
      root->get_contents().kind != vector_node_kind_t::ROOT ||
      ceph::os::validate_vector_record(entry) < 0) {
    return crimson::ct_error::input_output_error::make();
  }

  auto root_contents = root->get_contents();
  if (root_contents.leaves.empty()) {
    return create_vector_leaf(t, {entry}
    ).si_then([this, &t, root=std::move(root),
               root_contents=std::move(root_contents)](auto leaf) mutable {
      root_contents.logical_entry_count = 1;
      root_contents.leaves.push_back(make_descriptor(leaf));
      return replace_contents(
        t, std::move(root), std::move(root_contents));
    });
  }

  auto descriptor = std::upper_bound(
    root_contents.leaves.begin(),
    root_contents.leaves.end(),
    entry.entry_id,
    [](const auto &entry_id, const auto &leaf) {
      return entry_id < leaf.first_entry_id;
    });
  if (descriptor != root_contents.leaves.begin()) {
    --descriptor;
  }
  const size_t descriptor_index =
    std::distance(root_contents.leaves.begin(), descriptor);

  return read_vector_node(t, descriptor->laddr
  ).si_then([this, &t, root=std::move(root), entry,
             root_contents=std::move(root_contents),
             descriptor_index](auto leaf) mutable -> upsert_ret {
    if (!leaf_matches_descriptor(
          leaf->get_contents(), root_contents.leaves[descriptor_index])) {
      return crimson::ct_error::input_output_error::make();
    }

    auto leaf_contents = leaf->get_contents();
    auto position = std::lower_bound(
      leaf_contents.entries.begin(),
      leaf_contents.entries.end(),
      entry.entry_id,
      [](const auto &stored, const auto &entry_id) {
        return stored.entry_id < entry_id;
      });
    const bool inserted =
      position == leaf_contents.entries.end() ||
      position->entry_id != entry.entry_id;
    if (inserted) {
      leaf_contents.entries.insert(position, entry);
      leaf_contents.logical_entry_count++;
    } else {
      *position = entry;
    }

    const auto encoded_length =
      VectorNode::encode_contents(leaf_contents).length();
    if (encoded_length <= leaf->get_length()) {
      return replace_contents(t, std::move(leaf), std::move(leaf_contents)
      ).si_then([this, &t, root=std::move(root),
                 root_contents=std::move(root_contents),
                 descriptor_index, inserted](auto leaf) mutable {
        root_contents.leaves[descriptor_index] = make_descriptor(leaf);
        if (inserted) {
          root_contents.logical_entry_count++;
        }
        return replace_contents(
          t, std::move(root), std::move(root_contents));
      });
    }

    const auto split_position =
      find_split_position(leaf_contents.entries, leaf->get_length());
    if (!split_position) {
      return crimson::ct_error::enospc::make();
    }

    std::vector<vector_node_entry_t> right_entries(
      leaf_contents.entries.begin() + *split_position,
      leaf_contents.entries.end());
    leaf_contents.entries.erase(
      leaf_contents.entries.begin() + *split_position,
      leaf_contents.entries.end());
    leaf_contents.logical_entry_count = leaf_contents.entries.size();

    return create_vector_leaf(t, std::move(right_entries)
    ).si_then([this, &t, root=std::move(root), leaf=std::move(leaf),
               leaf_contents=std::move(leaf_contents),
               root_contents=std::move(root_contents),
               descriptor_index, inserted](auto right_leaf) mutable {
      return replace_contents(t, std::move(leaf), std::move(leaf_contents)
      ).si_then([this, &t, root=std::move(root),
                 right_leaf=std::move(right_leaf),
                 root_contents=std::move(root_contents),
                 descriptor_index, inserted](auto left_leaf) mutable {
        root_contents.leaves[descriptor_index] =
          make_descriptor(left_leaf);
        root_contents.leaves.insert(
          root_contents.leaves.begin() + descriptor_index + 1,
          make_descriptor(right_leaf));
        if (inserted) {
          root_contents.logical_entry_count++;
        }
        return replace_contents(
          t, std::move(root), std::move(root_contents));
      });
    });
  });
}

VectorNodeManager::scan_ret VectorNodeManager::scan_vector_entries(
  Transaction &t,
  VectorNodeRef root,
  scan_visitor_t visitor,
  bool measure_visitor)
{
  if (!root ||
      root->get_contents().kind != vector_node_kind_t::ROOT) {
    return crimson::ct_error::input_output_error::make();
  }

  return seastar::do_with(
    std::move(root),
    std::move(visitor),
    vector_scan_stats_t(),
    [this, &t, measure_visitor](auto &root, auto &visitor, auto &stats) {
      stats.extent_bytes_scanned = root->get_length();
      return trans_intr::do_for_each(
        root->get_contents().leaves,
        [this, &t, &visitor, &stats, measure_visitor](
            const auto &descriptor) {
          return read_vector_node(t, descriptor.laddr
          ).si_then([&visitor, &stats, descriptor, measure_visitor](auto leaf)
              -> read_iertr::future<> {
            const auto &contents = leaf->get_contents();
            if (!leaf_matches_descriptor(contents, descriptor) ||
                VectorNode::encode_contents(contents).length() !=
                  descriptor.encoded_bytes) {
              return crimson::ct_error::input_output_error::make();
            }
            stats.leaf_extents++;
            stats.extent_bytes_scanned += leaf->get_length();
            const auto visitor_begin = measure_visitor
              ? std::chrono::steady_clock::now()
              : std::chrono::steady_clock::time_point();
            for (const auto &entry : contents.entries) {
              visitor(entry);
              stats.logical_entries++;
            }
            if (measure_visitor) {
              stats.visitor_ns += std::chrono::duration_cast<
                std::chrono::nanoseconds>(
                  std::chrono::steady_clock::now() - visitor_begin).count();
            }
            return read_iertr::now();
          });
        }
      ).si_then([&root, &stats]
          -> read_iertr::future<vector_scan_stats_t> {
        if (stats.logical_entries !=
            root->get_contents().logical_entry_count) {
          return crimson::ct_error::input_output_error::make();
        }
        return read_iertr::make_ready_future<vector_scan_stats_t>(stats);
      });
    });
}

VectorNodeManager::remove_ret VectorNodeManager::remove_vector_tree(
  Transaction &t,
  VectorNodeRef root)
{
  if (!root ||
      root->get_contents().kind != vector_node_kind_t::ROOT) {
    return crimson::ct_error::input_output_error::make();
  }

  return seastar::do_with(
    std::move(root),
    [this, &t](auto &root) {
      return trans_intr::do_for_each(
        root->get_contents().leaves,
        [this, &t](const auto &descriptor) {
          return tm.remove(t, descriptor.laddr
          ).si_then([](auto) {
            return remove_iertr::now();
          });
        }
      ).si_then([this, &t, &root] {
        return tm.remove(t, root->template cast<LogicalChildNode>()
        ).si_then([](auto) {
          return remove_iertr::now();
        });
      });
    });
}

} // namespace crimson::os::seastore
