// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/vector_node.h"

#include <algorithm>

#include "common/error_code.h"
#include "include/ceph_assert.h"
#include "include/encoding.h"

namespace crimson::os::seastore {

namespace {

void encode_entry(const vector_node_entry_t &entry, ceph::bufferlist &bl)
{
  using ceph::encode;
  encode(entry.entry_id, bl);
  encode(entry.vector_data, bl);
}

vector_node_entry_t decode_entry(ceph::bufferlist::const_iterator &p)
{
  using ceph::decode;
  vector_node_entry_t entry;
  decode(entry.entry_id, p);
  decode(entry.vector_data, p);
  return entry;
}

}

ceph::bufferlist VectorNode::encode_contents(const vector_node_t &node)
{
  using ceph::encode;
  ceph::bufferlist bl;
  encode(node.format_version, bl);
  encode(node.data_type, bl);
  encode(node.dimension, bl);
  encode(static_cast<uint32_t>(node.entries.size()), bl);
  for (const auto &entry : node.entries) {
    encode_entry(entry, bl);
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
  decode(node.data_type, p);
  decode(node.dimension, p);
  uint32_t entries = 0;
  decode(entries, p);
  node.entries.reserve(entries);
  for (uint32_t i = 0; i < entries; ++i) {
    node.entries.push_back(decode_entry(p));
  }
  if (!VectorNode::is_sorted(node.entries)) {
    throw ceph::buffer::malformed_input("unsorted VectorNode entries");
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

void VectorNode::initialize(uint32_t data_type, uint32_t dimension)
{
  contents = vector_node_t{
    VECTOR_NODE_FORMAT_VERSION,
    data_type,
    dimension,
    {}
  };
  decoded = true;
  dirty = true;
  materialize();
}

bool VectorNode::is_sorted(const std::vector<vector_node_entry_t> &entries)
{
  return std::adjacent_find(
    entries.begin(),
    entries.end(),
    [](const auto &lhs, const auto &rhs) {
      return !(lhs.entry_id < rhs.entry_id);
    }) == entries.end();
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
  if (decoded) {
    return out << "data_type=" << contents.data_type
               << ", dimension=" << contents.dimension
               << ", entries=" << contents.entries.size();
  } else {
    return out << "undecoded";
  }
}

bool VectorNodeManager::is_valid_extent_size(extent_len_t length) const
{
  return length != 0 &&
         length <= VECTOR_NODE_MAX_BYTES &&
         is_aligned(length, tm.get_block_size()) &&
         is_aligned(VECTOR_NODE_MAX_BYTES, tm.get_block_size());
}

VectorNodeManager::create_ret VectorNodeManager::create_vector_node(
  Transaction &t,
  uint32_t data_type,
  uint32_t dimension)
{
  const auto block_size = tm.get_block_size();
  if (!is_valid_extent_size(block_size)) {
    return crimson::ct_error::enospc::make();
  }

  return tm.alloc_non_data_extent<VectorNode>(
    t,
    laddr_hint_t::create_global_md_hint(),
    block_size
  ).si_then([data_type, dimension](auto node) {
    node->initialize(data_type, dimension);
    return create_iertr::make_ready_future<VectorNodeRef>(std::move(node));
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

VectorNodeManager::upsert_ret VectorNodeManager::upsert_vector_entry(
  Transaction &t,
  VectorNodeRef node,
  const std::string &entry_id,
  const ceph::bufferlist &vector_data)
{
  ceph_assert(node);
  ceph_assert(node->decoded);

  auto candidate = node->contents;
  auto iter = std::lower_bound(
    candidate.entries.begin(),
    candidate.entries.end(),
    entry_id,
    [](const auto &entry, const auto &id) {
      return entry.entry_id < id;
    });
  if (iter != candidate.entries.end() && iter->entry_id == entry_id) {
    iter->vector_data = vector_data;
  } else {
    candidate.entries.insert(iter, vector_node_entry_t{entry_id, vector_data});
  }

  auto encoded = VectorNode::encode_contents(candidate);
  if (encoded.length() > node->get_length() ||
      encoded.length() > VECTOR_NODE_MAX_BYTES) {
    return crimson::ct_error::enospc::make();
  }

  auto mutable_node = tm.get_mutable_extent(
    t,
    node->cast<LogicalChildNode>())->cast<VectorNode>();
  mutable_node->contents = std::move(candidate);
  mutable_node->decoded = true;
  mutable_node->dirty = true;
  mutable_node->materialize();
  return mutate_iertr::make_ready_future<VectorNodeRef>(
    std::move(mutable_node));
}

VectorNodeManager::remove_ret VectorNodeManager::remove_vector_entry(
  Transaction &t,
  VectorNodeRef node,
  const std::string &entry_id)
{
  ceph_assert(node);
  ceph_assert(node->decoded);

  auto candidate = node->contents;
  auto iter = std::lower_bound(
    candidate.entries.begin(),
    candidate.entries.end(),
    entry_id,
    [](const auto &entry, const auto &id) {
      return entry.entry_id < id;
    });
  if (iter == candidate.entries.end() || iter->entry_id != entry_id) {
    return mutate_iertr::make_ready_future<std::pair<VectorNodeRef, bool>>(
      std::make_pair(std::move(node), false));
  }
  candidate.entries.erase(iter);

  auto encoded = VectorNode::encode_contents(candidate);
  if (encoded.length() > node->get_length() ||
      encoded.length() > VECTOR_NODE_MAX_BYTES) {
    return crimson::ct_error::enospc::make();
  }

  auto mutable_node = tm.get_mutable_extent(
    t,
    node->cast<LogicalChildNode>())->cast<VectorNode>();
  mutable_node->contents = std::move(candidate);
  mutable_node->decoded = true;
  mutable_node->dirty = true;
  mutable_node->materialize();
  return mutate_iertr::make_ready_future<std::pair<VectorNodeRef, bool>>(
    std::make_pair(std::move(mutable_node), true));
}

} // namespace crimson::os::seastore
