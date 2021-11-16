// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

std::ostream &segment_to_stream(std::ostream &out, const segment_id_t &t)
{
  if (t == NULL_SEG_ID)
    return out << "NULL_SEG";
  else if (t == ZERO_SEG_ID)
    return out << "ZERO_SEG";
  else if (t == FAKE_SEG_ID)
    return out << "FAKE_SEG";
  else
    return out << t;
}

std::ostream &offset_to_stream(std::ostream &out, const segment_off_t &t)
{
  if (t == NULL_SEG_OFF)
    return out << "NULL_OFF";
  else
    return out << t;
}

std::ostream &operator<<(std::ostream &out, const segment_id_t& segment)
{
  return out << "[" << (uint64_t)segment.device_id() << ","
    << segment.device_segment_id() << "]";
}


std::ostream &operator<<(std::ostream &out, const block_id_t& block)
{
  return out << "[" << (uint64_t)block.device_id() << ","
    << block.device_block_id() << "]";
}

std::ostream &block_to_stream(std::ostream &out, const block_id_t &t)
{
  if (t == NULL_BLK_ID)
    return out << "NULL_BLK";
  else if (t == ZERO_BLK_ID)
    return out << "BLK_SEG";
  else if (t == FAKE_BLK_ID)
    return out << "FAKE_BLK";
  else
    return out << t;
}

std::ostream &operator<<(std::ostream &out, const block_off_t& offset)
{
  return out << offset.off;
}

std::ostream &block_offset_to_stream(std::ostream &out, const block_off_t &t)
{
  if (t == NULL_BLK_OFF)
    return out << "NULL_OFF";
  else
    return out << t;
}

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs)
{
  out << "paddr_t<";
  if (rhs == P_ADDR_NULL) {
    out << "NULL_PADDR";
  } else if (rhs == P_ADDR_MIN) {
    out << "MIN_PADDR";
  } else if (rhs.get_device_id() == BLOCK_REL_ID) {
    out << "BLOCK_REG";
  } else if (rhs.get_device_id() == RECORD_REL_ID) {
    out << "RECORD_REG";
  } else if (rhs.get_device_id() == DELAYED_TEMP_ID) {
    out << "DELAYED_TEMP";
  } else if (rhs.get_addr_type() == addr_types_t::SEGMENT) {
    const seg_paddr_t& s = rhs.as_seg_paddr();
    segment_to_stream(out, s.get_segment_id());
    out << ", ";
    offset_to_stream(out, s.get_segment_off());
  } else if (rhs.get_addr_type() == addr_types_t::RANDOM_BLOCK) {
    const blk_paddr_t& s = rhs.as_blk_paddr();
    block_to_stream(out, s.get_block_id());
    out << ", ";
    block_offset_to_stream(out, s.get_block_off());
  }
  return out << ">";
}

std::ostream &operator<<(std::ostream &out, const journal_seq_t &seq)
{
  return out << "journal_seq_t(segment_seq="
	     << seq.segment_seq << ", offset="
	     << seq.offset
	     << ")";
}

std::ostream &operator<<(std::ostream &out, extent_types_t t)
{
  switch (t) {
  case extent_types_t::ROOT:
    return out << "ROOT";
  case extent_types_t::LADDR_INTERNAL:
    return out << "LADDR_INTERNAL";
  case extent_types_t::LADDR_LEAF:
    return out << "LADDR_LEAF";
  case extent_types_t::ONODE_BLOCK_STAGED:
    return out << "ONODE_BLOCK_STAGED";
  case extent_types_t::OMAP_INNER:
    return out << "OMAP_INNER";
  case extent_types_t::OMAP_LEAF:
    return out << "OMAP_LEAF";
  case extent_types_t::COLL_BLOCK:
    return out << "COLL_BLOCK";
  case extent_types_t::OBJECT_DATA_BLOCK:
    return out << "OBJECT_DATA_BLOCK";
  case extent_types_t::RETIRED_PLACEHOLDER:
    return out << "RETIRED_PLACEHOLDER";
  case extent_types_t::TEST_BLOCK:
    return out << "TEST_BLOCK";
  case extent_types_t::TEST_BLOCK_PHYSICAL:
    return out << "TEST_BLOCK_PHYSICAL";
  case extent_types_t::NONE:
    return out << "NONE";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const laddr_list_t &rhs)
{
  bool first = false;
  for (auto &i: rhs) {
    out << (first ? '[' : ',') << '(' << i.first << ',' << i.second << ')';
    first = true;
  }
  return out << ']';
}
std::ostream &operator<<(std::ostream &out, const paddr_list_t &rhs)
{
  bool first = false;
  for (auto &i: rhs) {
    out << (first ? '[' : ',') << '(' << i.first << ',' << i.second << ')';
    first = true;
  }
  return out << ']';
}

std::ostream &operator<<(std::ostream &lhs, const delta_info_t &rhs)
{
  return lhs << "delta_info_t("
	     << "type: " << rhs.type
	     << ", paddr: " << rhs.paddr
	     << ", laddr: " << rhs.laddr
	     << ", prev_crc: " << rhs.prev_crc
	     << ", final_crc: " << rhs.final_crc
	     << ", length: " << rhs.length
	     << ", pversion: " << rhs.pversion
	     << ")";
}

extent_len_t get_encoded_record_raw_mdlength(
  const record_t &record,
  size_t block_size) {
  extent_len_t metadata =
    (extent_len_t)ceph::encoded_sizeof_bounded<record_header_t>();
  metadata += sizeof(checksum_t) /* crc */;
  metadata += record.extents.size() *
    ceph::encoded_sizeof_bounded<extent_info_t>();
  for (const auto &i: record.deltas) {
    metadata += ceph::encoded_sizeof(i);
  }
  return metadata;
}

record_size_t get_encoded_record_length(
  const record_t &record,
  size_t block_size) {
  extent_len_t raw_mdlength =
    get_encoded_record_raw_mdlength(record, block_size);
  extent_len_t mdlength =
    p2roundup(raw_mdlength, (extent_len_t)block_size);
  extent_len_t dlength = 0;
  for (const auto &i: record.extents) {
    dlength += i.bl.length();
  }
  return record_size_t{raw_mdlength, mdlength, dlength};
}

ceph::bufferlist encode_record(
  record_size_t rsize,
  record_t &&record,
  size_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t current_segment_nonce)
{
  bufferlist data_bl;
  for (auto &i: record.extents) {
    data_bl.append(i.bl);
  }

  bufferlist bl;
  record_header_t header{
    rsize.mdlength,
    rsize.dlength,
    (uint32_t)record.deltas.size(),
    (uint32_t)record.extents.size(),
    current_segment_nonce,
    committed_to,
    data_bl.crc32c(-1)
  };
  encode(header, bl);

  auto metadata_crc_filler = bl.append_hole(sizeof(uint32_t));

  for (const auto &i: record.extents) {
    encode(extent_info_t(i), bl);
  }
  for (const auto &i: record.deltas) {
    encode(i, bl);
  }
  ceph_assert(bl.length() == rsize.raw_mdlength);

  if (bl.length() % block_size != 0) {
    bl.append_zero(
      block_size - (bl.length() % block_size));
  }
  ceph_assert(bl.length() == rsize.mdlength);

  auto bliter = bl.cbegin();
  auto metadata_crc = bliter.crc32c(
    ceph::encoded_sizeof_bounded<record_header_t>(),
    -1);
  bliter += sizeof(checksum_t); /* crc hole again */
  metadata_crc = bliter.crc32c(
    bliter.get_remaining(),
    metadata_crc);
  ceph_le32 metadata_crc_le;
  metadata_crc_le = metadata_crc;
  metadata_crc_filler.copy_in(
    sizeof(checksum_t),
    reinterpret_cast<const char *>(&metadata_crc_le));

  bl.claim_append(data_bl);
  ceph_assert(bl.length() == (rsize.dlength + rsize.mdlength));

  return bl;
}

bool can_delay_allocation(device_type_t type) {
  // Some types of device may not support delayed allocation, for example PMEM.
  return type <= RANDOM_BLOCK;
}

device_type_t string_to_device_type(std::string type) {
  if (type == "segmented") {
    return device_type_t::SEGMENTED;
  }
  if (type == "random_block") {
    return device_type_t::RANDOM_BLOCK;
  }
  if (type == "pmem") {
    return device_type_t::PMEM;
  }
  return device_type_t::NONE;
}

std::string device_type_to_string(device_type_t dtype) {
  switch (dtype) {
  case device_type_t::SEGMENTED:
    return "segmented";
  case device_type_t::RANDOM_BLOCK:
    return "random_block";
  case device_type_t::PMEM:
    return "pmem";
  default:
    ceph_assert(0 == "impossible");
  }
}

[[gnu::noinline]] bool is_relative(const paddr_t& paddr) {
  return paddr.get_device_id() == RECORD_REL_ID ||
	 paddr.get_device_id() == BLOCK_REL_ID;
}

[[gnu::noinline]] bool is_record_relative(const paddr_t& paddr) {
  return paddr.get_device_id() == RECORD_REL_ID;
}

[[gnu::noinline]] bool is_null(const paddr_t& paddr) {
  if (paddr.get_addr_type() == addr_types_t::SEGMENT) {
    auto& seg_addr = paddr.as_seg_paddr();
    return seg_addr.get_segment_id() == NULL_SEG_ID || paddr == P_ADDR_NULL;
  } else if (paddr.get_addr_type() == addr_types_t::RANDOM_BLOCK) {
    auto& blk_addr = paddr.as_blk_paddr();
    return blk_addr.get_block_id() == NULL_BLK_ID || paddr == P_ADDR_NULL;
  }
  ceph_assert(0 == "not supported type");
}

[[gnu::noinline]] bool is_block_relative(const paddr_t& paddr) {
  return paddr.get_device_id() == BLOCK_REL_ID;
}

[[gnu::noinline]] bool is_zero(const paddr_t& paddr) {
  if (paddr.get_addr_type() == addr_types_t::SEGMENT) {
    auto& seg_addr = paddr.as_seg_paddr();
    return seg_addr.get_segment_id() == ZERO_SEG_ID || paddr == P_ADDR_MIN;
  } else if (paddr.get_addr_type() == addr_types_t::RANDOM_BLOCK) {
    auto& blk_addr = paddr.as_blk_paddr();
    return blk_addr.get_block_id() == ZERO_BLK_ID || paddr == P_ADDR_MIN;
  }
  ceph_assert(0 == "not supported type");
}

[[gnu::noinline]] bool is_real(const paddr_t& paddr) {
  if (paddr.get_addr_type() == addr_types_t::SEGMENT) {
    return !is_zero(paddr) && !is_null(paddr);
  }
  ceph_assert(0 == "not supported type");
}


paddr_t::paddr_t(device_id_t id, device_segment_id_t sgt, segment_off_t offset) {
  static_cast<seg_paddr_t*>(this)->set_segment_id(segment_id_t{id, sgt});
  static_cast<seg_paddr_t*>(this)->set_segment_off(offset);
}

const seg_paddr_t& paddr_t::as_seg_paddr() const {
  assert(get_addr_type() == addr_types_t::SEGMENT);
  return *static_cast<const seg_paddr_t*>(this);
}

seg_paddr_t& paddr_t::as_seg_paddr() {
  assert(get_addr_type() == addr_types_t::SEGMENT);
  return *static_cast<seg_paddr_t*>(this);
}

const blk_paddr_t& paddr_t::as_blk_paddr() const {
  assert(get_addr_type() == addr_types_t::RANDOM_BLOCK);
  return *static_cast<const blk_paddr_t*>(this);
}

blk_paddr_t& paddr_t::as_blk_paddr() {
  assert(get_addr_type() == addr_types_t::RANDOM_BLOCK);
  return *static_cast<blk_paddr_t*>(this);
}

void paddr_t::set_device_id(device_id_t id, addr_types_t type) {
  dev_addr &= static_cast<common_addr_t>(
    std::numeric_limits<device_segment_id_t>::max());
  dev_addr |= static_cast<common_addr_t>(id & 0x8) << DEV_ADDR_LEN_BITS;
  dev_addr |= static_cast<common_addr_t>(type)
    << (std::numeric_limits<common_addr_t>::digits - 1);
}

#define PADDR_OPERATION(a_type, base, func)        \
  if (get_addr_type() == a_type) {                 \
    return static_cast<const base*>(this)->func;   \
  }

paddr_t paddr_t::operator-(paddr_t rhs) const {
  if (get_addr_type() == addr_types_t::SEGMENT) {
    auto& seg_addr = as_seg_paddr();
    return seg_addr - rhs;
  }
  ceph_assert(0 == "not supported type");
  return paddr_t{};
}

[[gnu::noinline]] paddr_t paddr_t::add_offset(int32_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_offset(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_offset(o))
  ceph_assert(0 == "not supported type");
  return paddr_t{};
}

[[gnu::noinline]] paddr_t paddr_t::add_relative(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_relative(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_relative(o))
  ceph_assert(0 == "not supported type");
  return paddr_t{};
}

[[gnu::noinline]] paddr_t paddr_t::add_block_relative(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_block_relative(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_block_relative(o))
  ceph_assert(0 == "not supported type");
  return paddr_t{};
}

[[gnu::noinline]] paddr_t paddr_t::add_record_relative(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, add_record_relative(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, add_record_relative(o))
  ceph_assert(0 == "not supported type");
  return paddr_t{};
}

[[gnu::noinline]] paddr_t paddr_t::maybe_relative_to(paddr_t o) const {
  PADDR_OPERATION(addr_types_t::SEGMENT, seg_paddr_t, maybe_relative_to(o))
  PADDR_OPERATION(addr_types_t::RANDOM_BLOCK, blk_paddr_t, maybe_relative_to(o))
  ceph_assert(0 == "not supported type");
  return paddr_t{};
}

bool paddr_t::operator==(const paddr_t& other) const {
  return dev_addr == other.dev_addr;
}
bool paddr_t::operator!=(const paddr_t& other) const {
  return dev_addr != other.dev_addr;
}
bool paddr_t::operator<(const paddr_t& other) const {
  return dev_addr < other.dev_addr;
}
bool paddr_t::operator<=(const paddr_t& other) const {
  return dev_addr <= other.dev_addr;
}
bool paddr_t::operator>(const paddr_t& other) const {
  return dev_addr > other.dev_addr;
}
bool paddr_t::operator>=(const paddr_t& other) const {
  return dev_addr >= other.dev_addr;
}

}
