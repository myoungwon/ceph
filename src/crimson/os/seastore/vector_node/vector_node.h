// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

#include "include/denc.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/onode.h"
#include <seastar/core/future.hh>
#include <seastar/core/coroutine.hh>
#include "crimson/common/errorator.h"
#include "crimson/common/coroutine.h"
#include "log_manager.h"

namespace crimson::os::seastore::vector_manager{

struct VectorNodeLayout;
struct delta_t {
  enum class op_t : uint_fast8_t {
    APPEND,
    REMOVE,
    ADD_NEXT,
    INIT,
    OVERWRITE
  } op;
  std::string key;
  ceph::bufferlist val;
  laddr_t next;

  DENC(delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.op, p);
    denc(v.key, p);
    denc(v.val, p);
    denc(v.next, p);
    DENC_FINISH(p);
  }

  void replay(LogKVNodeLayout &l);
};

class delta_buffer_t {
  std::vector<delta_t> buffer;
public:
  bool empty() const {
    return buffer.empty();
  }
  void insert_append(
    const std::string &key,
    const ceph::bufferlist &val) {
    buffer.push_back(
      delta_t{
        delta_t::op_t::APPEND,
        key,
        val
      });
  }
  void insert_next_addr(
      const laddr_t l) {
    buffer.push_back(
      delta_t{
	delta_t::op_t::ADD_NEXT,
	std::string(),
	bufferlist(),
	l
      });
  }

  void insert_init() {
    buffer.push_back(
      delta_t{
	delta_t::op_t::INIT,
	std::string(),
	bufferlist(),
	L_ADDR_NULL
      });
  }

  void insert_remove(bufferlist bl) {
    buffer.push_back(
      delta_t{
	delta_t::op_t::REMOVE,
	std::string(),
	bl,
	L_ADDR_NULL
      });
  }

  void replay(VectorNodeLayout &node) {
    for (auto &i: buffer) {
      i.replay(node);
    }
  }

  void insert_overwrite(
    const std::string &key,
    const ceph::bufferlist &val) {
    buffer.push_back(
      delta_t{
        delta_t::op_t::OVERWRITE,
        key,
        val
      });
  }

  void clear() {
    buffer.clear();
  }

#if 0
  std::optional<laddr_t> get_latest_dup_tail_addr() {
    std::optional<laddr_t> l = std::nullopt;
    for (auto it = buffer.rbegin(); it != buffer.rend(); ++it) {
      if (it->op == delta_t::op_t::ADD_DUP_ADDR) {
        l = it->prev;
	return l;
      }
    }
    return l;
  }
#endif

  std::optional<laddr_t> get_latest_next_leaf() {
    std::optional<laddr_t> l = std::nullopt;
    for (auto it = buffer.rbegin(); it != buffer.rend(); ++it) {
      if (it->op == delta_t::op_t::ADD_NEXT) {
        l = it->prev;
	return l;
      }

    }
    return l;
  }

  std::optional<bufferlist> get_latest_d_bitmap() {
    std::optional<bufferlist> ret = std::nullopt;
    for (auto it = buffer.rbegin(); it != buffer.rend(); ++it) {
      if (it->op == delta_t::op_t::REMOVE) {
	ret = it->val;
	return ret;
      }
    }
    return ret;
  }

  std::optional<delta_t> get_latest_write_delta() {
    std::optional<delta_t> ret = std::nullopt;
    for (auto it = buffer.rbegin(); it != buffer.rend(); ++it) {
      if (it->op == delta_t::op_t::APPEND ||
	it->op == delta_t::op_t::OVERWRITE) {
	ret = *it;
	return ret;
      }
    }
    return ret;
  }

  DENC(delta_buffer_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.buffer, p);
    DENC_FINISH(p);
  }

};
}
WRITE_CLASS_DENC(crimson::os::seastore::vector_manager::delta_t)
WRITE_CLASS_DENC(crimson::os::seastore::vector_manager::delta_buffer_t)

namespace crimson::os::seastore::vector_manager{

constexpr uint32_t VECTOR_NODE_BLOCK_SIZE = 16384;

const std::string BEGIN_KEY = "";
const std::string END_KEY(64, (char)(-1));

inline constexpr uint32_t get_vector_node_block_size() {
  return crimson::os::seastore::vector_manager::VECTOR_NODE_BLOCK_SIZE;
}

struct VectorNode;
using VectorNodeRef = TCachedExtentRef<VectorNode>;


constexpr uint32_t BITMAP_ARRAY_SIZE = ((VECTOR_NODE_BLOCK_SIZE / 4096) * 32 + 63) / 64;

struct d_bitmap_t {
  uint64_t bitmap[BITMAP_ARRAY_SIZE] = {0};
  static constexpr size_t BITS_PER_WORD = 64;
  static constexpr size_t MAX_ENTRY = BITS_PER_WORD * BITMAP_ARRAY_SIZE;

  d_bitmap_t() = default;
  void set_bitmap(size_t bit) {
    const size_t word = bit / BITS_PER_WORD;
    const size_t offset = bit % BITS_PER_WORD;
    assert(word < BITMAP_ARRAY_SIZE);
    bitmap[word] |= (1ULL << offset);
  }
  void set_bitmap_range(size_t begin, size_t end) {
    assert(begin <= end);
    for (size_t i = begin; i <= end; i++) {
      set_bitmap(i);
    }
  }
  bool is_set(size_t bit) {
    const size_t word = bit / BITS_PER_WORD;
    const size_t offset = bit % BITS_PER_WORD;
    assert(word < BITMAP_ARRAY_SIZE);
    return (bitmap[word] & (1ULL << offset)) != 0;
  }
  bool is_all_set(uint64_t num) const {
    constexpr uint64_t ALL_SET = std::numeric_limits<uint64_t>::max();
    assert(num <= BITMAP_ARRAY_SIZE * BITS_PER_WORD);
    const size_t full_words = num / BITS_PER_WORD;
    const size_t rem_bits   = num % BITS_PER_WORD;

    for (size_t i = 0; i < full_words; ++i) {
      if (bitmap[i] != ALL_SET)
	return false;
    }

    if (rem_bits != 0) {
      const uint64_t mask =
	(uint64_t{1} << rem_bits) - 1;
      if ((bitmap[full_words] & mask) != mask) {
	return false;
      }
    }
    return true;
  }
  void init() {
    for (uint32_t i = 0; i < BITMAP_ARRAY_SIZE; i++) {
      bitmap[i] = 0;
    }
  }

  DENC(d_bitmap_t, v, p) {
    DENC_START(1, 1, p);
    for (uint32_t i = 0; i < BITMAP_ARRAY_SIZE; i++) {
      denc(v.bitmap[i], p);
    }
    DENC_FINISH(p);
  }
};

struct d_bitmap_le_t {
  ceph_le64 bitmap[BITMAP_ARRAY_SIZE]{};

  d_bitmap_le_t() = default;
  operator d_bitmap_t() const {
    d_bitmap_t tmp;
    for (uint32_t i = 0; i < BITMAP_ARRAY_SIZE; i++) {
      tmp.bitmap[i] = uint64_t(bitmap[i]);
    }
    return tmp;
  }
  d_bitmap_le_t& operator=(d_bitmap_t &_bitmap) {
    for (uint32_t i = 0; i < BITMAP_ARRAY_SIZE; i++) {
      bitmap[i] = _bitmap.bitmap[i];
    }
    return *this;
  }
};


static constexpr uint32_t VECTOR_NODE_MAX_CONTENTS = 64;
static constexpr uint32_t VECTOR_NODE_MAX_NEIGHBORS = 64;
static constexpr uint32_t VECTOR_NODE_MAX_REL_OIDS = 32;
static constexpr uint32_t VECTOR_NODE_MAX_OID_ENCODED_LEN = 256;


enum class vector_entry_type_t {
  HOBJECT,
  HASH,
};

struct vector_entry_t {
  uint16_t type;
  uint32_t next_vector_offset = 0;
  uint32_t key_len = 0; // optional to indicate the vale corresponds which vector (key)
  uint32_t val_len = 0;

  vector_entry_t() = default;
  vector_entry_t(uint16_t type, uint32_t n_offset, uint32_t k_len = 0, uint32_t v_len = 0)
  : key_len(k_len), key_len(k_len), val_len(v_len) {}

  DENC(vector_entry_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.next_vector_offset, p);
    denc(v.key_len, p);
    denc(v.val_len, p);
    DENC_FINISH(p);
  }
};

struct vector_entry_le_t {
  ceph_le16 type{0};
  ceph_le32 next_vector_offset{0};
  ceph_le32 key_len{0};
  ceph_le32 val_len{0};

  vector_entry_le_t() = default;
  vector_entry_le_t(const vector_entry_le_t &) = default;
  explicit vector_entry_le_t(const vector_entry_t &key)
    : type(key.type),
      next_vector_offset(key.next_vector_offset),
      key_len(key.key_len),
      val_len(key.val_len) {}

  vector_entry_le_t& operator=(vector_entry_t key) {
    type = key.type;
    next_vector_offset = key.next_vector_offset;
    key_len = key.key_len;
    val_len = key.val_len;
    return *this;
  }

  operator vector_entry_t() const {
    return vector_entry_t{uint16_t(type),
      uint32_t(next_vector_offset),
      uint32_t(key_len),
      uint32_t(val_len)};
  }
};


/**
 * VectorNodeLayout
 *
 *  [ num_vectors ][ next pointer ][ d_bitmap ][ dup_count ][ Contents ][ Neighbors ][ Related OID ] ...
 *
 *  Fixed position
 *  - num_vectors:
 *  - next pointer
 *  - d_bitmap:
 *      bitmap to keep track of deleted entries.
 *  - Dup. count	
 *
 *  Variable position
 *  - Contents 
  *  - key: hash 
  *  - value: vector
  *  ...
 *  - Neighbors (OIDs) 
   *  - key: hash
   *  - value: hobject_t
   *  ...
 *  - Related OID  
   *  - key: hash
   *  - value: hobject_t --> should be the existing object in Ceph
   *  ...
 *
 */

/* TODO: large entry (> 4KiB) */

class VectorNodeLayout {
  using VectorNodeLayoutRef = boost::intrusive_ptr<VectorNodeLayout>;
  char *buf;
  extent_len_t len = 0;

  uint32_t reserved_len = 0;
  uint32_t reserved_size = 0;
  using L = absl::container_internal::Layout<ceph_le32, laddr_le_t, d_bitmap_le_t, ceph_le32, ceph_le32, vector_entry_le_t>; //, content_le_t, neighbor_le_t, rel_oid_le_t>;
  static constexpr L layout{1, 1, 1, 1, 1, 1};
public:
  template <bool is_const>
  class iter_t {
    friend class VectorNodeLayout;
    using parent_t = typename crimson::common::maybe_const_t<VectorNodeLayout, is_const>::type;

    parent_t node;
    uint32_t pos;

    iter_t(
      parent_t parent,
      uint32_t pos) : node(parent), pos(pos) {}

  public:
    iter_t(const iter_t &) = default;
    iter_t(iter_t &&) = default;
    iter_t &operator=(const iter_t &) = default;
    iter_t &operator=(iter_t &&) = default;

    operator iter_t<!is_const>() const {
      static_assert(!is_const);
      return iter_t<!is_const>(node, pos);
    }

    iter_t &operator*() { return *this; }
    iter_t *operator->() { return this; }

    iter_t operator++(int) {
      auto ret = *this;
      auto last = get_node_key();
      auto new_pos = node->get_size() == 0 ? 0 :
	pos + node->get_entry_size(last.key_len, last.val_len);
      pos = new_pos;
      return ret;
    }

    iter_t &operator++() {
      auto last = get_node_key();
      auto new_pos = node->get_size() == 0 ? 0 :
	pos + node->get_entry_size(last.key_len, last.val_len);
      pos = new_pos;
      return *this;
    }

    bool operator==(const iter_t &rhs) const {
      assert(node == rhs.node);
      return rhs.pos == pos;
    }

    bool operator!=(const iter_t &rhs) const {
      assert(node == rhs.node);
      return pos != rhs.pos;
    }

  private:
    vector_entry_t get_node_key() const {
      vector_entry_le_t kint = *((vector_entry_le_t*)get_node_key_ptr());
      return vector_entry_t(kint);
    }
    auto get_node_key_ptr() const {
      return reinterpret_cast<
	typename crimson::common::maybe_const_t<char, is_const>::type>(
	  node->get_node_key_ptr()) + pos;
    }

    uint32_t get_node_val_offset() const {
      return get_node_key().key_off;
    }
    auto get_node_val_ptr() const {
      return get_node_key_ptr() + sizeof(vector_entry_t);
    }

    void set_node_key(vector_entry_t _lb) {
      static_assert(!is_const);
      vector_entry_le_t lb;
      lb = _lb;
      *((vector_entry_le_t*)get_node_key_ptr()) = lb;
    }

    void set_node_val(const std::string &key, const ceph::bufferlist &val) {
      static_assert(!is_const);
      auto node_key = get_node_key();
      assert(key.size() == node_key.key_len);
      assert(val.length() == node_key.val_len);
      ::memcpy(get_node_val_ptr(), key.data(), key.size());
      auto bliter = val.begin();
      bliter.copy(node_key.val_len, get_node_val_ptr() + node_key.key_len);
    }
    void set_node_val( const ceph::bufferlist &val) {
      static_assert(!is_const);
      auto node_key = get_node_key();
      assert(val.length() == node_key.val_len);
      auto bliter = val.begin();
      bliter.copy(node_key.val_len, get_node_val_ptr() + node_key.key_len);
    }

  public:
    std::string get_key() const {
      if (get_node_key().key_len == 0) {
	return std::string();
      }
      return std::string(
	get_node_val_ptr(),
	get_node_key().key_len);
    }

    ceph::bufferlist get_val() const {
      auto node_key = get_node_key();
      ceph::bufferlist bl;
      bl.append(get_node_val_ptr() + node_key.key_len,
	node_key.val_len);
      return bl;
    }

    ceph::bufferlist get_val_shallow() const {
      auto node_key = get_node_key();
      ceph::bufferlist bl;
      ceph::bufferptr bptr(
	get_node_val_ptr() + node_key.key_len,
	node_key.val_len);
      bl.append(bptr);
      return bl;
    }
  };
  
  using const_iterator = iter_t<true>;
  using iterator = iter_t<false>;

  uint32_t get_size() const {
    ceph_le32 &size = *layout.template Pointer<0>(buf);
    return uint32_t(size);
  }

  laddr_t get_next() const {
    laddr_le_t &prev = *layout.template Pointer<1>(buf);
    return laddr_t(prev);
  }

  ceph_le32 *get_size_ptr() {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<0>(buf);
  }
  laddr_le_t *get_next_node_addr_ptr() {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<1>(buf);
  }
  d_bitmap_le_t *get_d_bitmap_ptr() {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<2>(buf);
  }
  ceph_le32 *get_dup_count_ptr() {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<3>(buf);
  }
  ceph_le32 *get_last_pos_ptr() {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<4>(buf);
  }
  vector_entry_le_t *get_node_key_ptr() {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<5>(buf);
  }
  const vector_entry_le_t *get_node_key_ptr() const {
    return L::Partial(1, 1, 1, 1, 1).template Pointer<5>(buf);
  }

  uint32_t get_start_off() const {
    return layout.Offset<5>();
  }

  const_iterator iter_rbegin() const {
    return const_iterator(this, get_last_pos());
  }
  const_iterator iter_end() const {
    const_iterator prev_iter(this, get_last_pos());
    auto last = prev_iter->get_node_key();
    return const_iterator(this, get_size() == 0 ? get_last_pos() :
      get_last_pos() + get_entry_size(last.key_len, last.val_len));
  }

  iterator iter_begin() {
    return iterator(
	this,
	0);
  }

  const_iterator iter_begin() const {
    return iter_cbegin();
  }

  const_iterator iter_cbegin() const {
    return const_iterator(
	this,
	0);
  }

  iterator iter_end() {
    iterator prev_iter(this, get_last_pos());
    auto last = prev_iter->get_node_key();
    return iterator(this, get_size() == 0 ? get_last_pos() :
      get_last_pos() + get_entry_size(last.key_len, last.val_len));
  }
public:

  VectorNodeLayout() : buf(nullptr) {}

  void set_layout_buf(char *_buf, extent_len_t _len) {
    assert(_len > 0);
    assert(buf == nullptr);
    assert(_buf != nullptr);
    buf = _buf;
    len = _len;
  }

  void set_next_node(laddr_t laddr) {
    laddr_le_t l;
    l = laddr;
    *get_next_node_addr_ptr() = l;
  }

  void set_size(uint32_t size) {
    ceph_le32 v(size);
    *get_size_ptr() = v;
  }

  void set_last_pos(uint32_t pos) {
    //ceph_assert(pos <= LOG_NODE_BLOCK_SIZE);
    ceph_le32 p;
    p = pos;
    *layout.template Pointer<4>(buf) = p;
  }

  uint32_t get_last_pos() const {
    ceph_le32 &pos = *layout.template Pointer<4>(buf);
    return uint32_t(pos);
  }

  d_bitmap_t get_d_bitmap() {
    d_bitmap_le_t &bitmap = *get_d_bitmap_ptr();
    return d_bitmap_t(bitmap);
  }

  void _set_d_bitmap(d_bitmap_t &_bitmap) {
    d_bitmap_le_t bitmap;
    bitmap = _bitmap;
    *get_d_bitmap_ptr() = bitmap;
  }

  void set_d_bitmap(size_t begin, size_t end) {
    auto bitmap = get_d_bitmap();
    bitmap.set_bitmap_range(begin, end);
    _set_d_bitmap(bitmap);
  }

  void init_bitmap() {
    d_bitmap_t bitmap;
    bitmap.init();
    _set_d_bitmap(bitmap);
  }

  void set_reserved_len(const uint32_t len) {
    reserved_len = len;
  }

  uint32_t get_reserved_len() const {
    return reserved_len;
  }

  void set_reserved_size(const uint32_t size) {
    reserved_size = size;
  }

  uint32_t get_reserved_size() const {
    return reserved_size;
  }

  uint16_t get_entry_size(size_t ksize, size_t vsize) const {
    return (sizeof(vector_entry_le_t) + ksize + vsize);
  }

  uint32_t free_space() const {
    assert(capacity() >= used_space());
    return capacity() - used_space();
  }

  uint32_t capacity() const {
    return len
      - (reinterpret_cast<char*>(layout.template Pointer<5>(buf))
      - reinterpret_cast<char*>(layout.template Pointer<0>(buf)));
  }

  uint32_t used_space() const {
    if (get_size() == 0) {
      return 0;
    }
    const_iterator iter(this, get_last_pos());
    auto k = iter->get_node_key();
    return get_last_pos() + get_entry_size(k.key_len, k.val_len);
  }

  void _append(vector_entry_type_t type, const std::string &key, const ceph::bufferlist &val) {
    iterator prev_iter(this, get_last_pos());
    auto last = prev_iter->get_node_key();
    iterator next_iter(this, get_size() == 0 ? get_last_pos() :
      get_last_pos() + get_entry_size(last.key_len, last.val_len));
    if (key == "") {
      next_iter.set_node_key(vector_entry_t(type, 0, 0, val.length()));
      next_iter.set_node_val(val);
    } else {
      next_iter.set_node_key(vector_entry_t(type, 0, key.size(), val.length()));
      next_iter.set_node_val(key, val);
    }
    if (get_size() >= 1) {
      set_last_pos(get_last_pos() + get_entry_size(last.key_len, last.val_len));
    }
    set_size(get_size() + 1);
  }

  void journal_append(
    const std::string &key,
    const ceph::bufferlist &val,
    delta_buffer_t *recorder) {
    recorder->insert_append(key, val);
    reserved_len += this->get_entry_size(key.size(), val.length());
    reserved_size += 1;
  }

  void journal_append_next_addr(
    const laddr_t l,
    delta_buffer_t *recorder) {
    recorder->insert_next_addr(l);
  }

  void journal_append_init(
    delta_buffer_t *recorder) {
    recorder->insert_init();
  }

  void journal_append_remove(delta_buffer_t *recorder, ceph::bufferlist bl);

  void append(
    const std::string &key,
    const ceph::bufferlist &val) {
    _append(key, val);
  }

  void init_vars() {
    init_bitmap();
    set_last_pos(0); 
    set_size(0);
    set_next_node(L_ADDR_NULL);
    set_reserved_len(0);
    set_reserved_size(0);
    
  }

  std::string get_last_key() const {
    const_iterator iter(this, get_last_pos());
    return iter->get_key();
  }

  friend class VectorNode;
};

struct VectorNode 
  : LogicalChildNode,
    VectorNodeLayout {
  static constexpr extent_types_t TYPE = extent_types_t::VECTOR_NODE;
  explicit LogNode(ceph::bufferptr &&ptr) : LogicalChildNode(std::move(ptr)) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_next_node(L_ADDR_NULL);
  }
  explicit VectorNode(extent_len_t length) : LogicalChildNode(length) {}

  VectorNode(const VectorNode &rhs)
    : LogicalChildNode(rhs, share_buffer_t()) {
    set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
    set_last_pos(*get_last_pos_ptr()); // shared buf
    set_size(get_size());
    set_reserved_len(rhs.get_reserved_len());
    set_reserved_size(rhs.get_reserved_size());
  }
  ~VectorNode() {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    assert(delta_buffer.empty());
    return CachedExtentRef(new VectorNode(*this));
  }

  crimson::os::seastore::extent_types_t get_type() const {
    return extent_types_t::VECTOR_NODE;
  }

  ceph::bufferlist get_delta() {
    ceph::bufferlist bl;
    if (!delta_buffer.empty()) {
      encode(delta_buffer, bl);
    }
    return bl;
  }

  void apply_delta(const ceph::bufferlist &bl) {
    assert(bl.length());
    delta_buffer_t buffer;
    auto bptr = bl.cbegin();
    decode(buffer, bptr);
    buffer.replay(*this);
  }

  mutable delta_buffer_t delta_buffer;
  delta_buffer_t *maybe_get_delta_buffer() {
    return is_mutation_pending() ? &delta_buffer : nullptr;
  }

  void append_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val);

  void overwrite_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val);

  /*
   *
   * set laddr directly if LogNode is not mutating
   * add laddr to delta_buffer if LogNode is mutating
   *
   */
  void set_next_addr(laddr_t l);

  void set_init_vars();

  enum class copy_t : uint8_t {
    SHALLOW,
    DEEP,
  };
  using get_value_ret = OMapManager::omap_get_value_ret;
  get_value_ret get_value(const std::string &key, copy_t c = copy_t::DEEP);

  void append_remove(ceph::bufferlist bl);

  // Remove all matching keys in LogNode
  bool remove_entry(const std::string key);

  void set_cur_bitmap(uint32_t begin, uint32_t end);
  d_bitmap_t get_cur_bitmap();
  void set_bitmap(d_bitmap_t map);

  // start and end should exist in the node
  std::optional<std::string> remove_entries(std::optional<std::string> start,
    std::optional<std::string> end)
  {
    std::string_view s(*start);
    std::string_view e(*end);
    if (s == e) {
      if (remove_entry(*start)) {
	return *start;
      }
      return std::nullopt;
    }

    auto iter = iter_begin();

    uint32_t index = 0;
    bool remove = false;
    std::string last;
    d_bitmap_t map = get_cur_bitmap();
    while(iter != iter_end()) {
      auto key = iter->get_key();
      if (s <= key && key <= e) {
	map.set_bitmap(index);
	remove = true;
	last = key;
      }
      index++;
      iter++;
    };
    if (remove) {
      set_bitmap(map);
    }
    return last;
  }

  bool is_removable();

#if 0
  bool log_has_larger_than(std::string_view str) const;

  bool log_less_than(std::string_view str) const;
#endif

  enum class range_t : uint8_t {
    HAS_BETWEEN,
    NO_BETWEEN,
  };

  range_t has_between(std::optional<std::string> start,
    std::optional<std::string> end) {
    std::string_view s(*start);
    std::string_view e(*end);
    auto iter = iter_begin();
    while(iter != iter_end()) {
      std::string k = iter->get_key();
      if (k <= e && k >= s) {
	return range_t::HAS_BETWEEN;
      } 
      iter++;
    };
    return range_t::NO_BETWEEN;
  }

  template <typename F>
  void for_each_live_entry(F&& fn);

  void list(const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    std::map<std::string, bufferlist> &kvs);

  std::ostream &print_detail_l(std::ostream &out) const final;

  laddr_t get_next_addr() const {
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      if (!delta_buffer.empty()) {
	auto ret = delta_buffer.get_latest_next_leaf();
	if (ret) {
	  return *ret;
	}
      }
    }
    return this->get_next();
  }

  uint32_t use_space() const {
    return this->used_space();
  }

  uint32_t get_capacity() const {
    return this->capacity();
  }

#if 0
  bool can_ow();

  int ow_gap_from_last_entry(const size_t key, const size_t val);
#endif

  bool expect_overflow(const std::string &key, size_t vsize, bool can_ow);
  bool expect_overflow(size_t ksize, size_t vsize) const {
    if (get_size() + reserved_size + 1 > d_bitmap_t::MAX_ENTRY) {
      return true;
    }
    return free_space() < get_entry_size(ksize, vsize) + reserved_len;
  }

  size_t get_max_val_length(size_t ksize) {
    return (capacity() - get_entry_size(ksize, 0));
  }

  void update_delta() {
    if (!delta_buffer.empty()) {
      delta_buffer.replay(*this);
      delta_buffer.clear();
    }
  }

  void logical_on_delta_write() final {
    update_delta();
    set_reserved_len(0);
    set_reserved_size(0);
  }

  // TODO: consistent view in a transaction
  void prepare_commit(Transaction &t) final {
    if (is_rewrite_transaction(t.get_src())) {
      return;
    }
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      ceph_assert(!delta_buffer.empty());
      update_delta();
    } else {
      assert(delta_buffer.empty());
    }
  }

  void on_fully_loaded() final {
    this->set_layout_buf(this->get_bptr().c_str(), this->get_bptr().length());
  }

  void init_range(std::string _begin, std::string _end) {
    assert(begin.empty());
    assert(end.empty());
    begin = std::move(_begin);
    end = std::move(_end);
  }

  std::string begin;
  std::string end;
};

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::vector_manager::vector_entry_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::vector_manager::d_bitmap_t)

