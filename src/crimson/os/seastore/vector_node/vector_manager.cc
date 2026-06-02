// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "vector_manager.h"
#include "vector_node.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::vector_manager{

#if 0
base_iertr::future<laddr_t> VectorManager::get_dup_addr_from_root(Transaction &t, laddr_t addr) {
  auto ext = co_await vector_load_extent<VectorNode>(
    t, addr, BEGIN_KEY, END_KEY);
  assert(ext);
  co_return ext->get_dup_tail_addr();
}
#endif

VectorManager::VectorManager(
  TransactionManager &tm)
  : tm(tm) {}

VectorManager::initialize_omap_ret
VectorManager::initialize_omap(
  Transaction &t,
  laddr_hint_t hint,
  omap_type_t omap_type)
{
  LOG_PREFIX(VectorManager::initialize_omap);
  DEBUGT("hint: {}", t, hint);
  auto extent = co_await tm.alloc_non_data_extent<VectorNode>(
    t, hint, Vector_NODE_BLOCK_SIZE
  ).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    TransactionManager::alloc_extent_iertr::pass_further{}
  );

  omap_root_t omap_root;
  omap_root.update(extent->get_laddr(), 1, hint,
    omap_type_t::VECTOR);
  t.get_omap_tree_stats().extents_num_delta++;
  co_return std::move(omap_root);
}

//
//
//
//
VectorManager::omap_set_keys_ret
VectorManager::omap_set_keys(
  omap_root_t &vector_root,
  Transaction &t, std::map<std::string, ceph::bufferlist> kvs)
{
  LOG_PREFIX(VectorManager::omap_set_vector);
  //DEBUGT("enter kv size {}", t, kvs.size());
  
  auto ext = co_await vector_load_extent<VectorNode>(
    t, vector_root.addr, BEGIN_KEY, END_KEY);
  ceph_assert(ext);
  auto resync_node = [&](VectorNodeRef e)
    -> vector_load_extent_iertr::future<CachedExtentRef> {
    CachedExtentRef node;
    Transaction::get_extent_ret ret;
    // To find mutable extent in the same transaction
    ret = t.get_extent(e->get_paddr(), &node);
    assert(ret == Transaction::get_extent_ret::PRESENT);
    if (!node) {
      // Do full reload if not cached
      node = co_await vector_load_extent<VectorNode>(
	t, e->get_laddr(), BEGIN_KEY, END_KEY);
    }
    ceph_assert(node);
    co_return std::move(node);
  };
  auto f = [&](const std::string &k, const bufferlist &v) 
    -> omap_set_key_ret {
    CachedExtentRef node = co_await resync_node(ext);
    VectorNodeRef vector_node = node->template cast<VectorNode>();
    co_await _vector_set_key(vector_root, t, vector_node, k, v);
    co_return;
  };
  auto alloc_vector_node = [&](laddr_t next_laddr)
    -> omap_set_key_iertr::future<VectorNodeRef> {
    return tm.alloc_non_data_extent<VectorNode>(
      t, vector_root.hint, LOG_NODE_BLOCK_SIZE
    ).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      omap_set_key_iertr::pass_further{}
    ).si_then([next_laddr](auto ext) {
      assert(ext);
      ext->set_next_addr(next_laddr);
      return omap_set_key_iertr::make_ready_future<VectorNodeRef>(ext);
    });
  };

  // TODO: search neighors

  for (auto &p : kvs) {
#if 0
    if (!is_vector_key(p.first)) {
      // remove duplicate keys first
      co_await remove_kv(t, vector_root.addr, p.first, nullptr);
    }
#endif
    laddr_t last_addr = vector_root.addr;
    co_await f(p.first, p.second);
    if (last_addr != vector_root.addr) {
      ext = co_await vector_load_extent<VectorNode>(
	t, vector_root.addr, BEGIN_KEY, END_KEY);
      last_addr = vector_root.addr;
    }
  }
  co_return;
}

VectorManager::omap_set_key_ret 
VectorManager::omap_set_key(
  omap_root_t &vector_root,
  Transaction &t,
  std::string key, ceph::bufferlist value)
{
  LOG_PREFIX(VectorManager::omap_set_key);
  DEBUGT("enter k={}", t, key);
  assert(vector_root.get_type() == omap_type_t::VECTOR);

  std::map<std::string, ceph::bufferlist> kvs;
  kvs.emplace(key, value);
  co_return co_await omap_set_keys(vector_root, t, std::move(kvs));
}

VectorManager::omap_set_key_ret
VectorManager::_vector_set_key(omap_root_t &vector_root,
  Transaction &t, VectorNodeRef tail,
  const std::string &key, const ceph::bufferlist &value)
{
  LOG_PREFIX(VectorManager::_vector_set_key);
  DEBUGT("enter key={}", t, key);
  assert(tail);
  if (!tail->expect_overflow(key.size(), value.length())) {
    auto mut = tm.get_mutable_extent(t, tail)->cast<VectorNode>();
    mut->append_kv(t, key, value);
    co_return;
  }

  auto extent = co_await tm.alloc_non_data_extent<VectorNode>(
    t, vector_root.hint, LOG_NODE_BLOCK_SIZE
  ).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    omap_set_key_iertr::pass_further{}
  );
  assert(extent);
  if (!is_dup_vector_key(key)) {
    // Normal vector key:
    // Advance the vector_root to the new tail extent.
    // Preserve the existing dup tail by inheriting it from the nextious tail.
    vector_root.update(extent->get_laddr(), vector_root.depth,
      vector_root.hint, vector_root.type);
    assert(tail->get_dup_tail_addr() != L_ADDR_NULL);
    extent->set_dup_tail_addr(tail->get_dup_tail_addr());
  } else {
    // Dup vector key:
    // Update the dup tail pointer in the current vector tail
    //   to point to the newly created dup extent.
    auto ext = co_await vector_load_extent<VectorNode>(
      t, vector_root.addr, BEGIN_KEY, END_KEY);
    auto mut = tm.get_mutable_extent(t, ext)->cast<VectorNode>();
    mut->set_dup_tail_addr(extent->get_laddr());
  }
  extent->append_kv(t, key, value);
  extent->set_next_addr(tail->get_laddr());
  co_return;
}

std::ostream &VectorNode::print_detail_l(std::ostream &out) const
{
  laddr_t l = this->get_next_addr();
  out << ", next=" << l
      << ", num=" << this->get_size()
      << ", used_space=" << this->use_space()
      << ", capacity=" << this->get_capacity()
      << ", last_pos=" << this->get_last_pos()
      << ", first_key=" << this->iter_cbegin().get_key()
      << ", last_key=" << this->get_last_key();
  if (has_laddr()) {
    out << ", begin=" << get_begin()
	<< ", end=" << get_end();
  }
  return out;
}

template <typename T>
requires std::is_same_v<VectorNode, T>
VectorManager::vector_load_extent_iertr::future<TCachedExtentRef<T>> 
VectorManager::vector_load_extent(
  Transaction &t,
  laddr_t laddr,
  std::string begin,
  std::string end)
{
  LOG_PREFIX(VectorManager::vector_load_extent);
  DEBUGT("laddr={}", t, laddr);
  assert(end <= END_KEY);
  auto size = VECTOR_NODE_BLOCK_SIZE;
  auto maybe_indirect_extent = co_await tm.read_extent<T>(t, laddr, size,
    [begin=std::move(begin), end=std::move(end)](T &extent) mutable {
      assert(!extent.is_seen_by_users());
      extent.init_range(std::move(begin), std::move(end));
    }
  ).handle_error_interruptible(
    vector_load_extent_iertr::pass_further{},
    crimson::ct_error::assert_all{ "Invalid error in vector_load_extent" }
  );

  assert(!maybe_indirect_extent.is_indirect());
  assert(!maybe_indirect_extent.is_clone);
  co_return std::move(maybe_indirect_extent.extent);
}

VectorManager::omap_get_value_ret
VectorManager::omap_get_value(
  const omap_root_t &vector_root, Transaction &t, std::string key)
{
  LOG_PREFIX(VectorManager::omap_get_value);
  DEBUGT("key={}", t, key);
  assert(vector_root.get_type() == omap_type_t::VECTOR);
  co_return co_await find_kv(t, vector_root.addr, key);
}

VectorManager::omap_list_ret
VectorManager::omap_list(
  const omap_root_t &vector_root,
  Transaction &t,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  OMapManager::omap_list_config_t config)
{
  LOG_PREFIX(VectorManager::omap_list);
  DEBUGT("first={}, last={}", t, first, last);
  assert(vector_root.get_type() == omap_type_t::VECTOR);
  std::map<std::string, bufferlist> kvs;
  co_await find_kvs(t, vector_root.addr, first, last, kvs);
  auto ret = omap_list_bare_ret(false, {});
  auto &[complete, result] = ret;
  result.insert(kvs.begin(), kvs.end());
  co_return std::move(ret);
}

VectorManager::omap_list_iertr::future<>
VectorManager::find_kvs(Transaction &t, laddr_t dst,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  std::map<std::string, bufferlist> &kvs)
{
  LOG_PREFIX(VectorManager::find_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
  if (dst == L_ADDR_NULL) {
    co_return;
  }
  auto extent = co_await vector_load_extent<VectorNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return;
  }
  extent->list(first, last, kvs);
  co_await find_kvs(t, extent->get_next_addr(), first, last, kvs);
  co_return;
}

VectorManager::omap_get_value_ret
VectorManager::find_kv(Transaction &t, laddr_t dst, const std::string &key)
{
  LOG_PREFIX(VectorManager::find_kv);
  DEBUGT("key={}, dst={}", t, key, dst);

  auto extent = co_await vector_load_extent<VectorNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return std::nullopt;
  }
  auto e = co_await extent->get_value(key);
  if (e == std::nullopt) {
    if(extent->get_next_addr() == L_ADDR_NULL) {
      co_return std::nullopt;
    }
    auto ret = co_await find_kv(t, extent->get_next_addr(), key);
    co_return ret;
  }
  co_return std::move(e);
}

VectorManager::omap_rm_key_ret
VectorManager::remove_node(Transaction &t, VectorNodeRef mut, VectorNodeRef next)
{
  LOG_PREFIX(VectorManager::remove_node);
  if (next == nullptr) { 
    // This is the tail, so just reinitialize the VectorNode.
    // A VectorNode for the pg vector should preserve the dup tail.
    laddr_t next_addr = mut->get_next_addr();
    laddr_t dup_tail_addr = mut->get_dup_tail_addr();
    mut->set_init_vars();
    mut->set_next_addr(next_addr);
    mut->set_dup_tail_addr(dup_tail_addr);
    co_return;
  }
  assert(mut);
  DEBUGT("mut={}, next={}", t, *mut, *next);
  laddr_t next_addr = mut->get_next_addr();
  co_await tm.remove(t, mut->get_laddr()
  ).handle_error_interruptible(
    omap_rm_key_iertr::pass_further{},
    crimson::ct_error::assert_all{"Invalid error in remove_node"}
  );
  auto mut_next = tm.get_mutable_extent(t, next)->template cast<VectorNode>();
  assert(mut_next);
  mut_next->set_next_addr(next_addr);
  co_return;
}

VectorManager::omap_rm_key_ret
VectorManager::remove_kv(Transaction &t, laddr_t dst, const std::string &key, VectorNodeRef next)
{
  LOG_PREFIX(VectorManager::remove_kv);
  DEBUGT("key={}, dst={}", t, key, dst);

  if (dst == L_ADDR_NULL) {
    co_return;
  }
  auto extent = co_await vector_load_extent<VectorNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return;
  }

  auto e = co_await extent->get_value(key, VectorNode::copy_t::SHALLOW);
  if (e == std::nullopt) {
    if(extent->get_next_addr() == L_ADDR_NULL) {
      co_return;
    }
    co_await remove_kv(t, extent->get_next_addr(), key, extent);
    co_return;
  }

  auto mut = tm.get_mutable_extent(t, extent)->template cast<VectorNode>();
  mut->remove_entry(key);
  if (mut->is_removable()) {
    co_await remove_node(t, mut, next);
  }
  co_return;
}

VectorManager::omap_rm_key_ret
VectorManager::remove_kvs(Transaction &t, laddr_t dst, 
  std::optional<std::string> first, 
  std::optional<std::string> last,
  VectorNodeRef next)
{
  LOG_PREFIX(VectorManager::remove_kvs);
  DEBUGT("first={}, last={}, dst={}", t, first, last, dst);
      
  if (dst == L_ADDR_NULL || first == std::nullopt) {
    co_return;
  }

  auto extent = co_await vector_load_extent<VectorNode>(
    t, dst, BEGIN_KEY, END_KEY);
  if (extent == nullptr) {
    co_return;
  }
  auto l = last;
  if (l && (*l).empty()) {
    l = std::nullopt;
  }

  laddr_t next_addr = extent->get_next_addr();

  if (is_vector_key(*first)) {
    // skip to search due to out of range
    if (l != std::nullopt && extent->vector_has_larger_than(*last)) {
      co_await remove_kvs(t, next_addr, first, last, extent);
      co_return;
    }
    // If time-seris vector, we don't need traversal anymore
    if (*first != std::string() && extent->vector_less_than(*first)) {
      co_return;
    }
  }

  VectorNode::range_t r = extent->has_between(first, l);
  VectorNodeRef p = extent;
  if (r == VectorNode::range_t::HAS_BETWEEN) {
    auto mut = tm.get_mutable_extent(t, extent)->template cast<VectorNode>();
    assert(mut);
    auto ret = mut->remove_entries(first, l);
    assert(ret);
    DEBUGT("remove {}, extent's last key of deleted entries={}",
      t, *extent, extent->get_last_key());
    p = mut;
    if (mut->is_removable()) {
      co_await remove_node(t, mut, next);
      if (next != nullptr) {
	p = co_await vector_load_extent<VectorNode>(
	  t, next->get_laddr(), BEGIN_KEY, END_KEY);
      }
    }
  }
  co_await remove_kvs(t, next_addr, first, last, p);
  co_return;
}

VectorManager::omap_rm_key_ret 
VectorManager::omap_rm_key(
  omap_root_t &vector_root,
  Transaction &t,
  std::string key)
{
  LOG_PREFIX(VectorManager::omap_rm_key);
  DEBUGT("key={}", t, key);
  assert(vector_root.get_type() == omap_type_t::VECTOR);
  co_await remove_kv(t, vector_root.addr, key, nullptr);
  co_return;
}

VectorManager::omap_rm_keys_ret
VectorManager::omap_rm_keys(
  omap_root_t& vector_root,
  Transaction& t,
  std::set<std::string> keys)
{
  LOG_PREFIX(VectorManager::omap_rm_keys);
  DEBUGT("key size={}", t, keys.size());
  assert(vector_root.get_type() == omap_type_t::VECTOR);

  std::set<std::string> dup_keys;
  auto begin = keys.lower_bound("dup_");
  auto end   = keys.lower_bound("dup`");
  while (begin != end) {
    auto nh = keys.extract(begin++);
    dup_keys.insert(std::move(nh));
  }

  co_await remove_key_set(keys, vector_root.addr);
  co_return;
}

VectorManager::omap_rm_key_range_ret 
VectorManager::omap_rm_key_range(
  omap_root_t &vector_root,
  Transaction &t,
  const std::string &first,
  const std::string &last)
{
#if 0
  LOG_PREFIX(VectorManager::omap_rm_key_range);
  DEBUGT("first={}, last={}", t, first, last);
  assert(vector_root.get_type() == omap_type_t::LOG);
  co_await remove_kvs(t, vector_root.addr, first, last, nullptr);
#endif
  co_return;
}

VectorManager::omap_clear_ret
VectorManager::omap_clear(omap_root_t &root, Transaction &t)
{
  LOG_PREFIX(VectorManager::omap_clear);
  DEBUGT("enter", t);
  assert(root.get_type() == omap_type_t::VECTOR);
  co_await remove_kvs(t, root.addr,
    std::optional<std::string>(),
    std::optional<std::string>(std::nullopt), nullptr);
  co_await tm.remove(t, root.get_location()
  ).handle_error_interruptible(
    omap_clear_iertr::pass_further{},
    crimson::ct_error::assert_all{"Invalid error in omap_clear"}
  );
  root.update(
    L_ADDR_NULL,
    0,
    laddr_hint_t::create_as_fixed(L_ADDR_MIN),
    root.get_type());
  co_return;
}

VectorManager::omap_iterate_ret 
VectorManager::omap_iterate(
  const omap_root_t &vector_root,
  Transaction &t,
  ObjectStore::omap_iter_seek_t &start_from,
  omap_iterate_cb_t callback)
{
  LOG_PREFIX(VectorManager::omap_iterate);
  DEBUGT("start={}", t, start_from.seek_position);
  assert(vector_root.get_type() == omap_type_t::VECTOR);

  std::string s = start_from.seek_position;
  std::map<std::string, bufferlist> kvs;
  co_await find_kvs(t, vector_root.addr, std::optional<std::string>(s),
    std::optional<std::string>(std::nullopt), kvs);
  if (start_from.seek_type == ObjectStore::omap_iter_seek_t::UPPER_BOUND) {
    auto it = kvs.find(s);
    if (it != kvs.end()) {
      kvs.erase(it);
    }
  }

  ObjectStore::omap_iter_ret_t ret;
  for (auto &p : kvs) {
    std::string result(p.second.c_str(), p.second.length());
    ret = callback(p.first, result);
    if (ret == ObjectStore::omap_iter_ret_t::STOP) {
      break;
    }
  }
  co_return co_await omap_iterate_iertr::make_ready_future<
    ObjectStore::omap_iter_ret_t>(std::move(ret));
}


}
