// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "vector_node.h"

namespace crimson::os::seastore::vector_manager{

void delta_t::replay(VectorNodeLayout &l) {
  if (op == op_t::APPEND) {
    l._append(key, val);
    return;
  } else if (op == op_t::ADD_NEXT) {
    l.set_next_node(prev);
  } else if (op == op_t::INIT) {
    l.set_last_pos(0); 
    l.set_size(0);
    l.set_prev_node(L_ADDR_NULL);
    l.set_reserved_len(0);
    l.set_reserved_size(0);
    l.init_bitmap();
  } else if (op == op_t::REMOVE) {
    d_bitmap_t bitmap;
    auto biter = val.cbegin();
    ceph::decode(bitmap, biter);
    l._set_d_bitmap(bitmap);
  } else if (op == op_t::OVERWRITE) {
    l._overwrite(key, val);
  }
}


void VectorNode::append_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append(key, val, p);
    return;
  }
  append(key, val);

}

void VectorNode::overwrite_kv(Transaction &t, const std::string &key,
    const ceph::bufferlist &val) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    //int gap = ow_gap_from_last_entry(key.size(), val.length());
    journal_overwrite(key, val, p);
    // TODO
#if 0
    if (gap > 0) {
      reserved_len += gap;
    }
#endif
    return;
  }
  overwrite(key, val);
}

void VectorNode::set_next_addr(laddr_t l) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append_prev_addr(l, p);
    return;
  }
  set_next_node(l);
}

void VectorNode::set_init_vars() {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append_init(p);
    return;
  }
  init_vars();
}

void VectorNode::append_remove(ceph::bufferlist bl) {
  auto p = maybe_get_delta_buffer();
  if (p) {
    journal_append_remove(p, bl);
    return;
  }
  d_bitmap_t bitmap;
  auto biter = bl.cbegin();
  decode(bitmap, biter);
  _set_d_bitmap(bitmap);
}

bool VectorNode::is_removable() {
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_d_bitmap();
    if (ret) {
      d_bitmap_t bitmap;
      auto biter = (*ret).cbegin();
      decode(bitmap, biter);
      return bitmap.is_all_set(get_size() + get_reserved_size());
    }
  }
  auto bitmap = get_d_bitmap();
  return bitmap.is_all_set(get_size());
}

void VectorNode::set_cur_bitmap(uint32_t begin, uint32_t end) {
  d_bitmap_t bitmap = get_d_bitmap();
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_d_bitmap();
    if (ret) {
      auto biter = (*ret).cbegin();
      decode(bitmap, biter);
    }
  } 
  bitmap.set_bitmap_range(begin, end);
  bufferlist bl;
  encode(bitmap, bl);
  append_remove(bl);
}

d_bitmap_t VectorNode::get_cur_bitmap() {
  d_bitmap_t bitmap = get_d_bitmap();
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_d_bitmap();
    if (ret) {
      auto biter = (*ret).cbegin();
      decode(bitmap, biter);
    } 
  } 
  return bitmap;
}

void VectorNode::set_bitmap(d_bitmap_t map) {
  bufferlist bl;
  encode(map, bl);
  append_remove(bl);
}

template <typename F>
void VectorNode::for_each_live_entry(F&& fn) {
  d_bitmap_t bitmap;
  if (auto p = maybe_get_delta_buffer()) {
    if (auto ret = p->get_latest_d_bitmap()) {
      auto it = (*ret).cbegin();
      decode(bitmap, it);
    }
  } else {
    bitmap = get_d_bitmap();
  }

  uint32_t index = 0;
  auto iter = iter_begin();
  while (iter != iter_end()) {
    if (!bitmap.is_set(index)) {
      if (fn(*iter, index)) {
	return;
      }
    }
    ++iter;
    ++index;
  }
}

void VectorNode::list(const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  std::map<std::string, bufferlist> &kvs) {
  std::string_view s = first ? std::string_view(*first) : std::string_view{""};
  std::string_view e = last ? std::string_view(*last) : std::string_view{};
  for_each_live_entry([&](const auto& ent, uint32_t index) -> bool {
    const auto k = ent.get_key();
    if (k >= s && (!last || k <= e)) {
      if (ent.get_chunk_idx() == 0) {
	// This is not multi block kv pair
	kvs[k] = ent.get_val();
      } else {
	bufferlist head = ent.get_val();
	auto it = kvs.find(k);
	if (it != kvs.end()) {
	  head.claim_append(kvs[k]);
	}
	kvs[k] = std::move(head);
      }
    }
    return false;
  });
}

VectorNode::get_value_ret VectorNode::get_value(const std::string &key, copy_t c)
{
  bufferlist bl;
  bool found = false;
  for_each_live_entry([&](const auto& ent, uint32_t index) -> bool {
    const auto k = ent.get_key();
    if (k == key) {
      if (c == copy_t::SHALLOW) {
	bl = ent.get_val_shallow();
      } else {
	bl = ent.get_val();
      }
      found = true;
      /* If key is time-series log,
       * duplicate does not exist. In this case, return latest one */
      if (is_log_key(k)) {
	found = true;
	return true;
      }
    }
    return false;
  });
  if (bl.length() > 0 || found) {
    return get_value_ret(
      interruptible::ready_future_marker{},
      std::move(bl));
  }

  return get_value_ret(
    interruptible::ready_future_marker{},
    std::nullopt);
}

bool VectorNode::remove_entry(const std::string key)
{
  auto iter = iter_begin();
  uint32_t index = 0;
  bool removed = false;
  while(iter != iter_end()) {
    if (iter->get_key() == key) {
      set_cur_bitmap(index, index);
      // Duplicate keys may exist if the old entry was removed.
      removed = true;
    }
    index++;
    iter++;
  };
  return removed;
}

#if 0
bool VectorNode::log_less_than(std::string_view str) const
{
  std::string last_key = get_last_key();
  if (is_log_key(last_key)) {
    return last_key < str;
  }
  auto iter = iter_begin();
  bool all_less = false;
  // perform full traversal to figure out last entry < str
  while(iter != iter_end()) {
    std::string key = iter->get_key();
    if (is_log_key(key)) {
      all_less = key < str;
    }
    iter++;
  };
  return all_less;
}

bool VectorNode::log_has_larger_than(std::string_view str) const
{
  auto iter = iter_begin();
  // return true if the first log entry > str
  while(iter != iter_end()) {
    std::string key = iter->get_key();
    if (!is_log_key(key)) {
      iter++;
      continue;
    }
    return key > str;
  };
  return false;
}

bool VectorNode::can_ow()
{
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_write_delta();
    if (ret && (*ret).key == get_ow_key()) {
      return true;
    } else if (ret && (*ret).key != get_ow_key()) {
      return false;
    }
  }
  if (is_ow_key(get_last_key())) {
    return true;
  }
  return false;
}

int VectorNodeLayout::_ow_gap_from_last_entry(const size_t key, const size_t val)
{
  iterator iter(this, get_last_pos());
  auto last = iter->get_node_key();
  assert(iter->get_key() == get_ow_key());
  return get_entry_size(key, val) 
    - get_entry_size(last.key_len, last.val_len);
}
#endif

void VectorNodeLayout::journal_append_remove(
  delta_buffer_t *recorder, 
  ceph::bufferlist bl) {
  recorder->insert_remove(bl);
}

bool VectorNode::expect_overflow(const std::string &key,
  size_t vsize, bool can_ow) {
  size_t ksize = key.size();
#if 0
  if (can_ow) { 
    int gap = ow_gap_from_last_entry(key.size(), vsize);
    uint64_t remain = capacity() - get_last_pos() - reserved_len;
    if (gap >= 0) {
      gap += static_cast<uint64_t>(gap);
    } else {
      uint64_t d = static_cast<uint64_t>(-gap);
      gap -= d;
    }
    return remain < get_entry_size(ksize, vsize);
  } else 
#endif
  if (get_size() + reserved_size + 1 > d_bitmap_t::MAX_ENTRY) {
    return true;
  } else if (is_ow_key(key) && !can_ow) {
    // guess there is enough space to store further entry in this node.
    // this makes sure that the last entry of this node is non-ow entry,
    // leading to reducing garbage collection for _fastinfo
    size_t next_expected_size = get_entry_size(ksize, vsize) + reserved_len;
    return free_space() < 
      get_entry_size(ksize, vsize) + reserved_len + next_expected_size;
  }
  return free_space() < get_entry_size(ksize, vsize) + reserved_len;
}

#if 0
int VectorNode::ow_gap_from_last_entry(const size_t key, const size_t val) {
  int gap = 0;
  auto p = maybe_get_delta_buffer();
  if (p) {
    auto ret = p->get_latest_write_delta();
    if (ret && (*ret).key == get_ow_key()) {
      if ((*ret).val.length() < val) {
	gap = val - (*ret).val.length();
      }
    } else {
      gap = _ow_gap_from_last_entry(key, val);
    }
  } else {
    gap = _ow_gap_from_last_entry(key, val);
  }
  return gap;
}
#endif

}
