// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <boost/range/combine.hpp>

#include "test/crimson/gtest_seastar.h"

#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/onode_manager/staged-fltree/fltree_onode_manager.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/dummy.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/tree_utils.h"
#include "crimson/os/seastore/vector_node.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::onode;
using CTransaction = ceph::os::Transaction;
using namespace std;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }

  ceph::bufferlist make_vector_payload(const std::string &contents) {
    ceph::bufferlist bl;
    bl.append(contents);
    return bl;
  }

  vector_node_entry_t make_vector_entry(
      std::string entry_id,
      const std::string &payload) {
    vector_node_entry_t entry;
    entry.entry_id = std::move(entry_id);
    entry.bucket_name = "bucket";
    entry.index_name = "index";
    entry.user_key = "key";
    entry.data_type = ceph::rados::vector_data_type_float32;
    entry.distance_metric =
      ceph::rados::vector_distance_metric_euclidean;
    entry.dimension = payload.size() / sizeof(float);
    entry.placement_algorithm =
      ceph::rados::vector_placement_algorithm_hash_v0;
    entry.placement_key = "abcd";
    entry.vector_hash = "01234567";
    entry.vector_data = make_vector_payload(payload);
    return entry;
  }

  void expect_vector_entries(
      const vector_node_t &node,
      std::initializer_list<std::pair<std::string, std::string>> entries) {
    ASSERT_EQ(entries.size(), node.entries.size());
    auto expected = entries.begin();
    for (const auto &actual : node.entries) {
      EXPECT_EQ(expected->first, actual.entry_id);
      EXPECT_TRUE(actual.vector_data.contents_equal(
        make_vector_payload(expected->second)));
      ++expected;
    }
  }
}

struct onode_item_t {
  uint32_t size;
  uint64_t id;
  uint64_t block_size;
  uint32_t cnt_modify = 0;

  void initialize(Transaction& t, Onode& value) const {
    auto &ftvalue = static_cast<FLTreeOnode&>(value);
    ftvalue.update_onode_size(t, size);
    auto laddr = laddr_t::from_byte_offset(id << laddr_t::UNIT_SHIFT);
    auto oroot = omap_root_t(laddr, cnt_modify,
      value.get_metadata_hint(block_size), omap_type_t::OMAP);
    ftvalue.update_omap_root(t, oroot);
    validate(value);
  }

  void validate(Onode& value) const {
    auto& layout = value.get_layout();
    ceph_assert(uint64_t(layout.size) == uint64_t{size});
    auto laddr = laddr_t::from_byte_offset(id << laddr_t::UNIT_SHIFT);
    ceph_assert(layout.omap_root.get(value.get_metadata_hint(block_size)).addr == laddr);
    ceph_assert(layout.omap_root.get(value.get_metadata_hint(block_size)).depth == cnt_modify);
  }

  void modify(Transaction& t, Onode& value) {
    validate(value);
    ++cnt_modify;
    initialize(t, value);
  }

  static onode_item_t create(std::size_t size, std::size_t id, uint64_t block_size) {
    ceph_assert(size <= std::numeric_limits<uint32_t>::max());
    return {(uint32_t)size, id, block_size};
  }
};

struct fltree_onode_manager_test_t
    : public seastar_test_suite_t, TMTestState {
  using iterator_t = typename KVPool<onode_item_t>::iterator_t;

  FLTreeOnodeManagerRef manager;

  seastar::future<> set_up_fut() final {
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown();
  }

  virtual seastar::future<> _init() final {
    return TMTestState::_init().then([this] {
      manager.reset(new FLTreeOnodeManager(*tm));
    });
  }

  virtual seastar::future<> _destroy() final {
    manager.reset();
    return TMTestState::_destroy();
  }

  virtual FuturizedStore::mkfs_ertr::future<> _mkfs() final {
    return TMTestState::_mkfs(
    ).safe_then([this] {
      return restart_fut();
    }).safe_then([this] {
      return repeat_eagain([this] {
        return seastar::do_with(
          create_mutate_transaction(),
          [this](auto &ref_t)
        {
          return with_trans_intr(*ref_t, [&](auto &t) {
            return manager->mkfs(t
            ).si_then([this, &t] {
              return submit_transaction_fut2(t);
            });
          });
        });
      });
    }).handle_error(
      crimson::ct_error::assert_all{"Invalid error in _mkfs"}
    );
  }

  template <typename F>
  void with_transaction(F&& f) {
    auto t = create_mutate_transaction();
    std::invoke(f, *t);
    submit_transaction(std::move(t));
  }

  template <typename F>
  void with_onode_write(iterator_t& it, F&& f) {
    with_transaction([this, &it, f=std::move(f)] (auto& t) {
      auto p_kv = *it;
      auto onode = with_trans_intr(t, [&](auto &t) {
        return manager->get_or_create_onode(t, p_kv->key);
      }).unsafe_get();
      std::invoke(f, t, *onode, p_kv->value);
    });
  }

  void validate_onode(iterator_t& it) {
    with_transaction([this, &it] (auto& t) {
      auto p_kv = *it;
      auto onode = with_trans_intr(t, [&](auto &t) {
        return manager->get_onode(t,  p_kv->key);
      }).unsafe_get();
      p_kv->value.validate(*onode);
    });
  }

  void validate_erased(iterator_t& it) {
    with_transaction([this, &it] (auto& t) {
      auto p_kv = *it;
      auto exist = with_trans_intr(t, [&](auto &t) {
        return manager->contains_onode(t, p_kv->key);
      }).unsafe_get();
      ceph_assert(exist == false);
    });
  }

  template <typename F>
  void with_onodes_process(
      const iterator_t& start, const iterator_t& end, F&& f) {
    std::vector<ghobject_t> oids;
    std::vector<onode_item_t*> items;
    auto it = start;
    while(it != end) {
      auto p_kv = *it;
      oids.emplace_back(p_kv->key);
      items.emplace_back(&p_kv->value);
      ++it;
    }
    with_transaction([&oids, &items, f=std::move(f)] (auto& t) mutable {
      std::invoke(f, t, oids, items);
    });
  }

  template <typename F>
  void with_onodes_write(
      const iterator_t& start, const iterator_t& end, F&& f) {
    with_onodes_process(start, end,
        [this, f=std::move(f)] (auto& t, auto& oids, auto& items) {
      auto onodes = with_trans_intr(t, [&](auto &t) {
        return manager->get_or_create_onodes(t, oids);
      }).unsafe_get();
      for (auto tup : boost::combine(onodes, items)) {
        OnodeRef onode;
        onode_item_t* p_item;
        boost::tie(onode, p_item) = tup;
        std::invoke(f, t, *onode, *p_item);
      }
    });
  }

  void validate_onodes(
      const iterator_t& start, const iterator_t& end) {
    with_onodes_process(start, end,
        [this] (auto& t, auto& oids, auto& items) {
      for (auto tup : boost::combine(oids, items)) {
        ghobject_t oid;
        onode_item_t* p_item;
        boost::tie(oid, p_item) = tup;
        auto onode = with_trans_intr(t, [&](auto &t) {
          return manager->get_onode(t, oid);
        }).unsafe_get();
        p_item->validate(*onode);
      }
    });
  }

  void validate_erased(
      const iterator_t& start, const iterator_t& end) {
    with_onodes_process(start, end,
        [this] (auto& t, auto& oids, auto& items) {
      for (auto& oid : oids) {
        auto exist = with_trans_intr(t, [&](auto &t) {
          return manager->contains_onode(t, oid);
        }).unsafe_get();
        ceph_assert(exist == false);
      }
    });
  }

  static constexpr uint64_t LIST_LIMIT = 10;
  void validate_list_onodes(KVPool<onode_item_t>& pool) {
    with_onodes_process(pool.begin(), pool.end(),
        [this] (auto& t, auto& oids, auto& items) {
      std::vector<ghobject_t> listed_oids;
      auto start = ghobject_t();
      auto end = ghobject_t::get_max();
      assert(start < end);
      assert(start < oids[0]);
      assert(oids[0] < end);
      while (start != end) {
        auto [list_ret, list_end] = with_trans_intr(t, [&](auto &t) {
          return manager->list_onodes(t, start, end, LIST_LIMIT);
        }).unsafe_get();
        listed_oids.insert(listed_oids.end(), list_ret.begin(), list_ret.end());
        start = list_end;
      }
      ceph_assert(oids.size() == listed_oids.size());
    });
  }

  fltree_onode_manager_test_t() {}
};

TEST_P(fltree_onode_manager_test_t, 1_single)
{
  run_async([this] {
    uint64_t block_size = tm->get_block_size();
    auto pool = KVPool<onode_item_t>::create_range({0, 1}, {128, 256}, block_size);
    auto iter = pool.begin();
    with_onode_write(iter, [](auto& t, auto& onode, auto& item) {
      item.initialize(t, onode);
    });
    validate_onode(iter);

    with_onode_write(iter, [](auto& t, auto& onode, auto& item) {
      item.modify(t, onode);
    });
    validate_onode(iter);

    validate_list_onodes(pool);

    with_onode_write(iter, [this](auto& t, auto& onode, auto& item) {
      OnodeRef onode_ref = &onode;
      with_trans_intr(t, [&](auto &t) {
        return manager->erase_onode(t, onode_ref);
      }).unsafe_get();
    });
    validate_erased(iter);
  });
}

TEST_P(fltree_onode_manager_test_t, vector_node_link_lifecycle)
{
  run_async([this] {
    auto pool = KVPool<onode_item_t>::create_range(
      {0, 2}, {128, 256}, tm->get_block_size());
    auto src_item = *pool.begin();
    auto dst_item = *(pool.begin() + 1);
    auto src_key = src_item->key;
    auto dst_key = dst_item->key;
    laddr_t vector_addr = L_ADDR_NULL;

    {
      VectorNodeManager vector_manager(*tm);
      auto t = create_mutate_transaction();
      auto onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_or_create_onode(trans, src_key);
      }).unsafe_get();
      auto &flonode = static_cast<FLTreeOnode&>(*onode);
      src_item->value.initialize(*t, *onode);
      EXPECT_EQ(sizeof(onode_layout_t), flonode.get_payload_size());
      EXPECT_FALSE(onode->has_vector_node());

      auto node = with_trans_intr(*t, [&](auto &trans) {
        return vector_manager.create_vector_root(trans);
      }).unsafe_get();
      vector_addr = node->get_laddr();
      node = with_trans_intr(*t, [&](auto &trans) {
        return vector_manager.upsert_vector_entry(
          trans, std::move(node),
          make_vector_entry("0000000a", "aaaa"));
      }).unsafe_get();
      ASSERT_NE(L_ADDR_NULL, vector_addr);
      onode->update_vector_node_laddr(*t, vector_addr);
      EXPECT_EQ(vector_addr, onode->get_vector_node_laddr());
      auto offloaded = onode->offload_data_and_md(*t);
      EXPECT_EQ(vector_addr, offloaded->get_vector_node_laddr());
      EXPECT_EQ(vector_addr, onode->get_vector_node_laddr());
      src_item->value.initialize(*t, *onode);
      submit_transaction(std::move(t));
    }

    {
      VectorNodeManager vector_manager(*tm);
      auto t = create_read_transaction();
      auto onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, src_key);
      }).unsafe_get();
      EXPECT_EQ(vector_addr, onode->get_vector_node_laddr());
      std::vector<vector_node_entry_t> entries;
      auto stats = with_trans_intr(*t, [&](auto &trans) {
        return vector_manager.read_vector_node(trans, vector_addr
        ).si_then([&](auto root) {
          return vector_manager.scan_vector_entries(
            trans, std::move(root), [&](const auto &entry) {
              entries.push_back(entry);
            });
        });
      }).unsafe_get();
      EXPECT_EQ(1u, stats.logical_entries);
      vector_node_t leaf;
      leaf.kind = vector_node_kind_t::LEAF;
      leaf.entries = std::move(entries);
      expect_vector_entries(leaf, {{"0000000a", "aaaa"}});
    }

    restart();

    {
      VectorNodeManager vector_manager(*tm);
      auto t = create_read_transaction();
      auto onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, src_key);
      }).unsafe_get();
      EXPECT_EQ(vector_addr, onode->get_vector_node_laddr());
      std::vector<vector_node_entry_t> entries;
      auto stats = with_trans_intr(*t, [&](auto &trans) {
        return vector_manager.read_vector_node(trans, vector_addr
        ).si_then([&](auto root) {
          return vector_manager.scan_vector_entries(
            trans, std::move(root), [&](const auto &entry) {
              entries.push_back(entry);
            });
        });
      }).unsafe_get();
      EXPECT_EQ(1u, stats.logical_entries);
      vector_node_t leaf;
      leaf.kind = vector_node_kind_t::LEAF;
      leaf.entries = std::move(entries);
      expect_vector_entries(leaf, {{"0000000a", "aaaa"}});
    }

    {
      auto t = create_mutate_transaction();
      auto src_onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, src_key);
      }).unsafe_get();
      auto dst_onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_or_create_onode(trans, dst_key);
      }).unsafe_get();
      dst_item->value.initialize(*t, *dst_onode);
      src_onode->swap_layout(*t, *dst_onode);
      EXPECT_FALSE(src_onode->has_vector_node());
      EXPECT_EQ(vector_addr, dst_onode->get_vector_node_laddr());
      submit_transaction(std::move(t));
    }

    {
      auto t = create_read_transaction();
      auto src_onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, src_key);
      }).unsafe_get();
      auto dst_onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, dst_key);
      }).unsafe_get();
      EXPECT_FALSE(src_onode->has_vector_node());
      EXPECT_EQ(vector_addr, dst_onode->get_vector_node_laddr());
    }

    restart();

    {
      auto t = create_read_transaction();
      auto src_onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, src_key);
      }).unsafe_get();
      auto dst_onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, dst_key);
      }).unsafe_get();
      EXPECT_FALSE(src_onode->has_vector_node());
      EXPECT_EQ(vector_addr, dst_onode->get_vector_node_laddr());
    }

    {
      VectorNodeManager vector_manager(*tm);
      auto t = create_mutate_transaction();
      auto onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, dst_key);
      }).unsafe_get();
      with_trans_intr(*t, [&](auto &trans) {
        return vector_manager.read_vector_node(trans, vector_addr
        ).si_then([&](auto root) {
          return vector_manager.remove_vector_tree(
            trans, std::move(root));
        });
      }).unsafe_get();
      onode->clear_vector_node_laddr(*t);
      EXPECT_FALSE(onode->has_vector_node());
      submit_transaction(std::move(t));
    }

    restart();

    {
      auto t = create_read_transaction();
      auto onode = with_trans_intr(*t, [&](auto &trans) {
        return manager->get_onode(trans, dst_key);
      }).unsafe_get();
      EXPECT_FALSE(onode->has_vector_node());
      EXPECT_EQ(L_ADDR_NULL, onode->get_vector_node_laddr());
    }
  });
}

TEST_P(fltree_onode_manager_test_t, 2_synthetic)
{
  run_async([this] {
    uint64_t block_size = tm->get_block_size();
    auto pool = KVPool<onode_item_t>::create_range(
        {0, 10000}, {32, 64, 128, 256, 512}, block_size);
    auto start = pool.begin();
    auto end = pool.end();
    with_onodes_write(start, end,
        [](auto& t, auto& onode, auto& item) {
      item.initialize(t, onode);
    });
    restart();
    validate_onodes(start, end);

    validate_list_onodes(pool);

    auto rd_start = pool.random_begin();
    auto rd_end = rd_start + 50;
    with_onodes_write(rd_start, rd_end,
        [](auto& t, auto& onode, auto& item) {
      item.modify(t, onode);
    });
    restart();
    validate_onodes(start, end);

    pool.shuffle();
    rd_start = pool.random_begin();
    rd_end = rd_start + 50;
    with_onodes_write(rd_start, rd_end,
        [](auto& t, auto& onode, auto& item) {
      item.modify(t, onode);
    });
    restart();
    validate_onodes(start, end);

    pool.shuffle();
    rd_start = pool.random_begin();
    rd_end = rd_start + 50;
    with_onodes_write(rd_start, rd_end,
        [this](auto& t, auto& onode, auto& item) {
      OnodeRef onode_ref = &onode;
      with_trans_intr(t, [&](auto &t) {
        return manager->erase_onode(t, onode_ref);
      }).unsafe_get();
    });
    restart();
    validate_erased(rd_start, rd_end);
    pool.erase_from_random(rd_start, rd_end);
    start = pool.begin();
    end = pool.end();
    validate_onodes(start, end);

    validate_list_onodes(pool);
  });
}

INSTANTIATE_TEST_SUITE_P(
  fltree_onode__manager_test,
  fltree_onode_manager_test_t,
  ::testing::Combine(
    ::testing::Values (
      "segmented",
      "circularbounded"
    ),
    ::testing::Values(
      integrity_check_t::FULL_CHECK)
  )
);
