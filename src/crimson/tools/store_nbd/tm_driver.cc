// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tm_driver.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::segment_manager::block;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

seastar::future<> TMDriver::write(
  off_t offset,
  bufferptr ptr)
{
  logger().debug("Writing offset {}", offset);
  assert(offset % segment_manager->get_block_size() == 0);
  assert((ptr.length() % (size_t)segment_manager->get_block_size()) == 0);
  return repeat_eagain([this, offset, ptr=std::move(ptr)] {
    return seastar::do_with(
      tm->create_transaction(),
      ptr,
      [this, offset](auto &t, auto &ptr) mutable {
	return tm->dec_ref(
	  *t,
	  offset
	).safe_then([](auto){}).handle_error(
	  crimson::ct_error::enoent::handle([](auto) { return seastar::now(); }),
	  crimson::ct_error::pass_further_all{}
	).safe_then([=, &t, &ptr] {
	  logger().debug("dec_ref complete");
	  return tm->alloc_extent<TestBlock>(
	    *t,
	    offset,
	    ptr.length());
	}).safe_then([=, &t, &ptr](auto ext) mutable {
	  assert(ext->get_laddr() == (size_t)offset);
	  assert(ext->get_bptr().length() == ptr.length());
	  ext->get_bptr().swap(ptr);
	  logger().debug("submitting transaction");
	  return tm->submit_transaction(std::move(t));
	});
      });
  }).handle_error(
    crimson::ct_error::assert_all{"store-nbd write"}
  );
}

TMDriver::read_extents_ret TMDriver::read_extents(
  Transaction &t,
  laddr_t offset,
  extent_len_t length)
{
  return seastar::do_with(
    lba_pin_list_t(),
    lextent_list_t<TestBlock>(),
    [this, &t, offset, length](auto &pins, auto &ret) {
      return tm->get_pins(
	t, offset, length
      ).safe_then([this, &t, &pins, &ret](auto _pins) {
	_pins.swap(pins);
	logger().debug("read_extents: mappings {}", pins);
	return crimson::do_for_each(
	  pins.begin(),
	  pins.end(),
	  [this, &t, &ret](auto &&pin) {
	    logger().debug(
	      "read_extents: get_extent {}~{}",
	      pin->get_paddr(),
	      pin->get_length());
	    return tm->pin_to_extent<TestBlock>(
	      t,
	      std::move(pin)
	    ).safe_then([this, &ret](auto ref) mutable {
	      ret.push_back(std::make_pair(ref->get_laddr(), ref));
	      logger().debug(
		"read_extents: got extent {}",
		*ref);
	      return seastar::now();
	    });
	  }).safe_then([&ret] {
	    return std::move(ret);
	  });
      });
    });
}

seastar::future<bufferlist> TMDriver::read(
  off_t offset,
  size_t size)
{
  logger().debug("Reading offset {}", offset);
  assert(offset % segment_manager->get_block_size() == 0);
  assert(size % (size_t)segment_manager->get_block_size() == 0);
  auto blptrret = std::make_unique<bufferlist>();
  auto &blret = *blptrret;
  return repeat_eagain([=, &blret] {
    return seastar::do_with(
      tm->create_transaction(),
      [=, &blret](auto &t) {
	return read_extents(*t, offset, size
	).safe_then([=, &blret](auto ext_list) mutable {
	  size_t cur = offset;
	  for (auto &i: ext_list) {
	    if (cur != i.first) {
	      assert(cur < i.first);
	      blret.append_zero(i.first - cur);
	      cur = i.first;
	    }
	    blret.append(i.second->get_bptr());
	    cur += i.second->get_bptr().length();
	  }
	  if (blret.length() != size) {
	    assert(blret.length() < size);
	    blret.append_zero(size - blret.length());
	  }
	});
      });
  }).handle_error(
    crimson::ct_error::assert_all{"store-nbd read"}
  ).then([blptrret=std::move(blptrret)]() mutable {
    logger().debug("read complete");
    return std::move(*blptrret);
  });
}

void TMDriver::init()
{
  auto segment_cleaner = std::make_unique<SegmentCleaner>(
    SegmentCleaner::config_t::get_default(),
    false /* detailed */);
  auto extent_allocator = std::make_unique<ExtentAllocator>(segment_manager.get());
  segment_cleaner->mount(*extent_allocator);
  auto journal = std::make_unique<SegmentJournal>(*extent_allocator);
  auto cache = std::make_unique<Cache>(*extent_allocator);
  auto lba_manager = lba_manager::create_lba_manager(*extent_allocator, *cache);

  journal->set_segment_provider(&*segment_cleaner);

  tm = std::make_unique<TransactionManager>(
    *extent_allocator,
    std::move(segment_cleaner),
    std::move(journal),
    std::move(cache),
    std::move(lba_manager));
}

void TMDriver::clear()
{
  tm.reset();
}

size_t TMDriver::get_size() const
{
  return segment_manager->get_size() * .5;
}

seastar::future<> TMDriver::mkfs()
{
  assert(config.path);
  segment_manager = std::make_unique<
    segment_manager::block::BlockSegmentManager
    >(*config.path);
  logger().debug("mkfs");
  seastore_meta_t meta;
  meta.seastore_id.generate_random();
  return segment_manager->mkfs(
    std::move(meta)
  ).safe_then([this] {
    logger().debug("");
    return segment_manager->mount();
  }).safe_then([this] {
    init();
    logger().debug("tm mkfs");
    return tm->mkfs();
  }).safe_then([this] {
    logger().debug("tm close");
    return tm->close();
  }).safe_then([this] {
    logger().debug("sm close");
    return segment_manager->close();
  }).safe_then([this] {
    clear();
    logger().debug("mkfs complete");
    return TransactionManager::mkfs_ertr::now();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid errror during TMDriver::mkfs"
    }
  );
}

seastar::future<> TMDriver::mount()
{
  return (config.mkfs ? mkfs() : seastar::now()
  ).then([this] {
    segment_manager = std::make_unique<
      segment_manager::block::BlockSegmentManager
      >(*config.path);
    return segment_manager->mount();
  }).safe_then([this] {
    init();
    return tm->mount();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid errror during TMDriver::mount"
    }
  );
};

seastar::future<> TMDriver::close()
{
  return segment_manager->close(
  ).safe_then([this] {
    return tm->close();
  }).safe_then([this] {
    clear();
    return seastar::now();
  }).handle_error(
    crimson::ct_error::assert_all{
      "Invalid errror during TMDriver::close"
    }
  );
}
