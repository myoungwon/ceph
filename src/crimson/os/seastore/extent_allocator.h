// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/random_block_manager.h"

namespace crimson::os::seastore {

class ExtentAllocator {
public:
  using access_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::permission_denied,
    crimson::ct_error::enoent>;

  using mount_ertr = access_ertr;
  using mount_ret = access_ertr::future<>;
  mount_ret mount() {
    if (segment_manager) {
      return segment_manager->mount();
    } else if (rbm_manager) {
      return rbm_manager->mount();
    } else {
      return crimson::ct_error::input_output_error::make();
    }
  }

  using mkfs_ertr = access_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  mkfs_ret mkfs(seastore_meta_t meta) {
    return segment_manager->mkfs(meta);
  }

  mkfs_ret mkfs(RandomBlockManager::mkfs_config_t config) {
    return rbm_manager->mkfs(config);
  }

  using allocate_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::enospc>;
  allocate_ertr::future<SegmentRef> allocate(segment_id_t id) {
    return segment_manager->open(id);
  }

  allocate_ertr::future<> allocate(
    Transaction &transaction,
    size_t size) {
    return rbm_manager->alloc_extent(transaction, size);
  }

  using release_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent>;
  release_ertr::future<> release(segment_id_t id) {
    return segment_manager->release(id);
  }

  release_ertr::future<> release(
    Transaction &transaction,
    blk_paddr_t start,
    size_t len) {
    return rbm_manager->free_extent(transaction, start, len);
  }

  using complete_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::ebadf,
    crimson::ct_error::enospc,
    crimson::ct_error::erange
    >;
  complete_ertr::future<> complete(Transaction &transaction) {
    return rbm_manager->complete_allocation(transaction);
  }

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) {
    return segment_manager->read(addr, len, out);
  }

  read_ertr::future<> read(blk_paddr_t addr, ceph::bufferptr &out) {
    return rbm_manager->read(addr, out);
  }

  read_ertr::future<ceph::bufferptr> read(paddr_t addr, size_t len) {
    auto ptrref = std::make_unique<ceph::bufferptr>(
      buffer::create_page_aligned(len));
    return read(addr, len, *ptrref).safe_then(
      [ptrref=std::move(ptrref)]() mutable {
	return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
      });
  }

  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::ebadf,
    crimson::ct_error::enospc,
    crimson::ct_error::erange
    >;
  write_ertr::future<> write(uint64_t addr, bufferptr &buf) {
    return rbm_manager->write(addr, buf);
  }

  /* Methods for discovering device geometry */
  size_t get_size() const {
    if (segment_manager) {
      return segment_manager->get_size();
    } else {
      return rbm_manager->get_size();
    }
  }
  size_t get_block_size() const {
    if (segment_manager) {
      return segment_manager->get_block_size();
    } else {
      return rbm_manager->get_block_size();
    }
  }
  size_t get_allocation_unit_size() const {
    if (segment_manager) {
      return segment_manager->get_segment_size();
    } else {
      return rbm_manager->get_block_size();
    }
  }
  segment_id_t get_num_allocation_units() const {
    ceph_assert(get_size() % get_allocation_unit_size() == 0);
    return ((segment_id_t)(get_size() / get_allocation_unit_size()));
  }
  const seastore_meta_t &get_meta() const {
    if (segment_manager) {
      return segment_manager->get_meta();
    } else {
      ceph_abort();
    }
  }

  ExtentAllocator(SegmentManager *segment_manager) :
    segment_manager(segment_manager),
    rbm_manager(nullptr) {
  }

  ExtentAllocator(RandomBlockManager *rbm_manager) :
    segment_manager(nullptr),
    rbm_manager(rbm_manager) {
  }

  ~ExtentAllocator() {
  }

private:
  SegmentManager *segment_manager;
  RandomBlockManager *rbm_manager;
};
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;

}
