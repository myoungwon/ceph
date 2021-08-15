// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <boost/iterator/counting_iterator.hpp>

#include "include/intarith.h"
#include "crimson/os/seastore/circular_bounded_journal.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const CBJournal::cbj_header_t &header)
{
  return out << "cbj_header_t(magin=" << header.magic
	     << ", uuid=" << header.uuid
	     << ", block_size=" << header.block_size 
	     << ", max_size=" << header.max_size 
	     << ", used_size=" << header.used_size 
	     << ", error=" << header.error 
	     << ", start_offset=" << header.start_offset
	     << ", stat_tid=" << header.start_tid 
	     << ", applied_to="<< header.applied_to
	     << ", committed_to="<< header.committed_to
	     << ", written_to=" << header.written_to
	     << ", flsg=" << header.flag 
	     << ", csum_type=" << header.csum_type 
	     << ", csum=" << header.csum
             << ")";
}


CBJournal::CBJournal(NVMeBlockDevice* device, const std::string path)
  : device(device), path(path) {}

/* For test */
CBJournal::mkfs_ret
CBJournal::mkfs(mkfs_config_t& config)
{
  return _open_device(path
  ).safe_then([this, &config]() {
    return read_super(config.start
    ).safe_then([this, &config](auto super) {
      logger().debug(" already exists ");
      return mkfs_ertr::now();
    }).handle_error(
      crimson::ct_error::enoent::handle([this, &config] (auto) {
	ceph::bufferlist bl;
	CBJournal::cbj_header_t head;
	try {
	  head.magic = CBJOURNAL_MAGIC;
	  head.uuid = uuid_d(); // TODO
	  head.block_size = device->block_size();
	  head.max_size = config.end - config.start 
	    - device->block_size();
	  head.used_size = 0;
	  head.error = 0;
	  head.start_offset = device->block_size();
	  head.committed_to = 0;
	  head.written_to = 0;
	  head.applied_to = 0;
	  head.flag = 0;
	  head.csum_type = 0;
	  head.csum = 0;
	  head.cur_segment_id = 0;
	  head.start = conf.start;
	  head.end = conf.end;
	  ::encode(head, bl);
	} catch (ceph::buffer::error &e) {
	  logger().debug("unable to encode super block to underlying deivce");
	  return crimson::ct_error::input_output_error::make();
	}
	logger().debug(
	  "initialize superblock in CBJournal, length {}",
	  bl.length());
	return device_write_bl(start, bl
	).safe_then([this, &head](auto p){
	  header = head; 
	  },
	  mkfs_ertr::pass_further{},
	  crimson::ct_error::all_same_way([](auto e) {
	    logger().debug("overwrite error");
	    ceph_assert(0 == "TODO");
	  })
	);
      }),
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error read_rbm_header in CBJournal::mkfs"
      }
    );
  }).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error open_device in CBJournal::mkfs"
  }).finally([this] {
    if (device) {
      return device->close();
    } else {
      return seastar::now();
    }
  });
}

CBJournal::open_for_write_ertr::future<> CBJournal::_open_device(
        const std::string path)
{
  ceph_assert(device);
  return device->open(path, seastar::open_flags::rw);
}

ceph::bufferlist CBJournal::encode_super()
{
  bufferlist bl;
  ::encode(wal_header, bl);
  return bl;
}

CBJournal::open_for_write_ret CBJournal::open_for_write()
{
  return open_for_write(0);
}

CBJournal::close_ertr::future<> CBJournal::close()
{
  sync_super(
  ).safe_then([this]() {
    return device->close();
  });
}

CBJournal::open_for_write_ret CBJournal::open_for_write(blk_paddr_t start)
{
  return _open_device(path
  ).safe_then([this]() {
    return read_super(start
    ).safe_then([this](auto p) {
      auto &[head, bl] = *p;
      header = head;
      written_to = header.writtent_to;
      committed_to = header.committed_to;
      applied_to = header.applied_to;
      cur_segmend_id = header.cur_segment_id;
      block_size = header.block_size;
      used_size = header.used_size;
      max_size = header.max_size ;
      return open_for_write_ret(
	open_for_write_ertr::ready_future_marker{},
	journal_seq_t{
	  header.cur_segment_id,
	  paddr_t{
	    header.written_to / block_size,
	    static_cast<segment_off_t>(header.written_to)}
	});
    }).handle_error(
      read_entry_ertr::all_same_way([](auto e) {
	logger().debug("read_super error");
	abort();
      })
    );
  }
}

CBJournal::write_block_ertr::future<> CBJournal::append_record(
  ceph::bufferlist bl,
  blk_paddr_t addr)
{
  bufferlist to_write;
  if (addr + bl.length() <= max_size) {
    to_write = bl;
  } else {
    to_write.substr_of(bl, 0, max_size - addr);
  } 
  logger().debug(
    "append_block, offset {}, length {}",
    addr,
    to_write.length());

  auto bptr = bufferptr(ceph::buffer::create_page_aligned(to_write.length()));
  auto iter = to_write.cbegin();
  iter.copy(to_write.length(), bptr.c_str());
  return device->write(addr, bptr 
  ).handle_error(
    write_block_ertr::pass_further{},
    crimson::ct_error::assert_all{ "TODO" }
  ).safe_then([this, laddr, bl=std::move(bl), length=to_write.length()] {
    if (bl.length() == length) {
      // complete
      return write_ertr::now();
    } else {
      auto next = get_start_addr();
      bufferlist next_write;
      next_write.substr_of(bl, length, bl.length() - length);
      auto bp = bufferptr(
	  ceph::buffer::create_page_aligned(next_write.length()));
      auto iter = next_write.cbegin();
      iter.copy(next_write.length(), bp.c_str());
      return device->write(next, bp
      ).handle_error(
	write_block_ertr::pass_further{},
	crimson::ct_error::assert_all{ "TODO" }
      ).safe_then([this, laddr, total_length = bl.length()] {
	return write_ertr::now();
      });
    }
  });
}

CBJournal::submit_record_ret CBJournal::submit_record(
  record_t &&record,
  OrderingHandle &handle) 
{
  assert(write_pipeline);
  auto rsize = get_encoded_record_length(
      record, nvme_manager.get_block_size());
  auto total = rsize.mdlength + rsize.dlength;
  if (total > max_record_length()) {
    auto &logger = crimson::get_logger(ceph_subsys_seastore);
    logger.error(
	"CBJournal::submit_record: record size {} exceeds max {}",
	total,
	max_record_length()
	);
    return crimson::ct_error::erange::make();
  }
  if (total > get_available_size()) {
    return crimson::ct_error::erange::make();
  }

  ceph::bufferlist to_write = encode_record(
    rsize, std::move(record), device->get_block_size(),
    committed_to, 0);
  if (written_to + rsize.mdlength > max_size + get_start_addr()) {
    written_to = get_start_addr();
  }
  auto target = written_to;
  written_to += to_write.length();
  logger().debug(
    "write_record, mdlength {}, dlength {}, target {}",
    rsize.mdlength,
    rsize.dlength,
    target);

  auto write_fut = append_record(bptr, target);
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut = std::move(write_fut)]() mutable {
    return std::move(write_fut
    ).handle_error(
      write_record_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in SegmentJournal::write_record"
      }
    );
  }).safe_then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).safe_then([this, target, segment_id=cur_segment_id+1] {
    logger().debug(
      "write_record: commit target {}",
      target);

    assert(committed_to < target);
    committed_to = target;
    cur_segment_id = cur_segment_id + 1;

    return write_record_ret(
      write_record_ertr::ready_future_marker{},
      paddr_t{
        segment_id,
        target});
  });
}


CBJournal::write_ret CBJournal::device_write_bl(
    blk_paddr_t offset, bufferlist &bl)
{
  auto length = bl.length();
  if (offset + length > max_size) {
    return crimson::ct_error::erange::make();
  }
  logger().debug(
    "overwrite in CBJournal, offset {}, length {}",
    offset,
    length);
  auto bptr = bufferptr(ceph::buffer::create_page_aligned(to_write.length()));
  auto iter = to_write.cbegin();
  iter.copy(to_write.length(), bptr.c_str());
  return device->write(offset, bptr
  ).handle_error(
    write_block_ertr::pass_further{},
    crimson::ct_error::assert_all{ "TODO" }
  ).safe_then([this, offset, length] {
    return write_ret(
      write_block_ertr::ready_future_marker{},
      std::make_pair(offset, length));
  });
}

CBJournal::read_super_ret CBJournal::read_super(blk_paddr_t start)
{
  return device->read(start, block_size
  ).safe_then( [this, start](bufferptr bptr) mutable
    -> read_super_ret {
    logger().debug("read_super: reading {}", start);
    bufferlist bl;
    bl.append(bptr);
    auto bp = bl.cbegin();
    cbj_header_t header;
    try {
      ::decode(header, bp);
    } catch (ceph::buffer::error &e) {
      return read_super_ret(
	read_entry_ertr::ready_future_marker{},
	std::nullopt);
    }
    written_to = header.written_to;
    applied_to = header.applied_to;
    committed_to = header.committed_to;
    return read_super_ret(
      read_entry_ertr::ready_future_marker{},
      std::make_pair(header, bl)
    );
  });
}

bool CBJournal::validate_metadata(const bufferlist &bl)
{
  auto bliter = bl.cbegin();
  auto test_crc = bliter.crc32c(
      ceph::encoded_sizeof_bounded<record_header_t>(),
      -1);
  ceph_le32 recorded_crc_le;
  decode(recorded_crc_le, bliter);
  uint32_t recorded_crc = recorded_crc_le;
  test_crc = bliter.crc32c(
      bliter.get_remaining(),
      test_crc);
  return test_crc == recorded_crc;
}

CBJournal::read_entry_ret CBJournal::return_record(const bufferlist &bl) 
{
  if (validate_metadata(bl)) {
    return read_entry_ret(
      read_entry_ertr::ready_future_marker{},
      std::make_pair(header, std::move(bl)));
  } else {
    return read_record_ret(
      read_record_ertr::ready_future_marker{},
      std::nullopt);
  }
}

CBJournal::read_entry_ret CBJournal::read_record(blk_paddr_t offset)
{
  blk_paddr_t addr = get_start_addr() + offset;
  auto read_length = block_size;
  if (offset + block_size > max_size) {
    addr = get_start_addr();
    read_length = max_size - offset;
  }
  logger().debug("read_record: reading record from addr {} read length {}", 
      addr, read_size);
  return device->read(addr, read_length 
  ).safe_then(
    [this, offset, addr, read_length](bufferptr bptr) mutable
    -> read_entry_ret {
      bufferlist bl;
      bl.append(bptr);
      auto bp = bl.cbegin();
      record_header_t header;
      try {
	::decode(header, bp);
      } catch (ceph::buffer::error &e) {
	return read_record_ret(
	  read_record_ertr::ready_future_marker{},
	  std::nullopt);
      }
      /*
       * |          journal          |
       *        | record 1 header |  | <- data 1
       *  record data 1 (remaining) |
       *
       *        <---- 1 block ----><--
       * -- 2 block --->
       *
       *  If record has logner than read_length and its data is located across
       *  the end of journal and the begining of journal, we need three reads
       *  ---reads of header, other remaining data before the end, and  
       *  the other remaining data from the begining.
       *
       */        
      if (header.mdlength + header.dlength > read_length) { 
	paddr_t next_read_addr = addr + read_length;
	auto next_read = header.total_length - read_length;
	logger().debug(" next_read_addr {}, next_read_length {} ", 
	    next_read_addr, next_read);
	if (max_size < next_read_addr + next_read) {
	  // In this case, need two more reads.
	  // The first is to read remain bytes to the end of cbjournal
	  // The second is to read the data at the begining of cbjournal 
	  next_read = max_size - (offset + read_length);
	}
	logger().debug("read_entry: additional reading addr {} length {}", 
			next_read_addr, 
			next_read);
	return device->read(
	    next_read_addr,
	    next_read
	).safe_then(
	  [this, header=header, bl=std::move(bl)](
	    auto &&bptail) mutable {
	    bl.push_back(bptail);
	    if (header.mdlength + header.dlength = bl.length()) { 
	      logger().debug("read_record: record length {} done", bl.length());
	      return read_record_ret(
		read_record_ertr::ready_future_marker{},
		std::make_pair(header, std::move(bl)));
	    } 
	    // need one more read
	    auto next_read_addr = get_start_addr();
	    return device->read(
	      next_read_addr,
	      header.mdlength + header.dlength - bl.length() 
	    ).safe_then(
	      [header=header, bl=std::move(bl)](
		auto &&bptail) mutable {
		bl.push_back(bptail);
		logger().debug("read_entry: complte size {}", bl.length());
		return return_record(bl);
	      });
	});
      } else {
	return return_record(bl);
      }
    });
}

CBJournal::write_ertr::future<>
CBJournal::sync_super()
{
  header.used_size = used_size;
  header.max_size = max_size;
  header.block_size = max_size;
  header.applied_to = applied_to;
  header.committed_to = committed_to;
  header.written_to = written_to;
  ceph::bufferlist bl;
  try {
    bl = encode_super();
  } catch (ceph::buffer::error &e) {
    logger().debug("unable to encode super block to underlying deivce");
    return crimson::ct_error::input_output_error::make();
  }
  logger().debug(
    "sync header of CBJournal, length {}",
    bl.length());
  return device_write_bl(start, bl
  ).safe_then([this](auto p){},
    create_ertr::pass_further{},
    crimson::ct_error::all_same_way([](auto e) {
      logger().debug("overwrite error");
      ceph_assert(0 == "TODO");
    })
  );
}

}
