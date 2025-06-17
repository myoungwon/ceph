// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

//#include <boost/iterator/counting_iterator.hpp>

//#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"



namespace crimson::os::seastore::logstore_manager{
#define LOG_NODE_SIZE 8192

struct LogStoreNode : LogicalChildNode {
  using LogStoreNodeRef = TCachedExtentRef<LogStoreNode>;
  static constexpr extent_types_t TYPE = extent_types_t::LOG_NODE;
  explicit LogStoreNode(ceph::bufferptr &&ptr) : LogicalChildNode(std::move(ptr)) {}
  explicit LogStoreNode(extent_len_t length) : LogicalChildNode(length) {}

  
  std::map<std::string, ceph::bufferlist> embeded_kvs;
  laddr_t next;
  std::string last_log;
  CachedExtentRef duplicate_for_write(Transaction &t) {
    return CachedExtentRef();
  }
  crimson::os::seastore::extent_types_t get_type() const
  {
    return extent_types_t::LOG_NODE;
  }
  ceph::bufferlist get_delta() {
    return bufferlist();
  }
  void apply_delta(const ceph::bufferlist &bl) {
    return;
  }
  std::ostream &print_detail_l(std::ostream &out) const final;
};

class LogStoreManager {
  public:
  using base_iertr = TransactionManager::base_iertr;
  using initialize_lsm_iertr = base_iertr;
  using initialize_lsm_ret = initialize_lsm_iertr::future<omap_root_t>;
  LogStoreManager(TransactionManager &tm);
  initialize_lsm_ret initialize_lsm(Transaction &t, laddr_t hint);
  using log_set_keys_iertr = base_iertr;
  using log_set_keys_ret = log_set_keys_iertr::future<>;
  log_set_keys_ret log_set_keys(omap_root_t &omap_root, Transaction &t,
    std::map<std::string, ceph::bufferlist>&& kv);
  TransactionManager &tm;
};


}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::logstore_manager::LogStoreNode> : fmt::ostream_formatter {};
#endif

