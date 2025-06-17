// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <string>
#include <vector>

//#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "log_node.h"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::logstore_manager{


LogStoreManager::LogStoreManager(
  TransactionManager &tm)
  : tm(tm) {}

LogStoreManager::initialize_lsm_ret
LogStoreManager::initialize_lsm(Transaction &t, laddr_t hint) 
{
  LOG_PREFIX(LogStoreManager::initialize_lsm);
  //DEBUGT("hint: {}", t, hint);
  INFOT("hint: {} init omw omap", t, hint);
  return tm.alloc_non_data_extent<LogStoreNode>(t, hint, LOG_NODE_SIZE
  ).si_then([hint, &t](auto&& root_extent) {
      omap_root_t omap_root;
      omap_root.update(root_extent->get_laddr(), 1, hint, omap_type_t::LOG);
      t.get_omap_tree_stats().extents_num_delta++;
      return initialize_lsm_iertr::make_ready_future<omap_root_t>(omap_root);
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    TransactionManager::alloc_extent_iertr::pass_further{}
  );
}

LogStoreManager::log_set_keys_ret
LogStoreManager::log_set_keys(omap_root_t &omap_root,
  Transaction &t, std::map<std::string, ceph::bufferlist>&& kv) 
{
  log_delta_t log;
  LOG_PREFIX(LogStoreManager::initialize_lsm);
  DEBUGT("enter", t);
#if 0
  LOG_PREFIX(LogStoreManager::initialize_lsm);
  for (auto &p : kv) {
    INFOT(" omw key: {} val size {} ", t, p.first, p.second.length());
  }
#endif
  log.kv = std::move(kv);
  t.add_logs(log);
  return log_set_keys_iertr::now();
}

std::ostream &LogStoreNode::print_detail_l(std::ostream &out) const
{
  //out << ", size=" << get_size()
   // << ", depth=" << get_meta().depth
  out  << ", is_root=" << is_btree_root();
  //if (get_size() > 0) {
    out << ", begin=" << get_begin()
      << ", end=" << get_end();
  //}
  return out;
}

}
