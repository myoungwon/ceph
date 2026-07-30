// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRADOS_VECTOR_PUT_H
#define CEPH_LIBRADOS_VECTOR_PUT_H

#include <memory>
#include <string>

#include "include/buffer.h"
#include "include/object.h"
#include "include/rados/librados.hpp"
#include "include/rados/vector_ops.h"
#include "librados/IoCtxImpl.h"
#include "osdc/Objecter.h"

namespace librados {
namespace vector_internal {

struct IoCtxImplReleaser {
  void operator()(IoCtxImpl *impl) const {
    if (impl != nullptr) {
      impl->put();
    }
  }
};

struct put_op_state_t {
  std::unique_ptr<IoCtxImpl, IoCtxImplReleaser> routed_ioctx;
  ::ObjectOperation op;
  ceph::bufferlist payload;
};

CEPH_RADOS_API int submit_put(IoCtxImpl *ioctx_impl,
                              const object_t& oid,
                              const std::string& locator_key,
                              const std::string& placement_algorithm,
                              const std::string& placement_key,
                              const std::string& vector_hash,
                              ceph::rados::put_vector_request_t req,
                              put_op_state_t *op_state,
                              v14_2_0::AioCompletion *completion);

} // namespace vector_internal
} // namespace librados

#endif
