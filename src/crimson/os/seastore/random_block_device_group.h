// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <set>

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/random_block_manager/nvmedevice.h"

namespace crimson::os::seastore {

class RandomBlockDeviceGroup {
public:
  RandomBlockDeviceGroup() {
    rb_devices.resize(DEVICE_ID_GLOBAL_MAX, nullptr);
  }
  const std::set<device_id_t>& get_device_ids() const {
    return device_ids;
  }
  std::vector<nvme_device::NVMeBlockDevice*> get_rb_devices() const {
    std::vector<nvme_device::NVMeBlockDevice*> ret;
    for (auto& device_id : device_ids) {
      auto rb_device = rb_devices[device_id];
      assert(rb_device->get_device_id() == device_id);
      ret.emplace_back(rb_device);
    }
    return ret;
  }
  void add_rbdevice(nvme_device::NVMeBlockDevice* rb) {
    auto device_id = rb->get_device_id();
    ceph_assert(!has_device(device_id));
    rb_devices[device_id] = rb;
    device_ids.insert(device_id);
  }

  bool has_device(device_id_t id) const {
    assert(id <= DEVICE_ID_GLOBAL_MAX);
    return device_ids.count(id) >= 1;
  }

private:
  std::vector<nvme_device::NVMeBlockDevice*> rb_devices;
  std::set<device_id_t> device_ids;
  Journal *cbjournal;
};

using RandomBlockDeviceGroupRef = std::unique_ptr<RandomBlockDeviceGroup>;

} // namespace crimson::os::seastore
