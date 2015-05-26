#!/usr/bin/python

# -*- coding: utf-8 -*-

# TODO:
# Ability to set CPU/Memory reservations
import re
try:
    import json
except ImportError:
    import simplejson as json

HAS_PYSPHERE = False
try:
    from pysphere import VIServer, VIProperty, MORTypes
    from pysphere.resources import VimService_services as VI
    from pysphere.vi_task import VITask
    from pysphere import VIException, VIApiException, FaultTypes
    HAS_PYSPHERE = True
except ImportError:
    pass

DOCUMENTATION = '''
---
module: vsphere_guest
short_description: Create/delete/manage a guest VM through VMware vSphere.
description:
     - Create/delete/reconfigure a guest VM through VMware vSphere. This module has a dependency on pysphere >= 1.7
version_added: "1.6"
options:
  vcenter_hostname:
    description:
      - The hostname of the vcenter server the module will connect to, to create the guest.
    required: true
    default: null
    aliases: []
  guest:
    description:
      - The virtual server name you wish to manage.
    required: true
  username:
    description:
      - Username to connect to vcenter as.
    required: true
    default: null
  password:
    description:
      - Password of the user to connect to vcenter as.
    required: true
    default: null
  resource_pool:
    description:
      - The name of the resource_pool to create the VM in.
    required: false
    default: None
  cluster:
    description:
      - The name of the cluster to create the VM in. By default this is derived from the host you tell the module to build the guest on.
    required: false
    default: None
  esxi:
    description:
      - Dictionary which includes datacenter and hostname on which the VM should be created. For standalone ESXi hosts, ha-datacenter should be used as the datacenter name
    required: false
    default: null
  state:
    description:
      - Indicate desired state of the vm.
    default: present
    choices: ['present', 'powered_off', 'absent', 'powered_on', 'restarted', 'reconfigured']
  from_template:
    version_added: "1.9"
    description:
      - Specifies if the VM should be deployed from a template (cannot be ran with state)
    default: no
    choices: ['yes', 'no']
  template_src:
    version_added: "1.9"
    description:
      - Name of the source template to deploy from
    default: None
  vm_disk:
    description:
      - A key, value list of disks and their sizes and which datastore to keep it in.
    required: false
    default: null
  vm_hardware:
    description:
      - A key, value list of VM config settings. Must include ['memory_mb', 'num_cpus', 'osid', 'scsi'].
    required: false
    default: null
  vm_nic:
    description:
      - A key, value list of nics, their types and what network to put them on.
    required: false
    default: null
  vm_extra_config:
    description:
      - A key, value pair of any extra values you want set or changed in the vmx file of the VM. Useful to set advanced options on the VM.
    required: false
    default: null
  vm_hw_version:
    description:
      - Desired hardware version identifier (for example, "vmx-08" for vms that needs to be managed with vSphere Client). Note that changing hardware version of existing vm is not supported.
    required: false
    default: null
    version_added: "1.7"
  vmware_guest_facts:
    description:
      - Gather facts from vCenter on a particular VM
    required: false
    default: null
  force:
    description:
      - Boolean. Allows you to run commands which may alter the running state of a guest. Also used to reconfigure and destroy.
    default: "no"
    choices: [ "yes", "no" ]

notes:
  - This module should run from a system that can access vSphere directly.
    Either by using local_action, or using delegate_to.
author: Richard Hoop <wrhoop@gmail.com>
requirements:
  - "python >= 2.6"
  - pysphere
'''


EXAMPLES = '''
# Create a new VM on an ESX server
# Returns changed = False when the VM already exists
# Returns changed = True and a adds ansible_facts from the new VM
# State will set the power status of a guest upon creation. Use powered_on to create and boot.
# Options ['state', 'vm_extra_config', 'vm_disk', 'vm_nic', 'vm_hardware', 'esxi'] are required together

- vsphere_guest:
    vcenter_hostname: vcenter.mydomain.local
    username: myuser
    password: mypass
    guest: newvm001
    state: powered_on
    vm_extra_config:
      vcpu.hotadd: yes
      mem.hotadd:  yes
      notes: This is a test VM
    vm_disk:
      disk1:
        size_gb: 10
        type: thin
        datastore: storage001
    vm_nic:
      nic1:
        type: vmxnet3
        network: VM Network
        network_type: standard
    vm_hardware:
      memory_mb: 2048
      num_cpus: 2
      osid: centos64Guest
      scsi: paravirtual
    esxi:
      datacenter: MyDatacenter
      hostname: esx001.mydomain.local

# Reconfigure the CPU and Memory on the newly created VM
# Will return the changes made

- vsphere_guest:
    vcenter_hostname: vcenter.mydomain.local
    username: myuser
    password: mypass
    guest: newvm001
    state: reconfigured
    vm_extra_config:
      vcpu.hotadd: yes
      mem.hotadd:  yes
      notes: This is a test VM
    vm_disk:
      disk1:
        size_gb: 10
        type: thin
        datastore: storage001
    vm_nic:
      nic1:
        type: vmxnet3
        network: VM Network
        network_type: standard
    vm_hardware:
      memory_mb: 4096
      num_cpus: 4
      osid: centos64Guest
      scsi: paravirtual
    esxi:
      datacenter: MyDatacenter
      hostname: esx001.mydomain.local

# Deploy a guest from a template
# No reconfiguration of the destination guest is done at this stage, a reconfigure would be needed to adjust memory/cpu etc..
- vsphere_guest:
    vcenter_hostname: vcenter.mydomain.local
    username: myuser
    password: mypass
    guest: newvm001
    from_template: yes
    template_src: centosTemplate
    cluster: MainCluster
    resource_pool: "/Resources"

# Task to gather facts from a vSphere cluster only if the system is a VMWare guest

- vsphere_guest:
    vcenter_hostname: vcenter.mydomain.local
    username: myuser
    password: mypass
    guest: newvm001
    vmware_guest_facts: yes


# Typical output of a vsphere_facts run on a guest
# If vmware tools is not installed, ipadresses with return None

- hw_eth0:
  - addresstype: "assigned"
    label: "Network adapter 1"
    macaddress: "00:22:33:33:44:55"
    macaddress_dash: "00-22-33-33-44-55"
    ipaddresses: ['192.0.2.100', '2001:DB8:56ff:feac:4d8a']
    summary: "VM Network"
  hw_guest_full_name: "newvm001"
  hw_guest_id: "rhel6_64Guest"
  hw_memtotal_mb: 2048
  hw_name: "centos64Guest"
  hw_processor_count: 2
  hw_product_uuid: "ef50bac8-2845-40ff-81d9-675315501dac"

# Remove a vm from vSphere
# The VM must be powered_off or you need to use force to force a shutdown

- vsphere_guest:
    vcenter_hostname: vcenter.mydomain.local
    username: myuser
    password: mypass
    guest: newvm001
    state: absent
    force: yes
'''


def add_scsi_controller(module, s, config, devices, type="paravirtual", bus_num=0, disk_ctrl_key=1):
    # add a scsi controller
    scsi_ctrl_spec = config.new_deviceChange()
    scsi_ctrl_spec.set_element_operation('add')

    if type == "lsi":
        # For RHEL5
        scsi_ctrl = VI.ns0.VirtualLsiLogicController_Def("scsi_ctrl").pyclass()
    elif type == "paravirtual":
        # For RHEL6
        scsi_ctrl = VI.ns0.ParaVirtualSCSIController_Def("scsi_ctrl").pyclass()
    elif type == "lsi_sas":
        scsi_ctrl = VI.ns0.VirtualLsiLogicSASController_Def(
            "scsi_ctrl").pyclass()
    elif type == "bus_logic":
        scsi_ctrl = VI.ns0.VirtualBusLogicController_Def("scsi_ctrl").pyclass()
    else:
        s.disconnect()
        module.fail_json(
            msg="Error adding scsi controller to vm spec. No scsi controller"
            " type of: %s" % (type))

    scsi_ctrl.set_element_busNumber(int(bus_num))
    scsi_ctrl.set_element_key(int(disk_ctrl_key))
    scsi_ctrl.set_element_sharedBus("noSharing")
    scsi_ctrl_spec.set_element_device(scsi_ctrl)
    # Add the scsi controller to the VM spec.
    devices.append(scsi_ctrl_spec)
    return disk_ctrl_key


def add_disk(module, s, config_target, config, devices, datastore, type="thin", size=200000, disk_ctrl_key=1, disk_number=0, key=0):
    # add a vmdk disk
    # Verify the datastore exists
    datastore_name, ds = find_datastore(module, s, datastore, config_target)
    # create a new disk - file based - for the vm
    disk_spec = config.new_deviceChange()
    disk_spec.set_element_fileOperation("create")
    disk_spec.set_element_operation("add")
    disk_ctlr = VI.ns0.VirtualDisk_Def("disk_ctlr").pyclass()
    disk_backing = VI.ns0.VirtualDiskFlatVer2BackingInfo_Def(
        "disk_backing").pyclass()
    disk_backing.set_element_fileName(datastore_name)
    disk_backing.set_element_diskMode("persistent")
    if type != "thick":
        disk_backing.set_element_thinProvisioned(1)
    disk_ctlr.set_element_key(key)
    disk_ctlr.set_element_controllerKey(int(disk_ctrl_key))
    disk_ctlr.set_element_unitNumber(int(disk_number))
    disk_ctlr.set_element_backing(disk_backing)
    disk_ctlr.set_element_capacityInKB(int(size))
    disk_spec.set_element_device(disk_ctlr)
    devices.append(disk_spec)


def add_cdrom(module, s, config_target, config, devices, default_devs, type="client", vm_cd_iso_path=None):
    # Add a cd-rom
    # Make sure the datastore exists.
    if vm_cd_iso_path:
        iso_location = vm_cd_iso_path.split('/', 1)
        datastore, ds = find_datastore(
            module, s, iso_location[0], config_target)
        iso_path = iso_location[1]

    # find ide controller
    ide_ctlr = None
    for dev in default_devs:
        if dev.typecode.type[1] == "VirtualIDEController":
            ide_ctlr = dev

    # add a cdrom based on a physical device
    if ide_ctlr:
        cd_spec = config.new_deviceChange()
        cd_spec.set_element_operation('add')
        cd_ctrl = VI.ns0.VirtualCdrom_Def("cd_ctrl").pyclass()

        if type == "iso":
            iso = VI.ns0.VirtualCdromIsoBackingInfo_Def("iso").pyclass()
            ds_ref = iso.new_datastore(ds)
            ds_ref.set_attribute_type(ds.get_attribute_type())
            iso.set_element_datastore(ds_ref)
            iso.set_element_fileName("%s %s" % (datastore, iso_path))
            cd_ctrl.set_element_backing(iso)
            cd_ctrl.set_element_key(20)
            cd_ctrl.set_element_controllerKey(ide_ctlr.get_element_key())
            cd_ctrl.set_element_unitNumber(0)
            cd_spec.set_element_device(cd_ctrl)
        elif type == "client":
            client = VI.ns0.VirtualCdromRemoteAtapiBackingInfo_Def(
                "client").pyclass()
            client.set_element_deviceName("")
            cd_ctrl.set_element_backing(client)
            cd_ctrl.set_element_key(20)
            cd_ctrl.set_element_controllerKey(ide_ctlr.get_element_key())
            cd_ctrl.set_element_unitNumber(0)
            cd_spec.set_element_device(cd_ctrl)
        else:
            s.disconnect()
            module.fail_json(
                msg="Error adding cdrom of type %s to vm spec. "
                " cdrom type can either be iso or client" % (type))

        devices.append(cd_spec)


def add_nic(module, s, nfmor, config, devices, nic_type="vmxnet3", network_name="VM Network", network_type="standard"):
    # add a NIC
    # Different network card types are: "VirtualE1000",
    # "VirtualE1000e","VirtualPCNet32", "VirtualVmxnet", "VirtualNmxnet2",
    # "VirtualVmxnet3"
    nic_spec = config.new_deviceChange()
    nic_spec.set_element_operation("add")

    if nic_type == "e1000":
        nic_ctlr = VI.ns0.VirtualE1000_Def("nic_ctlr").pyclass()
    elif nic_type == "e1000e":
        nic_ctlr = VI.ns0.VirtualE1000e_Def("nic_ctlr").pyclass()
    elif nic_type == "pcnet32":
        nic_ctlr = VI.ns0.VirtualPCNet32_Def("nic_ctlr").pyclass()
    elif nic_type == "vmxnet":
        nic_ctlr = VI.ns0.VirtualVmxnet_Def("nic_ctlr").pyclass()
    elif nic_type == "vmxnet2":
        nic_ctlr = VI.ns0.VirtualVmxnet2_Def("nic_ctlr").pyclass()
    elif nic_type == "vmxnet3":
        nic_ctlr = VI.ns0.VirtualVmxnet3_Def("nic_ctlr").pyclass()
    else:
        s.disconnect()
        module.fail_json(
            msg="Error adding nic to vm spec. No nic type of: %s" %
            (nic_type))

    if network_type == "standard":
        nic_backing = VI.ns0.VirtualEthernetCardNetworkBackingInfo_Def(
            "nic_backing").pyclass()
        nic_backing.set_element_deviceName(network_name)
    elif network_type == "dvs":
        # Get the portgroup key
        portgroupKey = find_portgroup_key(module, s, nfmor, network_name)
        # Get the dvswitch uuid
        dvswitch_uuid = find_dvswitch_uuid(module, s, nfmor, portgroupKey)

        nic_backing_port = VI.ns0.DistributedVirtualSwitchPortConnection_Def(
            "nic_backing_port").pyclass()
        nic_backing_port.set_element_switchUuid(dvswitch_uuid)
        nic_backing_port.set_element_portgroupKey(portgroupKey)

        nic_backing = VI.ns0.VirtualEthernetCardDistributedVirtualPortBackingInfo_Def(
            "nic_backing").pyclass()
        nic_backing.set_element_port(nic_backing_port)
    else:
        s.disconnect()
        module.fail_json(
            msg="Error adding nic backing to vm spec. No network type of:"
            " %s" % (network_type))

    nic_ctlr.set_element_addressType("generated")
    nic_ctlr.set_element_backing(nic_backing)
    nic_ctlr.set_element_key(4)
    nic_spec.set_element_device(nic_ctlr)
    devices.append(nic_spec)


def find_datastore(module, s, datastore, config_target):
    # Verify the datastore exists and put it in brackets if it does.
    ds = None
    for d in config_target.Datastore:
        if (d.Datastore.Accessible and
            (datastore and d.Datastore.Name == datastore)
                or (not datastore)):
            ds = d.Datastore.Datastore
            datastore = d.Datastore.Name
            break
    if not ds:
        s.disconnect()
        module.fail_json(msg="Datastore: %s does not appear to exist" %
                         (datastore))

    datastore_name = "[%s]" % datastore
    return datastore_name, ds


def find_portgroup_key(module, s, nfmor, network_name):
    # Find a portgroups key given the portgroup name.

    # Grab all the distributed virtual portgroup's names and key's.
    dvpg_mors = s._retrieve_properties_traversal(
        property_names=['name', 'key'],
        from_node=nfmor, obj_type='DistributedVirtualPortgroup')

    # Get the correct portgroup managed object.
    dvpg_mor = None
    for dvpg in dvpg_mors:
        if dvpg_mor:
            break
        for p in dvpg.PropSet:
            if p.Name == "name" and p.Val == network_name:
                dvpg_mor = dvpg
            if dvpg_mor:
                break

    # If dvpg_mor is empty we didn't find the named portgroup.
    if dvpg_mor is None:
        s.disconnect()
        module.fail_json(
            msg="Could not find the distributed virtual portgroup named"
            " %s" % network_name)

    # Get the portgroup key
    portgroupKey = None
    for p in dvpg_mor.PropSet:
        if p.Name == "key":
            portgroupKey = p.Val

    return portgroupKey


def find_dvswitch_uuid(module, s, nfmor, portgroupKey):
    # Find a dvswitch's uuid given a portgroup key.
    # Function searches all dvswitches in the datacenter to find the switch
    # that has the portgroup key.

    # Grab the dvswitch uuid and portgroup properties
    dvswitch_mors = s._retrieve_properties_traversal(
        property_names=['uuid', 'portgroup'],
        from_node=nfmor, obj_type='DistributedVirtualSwitch')

    dvswitch_mor = None
    # Get the dvswitches managed object
    for dvswitch in dvswitch_mors:
        if dvswitch_mor:
            break
        for p in dvswitch.PropSet:
            if p.Name == "portgroup":
                pg_mors = p.Val.ManagedObjectReference
                for pg_mor in pg_mors:
                    if dvswitch_mor:
                        break
                    key_mor = s._get_object_properties(
                        pg_mor, property_names=['key'])
                    for key in key_mor.PropSet:
                        if key.Val == portgroupKey:
                            dvswitch_mor = dvswitch

    # Get the switches uuid
    dvswitch_uuid = None
    for p in dvswitch_mor.PropSet:
        if p.Name == "uuid":
            dvswitch_uuid = p.Val

    return dvswitch_uuid


def spec_singleton(spec, request, vm):

    if not spec:
        _this = request.new__this(vm._mor)
        _this.set_attribute_type(vm._mor.get_attribute_type())
        request.set_element__this(_this)
        spec = request.new_spec()
    return spec


def vmdisk_id(vm, current_datastore_name):
    id_list = []
    for vm_disk in vm._disks:
        if current_datastore_name in vm_disk['descriptor']:
            id_list.append(vm_disk['device']['key'])
    return id_list


def deploy_template(vsphere_client, guest, resource_pool, template_src, esxi, module, cluster_name):
    vmTemplate = vsphere_client.get_vm_by_name(template_src)
    vmTarget = None

    try:
        cluster = [k for k,
                   v in vsphere_client.get_clusters().items() if v == cluster_name][0]
    except IndexError, e:
        vsphere_client.disconnect()
        module.fail_json(msg="Cannot find Cluster named: %s" %
                         cluster_name)

    try:
        rpmor = [k for k, v in vsphere_client.get_resource_pools(
            from_mor=cluster).items()
            if v == resource_pool][0]
    except IndexError, e:
        vsphere_client.disconnect()
        module.fail_json(msg="Cannot find Resource Pool named: %s" %
                         resource_pool)

    try:
        vmTarget = vsphere_client.get_vm_by_name(guest)
    except Exception:
        pass
    if not vmTemplate.properties.config.template:
        module.fail_json(
            msg="Target %s is not a registered template" % template_src
        )
    try:
        if vmTarget:
            changed = False
        else:
            vmTemplate.clone(guest, resourcepool=rpmor)
            changed = True
        vsphere_client.disconnect()
        module.exit_json(changed=changed)
    except Exception as e:
        module.fail_json(
            msg="Could not clone selected machine: %s" % e
        )


def reconfigure_vm(vsphere_client, vm, module, esxi, resource_pool, cluster_name, guest, vm_extra_config, vm_hardware, vm_disk, vm_nic, state, force):
    spec = None
    changed = False
    changes = {}
    request = VI.ReconfigVM_TaskRequestMsg()
    shutdown = False

    memoryHotAddEnabled = bool(vm.properties.config.memoryHotAddEnabled)
    cpuHotAddEnabled = bool(vm.properties.config.cpuHotAddEnabled)
    cpuHotRemoveEnabled = bool(vm.properties.config.cpuHotRemoveEnabled)

    # Change Memory
    if 'memory_mb' in vm_hardware:

        if int(vm_hardware['memory_mb']) != vm.properties.config.hardware.memoryMB:
            spec = spec_singleton(spec, request, vm)

            if vm.is_powered_on():
                if force:
                    # No hot add but force
                    if not memoryHotAddEnabled:
                        shutdown = True
                    elif int(vm_hardware['memory_mb']) < vm.properties.config.hardware.memoryMB:
                        shutdown = True
                else:
                    # Fail on no hot add and no force
                    if not memoryHotAddEnabled:
                        module.fail_json(
                            msg="memoryHotAdd is not enabled. force is "
                            "required for shutdown")

                    # Fail on no force and memory shrink
                    elif int(vm_hardware['memory_mb']) < vm.properties.config.hardware.memoryMB:
                        module.fail_json(
                            msg="Cannot lower memory on a live VM. force is "
                            "required for shutdown")

            # set the new RAM size
            spec.set_element_memoryMB(int(vm_hardware['memory_mb']))
            changes['memory'] = vm_hardware['memory_mb']

    # ====( Config Memory )====#
    if 'num_cpus' in vm_hardware:
        if int(vm_hardware['num_cpus']) != vm.properties.config.hardware.numCPU:
            spec = spec_singleton(spec, request, vm)

            if vm.is_powered_on():
                if force:
                    # No hot add but force
                    if not cpuHotAddEnabled:
                        shutdown = True
                    elif int(vm_hardware['num_cpus']) < vm.properties.config.hardware.numCPU:
                        if not cpuHotRemoveEnabled:
                            shutdown = True
                else:
                    # Fail on no hot add and no force
                    if not cpuHotAddEnabled:
                        module.fail_json(
                            msg="cpuHotAdd is not enabled. force is "
                            "required for shutdown")

                    # Fail on no force and cpu shrink without hot remove
                    elif int(vm_hardware['num_cpus']) < vm.properties.config.hardware.numCPU:
                        if not cpuHotRemoveEnabled:
                            module.fail_json(
                                msg="Cannot lower CPU on a live VM without "
                                "cpuHotRemove. force is required for shutdown")

            spec.set_element_numCPUs(int(vm_hardware['num_cpus']))

            changes['cpu'] = vm_hardware['num_cpus']

    # Update Disk set
    if vm_disk:
        spec = spec_singleton(spec, request, vm)
        spec, changes = _update_disk_set(vm, vm_disk, spec, module, vsphere_client, vm_hardware)

    if len(changes):
        # Exiting with changes array to see what we are trying to do
        # module.fail_json(msg=json.dumps(changes))
        if shutdown and vm.is_powered_on():
            try:
                vm.power_off(sync_run=True)
                vm.get_status()

            except Exception, e:
                module.fail_json(
                    msg='Failed to shutdown vm %s: %s' % (guest, e)
                )

        request.set_element_spec(spec)
        ret = vsphere_client._proxy.ReconfigVM_Task(request)._returnval

        # Wait for the task to finish
        task = VITask(ret, vsphere_client)
        status = task.wait_for_state([task.STATE_SUCCESS, task.STATE_ERROR])
        if status == task.STATE_SUCCESS:
            changed = True
        elif status == task.STATE_ERROR:
            module.fail_json(
                msg="Error reconfiguring vm: %s" % task.get_error_message())

        if vm.is_powered_off():
            try:
                vm.power_on(sync_run=True)
            except Exception, e:
                module.fail_json(
                    msg='Failed to power on vm %s : %s' % (guest, e)
                )

    vsphere_client.disconnect()
    if changed:
        module.exit_json(changed=True, changes=changes)

    module.exit_json(changed=False)


def _get_defined_disk_info(disk, vm_disk):
    datastore = None
    disk_size = None
    disk_type = None

    try:
        datastore = vm_disk[disk]['datastore']
    except KeyError:
        vsphere_client.disconnect()
        module.fail_json(
            msg="Error on %s definition. datastore needs to be"
                " specified." % disk)
    try:
        disk_size = int(vm_disk[disk]['size_gb'])
        disk_size = disk_size
    except (KeyError, ValueError):
        vsphere_client.disconnect()
        module.fail_json(msg="Error on %s definition. size needs to be specified as an integer." % disk)
    try:
        disk_type = vm_disk[disk]['type']
    except KeyError:
        vsphere_client.disconnect()
        module.fail_json(
            msg="Error on %s definition. type needs to be"
                " specified." % disk)

    return {"datastore": datastore,
            "size_gb": disk_size,
            "type": disk_type,
            "label": disk}


def _get_vm_disk_info(disk):
    datastore = None

    try:
        disk_full_name = disk['files'][0]["name"]
        rgx = re.compile("\[(.*)\]")
        match = rgx.match(disk_full_name)
        if match:
            datastore = match.group(1)
    except KeyError:
        module.fail_json(
            msg="Error getting information from vCenter for disk %s." % disk.label
        )

    disk_size = disk["capacity"]
    disk_label = disk["label"]
    disk_key = disk["device"]["key"]
    disk_unit = disk["device"]["unitNumber"]

    return {"datastore": datastore,
            "size_gb": disk_size / 1024 / 1024,
            "label": disk_label,
            "controller_key": disk_key,
            "unit_number": disk_unit}


def _map_disk_sets(set1, set2):
    labels1 = sorted([_index_label_map(ind, l["label"]) for ind, l in enumerate(set1)])
    labels2 = sorted([_index_label_map(ind, l["label"]) for ind, l in enumerate(set2)])
    return labels1, labels2


def _disk_labels_match(label1, label2):
    return _number_from_disk_label(label1) == _number_from_disk_label(label2)


def _index_label_map(index, label):
    '''
    Returns Tuple(index, diskNumber, label)
    '''
    return index, _number_from_disk_label(label), label


def _number_from_disk_label(label):
    rgx = re.compile("disk\s?(\d+)")
    num = rgx.search(label)
    if num:
        return num.group(1)
    else:
        return None


def _update_disk_set(vm, desired_disks, spec, module, vsphere_client, vm_hardware):
    current_disks = [dsk for dsk in vm._disks]
    vm_disk_details = [_get_vm_disk_info(v_dsk) for v_dsk in current_disks]
    desired_disk_details = [_get_defined_disk_info(def_disk, desired_disks) for def_disk in
                            sorted(desired_disks.iterkeys())]

    hfmor = _get_dc_mor_from_vm(vm).hostFolder._obj
    hostmor = vm.properties.runtime.host._obj
    crmor = _get_host_compute_resource_mor(vsphere_client, hostmor, hfmor)
    crprops = VIProperty(vsphere_client, crmor)

    config_target = _get_config_target(crprops, hostmor, vsphere_client)
    return _compare_disk_sets(vm_disk_details, desired_disk_details, spec, config_target,  module, vsphere_client, vm_hardware)


def _tuple_disks_lookup(tuple_list, disk_list, disk_nums):
    results = [[disk_list[t[0]] for t in tuple_list if t[1] == d][0] for d in disk_nums]
    if results is None:
        return []
    else:
        return results


def _tuple_disk_lookup(tuple_list, disk_list, disk_num):
    results = [disk_list[t[0]] for t in tuple_list if t[1] == disk_num]
    if len(results) > 0:
        return results[0]
    else:
        return None


def _compare_disks(vm_disk, desired_disk, spec, changes):
    changed = False
    dc = spec.new_deviceChange()
    dc.Operation = "edit"
    hd = VI.ns0.VirtualDisk_Def("hd").pyclass()
    hd.Key = -100
    hd.UnitNumber = vm_disk["unit_number"]
    hd.ControllerKey = vm_disk["controller_key"]

    if not _disk_labels_match(vm_disk["label"], desired_disk["label"]):
        return spec, {}
    if vm_disk["size_gb"] != desired_disk["size_gb"]:
        changes[vm_disk["label"]] = {"size_gb": desired_disk["size_gb"]}
        changed = True
        hd.CapacityInKB = desired_disk["size_gb"] * 1024 * 1024
    # Just not going to support moving datastores right now
    # if vm_disk["datastore"] != desired_disk["datastore"]:
    #     if changed:
    #         changes[vm_disk["label"]]["datastore"] = desired_disk["datastore"]
    #     else:
    #         changes[vm_disk["label"]] = {"datastore": desired_disk["datastore"]}
    if changed:
        dc.Device = hd
        if hasattr(spec, "DeviceChange"):
            spec.DeviceChange.push(dc)
        else:
            spec.DeviceChange = [dc]

    return spec, changes


def _convert_disk_list_to_dict(disks):
    disk_dict = {}
    for disk in range(len(disks)):
        disk_dict[disks[disk]["label"]] = disks[disk]
    return disk_dict


def _compare_disk_sets(vm_disks, desired_disks, spec, config_target, module, vsphere_client, vm_hardware):
    desired_count = len(desired_disks)
    changes = {}
    if desired_count < 1:
        module.fail_json(
            msg="Error. You must have at least one disk. Remove vm_disk if you don't want to change your configuration."
        )
    labels1, labels2 = _map_disk_sets(vm_disks, desired_disks)
    label1_set = set([x[1] for x in labels1])
    label2_set = set([y[1] for y in labels2])
    compare_labels = label1_set.intersection(label2_set)
    new_labels = label2_set - compare_labels
    new_disk_list = _tuple_disks_lookup(labels2, desired_disks, new_labels)
    compare_disk_list = zip(_tuple_disks_lookup(labels1, vm_disks, compare_labels),
                            _tuple_disks_lookup(labels2, desired_disks, compare_labels))

    for disk_tuple in compare_disk_list:
        spec, changes = _compare_disks(disk_tuple[0], disk_tuple[1], spec, changes)

    new_disk_dict = _convert_disk_list_to_dict(new_disk_list)
    changes.update(new_disk_dict)

    next_unit = vm_disks[-1]["unit_number"] + 1
    devices = []
    _add_vm_disks(module,
                  vsphere_client,
                  spec,
                  config_target,
                  devices,
                  vm_hardware,
                  new_disk_dict,
                  next_unit)
    if hasattr(spec, "DeviceChange"):
        spec.DeviceChange += devices
    else:
        spec.DeviceChange = devices

    return spec, changes


def _get_config_target(crprops, hostmor, vsphere_client):
    # get config target
    request = VI.QueryConfigTargetRequestMsg()
    _this = request.new__this(crprops.environmentBrowser._obj)
    _this.set_attribute_type(
        crprops.environmentBrowser._obj.get_attribute_type())
    request.set_element__this(_this)
    h = request.new_host(hostmor)
    h.set_attribute_type(hostmor.get_attribute_type())
    request.set_element_host(h)
    config_target = vsphere_client._proxy.QueryConfigTarget(request)._returnval
    return config_target


def _get_dc_mor(datacenter):
    dclist = [k for k,
             v in vsphere_client.get_datacenters().items() if v == datacenter]
    if dclist:
        dcmor=dclist[0]
        return dcmor
    else:
        vsphere_client.disconnect()
        module.fail_json(msg="Cannot find datacenter named: %s" % datacenter)
        return None


def _get_esx_host_mor(esxi_hostname):
    try:
        hostmor = [k for k,
                   v in vsphere_client.get_hosts().items() if v == esxi_hostname][0]
    except IndexError, e:
        vsphere_client.disconnect()
        module.fail_json(msg="Cannot find esx host named: %s" % esxi_hostname)

    return hostmor


def _get_host_compute_resource_mor(vsphere_client, hostmor, hfmor):
    # Grab the computerResource name and host properties
    crmors = vsphere_client._retrieve_properties_traversal(
        property_names=['name', 'host'],
        from_node=hfmor,
        obj_type='ComputeResource')
    crmor = None
    for cr in crmors:
        for p in cr.PropSet:
            if p.Name == "host":
                for h in p.Val.get_element_ManagedObjectReference():
                    if h == hostmor:
                        return cr.Obj
                if crmor:
                    break
    return crmor


def _get_dc_mor_from_vm(vm):
    parent = vm.properties.parent
    parent_name = parent.name
    current_mor = None
    while parent_name != "Datacenters":
        current_mor = parent
        if hasattr(parent, "parent"):
            parent = parent.parent
            parent_name = parent.name
        else:
            break
    return current_mor


def create_vm(vsphere_client, module, esxi, resource_pool, cluster_name, guest, vm_extra_config, vm_hardware, vm_disk, vm_nic, vm_hw_version, state):
    datacenter = esxi['datacenter']
    esxi_hostname = esxi['hostname']
    # Datacenter managed object reference
    dcmor = _get_dc_mor(datacenter)

    dcprops = VIProperty(vsphere_client, dcmor)

    # hostFolder managed reference
    hfmor = dcprops.hostFolder._obj

    # virtualmachineFolder managed object reference
    if vm_extra_config.get('folder'):
        if vm_extra_config['folder'] not in vsphere_client._get_managed_objects(MORTypes.Folder).values():
            vsphere_client.disconnect()
            module.fail_json(msg="Cannot find folder named: %s" % vm_extra_config['folder'])

        for mor, name in vsphere_client._get_managed_objects(MORTypes.Folder).iteritems():
            if name == vm_extra_config['folder']:
                vmfmor = mor
    else:
        vmfmor = dcprops.vmFolder._obj

    # networkFolder managed object reference
    nfmor = dcprops.networkFolder._obj

    # Grab the host managed object reference of the esxi_hostname
    hostmor = _get_esx_host(esxi_hostname)

    # Grab the computerResource managed object reference of the host we are
    # creating the VM on.
    crmor = _get_host_compute_resource_mor(vsphere_client, hostmor, hfmor)
    crprops = VIProperty(vsphere_client, crmor)

    # Get resource pool managed reference
    # Requires that a cluster name be specified.
    if resource_pool:
        try:
            cluster = [k for k,
                       v in vsphere_client.get_clusters().items() if v == cluster_name][0]
        except IndexError, e:
            vsphere_client.disconnect()
            module.fail_json(msg="Cannot find Cluster named: %s" %
                             cluster_name)

        try:
            rpmor = [k for k, v in vsphere_client.get_resource_pools(
                from_mor=cluster).items()
                if v == resource_pool][0]
        except IndexError, e:
            vsphere_client.disconnect()
            module.fail_json(msg="Cannot find Resource Pool named: %s" %
                             resource_pool)

    else:
        rpmor = crprops.resourcePool._obj

    # CREATE VM CONFIGURATION
    config_target = _get_config_target(crprops, hostmor, vsphere_client)

    # get default devices
    request = VI.QueryConfigOptionRequestMsg()
    _this = request.new__this(crprops.environmentBrowser._obj)
    _this.set_attribute_type(
        crprops.environmentBrowser._obj.get_attribute_type())
    request.set_element__this(_this)
    h = request.new_host(hostmor)
    h.set_attribute_type(hostmor.get_attribute_type())
    request.set_element_host(h)
    config_option = vsphere_client._proxy.QueryConfigOption(request)._returnval
    default_devs = config_option.DefaultDevice

    # add parameters to the create vm task
    create_vm_request = VI.CreateVM_TaskRequestMsg()
    config = create_vm_request.new_config()
    if vm_hw_version:
        config.set_element_version(vm_hw_version)
    vmfiles = config.new_files()
    datastore_name, ds = find_datastore(
        module, vsphere_client, vm_disk['disk1']['datastore'], config_target)
    vmfiles.set_element_vmPathName(datastore_name)
    config.set_element_files(vmfiles)
    config.set_element_name(guest)
    if 'notes' in vm_extra_config:
        config.set_element_annotation(vm_extra_config['notes'])
    config.set_element_memoryMB(int(vm_hardware['memory_mb']))
    config.set_element_numCPUs(int(vm_hardware['num_cpus']))
    config.set_element_guestId(vm_hardware['osid'])
    devices = []

    if vm_disk:
        _add_vm_disks(module, vsphere_client, config, config_target, devices, vm_hardware, vm_disk)

    if 'vm_cdrom' in vm_hardware:
        cdrom_iso_path = None
        cdrom_type = None
        try:
            cdrom_type = vm_hardware['vm_cdrom']['type']
        except KeyError:
            vsphere_client.disconnect()
            module.fail_json(
                msg="Error on %s definition. cdrom type needs to be"
                " specified." % vm_hardware['vm_cdrom'])
        if cdrom_type == 'iso':
            try:
                cdrom_iso_path = vm_hardware['vm_cdrom']['iso_path']
            except KeyError:
                vsphere_client.disconnect()
                module.fail_json(
                    msg="Error on %s definition. cdrom iso_path needs"
                    " to be specified." % vm_hardware['vm_cdrom'])
        # Add a CD-ROM device to the VM.
        add_cdrom(module, vsphere_client, config_target, config, devices,
                  default_devs, cdrom_type, cdrom_iso_path)
    if vm_nic:
        for nic in sorted(vm_nic.iterkeys()):
            try:
                nictype = vm_nic[nic]['type']
            except KeyError:
                vsphere_client.disconnect()
                module.fail_json(
                    msg="Error on %s definition. type needs to be "
                    " specified." % nic)
            try:
                network = vm_nic[nic]['network']
            except KeyError:
                vsphere_client.disconnect()
                module.fail_json(
                    msg="Error on %s definition. network needs to be "
                    " specified." % nic)
            try:
                network_type = vm_nic[nic]['network_type']
            except KeyError:
                vsphere_client.disconnect()
                module.fail_json(
                    msg="Error on %s definition. network_type needs to be "
                    " specified." % nic)
            # Add the nic to the VM spec.
            add_nic(module, vsphere_client, nfmor, config, devices,
                    nictype, network, network_type)

    config.set_element_deviceChange(devices)
    create_vm_request.set_element_config(config)
    folder_mor = create_vm_request.new__this(vmfmor)
    folder_mor.set_attribute_type(vmfmor.get_attribute_type())
    create_vm_request.set_element__this(folder_mor)
    rp_mor = create_vm_request.new_pool(rpmor)
    rp_mor.set_attribute_type(rpmor.get_attribute_type())
    create_vm_request.set_element_pool(rp_mor)
    host_mor = create_vm_request.new_host(hostmor)
    host_mor.set_attribute_type(hostmor.get_attribute_type())
    create_vm_request.set_element_host(host_mor)

    # CREATE THE VM
    taskmor = vsphere_client._proxy.CreateVM_Task(create_vm_request)._returnval
    task = VITask(taskmor, vsphere_client)
    task.wait_for_state([task.STATE_SUCCESS, task.STATE_ERROR])
    if task.get_state() == task.STATE_ERROR:
        vsphere_client.disconnect()
        module.fail_json(msg="Error creating vm: %s" %
                         task.get_error_message())
    else:
        # We always need to get the vm because we are going to gather facts
        vm = vsphere_client.get_vm_by_name(guest)

        # VM was created. If there is any extra config options specified, set
        # them here , disconnect from vcenter, then exit.
        if vm_extra_config:
            vm.set_extra_config(vm_extra_config)

        # Power on the VM if it was requested
        power_state(vm, state, True)

        vsphere_client.disconnect()
        module.exit_json(
            ansible_facts=gather_facts(vm),
            changed=True,
            changes="Created VM %s" % guest)


def _add_vm_disks(module, vsphere_client, config, config_target, devices, vm_hardware, vm_disk, start_key=0):
    disk_ctrl_key = add_scsi_controller(
        module, vsphere_client, config, devices, vm_hardware['scsi'], start_key)
    disk_num = start_key
    disk_key = start_key
    for disk in sorted(vm_disk.iterkeys()):
        try:
            datastore = vm_disk[disk]['datastore']
        except KeyError:
            vsphere_client.disconnect()
            module.fail_json(
                msg="Error on %s definition. datastore needs to be"
                " specified." % disk)
        try:
            disksize = int(vm_disk[disk]['size_gb'])
            # Convert the disk size to kiloboytes
            disksize = disksize * 1024 * 1024
        except (KeyError, ValueError):
            vsphere_client.disconnect()
            module.fail_json(msg="Error on %s definition. size needs to be specified as an integer." % disk)
        try:
            disktype = vm_disk[disk]['type']
        except KeyError:
            vsphere_client.disconnect()
            module.fail_json(
                msg="Error on %s definition. type needs to be"
                " specified." % disk)
        # Add the disk  to the VM spec.
        add_disk(
            module, vsphere_client, config_target, config,
            devices, datastore, disktype, disksize, disk_ctrl_key,
            disk_num, disk_key)
        disk_num = disk_num + 1
        disk_key = disk_key + 1


def delete_vm(vsphere_client, module, guest, vm, force):
    try:

        if vm.is_powered_on():
            if force:
                try:
                    vm.power_off(sync_run=True)
                    vm.get_status()

                except Exception, e:
                    module.fail_json(
                        msg='Failed to shutdown vm %s: %s' % (guest, e))
            else:
                module.fail_json(
                    msg='You must use either shut the vm down first or '
                    'use force ')

        # Invoke Destroy_Task
        request = VI.Destroy_TaskRequestMsg()
        _this = request.new__this(vm._mor)
        _this.set_attribute_type(vm._mor.get_attribute_type())
        request.set_element__this(_this)
        ret = vsphere_client._proxy.Destroy_Task(request)._returnval
        task = VITask(ret, vsphere_client)

        # Wait for the task to finish
        status = task.wait_for_state(
            [task.STATE_SUCCESS, task.STATE_ERROR])
        if status == task.STATE_ERROR:
            vsphere_client.disconnect()
            module.fail_json(msg="Error removing vm: %s %s" %
                             task.get_error_message())
        module.exit_json(changed=True, changes="VM %s deleted" % guest)
    except Exception, e:
        module.fail_json(
            msg='Failed to delete vm %s : %s' % (guest, e))


def power_state(vm, state, force):
    """
    Correctly set the power status for a VM determined by the current and
    requested states. force is forceful
    """
    power_status = vm.get_status()

    check_status = ' '.join(state.split("_")).upper()

    # Need Force
    if not force and power_status in [
        'SUSPENDED', 'POWERING ON',
        'RESETTING', 'BLOCKED ON MSG'
    ]:

        return "VM is in %s power state. Force is required!" % power_status

    # State is already true
    if power_status == check_status:
        return False

    else:
        try:
            if state == 'powered_off':
                vm.power_off(sync_run=True)

            elif state == 'powered_on':
                vm.power_on(sync_run=True)

            elif state == 'restarted':
                if power_status in ('POWERED ON', 'POWERING ON', 'RESETTING'):
                    vm.reset(sync_run=False)
                else:
                    return "Cannot restart VM in the current state %s" \
                        % power_status
            return True

        except Exception, e:
            return e

    return False


def gather_facts(vm):
    """
    Gather facts for VM directly from vsphere.
    """
    vm.get_properties()
    facts = {
        'module_hw': True,
        'hw_name': vm.properties.name,
        'hw_guest_full_name':  vm.properties.config.guestFullName,
        'hw_guest_id': vm.properties.config.guestId,
        'hw_product_uuid': vm.properties.config.uuid,
        'hw_processor_count': vm.properties.config.hardware.numCPU,
        'hw_memtotal_mb': vm.properties.config.hardware.memoryMB,
    }
    netInfo = vm.get_property('net')
    netDict = {}
    if netInfo:
        for net in netInfo:
            netDict[net['mac_address']] = net['ip_addresses']

    ifidx = 0
    for entry in vm.properties.config.hardware.device:

        if not hasattr(entry, 'macAddress'):
            continue

        factname = 'hw_eth' + str(ifidx)
        facts[factname] = {
            'addresstype': entry.addressType,
            'label': entry.deviceInfo.label,
            'macaddress': entry.macAddress,
            'ipaddresses': netDict.get(entry.macAddress, None),
            'macaddress_dash': entry.macAddress.replace(':', '-'),
            'summary': entry.deviceInfo.summary,
        }

        ifidx += 1

    return facts


class DefaultVMConfig(object):

    """
    Shallow and deep dict comparison for interfaces
    """

    def __init__(self, check_dict, interface_dict):
        self.check_dict, self.interface_dict = check_dict, interface_dict
        self.set_current, self.set_past = set(
            check_dict.keys()), set(interface_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
        self.recursive_missing = None

    def shallow_diff(self):
        return self.set_past - self.intersect

    def recursive_diff(self):

        if not self.recursive_missing:
            self.recursive_missing = []
            for key, value in self.interface_dict.items():
                if isinstance(value, dict):
                    for k, v in value.items():
                        if k in self.check_dict[key]:
                            if not isinstance(self.check_dict[key][k], v):
                                try:
                                    if v == int:
                                        self.check_dict[key][k] = int(self.check_dict[key][k])
                                    elif v == basestring:
                                        self.check_dict[key][k] = str(self.check_dict[key][k])
                                    else:
                                        raise ValueError
                                except ValueError:
                                    self.recursive_missing.append((k, v))
                        else:
                            self.recursive_missing.append((k, v))

        return self.recursive_missing


def config_check(name, passed, default, module):
    """
    Checks that the dict passed for VM configuration matches the required
    interface declared at the top of __main__
    """

    diff = DefaultVMConfig(passed, default)
    if len(diff.shallow_diff()):
        module.fail_json(
            msg="Missing required key/pair [%s]. %s must contain %s" %
                (', '.join(diff.shallow_diff()), name, default))

    if diff.recursive_diff():
        module.fail_json(
            msg="Config mismatch for %s on %s" %
                (name, diff.recursive_diff()))

    return True


def main():

    vm = None

    proto_vm_hardware = {
        'memory_mb': int,
        'num_cpus': int,
        'scsi': basestring,
        'osid': basestring
    }

    proto_vm_disk = {
        'disk1': {
            'datastore': basestring,
            'size_gb': int,
            'type': basestring
        }
    }

    proto_vm_nic = {
        'nic1': {
            'type': basestring,
            'network': basestring,
            'network_type': basestring
        }
    }

    proto_esxi = {
        'datacenter': basestring,
        'hostname': basestring
    }

    module = AnsibleModule(
        argument_spec=dict(
            vcenter_hostname=dict(required=True, type='str'),
            username=dict(required=True, type='str'),
            password=dict(required=True, type='str'),
            state=dict(
                required=False,
                choices=[
                    'powered_on',
                    'powered_off',
                    'present',
                    'absent',
                    'restarted',
                    'reconfigured'
                ],
                default='present'),
            vmware_guest_facts=dict(required=False, choices=BOOLEANS),
            from_template=dict(required=False, choices=BOOLEANS),
            template_src=dict(required=False, type='str'),
            guest=dict(required=True, type='str'),
            vm_disk=dict(required=False, type='dict', default={}),
            vm_nic=dict(required=False, type='dict', default={}),
            vm_hardware=dict(required=False, type='dict', default={}),
            vm_extra_config=dict(required=False, type='dict', default={}),
            vm_hw_version=dict(required=False, default=None, type='str'),
            resource_pool=dict(required=False, default=None, type='str'),
            cluster=dict(required=False, default=None, type='str'),
            force=dict(required=False, choices=BOOLEANS, default=False),
            esxi=dict(required=False, type='dict', default={}),


        ),
        supports_check_mode=False,
        mutually_exclusive=[['state', 'vmware_guest_facts'],['state', 'from_template']],
        required_together=[
            ['state', 'force'],
            [
                'state',
                'vm_disk',
                'vm_nic',
                'vm_hardware',
                'esxi'
            ],
            ['resource_pool', 'cluster'],
            ['from_template', 'resource_pool', 'template_src']
        ],
    )

    if not HAS_PYSPHERE:
        module.fail_json(msg='pysphere module required')

    vcenter_hostname = module.params['vcenter_hostname']
    username = module.params['username']
    password = module.params['password']
    vmware_guest_facts = module.params['vmware_guest_facts']
    state = module.params['state']
    guest = module.params['guest']
    force = module.params['force']
    vm_disk = module.params['vm_disk']
    vm_nic = module.params['vm_nic']
    vm_hardware = module.params['vm_hardware']
    vm_extra_config = module.params['vm_extra_config']
    vm_hw_version = module.params['vm_hw_version']
    esxi = module.params['esxi']
    resource_pool = module.params['resource_pool']
    cluster = module.params['cluster']
    template_src = module.params['template_src']
    from_template = module.params['from_template']

    # CONNECT TO THE SERVER
    viserver = VIServer()
    try:
        viserver.connect(vcenter_hostname, username, password)
    except VIApiException, err:
        module.fail_json(msg="Cannot connect to %s: %s" %
                         (vcenter_hostname, err))

    # Check if the VM exists before continuing
    try:
        vm = viserver.get_vm_by_name(guest)
    except Exception:
        pass

    if vm:
        # Run for facts only
        if vmware_guest_facts:
            try:
                module.exit_json(ansible_facts=gather_facts(vm))
            except Exception, e:
                module.fail_json(
                    msg="Fact gather failed with exception %s" % e)
        # Power Changes
        elif state in ['powered_on', 'powered_off', 'restarted']:
            state_result = power_state(vm, state, force)

            # Failure
            if isinstance(state_result, basestring):
                module.fail_json(msg=state_result)
            else:
                module.exit_json(changed=state_result)

        # Just check if there
        elif state == 'present':
            module.exit_json(changed=False)

        # Fail on reconfig without params
        elif state == 'reconfigured':
            reconfigure_vm(
                vsphere_client=viserver,
                vm=vm,
                module=module,
                esxi=esxi,
                resource_pool=resource_pool,
                cluster_name=cluster,
                guest=guest,
                vm_extra_config=vm_extra_config,
                vm_hardware=vm_hardware,
                vm_disk=vm_disk,
                vm_nic=vm_nic,
                state=state,
                force=force
            )
        elif state == 'absent':
            delete_vm(
                vsphere_client=viserver,
                module=module,
                guest=guest,
                vm=vm,
                force=force)

    # VM doesn't exist
    else:

        # Fail for fact gather task
        if vmware_guest_facts:
            module.fail_json(
                msg="No such VM %s. Fact gathering requires an existing vm"
                    % guest)

        elif from_template:
            deploy_template(
                vsphere_client=viserver,
                esxi=esxi,
                resource_pool=resource_pool,
                guest=guest,
                template_src=template_src,
                module=module,
                cluster_name=cluster
            )
        if state in ['restarted', 'reconfigured']:
            module.fail_json(
                msg="No such VM %s. States ["
                "restarted, reconfigured] required an existing VM" % guest)
        elif state == 'absent':
            module.exit_json(changed=False, msg="vm %s not present" % guest)

        # Create the VM
        elif state in ['present', 'powered_off', 'powered_on']:

            # Check the guest_config
            config_check("vm_disk", vm_disk, proto_vm_disk, module)
            config_check("vm_nic", vm_nic, proto_vm_nic, module)
            config_check("vm_hardware", vm_hardware, proto_vm_hardware, module)
            config_check("esxi", esxi, proto_esxi, module)

            create_vm(
                vsphere_client=viserver,
                module=module,
                esxi=esxi,
                resource_pool=resource_pool,
                cluster_name=cluster,
                guest=guest,
                vm_extra_config=vm_extra_config,
                vm_hardware=vm_hardware,
                vm_disk=vm_disk,
                vm_nic=vm_nic,
                vm_hw_version=vm_hw_version,
                state=state
            )

    viserver.disconnect()
    module.exit_json(
        changed=False,
        vcenter=vcenter_hostname)


# this is magic, see lib/ansible/module_common.py
#<<INCLUDE_ANSIBLE_MODULE_COMMON>>
if __name__ == '__main__':
    main()
