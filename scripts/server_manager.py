"""
Server Manager Module

This module provides functions for managing server allocation and deallocation
in the Couchbase server pool. Converted from the original JavaScript
server_manager.js.
"""

import json
import logging
import sys
import time

# Dynamic (XenServer-provisioned) VM pool defaults.
DYNVM_BUCKET_NAME = 'QE-dynserver-pool'
DEFAULT_VM_EXPIRY_MINUTES = 1440

# Xen host configs live as individual docs in a dedicated collection in the
# static QE-server-pool bucket, one doc per XenServer host, tagged
# poolId='dynamic_vm'.
XEN_HOSTS_KEYSPACE = "`QE-server-pool`.`_default`.`xen_hosts`"

# [common] section of the old .dynvmservice.ini, kept local rather than in
# Couchbase since these rarely change and dyn-VM triggers need to look them
# up on every create/expiry check.
DYNVM_COMMON_CONFIG = {
    "vm_expiry_minutes": 20160,
    "vm_network_timeout_secs": 400,
    "vm_windows_username": "Administrator",
    "vm_windows_password": "Membase123",
    "vm_linux_username": "root",
    "vm_linux_password": "couchbase",
}


class ServerManager:
    """Server Manager class for managing server allocation and deallocation."""

    def __init__(self, sdk_cluster_obj, bucket_name, logger=None,
                dynvm_bucket_name=DYNVM_BUCKET_NAME):
        """Initialize the server manager with Couchbase connection."""
        self.bucket_name = bucket_name
        self.cluster = sdk_cluster_obj
        self.bucket = self.cluster.bucket(bucket_name)
        self.default_collection = self.bucket.default_collection()
        self.logger = logger or logging.getLogger(__name__)

        # Dynamic VM pool: bucket/session are opened lazily since most
        # ServerManager instances (static-pool only) never touch them.
        self.dynvm_bucket_name = dynvm_bucket_name
        self._dynvm_collection_obj = None

    def get_dockers(self, username, count, pool_id='12hour'):
        """
        Get Docker servers from the server pool.

        Args:
            username: The username requesting the servers
            count: Number of servers requested
            pool_id: Pool ID (default: 12hour)

        Returns:
            str: IP address of allocated server
        """
        self.logger.info(f"get_dockers: username={username}, count={count}, "
                         f"pool_id={pool_id}")

        query_string = \
            f"SELECT ipaddr,availableServers,users FROM `QE-server-pool` " \
            f"WHERE serverType='docker' AND poolId='{pool_id}'"

        docker_ip_list = list()
        self.logger.debug(f"Query: {query_string}")

        try:
            results = self.cluster.query(query_string)

            self.logger.debug(f"Result: {results}")

            for result in results.rows():
                self.logger.debug(
                    f"Available servers: {result['availableServers']}")

                if result['availableServers'] >= count:
                    self.logger.debug(f"Incoming users are {result['users']}")

                    users = result['users'] if result['users'] else {}
                    users[username] = count

                    self.logger.info(f"get dockers, the user is {username}")
                    available_servers = result['availableServers'] - count

                    self.logger.debug(f"users are {json.dumps(users)}")

                    # Update the record
                    update_string = (
                        f"UPDATE `QE-server-pool`"
                        f" SET availableServers={available_servers},"
                        f" users='{json.dumps(users)}'"
                        f" WHERE ipaddr='{result['ipaddr']}'")

                    self.logger.debug(f"Query: {update_string}")
                    r = self.cluster.query(update_string)
                    for _ in r.rows(): pass
                    docker_ip_list.append(result['ipaddr'])

            # No available Docker servers found
            self.logger.warning("The current number of servers requested "
                                "is not available")
        except Exception as e:
            self.logger.error(f"Error in get_dockers: {str(e)}")
        return docker_ip_list

    def release_dockers(self, username, ipaddr):
        """
        Release Docker servers back to the pool.

        Args:
            username: The username releasing the servers
            ipaddr: The IP address of the server to release

        Returns:
            bool: True if successful, False if server not found
        """
        self.logger.info(f"release_dockers: username={username}, "
                         f"ipaddr={ipaddr}")

        query_string = (
            f"SELECT ipaddr,availableServers,users FROM `QE-server-pool` "
            f"WHERE ipaddr = '{ipaddr}'")
        self.logger.debug(f"Query: {query_string}")

        try:
            results = self.cluster.query(query_string)
            self.logger.debug(f"Result: {results}")

            if len(results.rows()) > 0:
                result = results.rows()[0]
                users = result['users'] if result['users'] else {}

                if len(users) == 0 or username not in users:
                    # This is a double delete
                    self.logger.info(f"{username} already released dockers")
                    return True
                else:
                    new_count = result['availableServers'] + users[username]
                    self.logger.debug(f"deleting user {json.dumps(users)}")
                    del users[username]
                    self.logger.debug(f"users are {json.dumps(users)}")

                    # Update the record
                    update_string = (
                        f"UPDATE `QE-server-pool` SET "
                        f"availableServers={new_count}, "
                        f"users='{json.dumps(users)}' WHERE ipaddr='{ipaddr}'")

                    self.logger.debug(f"Query: {update_string}")
                    r = self.cluster.query(update_string)
                    for _ in r.rows(): pass

                    self.logger.info(f"release_dockers: username={username}, "
                                     f"ipaddr={ipaddr}")
                    return True
            else:
                self.logger.error("Unknown server")
                return False

        except Exception as e:
            self.logger.error(f"Error in release_dockers: {str(e)}")
            return False

    def get_available_count(self, os_type, pool_id='12hour', docker=False):
        """
        Get available server count for a specific OS type.

        Args:
            os_type: OS type (e.g., 'centos', 'docker')
            pool_id: Pool ID (default: 12hour)

        Returns:
            int: Available server count
        """
        self.logger.info(f"os_type={os_type}, pool_id={pool_id}, "
                         f"docker={docker}")

        if not docker:
            # Regular server count
            query_string = (
                f"SELECT count(*) FROM `QE-server-pool` "
                f"WHERE state='available' AND os='{os_type}' "
                f"AND (poolId='{pool_id}' OR '{pool_id}' IN poolId)")
            self.logger.debug(f"Query: {query_string}")

            try:
                results = self.cluster.query(query_string)
                # Access the first row and get the count value
                for row in results.rows():
                    count = row['$1']
                    self.logger.info(f"Count: {count}")
                    return count
            except Exception as e:
                self.logger.error(f"Error in get_available_count: {str(e)}")
            return 0
        else:
            # Docker server count
            query_string = (
                f"SELECT ipaddr,availableServers,users FROM `QE-server-pool` "
                f"WHERE serverType='docker' AND os='{os_type}' AND "
                f"(poolId='{pool_id}' OR '{pool_id}' IN poolId)")
            self.logger.debug(f"Query: {query_string}")

            try:
                results = self.cluster.query(query_string)
                capacity_count = 0

                for result in results.rows():
                    available_servers = result['availableServers']
                    self.logger.debug(
                        f"Processing result: {result}, "
                        f"Available servers: {available_servers}")
                    if available_servers >= capacity_count:
                        capacity_count = available_servers

                return capacity_count
            except Exception as e:
                self.logger.error(f"Error in get_available_docker_count: "
                                  f"{str(e)}")
                return 0

    def add_server(self, ipaddr, os_type, version):
        """
        Add a new server to the pool.

        Args:
            ipaddr: IP address of the server
            os_type: OS type
            version: Server version
        """
        self.logger.info(f"add_server: ipaddr={ipaddr}, os={os_type}, "
                         f"version={version}")

        try:
            doc = {
                'ipaddr': ipaddr,
                'OS': os_type,
                'version': version,
                'state': 'available'
            }
            self.bucket.upsert(ipaddr, doc)
            self.logger.info(f"Server {ipaddr} added")

        except Exception as e:
            self.logger.error(f"Error in add_server: {str(e)}")

    def remove_server(self, ipaddr):
        """
        Remove a server from the pool.

        Args:
            ipaddr: IP address of the server to remove
        """
        self.logger.info(f"remove_server: ipaddr={ipaddr}")

        try:
            self.bucket.remove(ipaddr)
            self.logger.info(f"Server {ipaddr} removed")

        except Exception as e:
            self.logger.error(f"Error in remove_server: {str(e)}")

    def get_servers(self, username, count=1, os_type='centos', expires_in=1,
                    pool_id='12hour', dont_reserve=False):
        """
        Get servers from the pool using Couchbase transactions.

        Args:
            username: Username requesting servers
            count: Number of servers requested
            os_type: OS type
            expires_in: Expiration time in minutes
            pool_id: Pool ID
            dont_reserve: Whether to reserve servers

        Returns:
            list: List of server IP addresses
        """
        self.logger.info(f"get_servers: username={username}, count={count}, "
                         f"os={os_type}")
        server_list = list()

        try:
            # Use Couchbase transaction for atomic server allocation
            def allocate_servers(ctx):
                # Count available servers within transaction
                count_query = (
                    f"SELECT count(*) FROM `QE-server-pool` "
                    f"WHERE state='available' AND os='{os_type}' "
                    f"AND (poolId='{pool_id}' OR '{pool_id}' IN poolId)")

                count_result = ctx.query(count_query)
                available_count = 0
                for row in count_result.rows():
                    available_count = row['$1']
                    break

                self.logger.info(f"requested count: {count}, "
                                 f"available count: {available_count}")

                if count > available_count:
                    raise Exception("Not enough servers available")

                # Get servers within transaction
                get_servers_query = (
                    f"SELECT *, meta().id FROM `QE-server-pool` "
                    f"WHERE state='available' AND os='{os_type}' "
                    f"AND (poolId='{pool_id}' OR '{pool_id}' IN poolId) "
                    f"ORDER BY to_number(memory) ASC LIMIT {count}")

                self.logger.debug(f"Query: {get_servers_query}")

                get_results = ctx.query(get_servers_query)

                # Process results properly
                for result in get_results.rows():
                    # Extract the document data from the result
                    doc_data = result['QE-server-pool']

                    # Update document state
                    doc_data['state'] = 'booked'
                    doc_data['prevUser'] = doc_data.get('username', '')
                    doc_data['username'] = username

                    # Replace document within transaction
                    try:
                        target_id = ctx.get(self.default_collection, result['id'])
                        ctx.replace(target_id, doc_data)
                        server_list.append(doc_data['ipaddr'])
                    except Exception as e:
                        # Make sure the transaction aborts without retry
                        ctx.abort(Exception(f"Allocation failed: {e}"))

            # Execute the transaction
            _ = self.cluster.transactions.run(allocate_servers)
            self.logger.info(
                f"Allocated {len(server_list)} servers: {server_list}")

            if len(server_list) < count:
                raise Exception("Booked fewer than expected servers")

            t_result = self.cluster.query(
                f"SELECT ipaddr,state,username FROM `QE-server-pool` "
                f"WHERE ipaddr IN {server_list}")

            for row in t_result.rows():
                self.logger.debug(row)
                if row['username'] != username:
                    raise Exception("Username is not as current expected")
                elif row["state"] != "booked":
                    raise Exception("State is not booked")
        except Exception as e:
            self.logger.error(f"Error in get_servers transaction: {str(e)}")
            server_list.clear()
            raise e
        return server_list

    def release_ip(self, ipaddr, state='available'):
        """
        Release a specific IP address.

        Args:
            ipaddr: IP address to release
            state: New state for the server
        """
        self.logger.info(f"release_ip: ipaddr={ipaddr}, state={state}")

        update_string = (
            f"UPDATE `QE-server-pool` SET state='{state}'"
            f" WHERE ipaddr='{ipaddr}' AND state='booked'")

        self.logger.debug(f"Query: {update_string}")

        try:
            r = self.cluster.query(update_string)
            for _ in r.rows(): pass
            self.logger.info(f"IP {ipaddr} released")
        except Exception as e:
            self.logger.error(f"Error in release_ip: {str(e)}")

    def release_servers(self, username, state='available'):
        """
        Release all servers for a specific username using transactions.

        Args:
            username: Username whose servers should be released
            state: New state for the servers
        """
        self.logger.info(f"release_servers: username={username}, "
                         f"state={state}")

        try:
            # Use Couchbase transaction for atomic server release
            def release_servers_txn(ctx):
                # Query to find all servers for this username
                query = (
                    f"SELECT *, meta().id FROM `QE-server-pool` "
                    f"WHERE username='{username}' AND state='booked'")

                self.logger.debug(f"Query: {query}")

                results = ctx.query(query)
                released_count = 0

                # Process each server document
                for result in results.rows():
                    # Extract the document data from the result
                    doc_data = result['QE-server-pool']
                    meta_id = result['id']

                    # Update document state
                    doc_data['state'] = state
                    doc_data['prevUser'] = doc_data.get('username', '')
                    doc_data['username'] = ''

                    # Replace document within transaction
                    target_doc = ctx.get(self.default_collection, meta_id)
                    ctx.replace(target_doc, doc_data)
                    released_count += 1

                self.logger.info(
                    f"Released {released_count} servers for user {username}")
                return released_count

            # Execute the transaction
            result = self.cluster.transactions.run(release_servers_txn)
            self.logger.info(
                f"Successfully released {result} servers for user {username}")
            return result

        except Exception as e:
            self.logger.error(
                f"Error in release_servers transaction: {str(e)}")
            return 0

    def show_all(self):
        """
        Show all servers in the pool.

        Returns:
            list: List of all server documents
        """
        query_string = "SELECT * FROM `QE-server-pool`"

        try:
            results = self.cluster.query(query_string)
            server_list = [row for row in results.rows()]
            self.logger.info(f"show_all result: {len(server_list)} servers "
                             "found")
            return server_list

        except Exception as e:
            self.logger.error(f"Error in show_all: {str(e)}")
            return []

    # ------------------------------------------------------------------
    # Dynamic (XenServer-provisioned) VM pool.
    #
    # Replaces the external Flask REST hop (scripts/dyn_dispatcher.py /
    # test_infra_runner's dynvm_server_manager.py) with in-process XenAPI
    # calls, driven by a single Couchbase config doc instead of a local
    # .dynvmservice.ini file. VM lifecycle docs live in their own bucket
    # (`dynvm_bucket_name`, default `QE-dynserver-pool`) rather than the
    # static `QE-server-pool` bucket, so the two allocators stay decoupled.
    # ------------------------------------------------------------------

    def _dynvm_collection(self):
        """Lazily open the dynamic-VM-pool bucket's default collection."""
        if self._dynvm_collection_obj is None:
            bucket = self.cluster.bucket(self.dynvm_bucket_name)
            self._dynvm_collection_obj = bucket.default_collection()
        return self._dynvm_collection_obj

    def _run_xen_hosts_query(self, query_string):
        self.logger.debug(f"dyn_vm: xen_hosts query: {query_string}")
        try:
            results = self.cluster.query(query_string)
            rows = [row for row in results.rows()]
            self.logger.debug(f"dyn_vm: xen_hosts query returned {len(rows)} "
                              f"host doc(s)")
            return rows
        except Exception as e:
            self.logger.error(f"dyn_vm: error querying xen_hosts: {e}")
            return []

    @staticmethod
    def _generate_vm_names(username, count):
        """Mirror the legacy service's naming: a single VM keeps the bare
        descriptor as its name; N>1 VMs get a 1-based numeric suffix."""
        if count <= 0:
            return []
        if count == 1:
            return [username]
        return [f"{username}{i}" for i in range(1, count + 1)]

    def _xen_session(self, host):
        """Open an authenticated XenAPI session against one configured
        xenhost. Imported lazily so importing this module doesn't require
        XenAPI to be installed unless the dynamic-VM path is actually used."""
        import XenAPI
        self.logger.debug(f"dyn_vm: opening XenAPI session to "
                          f"{host.get('host_id', host.get('ip'))} "
                          f"({host.get('ip')})")
        session = XenAPI.Session("http://" + host['ip'])
        session.xenapi.login_with_password(host['username'],
                                           host['password'])
        return session

    def _get_xen_hosts(self, os_type, labels=None):
        """Enabled xenhosts (queried live from the xen_hosts collection)
        that both define an enabled template for os_type and match the
        requested labels (mirrors dyn_dispatcher.py's label matching: a
        labeled host only matches an overlapping label request; an
        unlabeled host only matches when no labels were requested).

        The `disabled=false` filter is applied server-side (a host taken
        out of rotation just never comes back from this query); a specific
        os_type template can still be marked `"disabled": true` within an
        otherwise-enabled host's doc, which is filtered here in Python.
        This is allocation-only - release/cleanup uses
        _get_all_xen_hosts() instead, since a VM may have been created
        before its host or template was disabled.
        """
        query_string = (
            f"SELECT data.* FROM {XEN_HOSTS_KEYSPACE} AS data "
            f"WHERE data.poolId = 'dynamic_vm' AND data.disabled = false")
        hosts = self._run_xen_hosts_query(query_string)
        matching = []
        for host in hosts:
            template_info = host.get('templates', {}).get(os_type)
            if not template_info or template_info.get('disabled'):
                continue
            host_labels = host.get('host_labels')
            if host_labels:
                if not labels or not (set(host_labels) & set(labels)):
                    continue
            elif labels:
                continue
            matching.append(host)
        self.logger.info(
            f"dyn_vm: {len(matching)}/{len(hosts)} enabled xen host(s) "
            f"usable for os={os_type}, labels={labels}: "
            f"{[h.get('host_id', h.get('ip')) for h in matching]}")
        return matching

    def _get_all_xen_hosts(self):
        """Every dynamic_vm xenhost doc, enabled or disabled - used only
        for release/cleanup (_find_host_with_vm), since a VM can outlive
        its host or template being disabled after the VM was created."""
        query_string = (
            f"SELECT data.* FROM {XEN_HOSTS_KEYSPACE} AS data "
            f"WHERE data.poolId = 'dynamic_vm'")
        return self._run_xen_hosts_query(query_string)

    def _get_vms_usage(self, session):
        """(vm_count, total_vcpus, total_memory_gb) across running,
        non-template VMs on this host."""
        vm_count = vcpus = memory_gb = 0
        for vm in session.xenapi.VM.get_all():
            record = session.xenapi.VM.get_record(vm)
            if (not record["is_a_template"]
                    and not record["is_control_domain"]
                    and record["power_state"] != 'Halted'):
                vm_count += 1
                vcpus += int(record["VCPUs_max"])
                memory_gb += int(int(record["memory_static_max"])
                                 / (1024 ** 3))
        return vm_count, vcpus, memory_gb

    def _get_host_usage(self, session):
        """(free_cpus, free_memory_gb, total_cpus, total_memory_gb)."""
        _, vm_cpus, _ = self._get_vms_usage(session)
        host_ref = session.xenapi.session.get_this_host(session.handle)
        host_record = session.xenapi.host.get_record(host_ref)
        cpu_total = int(host_record['cpu_info']['cpu_count'])
        cpu_free = max(cpu_total - vm_cpus, 0)
        metrics = session.xenapi.host_metrics.get_record(
            session.xenapi.host.get_metrics(host_ref))
        mem_free_gb = int(int(metrics['memory_free']) / (1024 ** 3))
        mem_total_gb = int(int(metrics['memory_total']) / (1024 ** 3))
        return cpu_free, mem_free_gb, cpu_total, mem_total_gb

    def _get_host_disks(self, session, storage_name):
        """(physical_size, physical_utilisation, free_size) in bytes for
        the SR matching storage_name."""
        psize = valloc = fsize = 0
        for pbd in session.xenapi.PBD.get_all():
            if pbd == 'OpaqueRef:NULL':
                continue
            try:
                sr = session.xenapi.PBD.get_SR(pbd)
                if (storage_name and session.xenapi.SR.get_name_label(sr)
                        .lower() != storage_name.lower()):
                    continue
                psize = session.xenapi.SR.get_physical_size(sr)
                valloc = session.xenapi.SR.get_physical_utilisation(sr)
                fsize = int(psize) - int(valloc)
                break
            except Exception as e:
                self.logger.debug(f"Error reading SR for PBD {pbd}: {e}")
        return psize, valloc, fsize

    def _xen_host_capacity(self, session, os_type, host):
        """How many more os_type VMs this host's free CPU/memory/disk can
        fit, independent of any configured host.vms.max cap."""
        template_info = host['templates'][os_type]
        is_windows = os_type.startswith('win')
        required_cpus = template_info.get('vcpus', 12 if is_windows else 8)
        required_memory_gb = template_info.get('memory', 6 if is_windows else 4)
        required_disk_gb = template_info.get('disk', 71 if is_windows else 35)

        _, _, fsize = self._get_host_disks(session, host.get('host_storage_name'))
        cpu_free, mem_free_gb, _, _ = self._get_host_usage(session)

        cpus_count = int(cpu_free / required_cpus) if required_cpus else 0
        memory_count = int(mem_free_gb / required_memory_gb) if required_memory_gb else 0
        fsize = fsize - int(0.1 * fsize)
        disk_count = int((fsize / (1024 ** 3)) / required_disk_gb) if required_disk_gb else 0
        if disk_count > 0:
            disk_count -= 1
        return max(min(cpus_count, memory_count, disk_count), 0)

    def _host_is_overprovisioned(self, session, os_type, host, additional_count=0):
        max_vms = host.get('vms_max', {}).get(os_type)
        if max_vms is None:
            return False
        provisioned_vms, _, _ = self._get_vms_usage(session)
        return provisioned_vms + additional_count > max_vms

    def _read_os_name(self, session, vm):
        try:
            vgm = session.xenapi.VM.get_guest_metrics(vm)
            return session.xenapi.VM_guest_metrics.get_os_version(vgm).get("name")
        except Exception:
            return None

    def _read_ip_address(self, session, vm):
        try:
            vgm = session.xenapi.VM.get_guest_metrics(vm)
            networks = session.xenapi.VM_guest_metrics.get_networks(vgm)
            return networks.get("0/ip") or networks.get("1/ip")
        except Exception:
            return None

    def _get_disks_size(self, session, vm):
        disks = []
        for vbd in session.xenapi.VM.get_VBDs(vm):
            if vbd == 'OpaqueRef:NULL':
                continue
            vdi = session.xenapi.VBD.get_VDI(vbd)
            if vdi and vdi != 'OpaqueRef:NULL':
                try:
                    disks.append(
                        session.xenapi.VDI.get_record(vdi)['virtual_size'])
                except Exception as e:
                    self.logger.debug(f"Error reading VDI {vdi}: {e}")
        return disks

    def _delete_all_disks(self, session, vm):
        for vbd in session.xenapi.VM.get_VBDs(vm):
            vdi = session.xenapi.VBD.get_VDI(vbd)
            if vdi and vdi != 'OpaqueRef:NULL':
                try:
                    session.xenapi.VDI.destroy(vdi)
                except Exception as e:
                    self.logger.error(f"Error destroying VDI {vdi}: {e}")

    def _create_vm(self, session, os_type, host, vm_name, cpus='default',
                   memory='default'):
        """Clone+boot one VM from the configured template for os_type on
        this already-authenticated session, waiting for a real (non-169.x)
        IP. Returns a dict of the fields the caller needs to persist a
        lifecycle doc. Raises on failure (including exhausting retries)."""
        template_info = host['templates'][os_type]
        template = template_info['template']
        network_hint = template_info.get('network') or host.get('host_network_id')
        start_time = time.time()
        self.logger.info(
            f"dyn_vm: creating VM {vm_name} (os={os_type}, "
            f"template={template}) on host "
            f"{host.get('host_id', host.get('ip'))}")

        pifs = session.xenapi.PIF.get_all_records()
        lowest = None
        if network_hint:
            for pif_ref, pif in pifs.items():
                if network_hint in pif['device']:
                    lowest = pif_ref
                    break
        if lowest is None:
            for pif_ref, pif in pifs.items():
                if lowest is None or pif['device'] < pifs[lowest]['device']:
                    lowest = pif_ref

        vms = session.xenapi.VM.get_all_records()
        template_refs = [ref for ref, rec in vms.items()
                         if rec['is_a_template'] and rec['name_label'] == template]
        if not template_refs:
            raise Exception(f"Could not find Xen template '{template}'")
        template_ref = template_refs[0]

        timeout_secs = int(
            DYNVM_COMMON_CONFIG.get('vm_network_timeout_secs') or 400)

        max_attempts = 3
        vm = None
        vm_os_name = ""
        vm_ip_addr = None
        succeeded = False

        for attempt in range(1, max_attempts + 1):
            self.logger.debug(
                f"dyn_vm: clone attempt {attempt}/{max_attempts} for "
                f"{vm_name}")
            vm = session.xenapi.VM.clone(template_ref, vm_name)
            network_ref = session.xenapi.PIF.get_network(lowest)
            for vif in session.xenapi.VIF.get_all():
                if (session.xenapi.VM.get_name_label(
                        session.xenapi.VIF.get_VM(vif)) == vm_name):
                    session.xenapi.VIF.move(vif, network_ref)
            session.xenapi.VM.set_PV_args(vm, "non-interactive")
            session.xenapi.VM.set_name_description(
                vm, f"{vm_name} from {template}")
            if cpus != "default":
                session.xenapi.VM.set_VCPUs_max(vm, int(cpus))
                session.xenapi.VM.set_VCPUs_at_startup(vm, int(cpus))
            if memory != "default":
                session.xenapi.VM.set_memory(vm, memory)
            session.xenapi.VM.provision(vm)
            session.xenapi.VM.start(vm, False, True)

            if "win" not in template:
                maxtime = time.time() + timeout_secs
                while (self._read_os_name(session, vm) is None
                       and time.time() < maxtime):
                    time.sleep(1)
                vm_os_name = self._read_os_name(session, vm) or ""
            else:
                # Windows guest agent needs time to bring networking up
                # before an IP will be reported.
                time.sleep(60)

            maxtime = time.time() + timeout_secs
            while ((self._read_ip_address(session, vm) is None
                    or self._read_ip_address(session, vm).startswith('169'))
                   and time.time() < maxtime):
                time.sleep(1)
            vm_ip_addr = self._read_ip_address(session, vm)

            if vm_ip_addr is None or vm_ip_addr.startswith('169'):
                self.logger.warning(
                    f"dyn_vm: attempt {attempt}/{max_attempts} for "
                    f"{vm_name} got no usable IP "
                    f"(vm_ip_addr={vm_ip_addr!r}); destroying and retrying")
                record = session.xenapi.VM.get_record(vm)
                if record["power_state"] != 'Halted':
                    session.xenapi.VM.hard_shutdown(vm)
                self._delete_all_disks(session, vm)
                session.xenapi.VM.destroy(vm)
                time.sleep(5)
                continue
            succeeded = True
            break

        if not succeeded:
            raise Exception(f"Couldn't get an IP for {vm_name} within timeout")

        record = session.xenapi.VM.get_record(vm)
        disks_info = ','.join(str(d) for d in self._get_disks_size(session, vm))
        xen_host_description = "unknown"
        for host_rec in session.xenapi.host.get_all_records().values():
            xen_host_description = host_rec['name_label']

        self.logger.info(
            f"dyn_vm: created VM {vm_name} ip={vm_ip_addr} "
            f"on host {host.get('host_id', host.get('ip'))} in "
            f"{round(time.time() - start_time)}s")

        return {
            'uuid': record["uuid"], 'ipaddr': vm_ip_addr,
            'os_version': vm_os_name, 'vcpus': record["VCPUs_max"],
            'memory_static_max': record["memory_static_max"],
            'disks_info': disks_info, 'origin': xen_host_description,
            'create_duration_secs': round(time.time() - start_time),
        }

    def _save_dynvm_doc(self, doc_key, doc_value, retries=3):
        while retries > 0:
            try:
                self._dynvm_collection().upsert(doc_key, doc_value)
                self.logger.debug(
                    f"dyn_vm: saved doc {doc_key} "
                    f"(state={doc_value.get('state')}) in "
                    f"{self.dynvm_bucket_name}")
                return True
            except Exception as e:
                self.logger.error(f"dyn_vm: error saving doc {doc_key}: {e}")
            time.sleep(1)
            retries -= 1
        return False

    def _query_dynvm_docs_by_name_pattern(self, username):
        """All non-deleted dynvm docs whose `name` is `username` or
        `username` + digits (i.e. every VM created for this descriptor),
        as a list of (doc_id, doc_value) pairs."""
        query_string = (
            f"SELECT META(b).id AS doc_id, b.* FROM "
            f"`{self.dynvm_bucket_name}` AS b "
            f"WHERE REGEXP_LIKE(b.name, '^{username}[0-9]*$') "
            f"AND b.state != 'deleted'")
        results = self.cluster.query(query_string)
        docs = []
        for row in results.rows():
            doc_id = row.pop('doc_id')
            docs.append((doc_id, row))
        return docs

    def _find_host_with_vm(self, vm_name):
        """Scan every configured xenhost for a VM with this name-label;
        return the matching host config dict, or None if not found
        anywhere (already destroyed, or never actually created)."""
        for host in self._get_all_xen_hosts():
            session = None
            try:
                session = self._xen_session(host)
                if len(session.xenapi.VM.get_by_name_label(vm_name)) > 0:
                    self.logger.info(
                        f"dyn_vm: found VM {vm_name} on host "
                        f"{host.get('host_id', host.get('ip'))}")
                    return host
            except Exception as e:
                self.logger.debug(
                    f"dyn_vm: error checking host {host.get('ip')} for "
                    f"VM {vm_name}: {e}")
            finally:
                if session:
                    try:
                        session.logout()
                    except Exception:
                        pass
        self.logger.warning(f"dyn_vm: VM {vm_name} not found on any "
                            f"configured xen host")
        return None

    def _delete_vm_on_host(self, session, vm_name):
        vms = session.xenapi.VM.get_by_name_label(vm_name)
        self.logger.info(
            f"dyn_vm: destroying {len(vms)} Xen VM(s) named {vm_name}")
        for vm in vms:
            record = session.xenapi.VM.get_record(vm)
            if record["power_state"] != 'Halted':
                session.xenapi.VM.hard_shutdown(vm)
            self._delete_all_disks(session, vm)
            session.xenapi.VM.destroy(vm)

    def get_dynamic_vms_available_count(self, os_type, labels=None):
        """Aggregate free capacity (respecting host.vms.max.<os_type> caps)
        across every configured xenhost for os_type."""
        self.logger.info(
            f"dyn_vm: checking available capacity for os={os_type}, "
            f"labels={labels}")
        total = 0
        for host in self._get_xen_hosts(os_type, labels):
            session = None
            try:
                session = self._xen_session(host)
                available = self._xen_host_capacity(session, os_type, host)
                max_vms = host.get('vms_max', {}).get(os_type)
                if max_vms is not None:
                    provisioned_vms, _, _ = self._get_vms_usage(session)
                    available = min(available, max(max_vms - provisioned_vms, 0))
                self.logger.debug(
                    f"dyn_vm: host {host.get('host_id', host.get('ip'))} "
                    f"has {available} slot(s) free for {os_type}")
                total += max(available, 0)
            except Exception as e:
                self.logger.warning(
                    f"dyn_vm: error checking capacity on host "
                    f"{host.get('ip')}: {e}")
            finally:
                if session:
                    try:
                        session.logout()
                    except Exception:
                        pass
        self.logger.info(
            f"dyn_vm: total available capacity for os={os_type}: {total}")
        return total

    def get_dynamic_vms(self, username, count, os_type, pool_id='dynamic',
                        expires_in_minutes=None, cpus='default',
                        memory='default', labels=None):
        """
        Provision `count` XenServer VMs of os_type, distributed across
        whichever configured xenhosts have capacity, and record a
        lifecycle doc per VM in the dynamic-VM-pool bucket.

        Best-effort: returns however many VMs actually got created (may be
        fewer than `count`) rather than raising, so callers can apply the
        same "got fewer than expected -> release + retry" handling they
        already use for the static pool.

        Returns:
            list: IP addresses of the successfully created VMs.
        """
        self.logger.info(
            f"dyn_vm: get_dynamic_vms: descriptor={username}, count={count}, "
            f"os={os_type}, poolId={pool_id}, expires_in_minutes="
            f"{expires_in_minutes}, labels={labels}")
        vm_names = self._generate_vm_names(username, count)
        hosts = self._get_xen_hosts(os_type, labels)
        if not hosts:
            self.logger.error(
                f"dyn_vm: no Xen host configured with a template for "
                f"os={os_type}; cannot create any of {vm_names}")
            return []

        max_expiry_minutes = int(
            DYNVM_COMMON_CONFIG.get('vm_expiry_minutes')
            or DEFAULT_VM_EXPIRY_MINUTES)
        expiry_minutes = min(
            int(expires_in_minutes) if expires_in_minutes else DEFAULT_VM_EXPIRY_MINUTES,
            max_expiry_minutes)
        self.logger.debug(
            f"dyn_vm: VM names to create: {vm_names}, expiry capped at "
            f"{expiry_minutes} minute(s)")

        ips = []
        remaining = list(vm_names)

        for host in hosts:
            if not remaining:
                break
            session = None
            try:
                session = self._xen_session(host)
                available = self._xen_host_capacity(session, os_type, host)
                to_create = min(available, len(remaining))
                self.logger.info(
                    f"dyn_vm: host {host.get('host_id', host.get('ip'))} "
                    f"has {available} free slot(s) for {os_type}; "
                    f"creating {to_create} of the remaining "
                    f"{len(remaining)} VM(s) here")
                for _ in range(to_create):
                    if self._host_is_overprovisioned(session, os_type, host):
                        self.logger.warning(
                            f"dyn_vm: host {host.get('ip')} is "
                            f"overprovisioned for {os_type}, skipping "
                            f"remaining capacity on it")
                        break
                    vm_name = remaining[0]
                    try:
                        result = self._create_vm(session, os_type, host,
                                                 vm_name, cpus, memory)
                    except Exception as e:
                        self.logger.error(
                            f"dyn_vm: failed to create VM {vm_name} on "
                            f"{host.get('ip')}: {e}; leaving it in the "
                            f"queue for the next host to attempt")
                        # Do NOT pop vm_name here - a create failure (e.g.
                        # insufficient SR space on just this host) must not
                        # permanently abandon it; break out to the next
                        # host in the outer loop so it gets another shot
                        # there instead of being silently dropped.
                        break
                    now = time.time()
                    doc = {
                        'ipaddr': result['ipaddr'], 'origin': result['origin'],
                        'os': os_type, 'state': 'available', 'poolId': pool_id,
                        'prevUser': '', 'username': vm_name, 'name': vm_name,
                        'ver': '12', 'memory': result['memory_static_max'],
                        'os_version': result['os_version'],
                        'cpu': result['vcpus'], 'disk': result['disks_info'],
                        'created_time': now,
                        'create_duration_secs': result['create_duration_secs'],
                        'expired_time': now + (expiry_minutes * 60),
                        'labels': labels or [],
                    }
                    self._save_dynvm_doc(result['uuid'], doc)
                    ips.append(result['ipaddr'])
                    remaining.pop(0)
            except Exception as e:
                self.logger.error(
                    f"dyn_vm: error provisioning on host "
                    f"{host.get('ip')}: {e}")
            finally:
                if session:
                    try:
                        session.logout()
                    except Exception:
                        pass

        if len(ips) < count:
            self.logger.warning(
                f"dyn_vm: get_dynamic_vms: only created {len(ips)}/{count} "
                f"VM(s) for descriptor={username}: {ips}")
        else:
            self.logger.info(
                f"dyn_vm: get_dynamic_vms: created all {len(ips)}/{count} "
                f"VM(s) for descriptor={username}: {ips}")
        return ips

    def release_dynamic_vms(self, username, expected_count=None):
        """
        Destroy every dynamic VM created for `username` (the dispatch job's
        descriptor) and mark its doc deleted. Discovers the VM set by
        querying the dynamic-VM-pool bucket rather than trusting a caller-
        supplied count, since the process that created the VMs has usually
        long since exited by the time this runs (it's invoked from the test
        executor's post-run cleanup, not the dispatcher itself).

        Args:
            username: the job descriptor VMs were named from.
            expected_count: optional, logged as a mismatch warning only.

        Returns:
            int: number of VMs actually destroyed.
        """
        self.logger.info(
            f"dyn_vm: release_dynamic_vms: descriptor={username}, "
            f"expected_count={expected_count}")
        try:
            docs = self._query_dynvm_docs_by_name_pattern(username)
        except Exception as e:
            self.logger.error(
                f"dyn_vm: error querying dynvm docs for "
                f"descriptor={username}: {e}")
            return 0

        self.logger.info(
            f"dyn_vm: found {len(docs)} dynvm doc(s) to release for "
            f"descriptor={username}: {[doc.get('name') for _, doc in docs]}")
        if expected_count is not None and len(docs) != int(expected_count):
            self.logger.warning(
                f"dyn_vm: release_dynamic_vms: found {len(docs)} VM doc(s) "
                f"for descriptor={username}, expected {expected_count}")

        released = 0
        for doc_id, doc in docs:
            vm_name = doc.get('name', username)
            try:
                host = self._find_host_with_vm(vm_name)
                if host is None:
                    self.logger.warning(
                        f"dyn_vm: no Xen host has a VM named {vm_name}; "
                        f"marking doc {doc_id} deleted without a XenAPI "
                        f"call")
                else:
                    session = self._xen_session(host)
                    try:
                        self._delete_vm_on_host(session, vm_name)
                    finally:
                        session.logout()

                now = time.time()
                doc['state'] = 'deleted'
                doc['deleted_time'] = now
                if doc.get('created_time'):
                    doc['live_duration_secs'] = round(now - doc['created_time'])
                self._save_dynvm_doc(doc_id, doc)
                released += 1
                self.logger.info(
                    f"dyn_vm: released VM {vm_name} (doc {doc_id})")
            except Exception as e:
                self.logger.error(
                    f"dyn_vm: error releasing VM {vm_name} "
                    f"(doc {doc_id}): {e}")

        self.logger.info(
            f"dyn_vm: release_dynamic_vms: released {released}/{len(docs)} "
            f"VM(s) for descriptor={username}")
        return released


if __name__ == "__main__":
    import argparse

    from couchbase.auth import PasswordAuthenticator
    from couchbase.cluster import Cluster
    from couchbase.options import ClusterOptions

    parser = argparse.ArgumentParser(
        description="Server Manager CLI (dynamic VM pool actions)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 server_manager.py --cluster-ip 172.23.217.21 \\
      --username Administrator --password password \\
      --bucket QE-server-pool --action release_dynamic_vms \\
      --descriptor gsi-plasma_basic-Aug-10-14:23:01-8.1.0 --count 2
        """)
    parser.add_argument('--cluster-ip', required=True,
                        help='Couchbase cluster IP address')
    parser.add_argument('--username', required=True,
                        help='Username for cluster authentication')
    parser.add_argument('--password', required=True,
                        help='Password for cluster authentication')
    parser.add_argument('--bucket', required=True,
                        help='Static QE-server-pool bucket name')
    parser.add_argument('--dynvm-bucket', dest='dynvm_bucket',
                        default=DYNVM_BUCKET_NAME,
                        help=f'Dynamic-VM-pool bucket name (default: '
                             f'{DYNVM_BUCKET_NAME})')
    parser.add_argument('--action', required=True,
                        choices=['release_dynamic_vms'])
    parser.add_argument('--descriptor', required=True,
                        help="VM name prefix (job descriptor/username) to "
                             "release")
    parser.add_argument('--count', type=int, default=None,
                        help='Expected VM count, logged as a mismatch '
                             'warning only')
    parser.add_argument('--log-level', dest='log_level', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR',
                                 'CRITICAL'])
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s: %(funcName)s:L%(lineno)d: %(levelname)s: '
               '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')
    log = logging.getLogger(__name__)

    cluster = Cluster(
        f'couchbase://{args.cluster_ip}',
        ClusterOptions(PasswordAuthenticator(args.username, args.password)))
    manager = ServerManager(cluster, args.bucket, logger=log,
                            dynvm_bucket_name=args.dynvm_bucket)

    return_code = 0
    if args.action == 'release_dynamic_vms':
        released = manager.release_dynamic_vms(
            args.descriptor, expected_count=args.count)
        print(f"Released {released} dynamic VM(s) for "
              f"descriptor={args.descriptor}")
        if args.count and released == 0:
            return_code = 1

    sys.exit(return_code)
