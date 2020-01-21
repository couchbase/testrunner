from clitest.cli_base import CliBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from membase.api.rest_client import RestConnection
from lib.builds import BeautifulSoup

class HealthcheckerTests(CliBaseTest):

    def setUp(self):
        super(HealthcheckerTests, self).setUp()
        self.report_folder_name = self.input.param("foler_name", "reports")
        self.doc_ops = self.input.param("doc_ops", None)
        self.expire_time = self.input.param("expire_time", 5)
        self.value_size = self.input.param("value_size", 256)
        if self.doc_ops is not None:
            self.doc_ops = self.doc_ops.split(";")
        self.path_to_store = self.input.param("store_path",
                                              "/tmp/cbhealthreport")

    def tearDown(self):
        super(HealthcheckerTests, self).tearDown()

    def healthchecker_test(self):

        gen_load = BlobGenerator('nosql', 'nosql-', self.value_size, end=self.num_items)
        gen_update = BlobGenerator('nosql', 'nosql-', self.value_size, end=(self.num_items // 2 - 1))
        gen_expire = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items // 2, end=(self.num_items * 3 // 4 - 1))
        gen_delete = BlobGenerator('nosql', 'nosql-', self.value_size, start=self.num_items * 3 // 4, end=self.num_items)
        self._load_all_buckets(self.master, gen_load, "create", 0)

        if(self.doc_ops is not None):
            if("update" in self.doc_ops):
                self._load_all_buckets(self.master, gen_update, "update", 0)
            if("delete" in self.doc_ops):
                self._load_all_buckets(self.master, gen_delete, "delete", 0)
            if("expire" in self.doc_ops):
                self._load_all_buckets(self.master, gen_expire, "update", self.expire_time)
                self.sleep(self.expire_time + 1)
        self._wait_for_stats_all_buckets(self.servers[:self.num_servers])

        self.shell.delete_files(self.report_folder_name)

        output, error = self.shell.execute_cbhealthchecker(self.couchbase_usrname, self.couchbase_password, self.command_options,
                                                           path_to_store=self.path_to_store)

        if self.os != "windows":
            if len(error) > 0 and ' '.join(error).find("The run finished successfully.") == -1:
                raise Exception("Command throw out error message. Please check the output of remote_util")
        for output_line in output:
            if output_line.find("ERROR") >= 0 or output_line.find("Error") >= 0:
                raise Exception("Command throw out error message. Please check the output of remote_util")
            if output_line.find('Exception launched') >= 0:
                raise Exception("There are python code exceptions when execute the cbhealthchecker")
        self.verify_results('%s %s' % (output, ' '.join(error)))


    def verify_results(self, command_output):
        for bucket in self.buckets:
            if command_output.find(bucket.name) == -1:
                raise Exception("cbhealthchecker does not generate report for %s" % bucket.name)

        if self.os in ["linux", "mac"]:
            command = "cd %s;du -s %s/*" % (self.path_to_store, self.report_folder_name)
            self.shell.use_sudo = False
            output, error = self.shell.execute_command(command)
            self.shell.log_command_output(output, error)
            empty_reports = False
            if len(error) > 0:
                raise Exception("unable to list file size. Check du command output for help")
            for output_line in output:
                output_line = output_line.split()
                file_size = int(output_line[0])
                if file_size == 0:
                    empty_reports = True
                    self.log.error("%s is empty" % (output_line[1]))

            if empty_reports:
                raise Exception("Collect empty cbhealthchecker reports")
        elif self.os == "windows":
            # try to figure out what command works for windows for verification
            pass
        self._check_params()
        self.shell.delete_files(self.report_folder_name)

    def _check_params(self):
        list_f = self.shell.list_files('%s/%s' % (self.path_to_store, self.report_folder_name))
        for f in list_f:
            if f["file"].find('chart') == -1 and f["file"].find('image') == -1 and\
                f["file"].find('js') == -1 and f["file"].find('css') == -1:
                path = '%s/%s/%s' % (self.path_to_store, self.report_folder_name, f["file"])
                list_reports = ['%s/minute.html' % path, '%s/day.html' % path,
                                '%s/week.html' % path, '%s/hour.html' % path]
        """ since this test only run in few minutes, some data will not have in
            day, week and month """
        for report in list_reports:
            output, _ = self.shell.execute_command("cat %s" % report)
            page = ' '.join(output)

            soup = BeautifulSoup.BeautifulSoup(page)
            self._check_navigation(soup)
            self._check_buckets(soup)
            if "minute" in report or "hour" in report:
                self._check_nodes_list(soup)


    def _check_navigation(self, soup):
        self.log.info("Check if index is present")
        for link_nav in ['./day.html', './hour.html', './minute.html', './month.html',
                         './week.html', './year.html']:
            is_present = False
            for link in soup.findAll('a'):
                if link.get('href') == link_nav:
                    is_present = True
                    break
            self.assertTrue(is_present, "there is no index for %s in generated file" % link_nav)

    def _check_buckets(self, soup):
        self.log.info("Check buckets")
        for bucket in self.buckets:
            is_checked = False
            for section in soup.findAll('section'):
                if section.get('id') == 'cluster-overview':
                    rows = section.findAll('table')[0].findAll('tr')
                    for row in rows:
                        if len(row.findAll('td')) == 0:
                            continue
                        if len(row.findAll('td')[0].findAll('a')) and \
                               row.findAll('td')[0].a.string == bucket.name:
                            if bucket.type:
                                self.assertEqual(row.findAll('td')[1].string, bucket.type,
                                                  "Bucket %s expected type is %s, actual is %s" % (
                                                        bucket.name, bucket.type,
                                                        row.findAll('td')[1].string))
                            is_checked = True
                    self.assertTrue(is_checked, "there was no bucket %s found" % bucket.name)

    def _check_nodes_list(self, soup):
        self.log.info("Check nodes")
        for node in RestConnection(self.master).node_statuses():
            is_checked = False
            for section in soup.findAll('section'):
                if section.get('id') == 'cluster-overview':
                    rows = section.findAll('table')[1].findAll('tr')
                    for i in range(len(rows)):
                        row = rows[i]
                        if len(row.findAll('td')) and \
                           row.findAll('td')[0].string == node.ip:
                            node_info = RestConnection([server for server in self.servers
                                        if server.ip == node.ip and \
                                        int(server.port) == int(node.port)][0]).get_nodes_self()
                            self.assertTrue(row.findAll('td')[1].string.find(node_info.version) != -1,
                                                  "Node %s expected version is %s, actual is %s" % (
                                                        node.ip, node_info.version,
                                                        row.findAll('td')[1].string))
                            self.assertTrue(row.findAll('td')[2].string.find(node.status) != -1,
                                                  "Node %s expected status is %s, actual is %s" % (
                                                        node.ip, node.status,
                                                        row.findAll('td')[2].string))
                            embeded_rows = rows[i + 1].findAll('table')[0].findAll('tr')

                            """ add more checks if needed below this """
                            for emb_row in embeded_rows:
                                """ check data path """
                                if len(emb_row.findAll('td')) and \
                                   emb_row.findAll('td')[1].string == 'Data path on disk':
                                    self.assertTrue(emb_row.findAll('td')[2].li.string.\
                                                    find(node_info.storage[0].path) != -1,
                                                    "Node %s expected data path is %s, actual is %s" % (
                                                    node.ip, node_info.storage[0].path,
                                                        emb_row.findAll('td')[2].li.string))
                                """ check index path """
                                if len(emb_row.findAll('td')) and \
                                   emb_row.findAll('td')[1].string == 'Index data path on disk':
                                    self.assertTrue(emb_row.findAll('td')[2].li.string.\
                                                    find(node_info.storage[0].index_path) != -1,
                                                 "Node %s expected index path is %s, actual is %s" % (
                                                        node.ip, node_info.storage[0].index_path,
                                                        emb_row.findAll('td')[2].li.string))
                                """ check current active items for 1 replica bucket only """
                                if len(emb_row.findAll('td')) and \
                                   emb_row.findAll('td')[1].string == 'Current number of active items':
                                    self.assertTrue(emb_row.findAll('td')[2].li.string.\
                                                    find(str(str(node_info.curr_items/len(self.buckets)))) != -1,
                                                    "Node %s expected active items is %s, actual is %s" % (
                                                    node.ip, str(node_info.curr_items), emb_row.findAll('td')[2].li.string))
                            is_checked = True
                    self.assertTrue(is_checked, "there was no node %s found" % node.ip)
