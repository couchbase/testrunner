#this class will contain methods which we
#use later to
# map a version # -> rpm url
from datetime import datetime
import time
import urllib.request, urllib.error, urllib.parse
import re
import socket
from . import BeautifulSoup
import testconstants
import logger
import traceback
import sys
from testconstants import WIN_CB_VERSION_3
from testconstants import SHERLOCK_VERSION
from testconstants import COUCHBASE_VERSION_2
from testconstants import COUCHBASE_VERSION_3
from testconstants import COUCHBASE_VERSION_2_WITH_REL
from testconstants import COUCHBASE_RELEASE_FROM_VERSION_3,\
                          COUCHBASE_RELEASE_FROM_SPOCK
from testconstants import COUCHBASE_FROM_VERSION_3, COUCHBASE_FROM_SPOCK,\
                          COUCHBASE_FROM_MAD_HATTER, COUCHBASE_FROM_601
from testconstants import CB_RELEASE_REPO
from testconstants import CB_LATESTBUILDS_REPO


class MembaseBuild(object):
    def __init__(self):
        self.url = ''
        self.name = ''
        self.time = ''
        self.size = ''
        self.product = ''
        self.product_version = ''
        self.build_number = 0
        self.os = ''
        self.deliverable_type = ''
        self.architecture_type = ''
        self.toy = ''
        self.change = None  # a MembaseChange
        self.url_latest_build = ''

    def __repr__(self):
        return self.__str__()

    #let's also have a json object for all these classes
    def __str__(self):
        url = 'url : {0}'.format(self.url)
        name = 'name : {0}'.format(self.name)
        product = 'product : {0}'.format(self.product)
        product_version = 'product_version : {0}'.format(self.product_version)
        os = 'os : {0}'.format(self.os)
        deliverable_type = 'deliverable_type : {0}'.format(self.deliverable_type)
        architecture_type = 'architecture_type : {0}'.format(self.architecture_type)
        if self.toy:
            toy = 'toy : {0}'.format(self.toy)
        else:
            toy = ''
        return '{0} {1} {2} {3} {4} {5} {6} {7}'.format(url, name, product, product_version, os, deliverable_type,
                                                    architecture_type, toy)


class MembaseChange(object):
    def __init__(self):
        self.url = ''
        self.name = ''
        self.time = ''
        self.build_number = ''


class BuildQuery(object):
    def __init__(self):
        self.log = logger.Logger.get_logger()
        pass

    # let's look at buildlatets or latest/sustaining or any other
    # location
    def parse_builds(self):
        #parse build page and create build object
        pass

    def find_build(self, builds, product, type, arch, version, toy='', openssl='', \
                   direct_build_url=None, distribution_version=None, \
                   distribution_type=""):
        if direct_build_url is None:
            if not isinstance(builds, list) and builds.url is not None:
                return builds
            else:
                for build in builds:
                    if build.product_version.find(version) != -1 and product == build.product\
                       and build.architecture_type == arch and type == build.deliverable_type\
                       and build.toy == toy:
                        return build
        elif direct_build_url != "":
            if "exe" in builds.deliverable_type:
                if "rel" in version and version[:5] in WIN_CB_VERSION_3:
                    version = version.replace("-rel", "")
            """ direct url only need one build """

            """ if the job trigger with url, no need to check version.
                remove builds.product_version.find(version) != -1 """
            if product == builds.product and builds.architecture_type == arch:
                return builds
            else:
                self.log.info("if build not found, url link may not match...")
        return None

    def find_membase_build(self, builds, product, deliverable_type, os_architecture, build_version, is_amazon=False):
        if is_amazon:
            build = BuildQuery().find_build(builds, product, deliverable_type,
                                            os_architecture, build_version)
            if build:
                build.url = build.url.replace("http://builds.hq.northscale.net", \
                                                  "http://packages.northscale.com.s3.amazonaws.com")
                build.url = build.url.replace("enterprise", "community")
                build.name = build.name.replace("enterprise", "community")
                return build

        for build in builds:
            if build.product_version.find(build_version) != -1 and product == build.product\
               and build.architecture_type == os_architecture and deliverable_type == build.deliverable_type:
                return build

        return None

    def find_membase_build_with_version(self, builds, build_version):
        for build in builds:
            if build.product_version == build_version or build.product_version.find(build_version) != -1:
                #or if it starts with that version ?
                return build
        return None

    def find_membase_release_build(self, product, deliverable_type, os_architecture,
                                    build_version, is_amazon=False, os_version=""):
        build_details = build_version
        if build_version[:5] in COUCHBASE_VERSION_2_WITH_REL:
            if build_version[-4:] != "-rel":
                build_details = build_details + "-rel"
        elif build_version.startswith("1.8.0"):
            build_details = "1.8.0r-55-g80f24f2"
            product = "couchbase-server-enterprise"
        build = MembaseBuild()
        build.deliverable_type = deliverable_type
        build.time = '0'
        build.size = '0'
        build.product_version = build_version
        build.architecture_type = os_architecture
        build.product = product
        os_name = ""
        build.name = '{1}_{2}_{0}.{3}'.format(build_version, product,
                                               os_architecture, deliverable_type)
        build.build_number = 0
        if deliverable_type == "exe":
            """ /3.0.1/couchbase-server-enterprise_3.0.1-windows_amd64.exe """
            if not re.match(r'[1-9].[0-9].[0-9]$', build_version):
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    arch_type = "amd64"
                    if "x86_64" not in os_architecture:
                        arch_type = "x86"
                    build.url = "{5}{0}/{1}_{4}-windows_{2}.{3}"\
                        .format(build_version[:build_version.find('-')],
                        product, arch_type, deliverable_type, build_details[:5],
                        CB_RELEASE_REPO)
                else:
                    if "2.5.2" in build_details[:5]:
                        build.url = "{5}{0}/{1}_{4}_{2}.setup.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                            build_details[:5], CB_RELEASE_REPO)
                    else:
                        build.url = "{5}{0}/{1}_{2}_{4}.setup.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                            build_details, CB_RELEASE_REPO)
            else:
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    arch_type = "amd64"
                    if "x86_64" not in os_architecture:
                        arch_type = "x86"
                    build.url = "{5}{0}/{1}_{4}-windows_{2}.{3}"\
                        .format(build_version, product, arch_type,
                        deliverable_type, build_details[:5], CB_RELEASE_REPO)
                else:
                    build.url = "{5}{0}/{1}_{2}_{4}.setup.{3}"\
                        .format(build_version, product, os_architecture,
                        deliverable_type, build_details, CB_RELEASE_REPO)
            build.url_latest_build = "{4}{0}_{1}_{3}.setup.{2}"\
                             .format(product, os_architecture, deliverable_type,
                                            build_details, CB_LATESTBUILDS_REPO)
        else:
            """ check match full version x.x.x-xxxx """
            if not re.match(r'[1-9].[0-9].[0-9]$', build_version):
                """  in release folder
                        /3.0.1/couchbase-server-enterprise-3.0.1-centos6.x86_64.rpm
                        /3.0.1/couchbase-server-enterprise_3.0.1-ubuntu12.04_amd64.deb
                        /3.0.2/couchbase-server-enterprise-3.0.2-centos6.x86_64.rpm
                      build release url:
                               http://builds.hq.northscale.net/releases/3.0.1/
                      build latestbuilds url:
                               http://builds.hq.northscale.net/latestbuilds/
                                  couchbase-server-enterprise_x86_64_3.0.1-1444.rpm
                """
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    if "rpm" in deliverable_type:
                        build.url = "{5}{0}/{1}-{4}-centos6.{2}.{3}"\
                                .format(build_version[:build_version.find('-')],
                                product, os_architecture, deliverable_type,
                                        build_details[:5], CB_RELEASE_REPO)
                    elif "deb" in deliverable_type:
                        os_architecture = "amd64"
                        os_name = "ubuntu12.04"
                        if  "ubuntu 14.04" in os_version:
                            os_name = "ubuntu14.04"
                        elif "ubuntu 16.04" in os_version:
                            os_name = "ubuntu16.04"
                        build.url = "{6}{0}/{1}_{4}-{5}_{2}.{3}"\
                                .format(build_version[:build_version.find('-')],
                                 product, os_architecture, deliverable_type,
                                 build_details[:5], os_name, CB_RELEASE_REPO)
                else:
                    if "2.5.2" in build_details[:5]:
                        build.url = "{5}{0}/{1}_{4}_{2}.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                                    build_details[:5], CB_RELEASE_REPO)
                    else:
                        build.url = "{5}{0}/{1}_{2}_{4}.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                                        build_details, CB_RELEASE_REPO)
            else:
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    if "rpm" in deliverable_type:
                        build.url = "{5}{0}/{1}-{4}-centos6.{2}.{3}"\
                            .format(build_version, product, os_architecture,
                            deliverable_type, build_details[:5], CB_RELEASE_REPO)
                    elif "deb" in deliverable_type:
                        os_architecture = "amd64"
                        os_name = "ubuntu12.04"
                        if  "ubuntu 14.04" in os_version:
                            os_name = "ubuntu14.04"
                        elif "ubuntu 16.04" in os_version:
                            os_name = "ubuntu16.04"
                        build.url = "{6}{0}/{1}_{4}-{5}_{2}.{3}"\
                            .format(build_version, product, os_architecture,
                            deliverable_type, build_details[:5], os_name,
                                                         CB_RELEASE_REPO)
                        """ http://builds.hq.northscale.net/releases/3.0.1/
                        couchbase-server-enterprise_3.0.1-ubuntu12.04_amd64.deb """
                else:
                    build.url = "{5}{0}/{1}_{2}_{4}.{3}"\
                        .format(build_version, product, os_architecture,
                        deliverable_type, build_details, CB_RELEASE_REPO)
            build.url_latest_build = "{4}{0}_{1}_{3}.{2}"\
                      .format(product, os_architecture, deliverable_type,
                               build_details, CB_LATESTBUILDS_REPO)
        # This points to the Internal s3 account to look for release builds
        if is_amazon:
            build.url = 'https://s3.amazonaws.com/packages.couchbase/releases/{0}/{1}_{2}_{0}.{3}'\
                .format(build_version, product, os_architecture, deliverable_type)
            build.url = build.url.replace("enterprise", "community")
            build.name = build.name.replace("enterprise", "community")
        return build

    def find_couchbase_release_build(self, product, deliverable_type, os_architecture,
                                    build_version, is_amazon=False, os_version="",
                                    direct_build_url=None):
        build_details = build_version
        if build_version[:5] in COUCHBASE_VERSION_2_WITH_REL:
            if build_version[-4:] != "-rel":
                build_details = build_details + "-rel"
        build = MembaseBuild()
        build.deliverable_type = deliverable_type
        build.time = '0'
        build.size = '0'
        build.product_version = build_version
        build.architecture_type = os_architecture
        build.product = product
        os_name = ""
        build.name = '{1}_{2}_{0}.{3}'.format(build_version, product,
                                               os_architecture, deliverable_type)
        build.build_number = 0
        if deliverable_type == "exe":
            """ /3.0.1/couchbase-server-enterprise_3.0.1-windows_amd64.exe """
            if not re.match(r'[1-9].[0-9].[0-9]$', build_version):
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    arch_type = "amd64"
                    if "x86_64" not in os_architecture:
                        arch_type = "x86"
                    build.url = "{5}{0}/{1}_{4}-windows_{2}.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, arch_type, deliverable_type, build_details[:5],
                            CB_RELEASE_REPO)
                else:
                    if "2.5.2" in build_details[:5]:
                        build.url = "{5}/{0}/{1}_{4}_{2}.setup.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                            build_details[:5], CB_RELEASE_REPO)
                    else:
                        build.url = "{5}{0}/{1}_{2}_{4}.setup.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                            build_details, CB_RELEASE_REPO)
            else:
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    arch_type = "amd64"
                    if "x86_64" not in os_architecture:
                        arch_type = "x86"
                    build.url = "{5}{0}/{1}_{4}-windows_{2}.{3}"\
                        .format(build_version, product, arch_type,
                         deliverable_type, build_details[:5], CB_RELEASE_REPO)
                else:
                    build.url = "{5}{0}/{1}_{2}_{4}.setup.{3}"\
                        .format(build_version, product, os_architecture,
                        deliverable_type, build_details, CB_RELEASE_REPO)
            build.url_latest_build = "{4}{0}_{1}_{3}.setup.{2}"\
                             .format(product, os_architecture,
                             deliverable_type, build_details, CB_LATESTBUILDS_REPO)
        elif deliverable_type == "msi":
            if not re.match(r'[1-9].[0-9].[0-9]$', build_version):
                if build_version[:5] in COUCHBASE_RELEASE_FROM_SPOCK:
                    arch_type = "amd64"
                    if "x86_64" not in os_architecture:
                        arch_type = "x86"
                    build.url = "{5}{0}/{1}_{4}-windows_{2}.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, arch_type, deliverable_type, build_details[:5],
                            CB_RELEASE_REPO)
            else:
                if build_version[:5] in COUCHBASE_RELEASE_FROM_SPOCK:
                    arch_type = "amd64"
                    if "x86_64" not in os_architecture:
                        arch_type = "x86"
                    build.url = "{5}{0}/{1}_{4}-windows_{2}.{3}"\
                        .format(build_version, product, arch_type,
                         deliverable_type, build_details[:5], CB_RELEASE_REPO)
        elif deliverable_type == "zip":
            if not re.match(r'[1-9].[0-9].[0-9]$', build_version):
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    os_name = "macos"
                    build.url = "{6}{0}/{1}_{4}-{5}_{2}.{3}"\
                                .format(build_version[:build_version.find('-')],
                                product, os_architecture, deliverable_type,
                                build_details[:5], os_name, CB_RELEASE_REPO)
        else:
            """ check match full version x.x.x-xxxx """
            if not re.match(r'[1-9].[0-9].[0-9]$', build_version):
                """  in release folder
                        /3.0.1/couchbase-server-enterprise-3.0.1-centos6.x86_64.rpm
                        /3.0.1/couchbase-server-enterprise_3.0.1-ubuntu12.04_amd64.deb
                        /3.0.2/couchbase-server-enterprise-3.0.2-centos6.x86_64.rpm
                      build release url:
                               http://builds.hq.northscale.net/releases/3.0.1/
                      build latestbuilds url:
                               http://builds.hq.northscale.net/latestbuilds/
                                  couchbase-server-enterprise_x86_64_3.0.1-1444.rpm
                """
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    if "rpm" in deliverable_type:
                        if "centos" in os_version.lower():
                            if "centos 7" in os_version.lower():
                                os_name = "centos7"
                            else:
                                os_name = "centos6"
                        elif "suse" in os_version.lower():
                            if "11" in os_version.lower():
                                os_name = "suse11"
                            elif "12" in os_version.lower():
                                os_name = "suse12"
                        elif "oracle linux" in os_version.lower():
                            os_name = "oel6"
                        elif "amazon linux 2" in os_version.lower():
                            if build_version[:5] in COUCHBASE_FROM_MAD_HATTER or \
                                            build_version[:5] in COUCHBASE_FROM_601:
                                os_name = "amzn2"
                            else:
                                self.fail("Amazon Linux 2 doesn't support version %s "
                                                                % build_version[:5])
                        elif "red hat" in os_version.lower():
                            if "8.0" in os_version.lower():
                                os_name = "rhel8"
                        else:
                            os_name = "centos6"
                        build.url = "{6}{0}/{1}-{4}-{5}.{2}.{3}"\
                                .format(build_version[:build_version.find('-')],
                                product, os_architecture, deliverable_type,
                                build_details[:5], os_name, CB_RELEASE_REPO)
                    elif "deb" in deliverable_type:
                        os_architecture = "amd64"
                        os_name = "ubuntu12.04"
                        if  "ubuntu 14.04" in os_version.lower():
                            os_name = "ubuntu14.04"
                        elif "ubuntu 16.04" in os_version.lower():
                            os_name = "ubuntu16.04"
                        elif "ubuntu 18.04" in os_version.lower():
                            if build_version[:5] in COUCHBASE_FROM_MAD_HATTER or \
                                build_version[:5] in COUCHBASE_FROM_601:
                                os_name = "ubuntu18.04"
                            else:
                                self.fail("ubuntu 18.04 doesn't support version %s "
                                                                % build_version[:5])
                        build.url = "{6}{0}/{1}_{4}-{5}_{2}.{3}"\
                                .format(build_version[:build_version.find('-')],
                                 product, os_architecture, deliverable_type,
                                 build_details[:5], os_name, CB_RELEASE_REPO)
                else:
                    if "2.5.2" in build_details[:5]:
                        if product == "moxi-server" and deliverable_type == "deb":
                            build.url = "{5}{0}/{1}_{4}_{2}_openssl098.{3}".format(
                                            build_version[:build_version.find('-')],
                                            product,
                                            os_architecture,
                                            deliverable_type,
                                            build_details[:5],
                                            CB_RELEASE_REPO)
                        else:
                            build.url = "{5}{0}/{1}_{4}_{2}.{3}".format(
                                            build_version[:build_version.find('-')],
                                            product,
                                            os_architecture,
                                            deliverable_type,
                                            build_details[:5],
                                            CB_RELEASE_REPO)
                    else:
                        build.url = "{5}{0}/{1}_{2}_{4}.{3}"\
                            .format(build_version[:build_version.find('-')],
                            product, os_architecture, deliverable_type,
                            build_details, CB_RELEASE_REPO)
            else:
                if build_version[:5] in COUCHBASE_RELEASE_FROM_VERSION_3:
                    if "rpm" in deliverable_type:
                        if "centos" in os_version.lower():
                            if "centos 7" in os_version.lower():
                                os_name = "centos7"
                            else:
                                os_name = "centos6"
                        elif "suse" in os_version.lower():
                            if "11" in os_version.lower():
                                os_name = "suse11"
                            elif "12" in os_version.lower():
                                os_name = "suse12"
                        elif "oracle linux" in os_version.lower():
                            os_name = "oel6"
                        elif "amazon linux 2" in os_version.lower():
                            if build_version[:5] in COUCHBASE_FROM_MAD_HATTER or \
                                            build_version[:5] in COUCHBASE_FROM_601:
                                os_name = "amzn2"
                            else:
                                self.fail("Amazon Linux 2 doesn't support version %s "
                                                                % build_version[:5])
                        else:
                            os_name = "centos6"
                        build.url = "{6}{0}/{1}-{4}-{5}.{2}.{3}"\
                                .format(build_version[:build_version.find('-')],
                                product, os_architecture, deliverable_type,
                                build_details[:5], os_name, CB_RELEASE_REPO)
                    elif "deb" in deliverable_type:
                        os_architecture = "amd64"
                        os_name = "ubuntu12.04"
                        if  "ubuntu 14.04" in os_version.lower():
                            os_name = "ubuntu14.04"
                        elif "ubuntu 16.04" in os_version.lower():
                            os_name = "ubuntu16.04"
                        elif "ubuntu 18.04" in os_version.lower():
                            if build_version[:5] in COUCHBASE_FROM_MAD_HATTER or \
                                build_version[:5] in COUCHBASE_FROM_601:
                                os_name = "ubuntu18.04"
                            else:
                                self.fail("ubuntu 18.04 doesn't support version %s "
                                                                % build_version[:5])
                        build.url = "{6}{0}/{1}_{4}-{5}_{2}.{3}"\
                            .format(build_version, product, os_architecture,
                            deliverable_type, build_details[:5], os_name,
                            CB_RELEASE_REPO)
                else:
                    build.url = "{5}{0}/{1}_{2}_{4}.{3}".format(build_version,
                                product, os_architecture, deliverable_type,
                                build_details, CB_RELEASE_REPO)
            build.url_latest_build = "{4}{0}_{1}_{3}.{2}".format(product,
                        os_architecture, deliverable_type, build_details,
                        CB_LATESTBUILDS_REPO)
        # This points to the Internal s3 account to look for release builds
        if is_amazon:
            """
                for centos only
                         https://s3.amazonaws.com/packages.couchbase.com/releases/
                         4.0.0/couchbase-server-enterprise-4.0.0-centos6.x86_64.rpm """
            build.url = "https://s3.amazonaws.com/packages.couchbase.com/releases/"\
                        "{0}/{1}-{0}-centos6.{2}.{3}" \
                        .format(build_version, product, os_architecture,
                                                       deliverable_type)
            build.url = build.url.replace("enterprise", "community")
            build.name = build.name.replace("enterprise", "community")
        if direct_build_url is not None and deliverable_type != "exe":
            build.url = direct_build_url
        return build

    def sort_builds_by_version(self, builds):
        membase_builds = list()
        for build in builds:
            if build.product == 'membase-server-enterprise':
                membase_builds.append(build)

        return sorted(membase_builds,
                      key=lambda membase_build: membase_build.build_number, reverse=True)

    def sort_builds_by_time(self, builds):
        membase_builds = list()
        for build in builds:
            if build.product == 'membase-server-enterprise':
                membase_builds.append(build)

        return sorted(membase_builds,
                      key=lambda membase_build: membase_build.time, reverse=True)


    def get_latest_builds(self):
        return self._get_and_parse_builds('http://builds.hq.northscale.net/latestbuilds')

    def get_sustaining_latest_builds(self):
        return self._get_and_parse_builds('http://builds.hq.northscale.net/latestbuilds/sustaining')

    def get_all_builds(self, version=None, timeout=None, direct_build_url=None, deliverable_type=None, \
                       architecture_type=None,edition_type=None, repo=None, toy="", \
                       distribution_version=None, distribution_type=""):
        try:
            latestbuilds, latestchanges = \
                self._get_and_parse_builds('http://builds.hq.northscale.net/latestbuilds', version=version, \
                                           timeout=timeout, direct_build_url=direct_build_url, \
                                           deliverable_type=deliverable_type, architecture_type=architecture_type, \
                                           edition_type=edition_type, repo=repo, toy=toy, \
                                           distribution_version=distribution_version, \
                                           distribution_type=distribution_type)
        except Exception as e:
            latestbuilds, latestchanges = \
                self._get_and_parse_builds('http://packages.northscale.com.s3.amazonaws.com/latestbuilds', \
                                           version=version, timeout=timeout, direct_build_url=direct_build_url)

        return latestbuilds, latestchanges


    #baseurl = 'http://builds.hq.northscale.net/latestbuilds/'
    def _get_and_parse_builds(self, build_page, version=None, timeout=None, direct_build_url=None, \
                              deliverable_type=None, architecture_type=None, edition_type=None, \
                              repo=None, toy="", distribution_version=None, distribution_type=""):
        builds = []
        changes = []
        if direct_build_url is not None and direct_build_url != "":
            query = BuildQuery()
            build = query.create_build_info_from_direct_url(direct_build_url)
            return build, changes
        elif repo is not None and edition_type is not None and \
             architecture_type is not None and deliverable_type is not None:
            query = BuildQuery()
            build = query.create_build_url(version, deliverable_type, architecture_type, \
                                           edition_type, repo, toy, distribution_version, \
                                           distribution_type)

            return build, changes
        else:
            page = None
            soup = None
            index_url = '/index.html'
            if version:
                if version.find("-") != -1:
                    index_url = "/index_" + version[:version.find("-")] + ".html"
                else:
                    index_url = "/index_" + version + ".html"
            #try this ten times
            for _ in range(0, 10):
                try:
                    self.log.info("Try collecting build information from url: %s" % (build_page + index_url))
                    if timeout:
                        socket.setdefaulttimeout(timeout)
                    page = urllib.request.urlopen(build_page + index_url)
                    soup = BeautifulSoup.BeautifulSoup(page)
                    break
                except:
                    time.sleep(5)
            if not page:
                raise Exception('unable to connect to %s' % (build_page + index_url))
            query = BuildQuery()
            for incident in soup('li'):
                contents = incident.contents
                build_id = ''
                build_description = ''
                for content in contents:
                    if BeautifulSoup.isString(content):
                        build_description = content.string
                    elif content.name == 'a':
                        build_id = content.string.string
                try:
                    if build_id.lower().startswith('changes'):
                        change = query.create_change_info(build_id, build_description)
                        change.url = '%s/%s' % (build_page, build_id)
                        changes.append(change)
                    else:
                        build = query.create_build_info(build_id, build_description)
                        build.url = '%s/%s' % (build_page, build_id)
                        builds.append(build)
                except Exception as e:
                    print("ERROR in creating build/change info for: Build_id: %s , Build_Description: %s" % (build_id, build_description))
                    print(traceback.print_exc(file=sys.stderr))
                    #raise e : Skipping parsing for this build information,
                    #Eventually, It will fail with build not found error at install.py:240
            for build in builds:
                for change in changes:
                    if change.build_number == build.product_version:
                        build.change = change
                        """ print 'change : ', change.url,change.build_number """
                        break
            return builds, changes

    def create_build_info_from_direct_url(self, direct_build_url):
        if direct_build_url is not None and direct_build_url != "":
            build = MembaseBuild()
            build.url = direct_build_url
            build.toy = ""
            build_info = direct_build_url.split("/")
            build_info = build_info[len(build_info)-1]
            """ windows build name: couchbase_server-enterprise-windows-amd64-3.0.0-892.exe
                                    couchbase-server-enterprise_3.5.0-952-windows_amd64.exe """
            build.name = build_info
            deliverable_type = ["exe", "msi", "rpm", "deb", "zip"]
            if build_info[-3:] in deliverable_type:
                build.deliverable_type = build_info[-3:]
                build_info = build_info[:-4]
            else:
                raise Exception('Check your url. Deliverable type %s does not support yet' \
                                 % (direct_build_url[-3:]))
            """ build name at this location couchbase-server-enterprise_x86_64_3.0.0-797-rel
                windows build name: couchbase_server-enterprise-windows-amd64-3.0.0-892 """

            """ Remove the code below when cb name is standardlized (MB-11372) """
            if "windows" in direct_build_url and build.deliverable_type == "exe" \
                and build_info not in SHERLOCK_VERSION:
                build_info = build_info.replace("-windows-amd64-", "_x86_64_")
                build_info = build_info.replace("couchbase_server", "couchbase-server")
            """ End remove here """

            """ sherlock build name
                centos 6: couchbase-server-enterprise-3.5.0-71-centos6.x86_64
                debian7:  couchbase-server-enterprise_3.5.0-10-debian7_amd64.deb
                debian8:  couchbase-server-enterprise_4.5.0-1194-debian8_amd64.deb
                ubuntu 12.04:
                    couchbase-server-enterprise_3.5.0-723-ubuntu12.04_amd64.deb
                mac:
                    couchbase-server-enterprise_3.5.0-1120-macos_x86_64.zip
                windows:
                    couchbase_server-enterprise-windows-amd64-3.5.0-926.exe
                    couchbase-server-enterprise_3.5.0-952-windows_amd64.exe"""

            if any( x + "-" in build_info for x in COUCHBASE_FROM_VERSION_3):
                deb_words = ["debian7", "debian8", "ubuntu12.04", "ubuntu14.04",
                             "ubuntu16.04", "ubuntu18.04", "windows", "macos"]
                if "centos" not in build_info and "suse" not in build_info:
                    tmp_str = build_info.split("_")
                    product_version = tmp_str[1].split("-")
                    product_version = "-".join([i for i in product_version \
                                                 if i not in deb_words])
                else:
                    product_version = build_info.split("-")
                    product_version = product_version[3] + "-" + product_version[4]
                if product_version[:5] in testconstants.COUCHBASE_VERSIONS:
                    build.product_version = product_version
                    if "centos" not in build_info and "suse" not in build_info:
                        build_info = build_info.replace("_" + product_version, "")
                    else:
                        build_info = build_info.replace("-" + product_version, "")
                if "x86_64" in build_info:
                    build.architecture_type = "x86_64"
                    if "centos" in build_info or "suse" in build_info:
                        build_info = build_info.replace(".x86_64", "")
                    elif "macos" in build_info:
                        build_info = build_info.replace("_x86_64", "")
                elif "x86" in build_info:
                    build.architecture_type = "x86"
                    build_info = build_info.replace(".x86", "")
                elif "_amd64" in build_info:
                    build.architecture_type = "x86_64"
                    build_info = build_info.replace("_amd64", "")
                elif "-amd64" in build_info:
                    build.architecture_type = "x86_64"
                    build_info = build_info.replace("-amd64", "")
                del_words = ["centos6", "debian7", "debian8", "debian9",
                             "ubuntu12.04", "ubuntu14.04", "ubuntu16.04", "ubuntu18.04",
                             "windows", "macos", "centos7", "suse11", "suse12", "amzn2"]
                if build_info.startswith("couchbase-server"):
                    build.product = build_info.split("-")
                    build.product = "-".join([i for i in build.product \
                                                 if i not in del_words])
                return build
            product_version = build_info.split("_")
            product_version = product_version[len(product_version)-1]
            if product_version[:5] in testconstants.COUCHBASE_VERSIONS:
                build.product_version = product_version
                build_info = build_info.replace("_" + product_version, "")
            else:
                raise Exception("Check your url. Couchbase server does not have "
                                       "version %s yet " % (product_version[:5]))

            if "x86_64" in build_info:
                build.architecture_type = "x86_64"
                build_info = build_info.replace("_x86_64", "")
            elif "x86" in build_info:
                build.architecture_type = "x86"
                build_info = build_info.replace("_x86", "")

            if build_info.startswith("couchbase-server"):
                build.product = build_info
            else:
                self.fail("unknown server name")
            return build

    def create_build_url(self, version, deliverable_type, architecture_type,
                              edition_type, repo, toy, distribution_version,
                                                         distribution_type):
        build = MembaseBuild()
        """
        version: 3.0.0-xx or 3.0.0-xx-rel
        deliverable_type: deb
        distribution_version: ubuntu12 or debian7
        architecture_type: x86_64
        edition_type: couchbase-server-enterprise or couchbase-server-community
        repo: http://builds.hq.northscale.net/latestbuilds/
        sherlock repo: http://latestbuilds.hq.couchbase.com/couchbase-server/sherlock
        sherlock build name with extra build number:
               /684/couchbase-server-enterprise-3.5.0-684-centos6.x86_64.rpm
               /1454/couchbase-server-enterprise-4.0.0-1454-centos6.x86_64.rpm
               /1796/couchbase-server-enterprise-4.0.0-1796-oel6.x86_64.rpm
               /723/couchbase-server-enterprise_3.5.0-723-ubuntu12.04_amd64.deb
               /723/couchbase-server-enterprise_3.5.0-732-debian7_amd64.deb
               /1194/couchbase-server-enterprise_4.5.0-1194-debian8_amd64.deb
               /1120/couchbase-server-enterprise_3.5.0-1120-macos_x86_64.zip
        toy=Ce
        build.name = couchbase-server-enterprise_x86_64_3.0.0-xx-rel.deb
        build.url = http://builds.hq.northscale.net/latestbuilds/
                              couchbase-server-enterprise_x86_64_3.0.0-xx-rel.deb
        For toy build: name  =
            couchbase-server-community_cent58-3.0.0-toy-toyName-x86_64_3.0.0-xx-toy.rpm
            http://latestbuilds.hq.couchbase.com/couchbase-server/
                toy-wied/14/couchbase-server-enterprise-1004.0.0-14-centos6.x86_64.rpm
                toy-nimish/16/couchbase-server-enterprise_1003.5.0-16-windows_amd64.exe
        For windows build diff - and _ compare to unix build
                       name = couchbase_server-enterprise-windows-amd64-3.0.0-998.exe
                              couchbase_server-enterprise-windows-amd64-3.0.2-1603.exe
                              couchbase_server-enterprise-windows-amd64-3.0.3-1716.exe
                              couchbase-server-enterprise_3.5.0-952-windows_amd64.exe
                              couchbase-server-enterprise_3.5.0-1390-windows_x86.exe
            From build 5.0.0-2924, we don't make any exe build.
            It will be all in msi
        """
        build.toy = "toy-" + toy
        build.deliverable_type = deliverable_type
        build.architecture_type = architecture_type
        build.distribution_version = distribution_version
        build.distribution_type = distribution_type

        os_name = ""
        setup = ""
        build_number = ""

        unix_deliverable_type = ["deb", "rpm", "zip"]
        if deliverable_type in unix_deliverable_type:
            if toy == "" and version[:5] not in COUCHBASE_VERSION_2 and \
                                   version[:5] not in COUCHBASE_VERSION_3:
                if "rel" not in version and toy == "":
                    build.product_version = version
                elif "-rel" in version:
                    build.product_version = version.replace("-rel", "")
            elif toy != "":
                build.product_version = version
            else:
                if "rel" not in version and toy == "":
                    build.product_version = version + "-rel"
                else:
                    build.product_version = version
        if deliverable_type in ["exe", "msi"]:
            if version[:5] in COUCHBASE_VERSION_2:
                setup = "setup."
            else:
                os_name= "windows-"
            if "rel" in version and version[:5] not in COUCHBASE_VERSION_2:
                build.product_version = version.replace("-rel", "")
            elif "rel" not in version and version[:5] in COUCHBASE_VERSION_2:
                build.product_version = version + "-rel"
            else:
                build.product_version = version
            if "couchbase-server" in edition_type and version[:5] in WIN_CB_VERSION_3:
                edition_type = edition_type.replace("couchbase-", "couchbase_")
            if version[:5] not in COUCHBASE_VERSION_2:
                if "x86_64" in architecture_type:
                    build.architecture_type = "amd64"
                elif "x86" in architecture_type:
                    build.architecture_type = "x86"
            """
                    In spock from build 2924 and later release, we only support
                    msi installation method on windows
            """
            if "-" in version and version.split("-")[0] in COUCHBASE_FROM_SPOCK:
                deliverable_type = "msi"

        if "deb" in deliverable_type and "centos6" in edition_type:
            edition_type = edition_type.replace("centos6", "ubuntu_1204")
        if "debian" in distribution_version:
            os_name = "debian7_"
        joint_char = "_"
        version_join_char = "_"
        if toy is not "":
            joint_char = "-"
        if "exe" in deliverable_type and version[:5] not in COUCHBASE_VERSION_2:
            joint_char = "-"
            version_join_char = "-"
        if toy == "" and version[:5] not in COUCHBASE_VERSION_2 and \
                                   version[:5] not in COUCHBASE_VERSION_3:
            """ format for sherlock build name
            /684/couchbase-server-enterprise-3.5.0-684-centos6.x86_64.rpm
            /1154/couchbase-server-enterprise-3.5.0-1154-centos7.x86_64.rpm
            /1454/couchbase-server-enterprise-4.0.0-1454-centos6.x86_64.rpm
            /1796/couchbase-server-enterprise-4.0.0-1796-oel6.x86_64.rpm
            /723/couchbase-server-enterprise_3.5.0-723-ubuntu12.04_amd64.deb
            /723/couchbase-server-enterprise_3.5.0-732-debian7_amd64.deb
            /795/couchbase_server-enterprise-windows-amd64-3.5.0-795.exe
            /952/couchbase-server-enterprise_3.5.0-952-windows_amd64.exe
            /1390/couchbase-server-enterprise_3.5.0-1390-windows_x86.exe
            /1120/couchbase-server-enterprise_3.5.0-1120-macos_x86_64.zip"""
            build_number = build.product_version.replace(version[:6], "")
            """ distribution version:    centos linux release 7.0.1406 (core)
                distribution version:    centos release 6.5 (final)  """
            rpm_version = "centos6"

            if "centos" in distribution_version or "red hat" in distribution_version or \
               "rhel" in distribution_version:
                if "centos 7" in distribution_version:
                    rpm_version = "centos7"
                elif "red hat enterprise linux server release 6" in distribution_version:
                    rpm_version = "centos6"
                elif "red hat enterprise linux server release 7" in distribution_version:
                    rpm_version = "centos7"
                elif "rhel8" in distribution_version:
                    rpm_version = "rhel8"
                build.name = edition_type + "-" + build.product_version + \
                   "-" + rpm_version + "." + build.architecture_type + \
                   "." + build.deliverable_type
            elif "suse" in distribution_version:
                if "suse 12" in distribution_version:
                    if version[:5] in COUCHBASE_FROM_SPOCK:
                        suse_version="suse12"
                        build.distribution_version = "suse12"
                    else:
                        self.fail("suse 12 does not support on this version %s "
                                                                  % version[:5])
                else:
                    suse_version="suse11"
                    build.distribution_version = "suse11"
                build.name = edition_type + "-" + build.product_version + \
                   "-" + suse_version + "." + build.architecture_type + \
                   "." + build.deliverable_type
            elif "oracle linux" in distribution_version:
                build.distribution_version = "oracle linux"
                os_name = "oel6"
                build.name = edition_type + "-" + build.product_version + \
                   "-" + os_name + "." + build.architecture_type + \
                   "." + build.deliverable_type
            elif "amazon linux release 2" in distribution_version:
                if version[:5] in COUCHBASE_FROM_MAD_HATTER or \
                                version[:5] in COUCHBASE_FROM_601:
                    build.distribution_version = "amazon linux 2"
                    os_name = "amzn2"
                    build.name = edition_type + "-" + build.product_version + \
                     "-" + os_name + "." + build.architecture_type + \
                        "." + build.deliverable_type
                else:
                    self.fail("Amazon Linux 2 doesn't support version %s "
                              % version[:5])
            else:
                os_name = ""
                joint_char = "-"

                """ sherlock build in unix only support 64-bit """
                build.architecture_type = "amd64"
                if  "ubuntu 12.04" in distribution_version:
                    os_name = "ubuntu12.04"
                elif "ubuntu 14.04" in distribution_version:
                    os_name = "ubuntu14.04"
                elif "ubuntu 16.04" in distribution_version:
                    os_name = "ubuntu16.04"
                elif "ubuntu 18.04" in distribution_version.lower():
                    if version[:5] in COUCHBASE_FROM_MAD_HATTER or \
                        version[:5] in COUCHBASE_FROM_601:
                        os_name = "ubuntu18.04"
                    else:
                        self.fail("ubuntu 18.04 doesn't support version %s "
                                                              % version[:5])
                elif "debian gnu/linux 7" in distribution_version:
                    build.distribution_version = "debian7"
                    os_name = "debian7"
                elif "debian gnu/linux 8" in distribution_version:
                    build.distribution_version = "debian8"
                    os_name = "debian8"
                elif "debian gnu/linux 9" in distribution_version:
                    build.distribution_version = "debian9"
                    os_name = "debian9"
                elif "windows" in distribution_version:
                    os_name = "windows"
                    if "x86_64" not in architecture_type:
                        build.architecture_type = "x86"
                elif "mac" in distribution_type:
                    os_name = "macos"
                    build.architecture_type = "x86_64"
                build.name = edition_type + "_" + build.product_version + \
                   joint_char + os_name + "_" +  build.architecture_type + \
                   "." + build.deliverable_type
            build.url = repo + build_number + "/" + build.name
        elif toy is not "":
            centos_version = "centos6"
            build_info = version.split("-")
            build_number = build_info[1]
            if "centos" in distribution_version:
                build.name = edition_type + "-" + build.product_version + \
                   "-" + centos_version + "." + build.architecture_type + \
                   "." + build.deliverable_type
            build.url = repo + build.toy + "/" +build_number \
                        + "/" + build.name
        elif version[:3] == "3.1":
            os_name = ""
            if "suse" in distribution_version:
                build.distribution_version = "suse11"
                os_name = "suse11_"
            elif "centos release 6" in distribution_version:
                build.distribution_version = "centos6"
                os_name = "centos6_"
            elif  "ubuntu 12.04" in distribution_version:
                os_name = "ubuntu_1204_"
            elif "debian gnu/linux 7" in distribution_version:
                build.distribution_version = "debian7"
                os_name = "debian7_"
            elif "debian gnu/linux 8" in distribution_version:
                build.distribution_version = "debian8"
                os_name = "debian8_"
            elif "windows" in distribution_version:
                os_name = "windows-"
                if "x86_64" not in architecture_type:
                    build.architecture_type = "x86"
            elif "mac" in distribution_type:
                build.architecture_type = "x86_64"
            build.name = edition_type + joint_char + os_name + \
                build.architecture_type +  version_join_char + \
                build.product_version + "." + setup + build.deliverable_type
            build.url = repo + build.name
        else:
            build.name = edition_type + joint_char + os_name + \
                build.architecture_type +  version_join_char + \
                build.product_version + "." + setup + build.deliverable_type
            build.url = repo + build.name


        """ reset build.architecture back to x86_64 in windows """
        build.architecture_type = architecture_type
        return build

    def create_build_info(self, build_id, build_decription):
        build = MembaseBuild()
        build.deliverable_type = self._product_deliverable_type(build_id)
        build.time = self._product_time(build_decription)
        build.size = self._product_size(build_decription)
        build.product_version = self._product_version(build_id)
        build.architecture_type = self._product_arch_type(build_id)
        build.product = self._product_name(build_id)
        build.name = build_id
        build.build_number = self._build_number(build)
        build.toy = self._product_toy(build_id)
        return build

    def create_change_info(self, build_id, build_decription):
        change = MembaseChange()
        change.name = build_id.strip()
        change.build_number = self._change_build_number(build_id)
        change.time = self._change_time(build_decription)
        return change


    def _product_name(self, build_id):
        list = build_id.split('_')
        if "centos6" in build_id:
            # return couchbase-server-ent/com_centos6
            return "_".join(list[:2])
        elif "ubuntu_1204" in build_id:
            # return couchbase-server-ent/com_ubuntu_1204
            return "_".join(list[:3])
        # this should be done w/ more generic rule for toy-split
        elif "cent54" in build_id:
            list = build_id.split("-toy")
            return list[0]
        else:
            return list[0]
        #the first one is the product

    def _product_arch_type(self, build_id):
        list = build_id.split('_')
        if '64' in build_id.split('_') or build_id.find('x86_64') != -1:
            return 'x86_64'
        elif 'x86' in build_id.split('_'):
            return 'x86'
        return ''


    def _product_toy(self, build_id):
        r = re.search("[^_]+-toy-([\w-]*)-x86", build_id)
        if r:
            return r.group(1)
        return ''

    def _change_time(self, build_description):
        list = build_description.split('/')
        timestamp = list[1].strip()
        timestamp = timestamp[:timestamp.index(')')]
        return datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')

    def _change_build_number(self, build_id):
        list = build_id.split('_')
        #get list[1] . get rid of .txt
        build_number = list[1].strip()
        if re.search('.txt', build_number):
            build_number = build_number[:build_number.index('.txt')]
            return build_number

    def _build_number(self, build):
        #get the first - and then the first - after that
        first_dash = build.product_version.find('-')
        if first_dash != -1:
            second_dash = build.product_version.find('-', first_dash + 1)
            if second_dash != -1:
                try:
                    return int(build.product_version[first_dash + 1:second_dash])
                except Exception:
                    return -1
        return -1

    def _product_version(self, build_id):
        list = build_id.split('_')
        version_item = ''
        for item in list:
            if re.match(r'[0-2].[0-9].[0-9]-[0-9]+-rel', item):
                version_item = item
                if list[-1].endswith('xml'):
                    break
                return version_item
        if version_item == '':
            for item in list:
                if item.endswith('.setup.exe') or item.endswith('rpm') or\
                   item.endswith('deb') or item.endswith('tar.gz') or item.endswith('zip'):
                    version_item = item
                    break
        if version_item != '':
            if version_item.endswith('.setup.exe'):
                return version_item[:version_item.index('.setup.exe')]
            elif version_item.endswith('.tar.gz'):
                return version_item[:version_item.index('.tar.gz')]
            elif version_item.endswith('.deb'):
                return version_item[:version_item.index('.deb')]
            elif version_item.endswith('.rpm'):
                return version_item[:version_item.index('.rpm')]
            elif version_item.endswith('.zip'):
                return version_item[:version_item.index('.zip')]
        return ''

    def _product_deliverable_type(self, build_id=''):
        list = build_id.split('_')
        version_item = ''
        for item in list:
            if item.endswith('.setup.exe') or item.endswith('rpm') or\
               item.endswith('deb') or item.endswith('tar.gz') or item.endswith('zip'):
                version_item = item
                break
        if version_item != '':
            if version_item.endswith('.setup.exe'):
                return 'exe'
            elif version_item.endswith('.tar.gz'):
                return 'tar.gz'
            elif version_item.endswith('.deb'):
                return 'deb'
            elif version_item.endswith('.rpm'):
                return 'rpm'
            elif version_item.endswith('.zip'):
                return 'zip'
        return ''

    def _product_time(self, build_description):
        list = build_description.split('/')
        timestamp = list[1].strip()
        timestamp = timestamp[:timestamp.index(')')]
        return datetime.strptime(timestamp, '%a %b %d %H:%M:%S %Y')

    def _product_size(self, build_description):
        list = build_description.split('/')
        filesize = list[0]
        filesize = filesize[filesize.index('(') + 1:]
        return filesize.strip()

#q = BuildQuery()
#builds, changes = q.get_latest_builds()
#for build in builds:
#    print build.product,' ',build.time ,' ',build.deliverable_type,' ',build.product_version ,'',build.size,'',build.architecture_type
#    if build.change:
#        change = build.change
#        print change.name,change.build_number,change.time,change.url

#for change in changes:
#    print change.name,change.build_number,change.time

#builds = q.get_membase_latest_builds()
#for build in builds:
#    print build.product,' ',build.time ,' ',build.deliverable_type,' ',build.product_version ,'',build.size,'',build.architecture_type

