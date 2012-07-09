#this class will contain methods which we
#use later to
# map a version # -> rpm url
from datetime import datetime
import time
import urllib2
import re
import BeautifulSoup


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
        self.change = None # a MembaseChange

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
        pass

    # let's look at buildlatets or latest/sustaining or any other
    # location
    def parse_builds(self):
        #parse build page and create build object
        pass

    def find_build(self,builds,product,type,arch,version,toy=''):
        for build in builds:
            if build.product_version.find(version) != -1 and product == build.product\
               and build.architecture_type == arch and type == build.deliverable_type\
               and build.toy == toy:
                return build
        return None

    def find_membase_build(self, builds, product, deliverable_type, os_architecture, build_version, is_amazon=False):
        if is_amazon:
            build = BuildQuery().find_build(builds, product, deliverable_type,
                                            os_architecture, build_version)
            if build:
                build.url = build.url.replace("http://builds.hq.northscale.net",\
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

    def find_membase_release_build(self, product, deliverable_type, os_architecture, build_version, is_amazon=False):
        build_details = build_version
        if build_version.startswith("1.7.2"):
            build_details = "1.7.2r-20-g6604356"
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
        build.name = '{1}_{2}_{0}.{3}'.format(build_version, product, os_architecture, deliverable_type)
        build.build_number = 0
        if deliverable_type == "exe":
            build.url = 'http://builds.hq.northscale.net/releases/{0}/{1}_{2}_{4}.setup.{3}'.format(build_version, product, os_architecture, deliverable_type, build_details)
        else:
            build.url = 'http://builds.hq.northscale.net/releases/{0}/{1}_{2}_{4}.{3}'.format(build_version, product, os_architecture, deliverable_type, build_details)
        # This points to the Internal s3 account to look for release builds
        if is_amazon:
            build.url = 'https://s3.amazonaws.com/packages.couchbase/releases/{0}/{1}_{2}_{0}.{3}'.format(build_version, product, os_architecture, deliverable_type)
            build.url = build.url.replace("enterprise", "community")
            build.name = build.name.replace("enterprise", "community")
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

    def get_all_builds(self):
        try:
            latestbuilds, latestchanges =\
                self._get_and_parse_builds('http://builds.hq.northscale.net/latestbuilds')
        except:
            latestbuilds, latestchanges =\
                self._get_and_parse_builds('http://packages.northscale.com.s3.amazonaws.com/latestbuilds')

        try:
            sustaining_builds, sustaining_changes =\
                self._get_and_parse_builds('http://builds.hq.northscale.net/latestbuilds/sustaining')
        except:
            sustaining_builds, sustaining_changes =\
                self._get_and_parse_builds('http://packages.northscale.com.s3.amazonaws.com/latestbuilds/sustaining')

        latestbuilds.extend(sustaining_builds)
        latestchanges.extend(sustaining_changes)
        return latestbuilds, latestchanges


    #baseurl = 'http://builds.hq.northscale.net/latestbuilds/'
    def _get_and_parse_builds(self, build_page):
        builds = []
        changes = []
        page = None
        soup = None
        #try this five times
        for i in range(0, 5):
            try:
                page = urllib2.urlopen(build_page + '/index.html')
                soup = BeautifulSoup.BeautifulSoup(page)
                break
            except:
                time.sleep(1)
        if not page:
            raise Exception('unable to connect to builds.hq')
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
            if build_id.lower().startswith('changes'):
                change = query.create_change_info(build_id, build_description)
                change.url = '%s/%s' % (build_page, build_id)
                changes.append(change)
            else:
                build = query.create_build_info(build_id, build_description)
                build.url = '%s/%s' % (build_page, build_id)
                builds.append(build)
                #now let's reconcile the builds and changes?

        for build in builds:
            for change in changes:
                if change.build_number == build.product_version:
                    build.change = change
                    #                    print 'change : ', change.url,change.build_number
                    break
            #let's filter those builds with version that starts with 'v'
        filtered_builds = []
        for build in builds:
        #            if not '{0}'.format(build.product_version).startswith('v'):
            filtered_builds.append(build)
        return  filtered_builds, changes

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
        return list[0]

        #the first one is the product

    def _product_arch_type(self, build_id):
        list = build_id.split('_')
        if '64' in build_id.split('_'):
            return 'x86_64'
        elif 'x86' in build_id.split('_'):
            return 'x86'
        return ''


    def _product_toy(self, build_id):
        r = re.search("[^_]+_toy-([\w-]*)-x86", build_id)
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
            if item.endswith('.setup.exe') or item.endswith('rpm') or\
               item.endswith('deb') or item.endswith('tar.gz'):
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
        return ''

    def _product_deliverable_type(self, build_id=''):
        list = build_id.split('_')
        version_item = ''
        for item in list:
            if item.endswith('.setup.exe') or item.endswith('rpm') or\
               item.endswith('deb') or item.endswith('tar.gz'):
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

