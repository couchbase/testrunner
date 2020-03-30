import sys
import urllib2

sys.path.append('lib')

from builds.build_query import BuildQuery

if __name__ == "__main__":
    args = sys.argv
    if len(args) == 2:
        version = args[1]
        builds, changes = BuildQuery().get_all_builds(version=version)
        build = BuildQuery().find_membase_build_with_version(builds, version)
        if build.change and build.change.url:
            try:
                print "downloading {0}".format(build.change.url)
                page = urllib2.urlopen(build.change.url)
                changes = open('changes.txt', 'w')
                changes.write('{0}'.format(page.read()))
                changes.close()
            except urllib2.HTTPError as error:
                print 'unable to download {0}'.format(build.change.url)
