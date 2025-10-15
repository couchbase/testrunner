from distutils.core import setup 


def get_version():
    return "1.0.0"

setup(
    name="cbtestlib",
    version=get_version(),
    description="Couchbase test library",
    long_description="Couchbase test library",
    author="Couchbase Inc",
    author_email="build@couchbase.com",
    install_requires=["paramiko",
                      "pymongo", 'couchbase', 'docker', 'requests'],
    packages=["cbtestlib",
              "cbtestlib.builds",
              "cbtestlib.cbkarma",
              "cbtestlib.couchbase",
              "cbtestlib.couchdb",
              "cbtestlib.membase",
              "cbtestlib.membase.api.httplib2",
              "cbtestlib.membase.api",
              "cbtestlib.membase.helper",
              "cbtestlib.membase.performance",
              "cbtestlib.memcached",
              "cbtestlib.memcached.helper",
              "cbtestlib.tasks",
              "cbtestlib.remote",
              "cbtestlib.perf_engines",
              "cbtestlib.perf_engines.libobserve",
              "cbtestlib.perf_engines.libstats",],
    url="http://www.couchbase.com/",
    keywords=["encoding", "i18n", "xml"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
