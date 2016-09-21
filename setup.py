from setuptools import setup, find_packages

def get_version():
    return "2.0.0"

setup(
    name="testrunner",
    version=get_version(),
    description="Couchbase test library",
    author="Couchbase Inc",
    author_email="build@couchbase.com",
    install_requires=["couchbase",
        "decorator",
        "ecdsa",
        "Fabric",
        "iniparse",
        "mercurial",
        "paramiko",
        "pycrypto",
        "pycurl",
        "pygpgme",
        "urlgrabber",
        "yum-metadata-parser",
        "httplib2",
        "boto",
        "fabric",
        "futures",
        "gevent",
        "greenlet",
        "btrc",
        "urllib3"
    ],
    packages =find_packages(),
    include_package_data=True,
    package_data={
          'lib': [
              '*.py'],
    },
    entry_points={
        'console_scripts': [
            'testrunner = testrunner.__main__:main'
       ]
    }, 
    url="http://www.couchbase.com/",
    keywords=["encoding", "i18n", "xml"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Development Status :: 5 - Production/Stable",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)

