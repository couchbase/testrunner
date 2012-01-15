from distutils.core import setup
import subprocess
from setuptools import find_packages

def get_version():
    return "1.0.0"

setup(
    name="couchbase-mcsoda",
    version=get_version(),
    description="mcsoda - high performance json/binary load generator",
    author="Couchbase Inc",
    author_email="build@couchbase.com",
    packages=["lib", "pytests/performance"],
    url="http://couchbase.org/",
    download_url="http://.../couchbase-mcsoda.tar.gz",
    keywords=["encoding", "i18n", "xml"],
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.6",
        "Development Status :: 4 - Beta",
        "Environment :: Other Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ],
#    entry_points={
#        'console_scripts': ['mcsoda = pytests.performance:main']
#    },
    long_description=open('README').read(),
    )
