from setuptools import setup

setup(
    name="cbbuildout",
    entry_points={
        "console_scripts": [
            "testrunner = testrunner:main",
            "install = scripts.install:main",
            "do_cluster = pytests.performance.do_cluster:main",
        ],
    },
)