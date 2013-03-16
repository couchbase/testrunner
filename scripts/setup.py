from setuptools import setup

setup(
    name="cbbuildout",
    entry_points={
        "console_scripts": [
            "testrunner = testrunner:main",
            "install = scripts.install:main",
            "do_cluster = pytests.performance.do_cluster:main",
            "post_perf_data = scripts.post_perf_data:main",
            "ssh = scripts.ssh:main",
            "grab_atops = scripts.perf.grab_atops:main",
            "collect_server_info = scripts.collect_server_info:main",
            "active_tasks = scripts.active_tasks:main",
            "aws_ini = scripts.aws_ini:main",
        ],
    },
)