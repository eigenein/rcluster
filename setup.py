#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import setuptools


class _CustomCommand(setuptools.Command):

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


class Pep8Command(_CustomCommand):

    description = "perform PEP8 checks"

    def run(self):
        build_py_command = self.get_finalized_command('build_py')
        paths = [path for (_, _, path) in build_py_command.find_all_modules()]

        import pep8
        style_guide = pep8.StyleGuide()
        check_report = style_guide.check_files(paths)
        check_report.print_statistics()

        if check_report.total_errors:
            raise SystemExit(os.EX_DATAERR)


class FlakeyCommand(_CustomCommand):

    description = "perform flakey checks"

    def run(self):
        import flakey

        build_py_command = self.get_finalized_command('build_py')
        error_count = 0
        for (_, _, path) in build_py_command.find_all_modules():
            warning = flakey.check_path(path)
            if isinstance(warning, flakey.checker.Checker):
                error_count += flakey.print_messages(warning)
            else:
                raise ValueError("Unexpected check_path result.")

        if error_count:
            raise SystemExit(os.EX_DATAERR)


setuptools.setup(
    # Name and version.
    name="rcluster",
    version="0.0.1",
    # Package directories.
    packages=[
        "rcluster",
        "rcluster.protocol",
        "rcluster.protocol.exceptions",
        "rcluster.protocol.replies",
        "rcluster.shard",
        "rcluster.shared",
        "rcluster.tests",
        "rcluster.tests.protocol",
    ],
    # Entry points.
    entry_points={
        "console_scripts": [
            "rcluster-shard = rcluster.shard:entry_point",
        ],
    },
    # Other files.
    package_data={
    },
    # Dependencies.
    install_requires=[
        # Tornado is used for all communications.
        "tornado>=2.4.1",
    ],
    # Custom commands.
    cmdclass={
        "pep8": Pep8Command,
        "flakey": FlakeyCommand,
    },
    # Enable "setup.py test".
    test_suite="rcluster.tests",
    # Allow archiving.
    zip_safe=True,
    # Package index metadata.
    author="Pavel Perestoronin",
    author_email="eigenein@gmail.com",
    maintainer="Pavel Perestoronin",
    maintainer_email="eigenein@gmail.com",
    description="Client-side Redis sharding",
    long_description=open("README.markdown", "rt").read(),
    license="Apache License, Version 2.0",
    keywords=["cluster", "redis", "sharding"],
    url="https://github.com/eigenein/rcluster",
    classifiers=[
        "Development Status :: 1 - Planning",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
    ],
)
