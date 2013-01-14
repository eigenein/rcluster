#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import setuptools


setuptools.setup(
    # Name and version.
    name="rcluster",
    version="0.0.1",
    # Package directories.
    packages=["rcluster", "rcluster.tests"],
    # Dependencies.
    install_requires=["redis>=2.7.2"],
    # Enable "setup.py test".
    test_suite="rcluster.tests",
    # Package index metadata.
    author="Pavel Perestoronin",
    author_email="eigenein@gmail.com",
    maintainer="Pavel Perestoronin",
    maintainer_email="eigenein@gmail.com",
    description="Client-side Redis sharding",
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
    ]
)
