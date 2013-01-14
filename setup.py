#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import setuptools


setuptools.setup(
    # Name and version.
    name="rcluster",
    version="0.0.1",
    # Package directories.
    package_dir={"": "src"},
    packages=setuptools.find_packages("src"),
    # Dependencies.
    install_requires=["redis>=2.7.2"],
    # Package index metadata.
    author="Pavel Perestoronin",
    author_email="eigenein@gmail.com",
    description="Client-side Redis sharding",
    license="Apache License, Version 2.0",
    keywords="cluster redis sharding",
    url="http://eigenein.github.com/rcluster",
)
