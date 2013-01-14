#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import find_packages, setup


setup(
    name="rcluster",
    version="0.0.1",
    packages=find_packages(),
    install_requires=["redis>=2.7.2"],
    author="Pavel Perestoronin",
    author_email="eigenein@gmail.com",
    description="Client-side Redis sharding",
    license="Apache License, Version 2.0",
    keywords="cluster redis sharding",
    url="http://eigenein.github.com/rcluster",
)
