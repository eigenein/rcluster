#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contains shared classes for Redis Cluster Proxy.
"""


class ClusterNodeState:
    """
    Stores the cluster node state.
    """

    def __init__(self, redis):
        self._redis = redis

    def read(self):
        pass


class ClusterState:
    """
    Stores the whole cluster state.
    """

    def __init__(self, redis):
        self._redis = redis

    def read(self):
        pass
