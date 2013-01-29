#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contains shared classes for Redis Cluster Proxy.
"""

import rcluster.utilities


class ClusterNodeState:
    """
    Stores the cluster node state.
    """

    pass


class ClusterState:
    """
    Stores the whole cluster state.
    """

    def __init__(self):
        self._time_stamp = rcluster.utilities.Timestamp.get()
