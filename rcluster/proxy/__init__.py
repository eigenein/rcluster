#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contains Redis proxy classes.
"""


class ClusterNode:
    """
    The cluster node. Maintains the state of the cluster node and
    communications with other cluster nodes.
    """

    def __init__(
        self,
        cluster_state,
        node_state,
        port_number=6380,
    ):
        self._cluster_state = cluster_state
        self._node_state = node_state
        self._port_number = port_number

    def start(self):
        pass


class Interface:
    """
    The cluster node external interface. Maintains communications with clients.
    """

    def __init__(
        self,
        cluster_state,
        node_state,
        port_number=6379,
    ):
        self._cluster_state = cluster_state
        self._node_state = node_state
        self._port_number = port_number

    def start(self):
        pass
