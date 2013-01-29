#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contains Redis proxy classes.
"""

import tornado.netutil
import tornado.web

import redis


class ClusterNode:
    """
    The cluster node. Maintains the state of the cluster node and
    communications with other cluster nodes.
    """

    _application = None

    _redis = None

    def __init__(
        self,
        cluster_state,
        node_state,
        port_number=6380,
        db_number=0,
    ):
        self._cluster_state = cluster_state
        self._node_state = node_state
        self._port_number = port_number
        self._db_number = db_number

    @property
    def db_number(self):
        return self._db_number

    @property
    def port_number(self):
        return self._port_number

    def start(self):
        self._redis = redis.StrictRedis(
            db=self._db_number,
        )
        self._application = tornado.web.Application([
            # (r"/", MainHandler),
        ])
        self._application.listen(self._port_number)


class Interface:
    """
    The cluster node external interface. Maintains communications with clients.
    """

    _server = None

    def __init__(
        self,
        cluster_state,
        node_state,
        port_number=6381,
    ):
        self._cluster_state = cluster_state
        self._node_state = node_state
        self._port_number = port_number

    @property
    def port_number(self):
        return self._port_number

    def start(self):
        self._server = tornado.netutil.TCPServer()
        self._server.listen(self._port_number)
