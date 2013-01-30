#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contains Redis proxy classes.
"""

import logging
import os

import tornado.netutil
import tornado.web

import redis

import rcluster.proxy.handlers
import rcluster.proxy.shared


class ClusterNode(tornado.web.Application):
    """
    The cluster node. Maintains the state of the cluster node and
    communications with other cluster nodes.
    """

    # Node ID length, in bytes.
    NODE_ID_LENGTH = 20

    # The logger instance.
    _logger = logging.getLogger("ClusterNode")

    # Tornado web application.
    _application = None

    # Redis connection.
    _redis = None

    # Node ID.
    _node_id = None

    # Cluster state.
    _cluster_state = None

    # Node state.
    _node_state = None

    def __init__(
        self,
        port_number=6380,
        db_number=1,
    ):
        super(ClusterNode, self).__init__([
            (r"/ping", rcluster.proxy.handlers.PingHandler),
        ])

        self._port_number = port_number
        self._db_number = db_number

    @property
    def db_number(self):
        return self._db_number

    @property
    def port_number(self):
        return self._port_number

    @property
    def node_id(self):
        return self._node_id

    def start(self):
        self._redis = redis.StrictRedis(
            db=self._db_number,
        )
        # Test connection.
        self._redis.ping()
        # Generate a node ID.
        node_id = os.urandom(self.NODE_ID_LENGTH)
        # Try to update the node ID.
        if not self._redis.setnx("node_id", node_id):
            # Read previously stored node ID.
            self._node_id = self._redis.get("node_id")
        else:
            # The node ID is successfully stored.
            self._node_id = node_id
        # Initialize the cluster state.
        self._cluster_state = rcluster.proxy.shared.ClusterState(self._redis)
        self._cluster_state.read()
        # Initialize the node state.
        self._node_state = rcluster.proxy.shared.ClusterNodeState(self._redis)
        self._node_state.read()
        # Initialize the web application to listen for other nodes.
        self.listen(self._port_number)

    def __id__(self):
        return self._node_id

    def log_request(self, handler):
        """
        Overrides the default function in order to customize log messages.
        """

        if "log_function" in self.settings:
            self.settings["log_function"](handler)
            return
        if handler.get_status() < 400:
            log_method = self._logger.debug
        elif handler.get_status() < 500:
            log_method = self._logger.warning
        else:
            log_method = self._logger.error
        request_time = 1000.0 * handler.request.request_time()
        log_method(
            "%d %s %.2fms",
            handler.get_status(),
            handler._request_summary(),
            request_time,
        )


class Interface:
    """
    The cluster node external interface. Maintains communications with clients.
    """

    _server = None

    def __init__(
        self,
        node,
        port_number=6381,
    ):
        self._node = node
        self._port_number = port_number

    @property
    def port_number(self):
        return self._port_number

    def start(self):
        self._server = tornado.netutil.TCPServer()
        self._server.listen(self._port_number)
