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
        """
        Starts the node.
        """

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
        # Initialize the state.
        self._state = rcluster.proxy.shared.State(self._redis)
        self._state.load()
        # Initialize the web application to listen for other nodes.
        self.listen(self._port_number)

    @property
    def state(self):
        return self._state

    def __id__(self):
        """
        Gets the node ID.
        """

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


class Interface(tornado.netutil.TCPServer):
    """
    The cluster node external interface. Maintains communications with clients.
    """

    _logger = logging.getLogger("Interface")

    def __init__(
        self,
        node,
        port_number=6381,
    ):
        super(Interface, self).__init__()

        self._node = node
        self._port_number = port_number

    @property
    def port_number(self):
        return self._port_number

    def start(self):
        self.listen(self._port_number)

    def handle_stream(self, stream, address):
        self._logger.info("Accepted connection from %s.", address)
        _InterfaceClientConnection(self._node, address, stream).start()


class _InterfaceClientConnection:
    """
    Serves the interface client connection.
    """

    _logger = logging.getLogger("_InterfaceClientConnection")

    def __init__(self, node, address, stream):
        self._node = node
        self._address = address
        self._stream = stream
        self._stream.set_close_callback(self._disconnected)

    def start(self):
        """
        Starts requests processing.
        """

        self._serve_request()

    def _serve_request(self):
        """
        Serves the upcoming request.
        """

        _InterfaceClientRequest(self._stream, self._serve_request).process()

    def _disconnected(self):
        """
        Called when client has disconnected.
        """

        self._logger.info("Connection with %s is closed.", self._address)


class _InterfaceClientRequest:
    """
    Represents a single request.
    """

    _logger = logging.getLogger("_InterfaceClientRequest")

    def __init__(self, stream, callback):
        self._stream = stream
        # Callback that should be called after the request is processed.
        self._callback = callback
        # Arguments left.
        self._argument_count = 0
        # Read arguments.
        self._arguments = list()
        # Command handlers.
        self._handlers = {
            b"PING": self._ping,
            b"ECHO": self._echo,
            b"QUIT": self._quit,
        }

    def process(self):
        """
        Processes the upcoming request.
        """

        self._stream.read_until(b"\r\n", callback=self._read_argument_count)

    def _read_argument_count(self, data):
        """
        Called when the line with the argument count is read.
        """

        try:
            line = data.decode("ascii")
            if not line or line[0] != "*":
                raise ValueError()
            self._argument_count = int(line[1:].rstrip())
        except ValueError:
            self._send_status(
                b"-ERR *<number of arguments> CR LF is expected.",
                close=True,
            )
        else:
            if self._argument_count > 0:
                self._stream.read_until(b"\r\n", self._read_argument_length)
            else:
                # There is no request - just skip any processing.
                self._callback()

    def _read_argument_length(self, data):
        """
        Called when the line with the argument length is read.
        """

        try:
            line = data.decode("ascii")
            if not line or line[0] != "$":
                raise ValueError()
            argument_length = int(line[1:].rstrip())
        except ValueError:
            self._send_status(
                b"-ERR $<number of bytes of argument> CR LF is expected.",
                close=True,
            )
        else:
            if argument_length > 0:
                self._stream.read_bytes(
                    argument_length,
                    callback=self._read_argument,
                )
            elif argument_length == 0:
                self._read_argument(bytes(0))
            else:
                # Negative argument length is treated as None value.
                self._read_argument(None)

    def _read_argument(self, data):
        """
        Called when the argument value is read.
        """

        self._arguments.append(data)
        self._argument_count -= 1
        self._stream.read_until(
            b"\r\n",
            callback=(
                self._read_argument_finish
                if self._argument_count
                else self._read_request
            ),
        )

    def _read_argument_finish(self, data):
        """
        Called when the argument value tail is read.
        """

        self._stream.read_until(b"\r\n", callback=self._read_argument_length)

    def _read_request(self, data):
        """
        Called when the entire request is read. The last argument tail
        is dropped.
        """

        handler = self._handlers.get(self._arguments[0])
        if handler:
            handler()
        else:
            self._send_status(b"-ERR Unknown command: " + self._arguments[0])

    def _send_status(self, status, close=False):
        """
        Sends the status back to the client.
        """

        self._stream.write(
            status + b"\r\n",
            callback=self._callback if not close else self._stream.close,
        )

    def _send_value(self, value):
        self._stream.write(self._encode_value(value), callback=self._callback)

    def _encode_value(self, value):
        if value is None:
            return b"$-1\r\n"
        elif isinstance(value, int):
            return b":" + bytes(str(value), "ascii") + b"\r\n"
        elif isinstance(value, bytes):
            return (
                b"$" +
                bytes(str(len(value)), "ascii") +
                b"\r\n" +
                value +
                b"\r\n"
            )
        else:
            raise ValueError("Don't know how to encode: %s" % value)

    def _ping(self):
        """
        PING handler.
        """

        if len(self._arguments) == 1:
            self._send_status(b"+PONG")
        else:
            self._send_status(b"-ERR Wrong number of arguments.")

    def _echo(self):
        """
        ECHO handler.
        """

        if len(self._arguments) == 2:
            self._send_value(self._arguments[1])
        else:
            self._send_status(b"-ERR Wrong number of arguments.")

    def _quit(self):
        """
        QUIT handler.
        """

        if len(self._arguments) == 1:
            self._send_status(b"+BYE", close=True)
        else:
            self._send_status(b"-ERR Wrong number of arguments.")
