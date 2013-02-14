#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Protocol classes.
"""

import logging
import traceback

import tornado.netutil

import rcluster.protocol.exceptions
import rcluster.protocol.replies


class CommandHandler:
    """
    Base command handler.
    """

    def __init__(self):
        self._logger = logging.getLogger("rcluster.protocol.CommandHandler")
        self._handlers = {
            b"PING": self._on_ping,
        }

    def handle(self, command, arguments):
        self._logger.info("Requesting the %s command.", command)

        handler = self._handlers.get(command.upper())

        if handler:
            return handler(arguments)
        else:
            raise rcluster.protocol.exceptions.UnknownCommandError()

    def _on_ping(self, arguments):
        if not arguments:
            return rcluster.protocol.replies.StatusReply(data=b"PONG")
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Wrong number of arguments.",
            )


class Server(tornado.netutil.TCPServer):
    """
    Redis Protocol server.
    """

    def __init__(
        self,
        port_number=6381,
        command_handler_factory=CommandHandler,
    ):
        super(Server, self).__init__()

        self._logger = logging.getLogger("rcluster.protocol.Server")
        self._port_number = port_number
        self._command_handler_factory = command_handler_factory

    def start(self):
        self.listen(self._port_number)

    def handle_stream(self, stream, address):
        self._logger.info("Accepted connection from %s.", address)
        _StreamHandler(
            stream,
            address,
            self._command_handler_factory(),
        ).start()


class _StreamHandler:
    """
    Handles the client connection.
    """

    def __init__(self, stream, address, command_handler):
        self._logger = logging.getLogger("rcluster.protocol._StreamHandler")
        self._stream = stream
        self._address = address
        self._command_handler = command_handler

        self._stream.set_close_callback(self._on_disconnected)

    def start(self):
        """
        Starts requests processing.
        """

        self._serve_request()

    def _serve_request(self):
        """
        Serves the incoming request.
        """

        _RequestHandler(
            self._stream,
            self._command_handler,
            self._serve_request,
        ).handle()

    def _on_disconnected(self):
        """
        Called when client has disconnected.
        """

        self._logger.info("Connection with %s is closed.", self._address)


class _RequestHandler:
    """
    Handles a single incoming request.
    """

    def __init__(self, stream, command_handler, callback):
        self._logger = logging.getLogger("rcluster.protocol._RequestHandler")
        self._stream = stream
        self._command_handler = command_handler
        self._callback = callback

        self._arguments = list()

    def handle(self):
        self._stream.read_until(b"\r\n", callback=self._on_read_argument_count)

    def _on_read_argument_count(self, data):
        """
        Called when the line with the argument count is read.
        """

        try:
            if not data or data[0] != ord("*"):
                raise ValueError()
            self._argument_count = int(data[1:].rstrip())
        except ValueError:
            self._reply(rcluster.protocol.replies.ErrorReply(
                    data=b"ERR *<number of arguments> CR LF is expected.",
                ),
                close_stream=True,
            )
        else:
            if self._argument_count > 0:
                self._stream.read_until(b"\r\n", self._on_read_argument_length)
            else:
                # There is no request - just skip any processing.
                self._callback()

    def _on_read_argument_length(self, data):
        """
        Called when the line with the argument length is read.
        """

        try:
            if not data or data[0] != ord("$"):
                raise ValueError()
            argument_length = int(data[1:].rstrip())
        except ValueError:
            self._reply(rcluster.protocol.replies.ErrorReply(
                    data=b"ERR $<number of bytes of argument> CR LF is expected.",
                ),
                close_stream=True,
            )
        else:
            if argument_length > 0:
                self._stream.read_bytes(
                    argument_length,
                    callback=self._on_read_argument_value,
                )
            elif argument_length == 0:
                self._on_read_argument_value(bytes(0))
            else:
                # Negative argument length is treated as None value.
                self._on_read_argument_value(None)

    def _on_read_argument_value(self, data):
        """
        Called when the argument value is read.
        """

        self._arguments.append(data)
        self._argument_count -= 1
        self._stream.read_until(
            b"\r\n",
            callback=(
                self._on_read_argument
                if self._argument_count
                else self._on_read_request
            ),
        )

    def _on_read_argument(self, data):
        """
        Called when the argument value tail is read.
        """

        self._stream.read_until(
            b"\r\n",
            callback=self._on_read_argument_length,
        )

    def _on_read_request(self, data):
        """
        Called when the entire request is read. The last argument tail
        is dropped.
        """

        # The very first argument is a command.
        command, *arguments = self._arguments

        try:
            reply = self._command_handler.handle(command, arguments)
        except rcluster.protocol.exceptions.CommandError as ex:
            reply = rcluster.protocol.replies.ErrorReply(
                data=ex.data,
            )
            close_stream = ex.close_stream
        except rcluster.protocol.exceptions.UnknownCommandError:
            reply = rcluster.protocol.replies.ErrorReply(
                data=b"ERR Unknown command: " + command,
            )
            close_stream = True
        except:
            self._logger.error(traceback.format_exc())
            reply = rcluster.protocol.replies.ErrorReply(
                data=b"ERR Internal server error.",
            )
            close_stream = True
        else:
            close_stream = False

        self._reply(reply, close_stream)

    def _reply(self, reply, close_stream=False):
        self._stream.write(
            rcluster.protocol.replies.ReplyEncoder.encode(reply),
            callback=(
                self._callback if not close_stream
                else self._stream.close
            ),
        )
