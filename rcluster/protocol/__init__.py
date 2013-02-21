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

    def __init__(self, handlers={}):
        self._logger = logging.getLogger("rcluster.protocol.CommandHandler")
        self._handlers = {
            b"PING": self._on_ping,
            b"ECHO": self._on_echo,
            b"QUIT": self._on_quit,
            b"INFO": self._on_info,
        }
        self._handlers.update(handlers)

    def handle(self, command, arguments):
        self._logger.debug("%s %s", command, repr(arguments))

        handler = self._handlers.get(command.upper())

        if handler:
            return handler(arguments)
        else:
            raise rcluster.protocol.exceptions.UnknownCommandError()

    def _get_info(self):
        """
        Gets the server state information.
        """

        return [
            b"# Server",
            b"commands:" + b",".join(self._handlers.keys()),
            b"",
        ]

    def _on_ping(self, arguments):
        if not arguments:
            return rcluster.protocol.replies.StatusReply(data=b"PONG")
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> PING",
            )

    def _on_echo(self, arguments):
        if len(arguments) == 1:
            return rcluster.protocol.replies.BulkReply(data=arguments[0])
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> ECHO data",
            )

    def _on_info(self, arguments):
        if not arguments:
            info = self._get_info()
            if not info:
                raise ValueError("Info should not be None or empty.")
            return rcluster.protocol.replies.BulkReply(data=b"\r\n".join(info))
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> INFO",
            )

    def _on_quit(self, arguments):
        if not arguments:
            return rcluster.protocol.replies.StatusReply(
                data=b"OK Bye!",
                quit=True,
            )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> QUIT",
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
            self._reply(
                rcluster.protocol.replies.ErrorReply(
                    data=b"ERR *<number of arguments> CR LF is expected.",
                ),
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
            self._reply(
                rcluster.protocol.replies.ErrorReply(
                    data=(
                        b"ERR $<number of bytes of argument>"
                        b" CR LF is expected."
                    ),
                ),
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
        except rcluster.protocol.exceptions.UnknownCommandError:
            reply = rcluster.protocol.replies.ErrorReply(
                data=b"ERR Unknown command: " + command,
            )
        except:
            self._logger.error(traceback.format_exc())
            reply = rcluster.protocol.replies.ErrorReply(
                data=b"ERR Internal server error.",
            )

        self._logger.debug("%s", repr(reply))
        self._reply(reply)

    def _reply(self, reply):
        data = rcluster.protocol.replies.ReplyEncoder.encode(reply)
        self._logger.debug("%s", data)
        if data is None:
            raise ValueError("Invalid reply value.")
        self._stream.write(
            data,
            callback=(
                self._callback if not reply.quit
                else self._stream.close
            ),
        )
