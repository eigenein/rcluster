#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Protocol classes.
"""

import itertools
import logging
import traceback

import tornado.tcpserver

import rcluster.protocol.exceptions
import rcluster.protocol.replies


class CommandHandler:
    """
    Base command handler.
    """

    def __init__(self, password=None, handlers={}):
        self._logger = logging.getLogger("rcluster.protocol.CommandHandler")
        self._password, self._authenticated = password, False
        self._handlers = {
            b"AUTH": self._on_auth,
            b"ECHO": self._on_echo,
            b"INFO": self._on_info,
            b"PING": self._on_ping,
            b"QUIT": self._on_quit,
        }
        self._handlers.update(handlers)

    def handle(self, command, arguments):
        self._logger.debug("%s %s", command, repr(arguments))

        command = command.upper()
        handler = self._handlers.get(command)

        if handler:
            if command != b"AUTH" and self._password and not self._authenticated:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR Not authenticated.",
                )
            return handler(arguments)
        else:
            raise rcluster.protocol.exceptions.UnknownCommandError()

    def _get_info(self, section=None):
        """
        Gets the server state information.
        """

        return {
            b"Server": {
                b"commands": b",".join(sorted(self._handlers.keys())),
            },
        } if section is None or section == b"Server" else dict()

    def _serialize_info_section(self, section_name, section):
        """
        Serializes the info section into an iterable of byte strings.
        """

        return itertools.chain(
            (b"# " + section_name, ),
            (name + b":" + value for name, value in section.items()),
        )

    def _on_auth(self, arguments):
        if len(arguments) == 1:
            if not self._password:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR Client sent AUTH, but no password is set.",
                )
            password = arguments[0].decode("utf-8")
            self._authenticated = (password == self._password)
            if not self._authenticated:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR Invalid password.",
                )
            return rcluster.protocol.replies.StatusReply(data=b"Authenticated.")
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> AUTH password",
            )

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
        if len(arguments) > 1:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> INFO [section]",
            )
        info = self._get_info(arguments[0] if arguments else None)
        return rcluster.protocol.replies.BulkReply(data=b"\r\n".join(
            itertools.chain(*(
                self._serialize_info_section(section_name, section)
                for section_name, section in info.items()
            )),
        ) + b"\r\n")

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


class Server(tornado.tcpserver.TCPServer):
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
            if reply is None:
                reply = rcluster.protocol.replies.NoneReply()
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
