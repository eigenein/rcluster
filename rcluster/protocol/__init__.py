#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Protocol classes.
"""

import itertools
import logging
import traceback
import socket

import tornado.iostream
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

    def _get_info(self, section=None):
        """
        Gets the server state information.
        """

        return {
            b"Server": {
                b"commands": b",".join(self._handlers.keys()),
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

    def _on_ping(self, arguments):
        if not arguments:
            return rcluster.protocol.replies.StatusReply(data=b"PONG")
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> PING",
            )

    def _on_echo(self, arguments):
        if len(arguments) == 1:
            return rcluster.protocol.replies.BulkReply(data=arguments[0].data)
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> ECHO data",
            )

    def _on_info(self, arguments):
        if len(arguments) > 1:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> INFO [section]",
            )
        info = self._get_info(arguments[0].data if arguments else None)
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
        self._address = address
        self._command_handler = command_handler

        self._stream = stream
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

        _MultiBulkReplyReader(
            stream=self._stream,
            callback=self._on_read_request,
            read_reply_type=True,
        ).read()

    def _on_read_request(self, request):
        """
        Called when the entire request is read. The last argument tail
        is dropped.
        """

        # The very first argument is a command.
        command, *arguments = request.replies

        try:
            reply = self._command_handler.handle(command.data, arguments)
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
            raise ValueError("Reply is None.")
        self._stream.write(
            data,
            callback=(
                self._serve_request if not reply.quit
                else self._read_reader.close
            ),
        )

    def _on_disconnected(self):
        """
        Called when client has disconnected.
        """

        self._logger.info("Connection with %s is closed.", self._address)


class _ReplyReader:
    """
    Reads Protocol Redis replies.
    """

    def __init__(self, stream, callback):
        self._logger = logging.getLogger("rcluster.protocol._ReplyReader")
        self._stream = stream
        self._callback = callback

    def read(self):
        # Read the reply type.
        self._stream.read_bytes(1, callback=self._on_read_reply_type)

    def _on_read_reply_type(self, data):
        reply_type = data[0]
        if reply_type == ord("+"):
            self._stream.read_until(
                b"\r\n",
                callback=self._on_read_status_reply,
            )
        elif reply_type == ord("-"):
            self._stream.read_until(
                b"\r\n",
                callback=self._on_read_error_reply,
            )
        elif reply_type == ord(":"):
            self._stream.read_until(
                b"\r\n",
                callback=self._on_read_integer_reply,
            )
        elif reply_type == ord("$"):
            # Reply type is already read.
            _BulkReplyReader(self._stream, self._callback, False).read()
        elif reply_type == ord("*"):
            # Reply type is already read.
            _MultiBulkReplyReader(self._stream, self._callback, False).read()
        else:
            self._stream.write(
                rcluster.protocol.replies.ReplyEncoder.encode(
                    rcluster.protocol.replies.ErrorReply(
                        data=b"ERR Unknown reply type: " +
                        bytes(repr(reply_type), "ascii"),
                    )
                ),
                callback=self._stream.close,
            )

    def _on_read_status_reply(self, data):
        self._callback(rcluster.protocol.replies.StatusReply(data=data))

    def _on_read_error_reply(self, data):
        self._callback(rcluster.protocol.replies.ErrorReply(data=data))

    def _on_read_integer_reply(self, data):
        try:
            value = int(str(data, "ascii"))
        except ValueError as ex:
            self._stream.write(
                rcluster.protocol.replies.ReplyEncoder.encode(
                    rcluster.protocol.replies.ErrorReply(
                        data=b"ERR ValueError: " +
                        bytes(str(ex), "utf-8"),
                    )
                ),
                callback=self._stream.close,
            )
        else:
            self._callback(rcluster.protocol.replies.IntegerReply(value=value))


class _BulkReplyReader:
    """
    Reads BulkReply.
    """

    def __init__(
        self,
        stream,
        callback,
        read_reply_type,
    ):
        self._stream = stream
        self._callback = callback
        self._read_reply_type = read_reply_type
        pass

    def read(self):
        pass


class _MultiBulkReplyReader:
    def __init__(
        self,
        stream,
        callback,
        read_reply_type,
    ):
        self._stream = stream
        self._callback = callback
        self._read_reply_type = read_reply_type
        pass

    def read(self):
        # Should use _BulkReplyReader with read_reply_type=True.
        pass


class Client:
    """
    Redis Protocol client.
    """

    def __init__(
        self,
        host="localhost",
        port_number=6379,
        db=0,
    ):
        self._address = (host, port_number)
        self._db = db
        self._stream = tornado.iostream.IOStream(
            socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0),
        )

    def gets(self, keys):
        # TODO: _execute_commands(...)
        pass

    def setnx(self, key, data):
        self._execute_command(
            [b"SETNX", key, data],
        )

    def _execute_commands(self, arguments):
        """
        Executes the commands synchronously.
        """

        # TODO: init events, set in a local callback function and wait for
        # the events.
        pass

    def _execute_command_async(self, arguments, callback):
        """
        Executes the command asynchronously.
        """

        data = rcluster.protocol.replies.ReplyEncoder.encode(
            rcluster.protocol.replies.MultiBulkReply(
                replies=(
                    rcluster.protocol.replies.BulkReply(
                        data=argument,
                    )
                    for argument in arguments
                ),
            )
        )
        self._stream.write(data, callback=callback)
