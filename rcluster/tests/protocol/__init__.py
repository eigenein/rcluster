#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import rcluster.protocol
import rcluster.protocol.replies


class TestReplyReader(unittest.TestCase):
    def test_read_status_reply(self):
        result = dict()

        stream = _Stream(b"+OK Hello World\r\n")
        rcluster.protocol._ReplyReader(
            stream,
            callback=lambda reply: self._validate(reply, result),
        ).read()

        self._assert_result(stream, result)
        self.assertEqual(
            rcluster.protocol.replies.StatusReply.reply_type,
            result["reply_type"],
        )
        self.assertEqual(
            b"OK Hello World",
            result["data"],
        )

    def test_read_error_reply(self):
        result = dict()

        stream = _Stream(b"-ERR Hello World\r\n")
        rcluster.protocol._ReplyReader(
            stream,
            callback=lambda reply: self._validate(reply, result),
        ).read()

        self._assert_result(stream, result)
        self.assertEqual(
            rcluster.protocol.replies.ErrorReply.reply_type,
            result["reply_type"],
        )
        self.assertEqual(b"ERR Hello World", result["data"])

    def test_read_integer_reply(self):
        result = dict()

        stream = _Stream(b":12345\r\n")
        rcluster.protocol._ReplyReader(
            stream,
            callback=lambda reply: self._validate(reply, result),
        ).read()

        self._assert_result(stream, result)
        self.assertEqual(
            rcluster.protocol.replies.IntegerReply.reply_type,
            result["reply_type"],
        )
        self.assertEqual(12345, result["value"])

    def _assert_result(self, stream, result):
        self.assertTrue(
            result,
            msg="Result is empty. Stream: %s" % stream,
        )

    def _validate(self, reply, result):
        result["reply_type"] = reply.reply_type
        if hasattr(reply, "data"):
            result["data"] = reply.data
        if hasattr(reply, "value"):
            result["value"] = reply.value


class _Stream:
    """
    Stream stub.
    """

    def __init__(self, data):
        self._data = data
        self._output = b""
        self._is_closed = False

    @property
    def data(self):
        return self._data

    @property
    def output(self):
        return self._output

    def read_bytes(self, num_bytes, callback):
        if self._is_closed:
            raise ValueError("Stream is closed.")
        read_data = self._data[:num_bytes]
        self._data = self._data[num_bytes:]
        callback(read_data)

    def read_until(self, delimiter, callback):
        if self._is_closed:
            raise ValueError("Stream is closed.")
        position = self._data.find(delimiter)
        if position == -1:
            raise ValueError("Delimiter is not found.")
        read_data = self._data[:position]
        self._data = self._data[position + len(delimiter):]
        callback(read_data)

    def write(self, data, callback):
        if self._is_closed:
            raise ValueError("Stream is closed.")
        self._output += data

    def close(self):
        self._is_closed = True

    def __str__(self):
        return str((self._data, self._output))
