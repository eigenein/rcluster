#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from rcluster.protocol.replies import (
    StatusReply,
    ErrorReply,
    IntegerReply,
    BulkReply,
    MultiBulkReply,
    NoneReply,

    ReplyEncoder,
)

import unittest


class TestReplyEncoder(unittest.TestCase):
    def test_encode_status(self):
        self.assertEqual(
            b"+OK\r\n",
            ReplyEncoder.encode(StatusReply(data=b"OK")),
        )

    def test_encode_error(self):
        self.assertEqual(
            b"-ERR\r\n",
            ReplyEncoder.encode(ErrorReply(data=b"ERR")),
        )

    def test_encode_integer(self):
        self.assertEqual(
            b":1000\r\n",
            ReplyEncoder.encode(IntegerReply(value=1000)),
        )

    def test_encode_bulk(self):
        self.assertEqual(
            b"$6\r\nfoobar\r\n",
            ReplyEncoder.encode(BulkReply(data=b"foobar")),
        )

    def test_encode_multi_bulk(self):
        data = ReplyEncoder.encode(MultiBulkReply(replies=[
            BulkReply(data=b"foo"),
            BulkReply(data=b"bar"),
            BulkReply(data=b"Hello"),
            BulkReply(data=b"World"),
        ]))
        self.assertEqual((
                b"*4\r\n"
                b"$3\r\n"
                b"foo\r\n"
                b"$3\r\n"
                b"bar\r\n"
                b"$5\r\n"
                b"Hello\r\n"
                b"$5\r\n"
                b"World\r\n"
            ),
            data,
        )

    def test_encode_none(self):
        self.assertEqual(
            b"$-1\r\n",
            ReplyEncoder.encode(NoneReply()),
        )
