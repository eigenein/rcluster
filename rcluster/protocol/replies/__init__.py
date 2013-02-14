#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Defines classes for Redis Protocol replies.
"""

import collections


StatusReply = collections.namedtuple(
    "StatusReply", [
        "data",
    ],
)


ErrorReply = collections.namedtuple(
    "ErrorReply", [
        "data",
    ],
)


IntegerReply = collections.namedtuple(
    "IntegerReply", [
        "value",
    ],
)


BulkReply = collections.namedtuple(
    "BulkReply", [
        "value",
    ],
)


MultiBulkReply = collections.namedtuple(
    "MultiBulkReply", [
        "values",
    ],
)


class ReplyEncoder:
    """
    Encodes the reply.
    """

    @classmethod
    def encode(cls, reply):
        if reply is None:
            return b"$-1\r\n"
        elif isinstance(reply, StatusReply):
            return cls._encode_status(reply)
        elif isinstance(reply, ErrorReply):
            return cls._encode_error(reply)
        elif isinstance(reply, IntegerReply):
            return cls._encode_integer(reply)
        elif isinstance(reply, BulkReply):
            return cls._encode_bulk(reply)
        elif isinstance(reply, MultiBulkReply):
            return cls._encode_multi_bulk(reply)
        else:
            raise ValueError("Don't know how to encode %s." % repr(reply))

    @classmethod
    def _encode_status(cls, status_reply):
        return b"+" + status_reply.data + b"\r\n"

    @classmethod
    def _encode_error(cls, error_reply):
        return b"-" + error_reply.data + b"\r\n"

    @classmethod
    def _encode_integer(cls, integer_reply):
        return b":" + bytes(str(integer_reply.value), "ascii") + b"\r\n"

    @classmethod
    def _encode_bulk(cls, bulk_reply):
        pass

    @classmethod
    def _encode_multi_bulk(cls, multi_bulk_reply):
        pass
