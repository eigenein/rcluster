#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Defines classes for Redis Protocol replies.
"""


class _Reply:
    STATUS_REPLY = 0
    ERROR_REPLY = 1
    INTEGER_REPLY = 2
    BULK_REPLY = 3
    MULTI_BULK_REPLY = 4

    def __init__(self, quit=False):
        self.quit = quit

    def __repr__(self):
        return "%s(quit=%s)" % (self.__class__.__name__, self.quit)


class _DataReply(_Reply):
    def __init__(self, data, quit=False):
        super(_DataReply, self).__init__(quit=quit)
        self.data = data

    def __repr__(self):
        return "%s(data=%s, quit=%s)" % (
            self.__class__.__name__,
            self.data,
            self.quit,
        )


class StatusReply(_DataReply):
    reply_type = _Reply.STATUS_REPLY

    def __init__(self, data, quit=False):
        super(StatusReply, self).__init__(data=data, quit=quit)


class ErrorReply(_DataReply):
    reply_type = _Reply.ERROR_REPLY

    def __init__(self, data, quit=True):
        super(ErrorReply, self).__init__(data=data, quit=quit)


class IntegerReply(_Reply):
    reply_type = _Reply.INTEGER_REPLY

    def __init__(self, value=0, quit=False):
        super(IntegerReply, self).__init__(quit=quit)
        self.value = value

    def __repr__(self):
        return "IntegerReply(value=%s, quit=%s)" % (self.value, self.quit)


class BulkReply(_DataReply):
    reply_type = _Reply.BULK_REPLY

    def __init__(self, data, quit=False):
        super(BulkReply, self).__init__(data=data, quit=quit)


class MultiBulkReply(_Reply):
    reply_type = _Reply.MULTI_BULK_REPLY

    def __init__(self, replies=[], quit=False):
        super(MultiBulkReply, self).__init__(quit=quit)
        self.replies = replies

    def __repr__(self):
        return "MultiBulkReply(replies=%s, quit=%s)" % (
            repr(self.replies),
            self.quit,
        )


class ReplyEncoder:
    """
    Encodes the reply.
    """

    @classmethod
    def encode(cls, reply):
        if reply is None:
            return b"$-1\r\n"
        elif reply.reply_type == _Reply.STATUS_REPLY:
            return cls._encode_status(reply)
        elif reply.reply_type == _Reply.ERROR_REPLY:
            return cls._encode_error(reply)
        elif reply.reply_type == _Reply.INTEGER_REPLY:
            return cls._encode_integer(reply)
        elif reply.reply_type == _Reply.BULK_REPLY:
            return cls._encode_bulk(reply)
        elif reply.reply_type == _Reply.MULTI_BULK_REPLY:
            return cls._encode_multi_bulk(reply)

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
        return (
            b"$" + bytes(str(len(bulk_reply.data)), "ascii") + b"\r\n" +
            bulk_reply.data + b"\r\n"
        )

    @classmethod
    def _encode_multi_bulk(cls, multi_bulk_reply):
        return (
            b"*" + bytes(str(len(multi_bulk_reply.replies)), "ascii") +
            b"\r\n" +
            b"".join(
                cls._encode_bulk(bulk_reply)
                for bulk_reply in multi_bulk_reply.replies
            )
        )
