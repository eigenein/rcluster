#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Common utility functions and classes.
"""

import binascii
import datetime


class Timestamp:

    epoch_start = datetime.datetime(year=1970, month=1, day=1)

    @classmethod
    def get(cls):
        """
        Gets the current UTC time in UNIX timestamp format.
        """

        return datetime.datetime.utcnow() - cls.epoch_start


class Converter:

    @classmethod
    def hexlify_bytes(cls, bytes):
        return binascii.hexlify(bytes).decode("ascii")
