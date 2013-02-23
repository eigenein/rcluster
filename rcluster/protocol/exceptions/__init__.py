#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis protocol exceptions.
"""


class UnknownCommandError(Exception):
    """
    Unknown command is read.
    """

    pass


class CommandError(Exception):
    def __init__(self, data, close_stream=False):
        super(CommandError, self).__init__("Command error.")

        self._data = data
        self._close_stream = close_stream

    @property
    def data(self):
        return self._data

    @property
    def close_stream(self):
        return self._close_stream
