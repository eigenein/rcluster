#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Contains shared classes for Redis Cluster Proxy.
"""


class State:
    """
    Stores the node state.
    """

    # Cluster node roles.
    NONE, MASTER, SLAVE = range(3)

    _role = NONE

    def __init__(self, redis):
        self._redis = redis

    def load(self):
        pass

    @property
    def role(self):
        return self._role
