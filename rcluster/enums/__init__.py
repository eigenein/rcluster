#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class RedisClusterState:
    """
    Represents a Redis cluster state.
    """

    # The cluster can't work.
    FAIL = 0

    # The cluster can work. All the slots are served by connections are not
    # flagged as FAIL.
    OK = 1


class RedisConnectionState:
    """
    Represents a single Redis connection state.
    """

    # The connection is not alive.
    FAIL = 0

    # The connection is considered to be a master connection.
    MASTER = 1

    # The connection is considered to be a slave connection.
    SLAVE = 2
