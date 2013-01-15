#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis


class RedisCluster:
    """
    Implementation of a Redis Cluster.

    This class provides an interface to a cluster of Redis servers.
    """

    def __init__(
        self,
        *connections,
        slot_mask=0xFFF,
        replicaness=2.0,
    ):
        """
        Initializes a new instance.

        :param connections: initial `redis.StrictRedis` instances.
        :param slot_mask: mask used to determine a slot for a key.
        :param replicaness: defines the approximate amount of data copies.
        """

        self._slot_mask = slot_mask
        self._replicaness = replicaness

    @property
    def state(self):
        """
        Gets the cluster state.
        """

        raise NotImplementedError("state")

    @property
    def slot_mask(self):
        """
        Gets the mask used to determine a slot for a key.
        """

        return self._slot_mask

    @property
    def replicaness(self):
        """
        Gets the defined approximate amount of data copies.
        """

        return self._replicaness

    @replicaness.setter
    def replicaness(self, value):
        """
        Defines the approximate amount of data copies.
        """

        raise NotImplementedError("replicaness.setter")

    def add_connection(self, connection):
        """
        Adds the connection to the cluster.
        """

        raise NotImplementedError("add_connection")

    def remove_connection(self, connection):
        """
        Removes the connection from the cluster.
        """

        raise NotImplementedError("remove_connection")

    def refresh_connections(self):
        """
        Updates the connection states and re-assigns connection roles if needed.
        """

        raise NotImplementedError("refresh_connections")
