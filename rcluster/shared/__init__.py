#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared variables, functions and classes.
"""

import heapq


# The key that stores the shard ID.
SHARD_ID_KEY = "rcluster:shard:id"

# Default Redis server port.
DEFAULT_REDIS_PORT = 6379
# Default port to connect to Redis Cluster.
DEFAULT_SHARD_PORT = 6380

# The mask to convert a hash into the slot number.
HASH_MASK = 0xFFF
SLOT_COUNT = 4096


class PriorityQueue:
    """
    Wraps heapq module functions into the class.
    http://stackoverflow.com/a/8875823/359730
    """

    def __init__(self, initial=None, key=lambda value: value):
        self.key = key
        if initial:
            self._data = [(key(item), item) for item in initial]
            heapq.heapify(self._data)
        else:
            self._data = []

    def push(self, item):
        """
        Push the value item onto the heap, maintaining the heap invariant.
        """

        heapq.heappush(self._data, (self.key(item), item))

    def pop(self):
        """
        Pop and return the smallest item from the heap, maintaining the heap
        invariant. If the heap is empty, IndexError is raised.
        """
        return heapq.heappop(self._data)[1]

    def __iter__(self):
        # Select only the payload, not the key.
        return (t[1] for t in self._data)
