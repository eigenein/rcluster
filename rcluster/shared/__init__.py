#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared variables, functions and classes.
"""

import heapq


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
        return (item[1] for item in self._data)
