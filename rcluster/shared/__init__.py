#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shared variables, functions and classes.
"""

# The key that stores the shard ID.
SHARD_ID_KEY = "rcluster:shard:id"

# Default Redis server port.
DEFAULT_REDIS_PORT = 6379
# Default port to connect to Redis Cluster.
DEFAULT_SHARD_PORT = 6380

# The mask to convert a hash into the slot number.
HASH_MASK = 0xFFF
SLOT_COUNT = 4096
