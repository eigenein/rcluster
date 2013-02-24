#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import random
import unittest

import redis

import rcluster.shard


class TestShard(unittest.TestCase):
    def test_add_shard(self):
        shard = rcluster.shard.Shard(0)
        shard_id = shard.add_shard("localhost", 6380, 0)

        self.assertTrue(shard_id, "Shard ID is empty.")
        self.assertTrue(shard.is_shard_alive(shard_id), "Shard is not alive.")

    def test_remove_shard(self):
        shard = rcluster.shard.Shard(0)
        shard_id = shard.add_shard("localhost", 6380, 0)
        shard.remove_shard(shard_id)

        self.assertFalse(
            shard.is_shard_alive(shard_id),
            "Removed shard is alive.",
        )

    def test_set_get_key(self):
        shard = rcluster.shard.Shard(0)
        shard.add_shard("localhost", 6380, 0)

        key, data = self._key(), os.urandom(32)
        shard.set(key, data)

        self.assertEqual(data, shard.get(key), "Data is not read.")

    def test_fault_tolerance_2_shards_2_replicas_1_fault(self):
        shard = rcluster.shard.Shard(0)
        shard.replicaness = 2
        shard.add_shard("localhost", 6380, 0)
        shard2_id = shard.add_shard("localhost", 6381, 0)

        key, data = self._key(), os.urandom(32)
        shard.set(key, data)
        shard.remove_shard(shard2_id)

        self.assertEqual(data, shard.get(key), "Data is not read.")

    def test_shutdown_redis(self):
        shard = rcluster.shard.Shard(0)
        shard.add_shard("localhost", 6380, 0)

        key, data = self._key(), os.urandom(32)
        shard.set(key, data)
        self._shutdown_redis(6380)

        # Check that this will not fail.
        self.assertIsNone(shard.get(key), "Data must be unavailable.")

    def _key(self):
        return "".join(
            random.choice("abcdef")
            for x in range(32)
        )

    def _shutdown_redis(self, port_number):
        redis.StrictRedis(port=port_number).shutdown()
