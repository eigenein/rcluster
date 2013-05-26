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

    def test_set_del_key(self):
        shard = rcluster.shard.Shard(0)
        shard_id = shard.add_shard("localhost", 6380, 0)

        key, data = self._key(), os.urandom(32)
        shard.set(key, data)

        shard.delete(key)

        self.assertIsNone(shard.get(key), "Key should be deleted.")

    def test_fault_tolerance_2_shards_2_replicas_1_fault(self):
        shard = rcluster.shard.Shard(0)
        shard.replicaness = 2
        shard.add_shard("localhost", 6380, 0)
        shard2_id = shard.add_shard("localhost", 6381, 0)

        key, data = self._key(), os.urandom(32)
        shard.set(key, data)
        shard.remove_shard(shard2_id)

        self.assertEqual(data, shard.get(key), "Data is not read.")

    def test_timestamp(self):
        shard = rcluster.shard.Shard(0)
        shard.replicaness = 2

        # Set up two shards.
        shard.add_shard("localhost", 6380, 0)
        shard2_id = shard.add_shard("localhost", 6381, 0)

        # Set the key.
        key, data = self._key(), os.urandom(32)
        shard.set(key, data)

        # Remove the "failed" shard.
        shard.remove_shard(shard2_id)

        # Set the new data.
        data = os.urandom(32)
        shard.set(key, data)

        # Add the "restored" shard back.
        shard.add_shard("localhost", 6381, 0)

        # Verify that the new data is returned.
        self.assertEqual(data, shard.get(key), "The new data must be returned.")

    def _key(self):
        return "".join(
            random.choice("abcdef")
            for x in range(32)
        )

    def _shutdown_redis(self, port_number):
        redis.StrictRedis(port=port_number).shutdown()
