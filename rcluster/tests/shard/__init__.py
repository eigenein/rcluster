#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import rcluster.shard


class TestShard(unittest.TestCase):
    def test_add_shard(self):
        shard = rcluster.shard.Shard(0)
        shard_id = shard.add_shard("localhost", 6380, 0)

        self.assertTrue(shard_id, "Shard ID is empty.")
        self.assertTrue(shard.is_shard_alive(shard_id), "Shard is not alive.")
