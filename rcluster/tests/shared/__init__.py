#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import rcluster.shared


class TestModule(unittest.TestCase):
    def test_merge(self):
        self.assertSequenceEqual(
            [11, 18, 27],
            list(rcluster.shared.merge(
                lambda a, b, c: a * b + c,
                [1, 2, 3],
                [4, 5, 6],
                [7, 8, 9],
            )),
        )

    def test_none_coalescing_1(self):
        self.assertEqual(
            0,
            rcluster.shared.none_coalescing(0, None),
        )

    def test_none_coalescing_2(self):
        self.assertEqual(
            0,
            rcluster.shared.none_coalescing(None, 0),
        )
