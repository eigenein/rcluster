#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import unittest

import rcluster.utilities


class Converter(unittest.TestCase):
    def test_hexlify_bytes(self):
        self.assertEqual(
            "0123456789abcdef",
            rcluster.utilities.Converter.hexlify_bytes(
                b"\x01\x23\x45\x67\x89\xab\xcd\xef",
            ),
        )
