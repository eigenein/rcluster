#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
The module contains request handlers for communications between the cluster
nodes.
"""

import json

import tornado.web

import rcluster.utilities


class PingHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(json.dumps(
            obj={
                "node_id": rcluster.utilities.Converter.hexlify_bytes(
                    self.application.node_id,
                ),
                "role": self.application.state.role,
            },
            indent=2,
        ))

    def set_default_headers(self):
        self.set_header("Content-Type", "application/json")
