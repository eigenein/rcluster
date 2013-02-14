#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Shard.
"""

import logging

import tornado.ioloop

import rcluster.protocol


class Shard(rcluster.protocol.Server):
    def __init__(self, port_number=6379):
        super(Shard, self).__init__(
            port_number=port_number,
            command_handler_factory=self._create_handler,
        )

    def _create_handler(self):
        return _ShardCommandHandler(self)


class _ShardCommandHandler(rcluster.protocol.CommandHandler):
    def __init__(self, shard):
        super(_ShardCommandHandler, self).__init__()

        self._shard = shard


# TODO: parse options.
# TODO: setup logging level.
# TODO: setup port.
# TODO: and etc.
def entry_point():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
    )

    Shard(6380).start()

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass
