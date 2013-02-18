#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Shard.
"""

import argparse
import logging
import os
import traceback

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


def _create_argument_parser():
    parser = argparse.ArgumentParser(
        description=globals()["__doc__"],
        formatter_class=argparse.RawTextHelpFormatter,
        prog="rcluster-shard",
    )
    parser.add_argument(
        "--log-level",
        dest="log_level",
        type=str,
        metavar="LEVEL",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"],
        default="DEBUG",
        help="logging level (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        dest="port_number",
        type=int,
        metavar="PORT",
        default=6380,
        help="port number to listen to (default: %(default)s)",
    )
    return parser


def entry_point():
    args = _create_argument_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
    )

    Shard(args.port_number).start()

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logging.info("Keyboard interrupt.")
    except:
        logging.fatal(traceback.format_exc())
        return os.EX_SOFTWARE

    return 0
