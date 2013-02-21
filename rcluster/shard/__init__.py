#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Shard.
"""

import argparse
import logging
import os
import traceback

import redis
import redis.exceptions
import tornado.ioloop

import rcluster.protocol


class Shard(rcluster.protocol.Server):
    def __init__(self, port_number, cluster_state):
        super(Shard, self).__init__(
            port_number=port_number,
            command_handler_factory=self._create_handler,
        )

        self._cluster_state = cluster_state

    def _create_handler(self):
        return _ShardCommandHandler(self)


class _ShardCommandHandler(rcluster.protocol.CommandHandler):
    def __init__(self, shard):
        super(_ShardCommandHandler, self).__init__()

        self._shard = shard


class _ClusterState:
    """
    Stores the cluster state.
    """

    def __init__(self, redis):
        """
        Initializes a new instance with the Redis instance
        (typically, the master one).
        """

        self._redis = redis


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
    parser.add_argument(
        "--master-host",
        dest="master_host",
        type=str,
        metavar="HOST",
        default="localhost",
        help="Redis master instance host (default: %(default)s)",
    )
    parser.add_argument(
        "--master-port",
        dest="master_port",
        type=int,
        metavar="PORT",
        default=6379,
        help="Redis master instance port (default: %(default)s)",
    )
    return parser


def entry_point():
    args = _create_argument_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
    )
    logger = logging.getLogger("rcluster.shard")

    master_redis = redis.StrictRedis(
        host=args.master_host,
        port=args.master_port,
    )
    logger.info("Ping Redis master instance ...")
    try:
        master_redis.ping()
    except redis.exceptions.ConnectionError:
        logger.fatal("Redis master instance is not available.")
        return os.EX_UNAVAILABLE
    else:
        Shard(args.port_number, master_redis).start()

    try:
        logger.info("Redis master is OK, IO loop is being started.")
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt.")
    except:
        logger.fatal(traceback.format_exc())
        return os.EX_SOFTWARE

    return os.EX_OK
