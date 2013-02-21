#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Shard.
"""

import argparse
import logging
import os
import time
import traceback

import redis
import redis.exceptions
import tornado.ioloop

import rcluster.protocol
import rcluster.protocol.exceptions
import rcluster.protocol.replies
import rcluster.shard.exceptions
import rcluster.shared


class Shard(rcluster.protocol.Server):
    def __init__(self, port_number, cluster_state):
        super(Shard, self).__init__(
            port_number=port_number,
            command_handler_factory=self._create_handler,
        )

        self._logger = logging.getLogger("rcluster.shard.Shard")
        self._cluster_state = cluster_state

    @property
    def cluster_state(self):
        return self._cluster_state.state

    def add_shard(self, host, port_number=rcluster.shared.DEFAULT_REDIS_PORT):
        # TODO.
        return self._cluster_state.state

    def _create_handler(self):
        return _ShardCommandHandler(self)


class _ClusterState:
    """
    Stores the cluster state.
    """

    # Global cluster states.

    # The cluster contains no shards.
    EMPTY = 0
    # Available for read, but some slots are not available.
    READ_ONLY_PARTIAL = 1
    # Available for read only.
    READ_ONLY = 2
    # The cluster is OK.
    OK = 3

    # Key templates.

    ## Global cluster state.
    CLUSTER_STATE_KEY = "rcluster:state"
    CLUSTER_STATE_TIMESTAMP_KEY = "rcluster:state:timestamp"

    ## Parent key to store the shard information.
    SHARD_ENTRY_TEMPLATE = "rcluster:shards:%s"

    ## Shard IDs.
    SHARD_IDS_KEY = SHARD_ENTRY_TEMPLATE % "all"

    ## Keys to locate the shard Redis instance.
    SHARD_HOST_KEY_TEMPLATE = SHARD_ENTRY_TEMPLATE % "%s:host"
    SHARD_PORT_KEY_TEMPLATE = SHARD_ENTRY_TEMPLATE % "%s:port"

    def __init__(self, redis):
        """
        Initializes a new instance with the Redis instance
        (typically, the master one).
        """

        self._redis = redis

    @property
    def state(self):
        """
        Gets the global cluster state.
        """

        try:
            return self._redis.get(self.CLUSTER_STATE_KEY) or self.EMPTY
        except Exception as ex:
            raise rcluster.shard.exceptions.ClusterStateOperationError(
                "Failed to get the global cluster state.",
            ) from ex

    @state.setter
    def state(self, new_state):
        """
        Sets the global cluster state.
        """

        try:
            with self._redis.pipeline() as pipeline:
                pipeline.set(self.CLUSTER_STATE_KEY, new_state)
                pipeline.set(
                    self.CLUSTER_STATE_TIMESTAMP_KEY,
                    self._timestamp(),
                )
                pipeline.execute()
        except Exception as ex:
            raise rcluster.shard.exceptions.ClusterStateOperationError(
                "Failed to set the global cluster state.",
            ) from ex

    def _timestamp(self):
        return int(time.time())


class _ShardCommandHandler(rcluster.protocol.CommandHandler):
    def __init__(self, shard):
        super(_ShardCommandHandler, self).__init__({
            b"ADDSHARD": self._get_handler(self._on_add_shard),
        })

        self._shard = shard

    def _get_info(self):
        info = super()._get_info()
        info.extend([
            b"# Cluster",
            b"state:" + bytes(str(self._shard.cluster_state), "ascii"),
            b"",
        ])
        return info

    def _get_handler(self, handler):
        """
        Wraps the handler to handle shard-specific exceptions.
        """

        def wrapper(arguments):
            try:
                return handler(arguments)
            except rcluster.shard.exceptions.ClusterStateOperationError:
                return rcluster.protocol.replies.ErrorReply(
                    data=b"ERR Failed to get or update cluster state.",
                    quit=False,
                )

        wrapper.__doc__ = handler.__doc__
        return wrapper

    def _on_add_shard(self, arguments):
        if len(arguments) == 2:
            try:
                port_number = int(arguments[1])
            except ValueError as ex:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR Port should be an integer.",
                ) from ex
        elif len(arguments) != 1:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> ADDSHARD host [port]",
            )
        else:
            port_number = rcluster.shared.DEFAULT_REDIS_PORT
        host = arguments[0]

        try:
            state = self._shard.add_shard(host, port_number)
        except Exception as ex:
            # TODO.
            return rcluster.protocol.replies.ErrorReply(
                value=b"ERR Internal server error.",
                quit=False,
            )
        else:
            return rcluster.protocol.replies.IntegerReply(value=state)


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
        default=rcluster.shared.DEFAULT_SHARD_PORT,
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
        default=rcluster.shared.DEFAULT_REDIS_PORT,
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
        Shard(args.port_number, _ClusterState(master_redis)).start()

    try:
        logger.info("Redis master is OK, IO loop is being started.")
        logger.info(
            "Will be accepting connections on port %s." % args.port_number,
        )
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt.")
    except:
        logger.fatal(traceback.format_exc())
        return os.EX_SOFTWARE

    return os.EX_OK
