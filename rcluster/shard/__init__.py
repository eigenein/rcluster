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
import uuid

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
    def state(self):
        return self._cluster_state.state

    @property
    def replicaness(self):
        return self._cluster_state.replicaness

    @property
    def shards(self):
        return dict(self._cluster_state.shards)

    def start(self):
        super().start()
        # Initial balancing.
        self._refresh_state()
        self._do_balancing()

    def add_shard(self, host, port_number=rcluster.shared.DEFAULT_REDIS_PORT):
        try:
            shard_id = uuid.uuid4()
            connection = redis.StrictRedis(host, port_number)
            # Obtain the shard ID.
            if not connection.setnx(
                rcluster.shared.SHARD_ID_KEY,
                shard_id,
            ):
                shard_id = connection.get(rcluster.shared.SHARD_ID_KEY)
        except redis.exceptions.ConnectionError as ex:
            raise rcluster.exceptions.ShardIsNotAvailable() from ex
        else:
            self._cluster_state.add_shard(
                shard_id,
                host,
                port_number,
                _ClusterState.SHARD_OK,
            )
            # Refresh state.
            self._refresh_state()
            # And re-balance.
            self._do_balancing()
            # Finally, return the new state.
            return self._cluster_state.state

    def _create_handler(self):
        return _ShardCommandHandler(self)

    def _refresh_state(self):
        shards = self._cluster_state.shards

        if not shards:
            self._cluster_state.state = _ClusterState.EMPTY
            return

        # Collect unavailable slots.
        unavailable_slots = {
            slot for slot in self._cluster_state.slots
            # If there is no any available shard with this slot.
            if not any(
                self._shards[shard_id]["state"] == _ClusterState.SHARD_OK
                for shard_id in slot["shards"]
                if shard_id in self._shards
            )
        }
        # Check for unavailable slots.
        if unavailable_slots:
            self._cluster_state.state = _ClusterState.PARTIAL
        else:
            self._cluster_state.state = _ClusterState.OK

    def _do_balancing(self):
        # TODO.
        pass


class _ClusterState:
    """
    Stores the cluster state.
    """

    # Global cluster states.

    ## The cluster contains no shards.
    EMPTY = 0
    ## Available for read only as some slots are not available.
    PARTIAL = 1
    ## The cluster is OK.
    OK = 2

    # Shard states.

    ## The shard is OK.
    SHARD_OK = 0
    ## The shard is unavailable or failing.
    SHARD_UNAVAILABLE = 1

    # Key templates.

    ## Global cluster state.
    CLUSTER_STATE_KEY = "rcluster:state"
    CLUSTER_STATE_TIMESTAMP_KEY = "rcluster:state:timestamp"
    CLUSTER_REPLICANESS_KEY = "rcluster:state:replicaness"

    ## Parent key to store the shard information.
    SHARD_ENTRY_TEMPLATE = "rcluster:shards:%s"

    ## Shard IDs.
    SHARD_IDS_KEY = SHARD_ENTRY_TEMPLATE % "all"

    ## Keys to locate the shard Redis instance.
    SHARD_HOST_KEY_TEMPLATE = SHARD_ENTRY_TEMPLATE % "%s:host"
    SHARD_PORT_KEY_TEMPLATE = SHARD_ENTRY_TEMPLATE % "%s:port"
    SHARD_STATE_KEY_TEMPLATE = SHARD_ENTRY_TEMPLATE % "%s:state"

    def __init__(self, redis):
        """
        Initializes a new instance with the Redis instance
        (typically, the master one).
        """

        self._logger = logging.getLogger("rcluster.shard._ClusterState")

        self._redis = redis
        # Cached global cluster state.
        self._state = None
        # Cached replicaness value.
        self._replicaness = 1
        # Cached shards information.
        self._shards = {}
        # Cached slots information.
        self._slots = {}

    @property
    def state(self):
        """
        Gets the global cluster state.
        """

        return self._state

    @state.setter
    def state(self, value):
        if value == self._state:
            return value
        try:
            self._redis.set(_ClusterState.CLUSTER_STATE_KEY, value)
        except Exception as ex:
            raise rcluster.shard.exceptions.ClusterStateOperationError(
                "Failed to get the cluster state.",
            ) from ex
        else:
            self._state = value
            self._logger.info("Cluster state changed to %s.", repr(value))
            return value

    @property
    def replicaness(self):
        return self._replicaness

    @property
    def shards(self):
        """
        Gets shards information.
        """

        return self._shards

    @property
    def slots(self):
        return self._slots

    def initialize(self):
        """
        Initializes the cluster state.
        """

        try:
            self._state = int(self._redis.get(
                _ClusterState.CLUSTER_STATE_KEY,
            ) or _ClusterState.EMPTY)
            self._logger.info(
                "Initial cluster state is %s.",
                repr(self._state),
            )
            self._replicaness = int(self._redis.get(
                _ClusterState.CLUSTER_REPLICANESS_KEY,
            ) or 1)
            shard_ids = self._redis.smembers(
                _ClusterState.SHARD_IDS_KEY,
            ) or []

            for shard_id in shard_ids:
                shard_id_str = str(shard_id, "utf-8")
                self._shards[shard_id] = {
                    "host": self._redis.get(
                        _ClusterState.SHARD_HOST_KEY_TEMPLATE % shard_id_str,
                    ),
                    "port": int(self._redis.get(
                        _ClusterState.SHARD_PORT_KEY_TEMPLATE % shard_id_str,
                    )),
                    "state": int(self._redis.get(
                        _ClusterState.SHARD_STATE_KEY_TEMPLATE % shard_id_str,
                    )),
                }
        except Exception as ex:
            raise rcluster.shard.exceptions.ClusterStateOperationError(
                "Failed to get the cluster state.",
            ) from ex

    def add_shard(self, shard_id, host, port_number, state):
        """
        Adds the shard to the cluster state or updates the shard information.
        """

        if isinstance(shard_id, str):
            shard_id_str, shard_id = shard_id, bytes(shard_id, "utf-8")
        else:
            shard_id_str = str(shard_id, "utf-8")
        if isinstance(host, str):
            host = bytes(shard_id, "utf-8")

        try:
            with self._redis.pipeline(transaction=False) as pipeline:
                pipeline.set(
                    _ClusterState.SHARD_HOST_KEY_TEMPLATE % shard_id_str,
                    host,
                )
                pipeline.set(
                    _ClusterState.SHARD_PORT_KEY_TEMPLATE % shard_id_str,
                    port_number,
                )
                pipeline.set(
                    _ClusterState.SHARD_STATE_KEY_TEMPLATE % shard_id_str,
                    state,
                )
                pipeline.set(
                    _ClusterState.CLUSTER_STATE_TIMESTAMP_KEY,
                    self._timestamp(),
                )
                pipeline.sadd(
                    _ClusterState.SHARD_IDS_KEY,
                    shard_id,
                )
                pipeline.execute()
        except Exception as ex:
            raise rcluster.shard.exceptions.ClusterStateOperationError(
                "Failed to add the shard.",
            ) from ex
        else:
            self._shards[shard_id] = {
                "host": host,
                "port_number": port_number,
                "state": state,
            }

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
        info.update({
            b"Cluster": {
                b"state": bytes(str(self._shard.state), "ascii"),
                b"replicaness": bytes(str(self._shard.replicaness), "ascii"),
            },
            b"Shards": {
                shard_id: (
                    shard["host"] + b" " +
                    bytes(str(shard["port_number"]), "ascii") + b" " +
                    bytes(str(shard["state"]), "ascii")
                ) for shard_id, shard in self._shard.shards.items()
            },
        })
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
        logger.info("Redis master instance is OK.")

    logger.info("Initializing the cluster state ...")
    cluster_state = _ClusterState(master_redis)
    cluster_state.initialize()

    logger.info("Done. Starting the shard ...")
    Shard(args.port_number, cluster_state).start()
    logger.info("Done. The cluster state is %s." % cluster_state.state)

    logger.info("IO loop is being started.")
    logger.info(
        "Will be accepting connections on port %s." % args.port_number,
    )

    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt.")
    except:
        logger.fatal(traceback.format_exc())
        return os.EX_SOFTWARE

    return os.EX_OK
