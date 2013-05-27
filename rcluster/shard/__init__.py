#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Shard.
"""

import argparse
import datetime
import logging
import operator
import os
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
    SHARD_ID_KEY = "rcluster:shard:id"

    EPOCH = datetime.datetime(1970, 1, 1)

    def __init__(self, port_number, password=None):
        super(Shard, self).__init__(
            port_number=port_number,
            command_handler_factory=self._create_handler,
        )

        self._logger = logging.getLogger("rcluster.shard.Shard")
        self._replicaness = 1
        self._connections = dict()
        self._db_size = dict()
        self._password = password

    @property
    def replicaness(self):
        return self._replicaness

    @replicaness.setter
    def replicaness(self, replicaness):
        self._replicaness = replicaness

    def add_shard(self, host, port_number, db):
        self._logger.info(
            "Adding shard: %s:%s/%d ...",
            host,
            port_number,
            db,
        )
        connection = redis.StrictRedis(
            host=host,
            port=port_number,
            db=db,
        )
        shard_id = bytes(uuid.uuid4().hex, "utf-8")
        try:
            if not connection.setnx(Shard.SHARD_ID_KEY, shard_id):
                shard_id = connection.get(Shard.SHARD_ID_KEY)
            db_size = connection.dbsize()
        except redis.exceptions.ConnectionError as ex:
            raise rcluster.shard.exceptions.ShardConnectionError(
                "Could not connect to the specified shard.",
            ) from ex
        else:
            self._connections[shard_id] = connection
            self._db_size[shard_id] = db_size
            self._logger.info(
                "Shard %s is added (db_size: %s).",
                shard_id,
                db_size,
            )
            return shard_id

    def remove_shard(self, shard_id):
        try:
            del self._connections[shard_id]
        except KeyError:
            pass

    def is_shard_alive(self, shard_id):
        """
        Checks whether the connection to the specified shard is alive.
        """

        connection = self._connections.get(shard_id)
        if connection is None:
            return False
        try:
            connection.ping()
        except redis.exceptions.ConnectionError:
            return False
        else:
            return True

    def get(self, key):
        latest_timestamp, latest_data = 0, None

        for shard_id, connection in self._connections.items():
            try:
                with connection.pipeline(transaction=True) as pipeline:
                    data_key, timestamp_key = self._wrap_key(key)
                    pipeline.get(data_key).get(timestamp_key).dbsize()
                    data, timestamp, db_size = pipeline.execute()
            except redis.exceptions.ConnectionError:
                # Failed to get the value from this shard. It is failed -
                # just ignore it.
                pass
            else:
                # Timestamp might not be set for the first time.
                timestamp = (timestamp and int(timestamp)) or 0
                if not latest_timestamp or latest_timestamp < timestamp:
                    latest_data, latest_timestamp = data, timestamp
                # Update DBSIZE.
                self._db_size[shard_id] = db_size

        return latest_data

    def set(self, key, data):
        """
        Sets the specified key.
        """

        # Wrap key name.
        data_key, timestamp_key = self._wrap_key(key)
        # Find available shards from the least busy.
        shards = (
            (shard_id, self._connections[shard_id], db_size)
            # Sort by db_size increasing.
            for shard_id, db_size in sorted(
                self._db_size.items(),
                key=operator.itemgetter(1),
            )
            # Check that the connection is still available.
            if shard_id in self._connections
        )

        while True:
            # Replicas counter - we need self._replicaness keys set
            # with this timestamp.
            timestamp, replicas_left = self.time(), self._replicaness
            try:
                for shard_id, connection, db_size in shards:
                    try:
                        with connection.pipeline(transaction=True) as pipeline:
                            pipeline.watch(data_key)
                            pipeline.watch(timestamp_key)
                            pipeline.multi()

                            # Delete old data.
                            pipeline.delete(data_key)
                            pipeline.delete(timestamp_key)

                            # Determine whether we also need to set the key.
                            set_key = replicas_left != 0
                            if set_key:
                                if data is not None:
                                    pipeline.set(data_key, data)
                                else:
                                    pipeline.delete(data_key)
                                pipeline.set(timestamp_key, timestamp)

                            # Anyway, update cached DBSIZE value.
                            pipeline.dbsize()

                            # DBSIZE is the last item.
                            self._db_size[shard_id] = pipeline.execute()[-1]
                    except redis.exceptions.ConnectionError as ex:
                        self._logger.debug(str(ex))
                        # Skip failed target.
                    else:
                        if set_key:
                            # We set the replica.
                            replicas_left -= 1
            except redis.exceptions.WatchError:
                # Other rcluster.shard has modified the key - retry.
                continue
            else:
                # All transactions has succeeded.
                break

        # Success if the key is set at least once.
        return replicas_left != self._replicaness

    def delete(self, key):
        return self.set(key, None)

    def time(self):
        """
        Gets the current time.
        """

        return self._to_timestamp(datetime.datetime.now())

    def lastsave(self):
        latest_timestamp = 0

        for shard_id, connection in self._connections.items():
            try:
                timestamp = self._to_timestamp(connection.lastsave()) // 1000000
            except redis.exceptions.ConnectionError:
                # Failed to get the value from this shard. It is failed -
                # just ignore it.
                pass
            else:
                if not latest_timestamp or latest_timestamp < timestamp:
                    latest_timestamp = timestamp

        return latest_timestamp

    def _to_timestamp(self, time):
        delta = time - self.EPOCH
        return delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 1000000

    def _wrap_key(self, key):
        rc_key = b"rc:" + bytes(key, "utf-8")
        return rc_key, rc_key + b":ts"

    def _create_handler(self):
        return _ShardCommandHandler(self, self._password)


class _ShardCommandHandler(rcluster.protocol.CommandHandler):
    def __init__(self, shard, password=None):
        super(_ShardCommandHandler, self).__init__(
            password, {
                b"ADDSHARD": self._on_add_shard,
                b"CONFIG": self._on_config,
                b"DEL": self._on_del,
                b"GET": self._on_get,
                b"LASTSAVE": self._on_lastsave,
                b"SET": self._on_set,
                b"SETREPLICANESS": self._on_set_replicaness,
                b"TIME": self._on_time,
            },
        )

        self._logger = logging.getLogger("rcluster.shard._ShardCommandHandler")
        self._shard = shard

    def _get_info(self, section):
        info = super()._get_info(section)
        if section is None or section == b"Shards":
            status = b"".join(
                b"." if self._shard.is_shard_alive(shard_id) else b"F"
                for shard_id in self._shard._connections
            )
            count = bytes(str(len(self._shard._connections)), "ascii")
            info.update({
                b"Shards": {
                    b"count": count,
                    b"status": status,
                },
            })
        if section is None or section == b"Cluster":
            replicaness_value = bytes(
                str(self._shard.replicaness),
                "ascii",
            )
            info.update({
                b"Cluster": {
                    b"replicaness": replicaness_value,
                }
            })
        return info

    def _on_add_shard(self, arguments):
        if len(arguments) == 3:
            host, port_number, db = arguments
            try:
                host = str(host, "utf-8")
                port_number = int(port_number)
                db = int(db)
            except ValueError as ex:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR " + bytes(str(ex), "utf-8"),
                )
            else:
                try:
                    shard_id = self._shard.add_shard(host, port_number, db)
                except rcluster.shard.exceptions.ShardConnectionError:
                    return rcluster.protocol.replies.ErrorReply(
                        data=b"ERR Could not connect to the shard.",
                    )
                else:
                    return rcluster.protocol.replies.StatusReply(
                        data=b"OK Shard " + shard_id + b" is added",
                    )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> ADDSHARD host port_number db",
            )

    def _on_get(self, arguments):
        if len(arguments) == 1:
            key = str(arguments[0], "utf-8")
            self._logger.debug("GET %s" % key)
            data = self._shard.get(key)
            if data is not None:
                return rcluster.protocol.replies.BulkReply(data=data)
            else:
                return rcluster.protocol.replies.NoneReply()
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> GET key",
            )

    def _on_set(self, arguments):
        if len(arguments) == 2:
            key, data = str(arguments[0], "utf-8"), arguments[1]
            self._logger.debug("SET %s bytes(%s)" % (key, len(data)))
            if self._shard.set(key, data):
                return rcluster.protocol.replies.StatusReply(
                    data=b"OK",
                )
            else:
                return rcluster.protocol.replies.ErrorReply(
                    data=b"ERR The key is not set - possible cluster failure.",
                )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> SET key data",
            )

    def _on_del(self, arguments):
        if len(arguments) != 0:
            keys_removed = 0
            for key in arguments:
                key = str(key, "utf-8")
                self._logger.debug("DEL %s" % key)
                if self._shard.delete(key):
                    keys_removed += 1
            return rcluster.protocol.replies.IntegerReply(value=keys_removed)
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> DEL key [key ...]",
            )

    def _on_set_replicaness(self, arguments):
        if len(arguments) == 1:
            try:
                replicaness = int(arguments[0])
            except ValueError as ex:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR " + bytes(str(ex), "utf-8"),
                )
            else:
                return self._set_replicaness(replicaness)
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> SETREPLICANESS replicaness",
            )

    def _set_replicaness(self, replicaness):
        """
        Sets the replicaness. Returns the corresponding status.
        """

        self._logger.debug("_set_replicaness %s", replicaness)

        if replicaness >= 1:
            self._shard.replicaness = replicaness
            return rcluster.protocol.replies.StatusReply(
                data=(
                    b"OK"
                    if replicaness <= len(self._shard._connections)
                    else b"OK Add more shards."
                ),
            )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Invalid replicaness value.",
            )

    def _on_time(self, arguments):
        if len(arguments) == 0:
            seconds, microseconds = divmod(self._shard.time(), 1000000)
            return rcluster.protocol.replies.MultiBulkReply(
                replies=(
                    rcluster.protocol.replies.BulkReply(data=bytes(str(seconds), "ascii")),
                    rcluster.protocol.replies.BulkReply(data=bytes(str(microseconds), "ascii")),
                ),
            )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> TIME",
            )

    def _on_config(self, arguments):
        if len(arguments) != 0 and arguments[0].upper() == b"SET":
            return self._on_config_set(arguments[1:])
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> CONFIG SET [key ...]",
            )

    def _on_config_set(self, arguments):
        self._logger.debug("CONFIG SET %s", arguments)
        if len(arguments) != 0:
            parameter = arguments[0]
            if parameter == b"replicaness":
                if len(arguments) == 2:
                    try:
                        replicaness = int(arguments[1])
                    except ValueError as ex:
                        raise rcluster.protocol.exceptions.CommandError(
                            data=b"ERR " + bytes(str(ex), "utf-8"),
                        )
                    else:
                        return self._set_replicaness(replicaness)
                else:
                    raise rcluster.protocol.exceptions.CommandError(
                        data=b"ERR Expected> CONFIG SET replicaness value",
                    )
            else:
                raise rcluster.protocol.exceptions.CommandError(
                    data=b"ERR Unsupported CONFIG parameter: " + parameter,
                )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> CONFIG SET key [value ...]",
            )

    def _on_lastsave(self, arguments):
        if len(arguments) == 0:
            return rcluster.protocol.replies.IntegerReply(value=self._shard.lastsave())
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> LASTSAVE",
            )


def _create_argument_parser():
    parser = argparse.ArgumentParser(
        description=globals()["__doc__"],
        formatter_class=argparse.RawTextHelpFormatter,
        prog="rcluster-shard",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "FATAL"],
        default="DEBUG",
        dest="log_level",
        help="logging level (default: %(default)s)",
        metavar="LEVEL",
        type=str,
    )
    parser.add_argument(
        "--port",
        default=rcluster.shared.DEFAULT_SHARD_PORT,
        dest="port_number",
        help="port number to listen to (default: %(default)s)",
        metavar="PORT",
        type=int,
    )
    parser.add_argument(
        "--password",
        default=None,
        dest="password",
        help="password",
        metavar="PASSWORD",
        type=str,
    )
    return parser


def entry_point():
    args = _create_argument_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
    )
    logger = logging.getLogger("rcluster.shard")

    logger.info("Starting the shard ...")
    Shard(args.port_number, args.password).start()

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
