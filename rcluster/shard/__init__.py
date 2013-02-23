#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Shard.
"""

import argparse
import logging
import os
import traceback
import uuid

import tornado.ioloop

import rcluster.protocol
import rcluster.protocol.exceptions
import rcluster.protocol.replies
import rcluster.shard.exceptions
import rcluster.shared


class Shard(rcluster.protocol.Server):
    SHARD_ID_KEY = b"rcluster:shard:id"

    def __init__(self, port_number):
        super(Shard, self).__init__(
            port_number=port_number,
            command_handler_factory=self._create_handler,
        )

        self._logger = logging.getLogger("rcluster.shard.Shard")
        self._shards = dict()

    def add_shard(self, host, port_number, db):
        self._logger.info(
            "Adding shard: %s:%s/%d ...",
            host,
            port_number,
            db,
        )
        connection = rcluster.protocol.Client(
            host=host,
            port_number=port_number,
            db=db,
        )
        shard_id = uuid.uuid4().bytes
        try:
            if not connection.setnx(Shard.SHARD_ID_KEY, shard_id):
                shard_id = connection.get(Shard.SHARD_ID_KEY)
            db_size = connection.dbsize()
        except Exception as ex:
            # TODO: specific exception class.
            raise rcluster.shard.exceptions.ShardConnectionError(
                "Could not connect to the specified shard.",
            ) from ex
        else:
            self._shards[shard_id] = {
                "connection": connection,
                "db_size": db_size,
            }

    def get(self, key):
        pass

    def set(self, key, data):
        pass

    def _create_handler(self):
        return _ShardCommandHandler(self)


class _ShardCommandHandler(rcluster.protocol.CommandHandler):
    def __init__(self, shard):
        super(_ShardCommandHandler, self).__init__({
            b"ADDSHARD": self._on_add_shard,
            b"GET": self._on_get,
            b"SET": self._on_set,
        })

        self._logger = logging.getLogger("rcluster.shard._ShardCommandHandler")
        self._shard = shard

    def _get_info(self, section):
        info = super()._get_info(section)
        if section is None or section == b"Shards":
            info.update({
                b"Shards": {
                    b"count": bytes(str(len(self._shard._shards)), "ascii"),
                },
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
                # TODO: add shard.
                pass
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> ADDSHARD host port_number db",
            )

    def _on_get(self, arguments):
        if len(arguments) == 1:
            key = str(arguments[0].data, "utf-8")
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
            key, data = str(arguments[0].data, "utf-8"), arguments[1].data
            self._logger.debug("SET %s bytes(%s)" % (key, len(data)))
            self._shard.set(key, data)
            return rcluster.protocol.replies.StatusReply(
                data=b"OK",
            )
        else:
            raise rcluster.protocol.exceptions.CommandError(
                data=b"ERR Expected> SET key data",
            )


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
    return parser


def entry_point():
    args = _create_argument_parser().parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
    )
    logger = logging.getLogger("rcluster.shard")

    logger.info("Starting the shard ...")
    Shard(args.port_number).start()

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
