#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Different Redis Cluster tools.
"""

import argparse
import logging
import os
import uuid

import redis
import redis.exceptions

import rcluster.shared


def shard_set_id():
    """
    Sets the target shard ID.
    """

    parser = argparse.ArgumentParser(
        description="Set shard ID.",
        formatter_class=argparse.RawTextHelpFormatter,
        prog="rcluster-shard-set-id",
    )
    parser.add_argument(
        "--host",
        dest="host",
        type=str,
        metavar="HOST",
        default="localhost",
        help="Redis host (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        dest="port_number",
        type=int,
        metavar="PORT",
        default=rcluster.shared.DEFAULT_REDIS_PORT,
        help="Redis port (default: %(default)s)",
    )
    parser.add_argument(
        "--id",
        dest="shard_id",
        type=str,
        metavar="SHARD_ID",
        default=None,
        help="target shard ID (generated if empty)",
    )
    parser.add_argument(
        "--force",
        dest="force",
        default=False,
        action="store_true",
        help="overwrite existing ID",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
    )
    logger = logging.getLogger("shard_set_id")

    connection = redis.StrictRedis(
        host=args.host,
        port=args.port_number,
    )
    logger.info("Checking Redis connection ...")
    try:
        connection.ping()
    except redis.exceptions.ConnectionError:
        logger.fatal("Could not connect to Redis.")
        return os.EX_UNAVAILABLE
    else:
        if not args.shard_id:
            shard_id = uuid.uuid4()
            logger.info("Generated shard ID: %s.", shard_id)
        else:
            shard_id = args.shard_id
        logger.info("Setting the shard ID ...")
        shard_id_key = rcluster.shared.SHARD_ID_KEY
        if args.force:
            connection.set(shard_id_key, shard_id)
            logger.info("The shard ID is successfully updated.")
        else:
            if not connection.setnx(shard_id_key, shard_id):
                logger.error(
                    "The shard ID already exists."
                    " Use --force to forcibly set it."
                )
            else:
                logger.info("The shard ID is successfully set.")

    return os.EX_OK
