#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Benchmarks the redis (or rcluster-shard).
"""

import argparse
import logging
import os
import sys
import time

import redis


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=globals()["__doc__"],
        formatter_class=argparse.RawTextHelpFormatter,
        prog="benchmark.py",
    )
    parser.add_argument(
        "--port",
        default=6379,
        dest="port",
        metavar="PORT",
        required=False,
        type=int,
    )

    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    logging.info("Connecting ...")
    redis = redis.StrictRedis(port=args.port)

    logging.info("Testing SET ...")
    time_start = time.time()
    for i in range(10000):
        key, value = os.urandom(32), os.urandom(32)
        redis.set(key, value)
    time_end = time.time()
    time_elapsed = time_end - time_start
    logging.info("Done in %.6fs (%.6fs per operation)", time_elapsed, time_elapsed / i)

    logging.info("Testing GET ...")
    time_start = time.time()
    for i in range(10000):
        redis.get(key)
    time_end = time.time()
    time_elapsed = time_end - time_start
    logging.info("Done in %.6fs (%.6fs per operation)", time_elapsed, time_elapsed / i)

    sys.exit(os.EX_OK)
