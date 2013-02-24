#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Stops the redis instance at the specified port.
"""

import argparse
import os
import sys

import redis


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=globals()["__doc__"],
        formatter_class=argparse.RawTextHelpFormatter,
        prog="stop-redis.py",
    )
    parser.add_argument(
        "--port",
        type=int,
        required=True,
        metavar="PORT",
        dest="port",
    )

    args = parser.parse_args()
    redis.StrictRedis(port=args.port).shutdown()

    sys.exit(os.EX_OK)
