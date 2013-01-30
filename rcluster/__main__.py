#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Proxy startup script.
"""

import argparse
import logging

import tornado.ioloop

import rcluster.proxy
import rcluster.utilities


def parse_args():
    parser = argparse.ArgumentParser(
        description=globals()["__doc__"],
        formatter_class=argparse.RawTextHelpFormatter,
        prog="python3 -m rcluster",
    )

    parser.add_argument(
        "--db",
        metavar="DB_NUMBER",
        type=int,
        dest="db_number",
        default=1,
        help="Redis database number (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        metavar="LOG_LEVEL",
        choices=["DEBUG", "INFO", "WARN", "ERROR", ],
        dest="log_level",
        default="DEBUG",
        help="logging level (default: %(default)s)",
    )
    parser.add_argument(
        "--node-port",
        metavar="NODE_PORT",
        type=int,
        dest="node_port_number",
        default=6380,
        help="node port number (default: %(default)s)",
    )
    parser.add_argument(
        "--interface-port",
        metavar="INTERFACE_PORT",
        type=int,
        dest="interface_port_number",
        default=6381,
        help="interface port number (default: %(default)s)",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    logging.basicConfig(
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
        level=getattr(logging, args.log_level),
    )
    logger = logging.getLogger(__name__)

    logger.info("Starting a cluster node ...")
    node = rcluster.proxy.ClusterNode(
        db_number=args.db_number,
        port_number=args.node_port_number,
    )
    node.start()
    logger.info(
        "Cluster node %s is started on port %s (DB #%s).",
        rcluster.utilities.Converter.hexlify_bytes(node.node_id),
        node.port_number,
        node.db_number,
    )

    logger.info("Starting a cluster node interface ...")
    interface = rcluster.proxy.Interface(
        node=node,
        port_number=args.interface_port_number,
    )
    interface.start()
    logger.info(
        "Cluster node interface is started on port %s.",
        interface.port_number,
    )

    logger.info("I/O loop is being started.")
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt.")
