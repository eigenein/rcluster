#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Redis Cluster Proxy startup script.
"""

import logging

import tornado.ioloop

import rcluster.proxy
import rcluster.proxy.shared


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s [%(process)d] %(name)s %(levelname)s: %(message)s",
        level=logging.INFO,
    )
    logger = logging.getLogger(__name__)

    logger.info("Initializating ...")
    cluster_state = rcluster.proxy.shared.ClusterState()
    node_state = rcluster.proxy.shared.ClusterNodeState()

    node = rcluster.proxy.ClusterNode(cluster_state, node_state)
    node.start()
    interface = rcluster.proxy.Interface(cluster_state, node_state)
    interface.start()

    logger.info("Cluster Proxy is being started.")
    tornado.ioloop.IOLoop.start()
