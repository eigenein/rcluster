#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class ClusterStateOperationError(Exception):
    """
    Failed to get or update the cluster state.
    """

    pass


class ShardIsNotAvailable(Exception):
    """
    The shard is not available or failed.
    """

    pass
