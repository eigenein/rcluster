# rcluster

Client-side Redis sharding

## Installation Requirements

* `python3-setuptools`

## Installation

```bash
python setup.py install
```

## Running

```bash
rcluster-shard [-h] [--log-level LEVEL] [--port PORT]
```

`rcluster-shard` talks to clients via [unified request protocol](http://redis.io/topics/protocol).

## Supported Commands

* `ADDSHARD host port db`
* `GET key`
* `SET key data`
* `SETREPLICANESS replicaness`
* `INFO [section]`
* `PING`
* `ECHO data`
* `QUIT`
