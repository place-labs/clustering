# clustering

[![Build Status](https://travis-ci.com/place-labs/clustering.svg?branch=master)](https://travis-ci.com/place-labs/clustering)

`Clustering` class implements simple clustering logic through etcd as a distributed consistent key-value store.
A `on_stable` callback is fired for the leader once the cluster has stabilized.

Running `$ shards build` will create a simple proof of concept app, run it with `./bin/poc`

## Usage

The `stabilize : Array(NamedTuple(ip: String, port: Int32)) -> Void` arg of `Clustering` defines the class's stabilization logic that is fired upon cluster join, leave, and election events.
The array of node data is all of the nodes in the cluster.

## Implementation

A single etcd lease is granted per node so if a node drops out of the cluster,
all associated key/values will expire from the cluster.

### Watchfeeds

There are 4 etcd watchfeeds

- election: notify nodes still in cluster that there is no longer a leader
- readiness: propagate readiness against a cluster version
- version: set by leader when there's a change in cluster state, nodes must 'ready' themselves to this signal
- discovery: keeps track of nodes in etcd under a service namespace

## Dependencies

- etcd `~> v3.3`
