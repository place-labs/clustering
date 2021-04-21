# clustering

[![CI](https://github.com/place-labs/clustering/actions/workflows/ci.yml/badge.svg)](https://github.com/place-labs/clustering/actions/workflows/ci.yml)

`Clustering` provides a generic interface for implementing different clustering backends


## Concepts

When dealing with a cluster you may want to know:

1. list of nodes in the cluster
2. when the list of nodes changes, your application might need to
   - know which node in the cluster is the leader (not all nodes are equal)
   - rebalance the cluster / redistribute work or data between the nodes
   - know when the cluster has stabilised after a rebalance has occurred

Clustering provides a simple interface for obtaining this information.


## Example

Using the example implementation in spec folder

```crystal
require "clustering"

# configure the service that this node is a member of
cluster = ClusteringExample.new("service_name", URI.parse "http://this_node/service")

# implement the callbacks
cluster.on_rebalance do |nodes, rebalance_complete_cb|
  # perform rebalance operations here or update internal state with the list of nodes

  # `nodes` is a https://github.com/caspiano/rendezvous-hash which is an ideal
  # method for determining what should be running or stored on each node

  # notify the cluster once rebalance is complete
  rebalance_complete_cb.call
end

cluster.on_cluster_stable do
  # this callback is only fired on the leader node
  # the leader can now perform house keeping operations against a stable cluster
end

# register this node with the cluster
cluster.register

```

If you have code only interested in getting the list of nodes in a cluster,
versus actually joining a cluster, you can use the `Discovery` class

```crystal
require "clustering"

lookup = Clustering::Discovery.new ClusteringExample.new("service_name")
lookup.nodes

```

The main purpose of this class is to cache results for a period of time so as
not to overload your clustering service (etcd / zookeeper etc) and to not slow
down operations by requiring a fetch every time

Your clustering service might support monitoring too, which means it's storing
the latest known state and polling is not required


## Implementations

* [redis](https://github.com/place-labs/redis_service_manager)
