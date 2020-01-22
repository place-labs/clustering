require "etcd"
require "hound-dog"
require "redis"
require "uuid"

module Coordination
  # Performed to align nodes in the cluster
  abstract def stabilize(nodes : Array(HoundDog::Node))

  # Implement a etcd_client generator
  abstract def etcd_client : Etcd::Client

  # Single redis client for a subscription
  abstract def redis_client : Redis::Client

  # For shared connection queries
  abstract def redis_pool : Redis::PooledClient

  getter? leader : Bool = false
  getter? ready : Bool = false

  getter cluster_version : UInt64 = 0_u64
  getter ready_channel = Channel(Bool).new

  # Node Metadata
  getter ip : String
  getter port : Int32

  # Only a single etcd lease is required for the node's responsibilities.
  # - Attach discovery lease to every key.
  # - If a node dies, all keys on the lease disappear simultaneously.

  abstract def discovery : HoundDog::Discovery

  def initialize
    etcd = etcd_client
    @election_watcher = etcd.watch.watch(@@election_key, filters: [Etcd::Watch::Watcher::WatchFilter::NOPUT]) {
      election
    }
    @readiness_watcher = etcd.watch.watch_prefix(@@readiness_key) {
      if cluster_consistent? && leader?
        redis_pool.publish(@@version_channel_name, cluster_version)
      end
    }
    @version_watcher = etcd.watch.watch(@@cluster_version_key, filters: [Etcd::Watch::Watcher::WatchFilter::NODELETE]) { |v|
      set_ready(false)
      cluster_version_update
    }
  end

  class_getter election_key : String = "leader"
  class_getter readiness_key : String = "node_version"
  class_getter cluster_version_key : String = "cluster_version"
  class_getter version_channel_name : String = "cluster_version"

  def service
    discovery.service
  end

  # TODO: Modify to hangle the case of a cluster merge
  private getter election_watcher : Etcd::Watch::Watcher

  private getter readiness_watcher : Etcd::Watch::Watcher

  private getter version_watcher : Etcd::Watch::Watcher

  def start
    discovery.register do
      cluster_change
    end

    spawn(same_thread: true) { election_watcher.start }
    spawn(same_thread: true) { readiness_watcher.start }
    spawn(same_thread: true) { consume_stabilization_events }
    Fiber.yield

    self
  end

  def leader_node
    etcd_client.get(@@election_key).try(&->HoundDog::Service.node(String))
  end

  # Handle a node joining/leaving the cluster
  def cluster_change
    # TODO: Logic to put off stabilisation if cluster is initializing
    @cluster_version = increment_version if is_leader?
    # Update the cluster version if leader
    # Queue the cluster version
  end

  # Node responsibilities
  #############################################################################

  private getter rebalance_channel : Channel({Array(HoundDog::Service::Node), UInt64}) = Channel({Array(HoundDog::Service::Node), UInt64}).new

  # Triggered when a there's a new cluster version
  #
  # Node enters stabilization.
  def cluster_version_update(version)
    # Sends a copy of received version and current cluster nodes
    rebalance_channel.send({discovery.nodes.dup, version})
  end

  # Consume rebalance events until fiber channel empty
  # - this ensures that the node will have only the latest version

  def consume_stabilization_events
    loop do
      nodes, version = rebalance_channel.receive
      next if version < @current_version
      _stabilize(version, nodes)
    end
  end

  private def _stabilize(cluster_version, nodes)
    stabilize(nodes)
    set_ready(cluster_version)
  end

  private def set_ready(version : UInt64)
    raise "Node must be registered before participating in cluster" unless discovery.registered

    # Set the ready key for this node in etcd
    etcd_client.kv.put(node_ready_key, version, discovery.lease_id.as(Int64))
  end

  # Election
  #############################################################################

  # Try to acquire the leader role
  #
  private def election
    raise "Node must be registered before participating in election" unless discovery.registered?

    etcd = etcd_client
    lease_id = discovery.lease_id

    # Determine leader status
    @leader = if (kv = etcd.kv.range(@@election_key).kvs.first?)
                # Check if it is the same as the current node
                node = HoundDog::Service.node(kv.value.as(String))

                node[:ip] == ip && node[:port] == port && lease_id == kv.lease
              else
                # Attempt to set self as if a leader is not already present
                etcd.kv.put_not_exists(@@election_key, lease_id)
              end
  end

  # Cluster readiness
  #############################################################################

  # Whenever there's an event under the readiness namespace, the node constructs a readiness hash
  # Once all the nodes are at the same version, leader fires the 'cluster_ready' event in redis
  #                        Node   => Cluster Version
  getter readiness = {} of String => String

  def cluster_consistent?
    # Get values under the "readiness key"
    @readiness = etcd_client.kv.range_prefix(@@readiness_key).kvs.reduce({} of String => String) do |ready, kv|
      if value = kv.value
        ready[strip_namespace(kv.key, @@readiness_key)] = value
      end
      ready
    end
    readiness.all? { |_, v| v == @current_version }
  end

  # Helpers
  #############################################################################

  private def strip_namespace(key, namespace)
    key.lchop("#{namespace}/")
  end

  private def node_ready_key
    "#{readiness_key}/#{discovery_value}"
  end

  private def discovery_value
    "#{ip}:#{port}"
  end

  private def increment_version
    etcd = etcd_client

    # first, set key to 1 if not present
    if etcd.kv.put_not_exists(key, 1_u64)
      1_u64
    else
      value = etcd.kv.get(key).try(&.to_u64) || 1_u64
      until etcd.kv.compare_and_swap(key, value + 1_u64, value)
        value = etcd.kv.get(key).try(&.to_u64) || 1_u64
      end
      value + 1_u64
    end
  end

  # Necessary to avoid use of instance variables at the top level
  def set_ready(ready)
    @ready = ready
  end

  def set_version(version)
    @cluster_version = version
  end
end
