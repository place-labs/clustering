require "etcd"
require "hound-dog"
require "redis"
require "ulid"

module Coordination
  # Performed to align nodes in the cluster
  abstract def stabilize(nodes : Array(HoundDog::Node))

  # Implement a etcd_client generator
  abstract def etcd_client : Etcd::Client

  # Single redis client for a subscription
  abstract def redis_client : Redis::Client

  # For shared connection queries
  abstract def redis_pool : Redis::PooledClient

  abstract def logger : Logger

  getter? leader : Bool = false

  getter cluster_version : String = ""

  # Node Metadata
  getter ip : String
  getter port : Int32

  # Only a single etcd lease is required for the node's responsibilities.
  # - Attach discovery lease to every key.
  # - If a node dies, all keys on the lease disappear simultaneously.

  abstract def discovery : HoundDog::Discovery

  def initialize
    @election_watcher = etcd_client.watch.watch(@@election_key, filters: [Etcd::Watch::Watcher::WatchFilter::NOPUT]) { handle_election }
    @readiness_watcher = etcd_client.watch.watch_prefix(@@readiness_key) { handle_readiness_event }
    @version_watcher = etcd_client.watch.watch(@@cluster_version_key) { |v| handle_version_change(v) }
  end

  @@meta_namespace : String = "cluster"
  @@election_key : String = "#{@@meta_namespace}/leader"
  @@readiness_key : String = "#{@@meta_namespace}/node_version"
  @@cluster_version_key : String = "#{@@meta_namespace}/cluster_version"
  @@version_channel_name : String = "#{@@meta_namespace}/cluster_version"

  def service
    discovery.service
  end

  def handle_readiness_event
    if cluster_consistent? && leader?
      redis_pool.publish(@@version_channel_name, cluster_version)
    end
  rescue e
    logger.error("While handling readiness event #{e.inspect_with_backtrace}")
  end

  def handle_version_change(value)
    version = value.first?.try(&.kv.value) || ""
    logger.info("version=#{version} is_leader?=#{leader?} message=version change")
    set_ready(version)
    cluster_version_update(version)
  rescue e
    logger.error("While watching cluster version #{e.inspect_with_backtrace}")
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
    etcd_client.kv.get(@@election_key).try(&->HoundDog::Service.node(String))
  end

  # Handle a node joining/leaving the cluster
  def cluster_change
    @cluster_version = update_version if leader?
  rescue e
    logger.error("During cluster change #{e.inspect_with_backtrace}")
  end

  # Node responsibilities
  #############################################################################

  private getter stabilize_channel : Channel({Array(HoundDog::Service::Node), String}) = Channel({Array(HoundDog::Service::Node), String}).new

  # Triggered when a there's a new cluster version
  #
  # Node enters stabilization.
  def cluster_version_update(version)
    # Sends a copy of received version and current cluster nodes
    stabilize_channel.send({discovery.nodes.dup, version})
  end

  # Consume stabilization events until fiber channel empty
  #
  # Ensures that the node will have only the latest version
  def consume_stabilization_events
    loop do
      nodes, version = stabilize_channel.receive
      next if version < cluster_version
      _stabilize(version, nodes)
    end
  rescue e
    logger.error("While consuming stabilization event #{e.inspect_with_backtrace}")
  end

  private def _stabilize(cluster_version, nodes)
    stabilize(nodes)
    set_ready(cluster_version)
  end

  private def set_ready(version : String)
    raise "Node must be registered before participating in cluster" unless discovery.registered?

    @cluster_version = version
    # Set the ready key for this node in etcd
    etcd_client.kv.put(@@readiness_key, version, discovery.lease_id.as(Int64))
  end

  # Election
  #############################################################################

  # Try to acquire the leader role
  #
  private def handle_election
    raise "Node must be registered before participating in election" unless discovery.registered?

    etcd = etcd_client
    lease_id = discovery.lease_id.as(Int64)

    # Determine leader status
    @leader = if (kv = etcd.kv.range(@@election_key).kvs.first?)
                # Check if it is the same as the current node
                node = HoundDog::Service.node(kv.value.as(String))

                node[:ip] == ip && node[:port] == port && lease_id == kv.lease
              else
                # Attempt to set self as if a leader is not already present
                etcd.kv.put_not_exists(@@election_key, HoundDog::Service.key_value({ip: ip, port: port}), lease_id)
              end
    logger.info("is_leader?=#{leader?} leader=#{leader_node}")
  rescue e
    logger.error("While participating in election #{e.inspect_with_backtrace}")
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
    readiness.all? { |_, v| v == cluster_version }
  end

  # Helpers
  #############################################################################

  private def strip_namespace(key, namespace)
    key.lchop("#{namespace}/")
  end

  private def node_ready_key
    "#{@@readiness_key}/#{discovery_value}"
  end

  private def discovery_value
    "#{ip}:#{port}"
  end

  private def update_version
    version = ULID.generate
    lease_id = discovery.lease_id.as(Int64)

    etcd_client.kv.put(@@cluster_version_key, version, lease_id)
    logger.info("version=#{version} message=set version")
    version
  end
end
