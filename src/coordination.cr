require "etcd"
require "hound-dog"
require "redis"
require "ulid"

module Coordination
  # Performed to align nodes in the cluster
  abstract def stabilize(nodes : Array(HoundDog::Node))

  # Implement a etcd_client generator
  abstract def etcd_client : Etcd::Client

  # For shared connection queries
  abstract def redis_pool : Redis::PooledClient

  abstract def logger : Logger

  getter? leader : Bool = false

  getter cluster_version : String = ""

  # Node Metadata
  getter ip : String
  getter port : Int32

  abstract def discovery : HoundDog::Discovery

  delegate service, to: discovery

  def initialize
    @election_watcher = etcd_client.watch.watch(@@election_key, filters: [Etcd::Watch::Watcher::WatchFilter::NOPUT]) do |e|
      logger.debug("etcd_event=election event=#{e.inspect}")
      handle_election
    end

    @readiness_watcher = etcd_client.watch.watch_prefix(@@readiness_key) do |e|
      logger.debug("etcd_event=ready event=#{e.inspect}")
      handle_readiness_event
    end

    @version_watcher = etcd_client.watch.watch(@@cluster_version_key) do |e|
      logger.debug("etcd_event=version event=#{e.inspect}")
      handle_version_change(e)
    end
  end

  @@meta_namespace = "cluster"
  @@election_key = "#{@@meta_namespace}/leader"
  @@readiness_key = "#{@@meta_namespace}/node_version"
  @@cluster_version_key = "#{@@meta_namespace}/cluster_version"
  @@redis_version_channel = "#{@@meta_namespace}/cluster_version"

  class_getter meta_namespace : String
  class_getter election_key : String
  class_getter readiness_key : String
  class_getter cluster_version_key : String
  class_getter redis_version_channel : String

  def handle_readiness_event
    # Only publish ready version if folowing conditions are met...
    # - node is the leader
    # - cluster's version state is consistent
    # - consistent state has not already been confirmed
    if leader? && cluster_consistent? && previous_ready_states != ready_states
      @previous_ready_states = @ready_states.dup
      redis_pool.publish(@@redis_version_channel, cluster_version)
    end
  rescue e
    logger.error("While handling readiness event #{e.inspect_with_backtrace}")
  end

  def handle_version_change(value)
    version = value.first?.try(&.kv.value) || ""
    logger.debug("v=#{version} l?=#{leader?} message=version change")
    cluster_version_update(version)
  rescue e
    logger.error("While watching cluster version #{e.inspect_with_backtrace}")
  end

  private getter election_watcher : Etcd::Watch::Watcher
  private getter readiness_watcher : Etcd::Watch::Watcher
  private getter version_watcher : Etcd::Watch::Watcher

  def start
    discovery.register do
      cluster_change
    end

    spawn(same_thread: true) do
      election_watcher.start
    end

    spawn(same_thread: true) do
      version_watcher.start
    end

    spawn(same_thread: true) do
      readiness_watcher.start
    end

    spawn(same_thread: true) do
      consume_stabilization_events
    end

    Fiber.yield

    # Attempt to grab the leadership on start up
    handle_election

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
      next if !version.empty? && version < cluster_version
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
    etcd_client.kv.put(node_ready_key, version, discovery.lease_id.as(Int64))
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
    logger.info("leader?=#{leader?} leader=#{leader_node}")
    update_version if leader?
  rescue e
    logger.error("While participating in election #{e.inspect_with_backtrace}")
  end

  # Cluster readiness
  #############################################################################

  # Whenever there's an event under the readiness namespace, the node constructs a readiness hash
  # Once all the nodes are at the same version, leader fires the 'cluster_ready' event in redis
  #                           Node   => Cluster Version
  getter ready_states = {} of String => String
  getter previous_ready_states = {} of String => String

  # check the previous readiness hash
  # is it the same, then it is consistent but don't publish

  def cluster_consistent?
    # Get values under the "readiness key"
    @ready_states = etcd_client.kv.range_prefix(@@readiness_key).kvs.reduce({} of String => String) do |ready, kv|
      if value = kv.value
        ready[strip_namespace(kv.key, @@readiness_key)] = value
      end
      ready
    end
    ready_states.all? { |_, v| v == cluster_version }
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
    logger.info("v=#{version} message=set version")
    version
  end
end
