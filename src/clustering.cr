require "etcd"
require "hound-dog"
require "redis"
require "ulid"

module Clustering
  # Performed to align nodes in the cluster
  abstract def stabilize(nodes : Array(HoundDog::Node))

  # Implement a etcd_client generator
  abstract def etcd_client : Etcd::Client

  # For shared connection queries
  abstract def redis : Redis::Client

  abstract def logger : Logger

  # Whether node is the cluster leader
  getter? leader : Bool = false

  # The version the current node is stable against
  getter cluster_version : String = ""

  # Node Metadata
  getter ip : String
  getter port : Int32

  # Provides cluster node discovery
  abstract def discovery : HoundDog::Discovery

  delegate service, to: discovery

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

  private getter election_watcher : Etcd::Watch::Watcher
  private getter readiness_watcher : Etcd::Watch::Watcher
  private getter version_watcher : Etcd::Watch::Watcher

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

  # Starts the node's clustering processes.
  # - discovery (via hound-dog)
  # - election_watcher (election event consumer)
  # - readiness_watcher (cluster node version event consumer)
  # - version_watcher (version change event consumer)
  # - consume_stabilization_events (created by version_watcher)
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

    # Attempt to attain leadership on initialization
    handle_election

    self
  end

  def leader_node
    etcd_client.kv.get(@@election_key).try(&->HoundDog::Service.node(String))
  end

  # Leader updates the version in etcd when
  # - New leader is elected
  # - There is a change in the number of nodes in a cluster
  private def update_version
    version = ULID.generate
    lease_id = discovery.lease_id.as(Int64)

    etcd_client.kv.put(@@cluster_version_key, version, lease_id)
    logger.info("v=#{version} message=set version")
  end

  # Node stabilization
  #############################################################################

  private getter stabilize_channel : Channel({Array(HoundDog::Service::Node), String}) = Channel({Array(HoundDog::Service::Node), String}).new

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

  # Leader publishes a new version upon nodes joining/leaving the cluster
  #
  def cluster_change
    update_version if leader?
  rescue e
    logger.error("During cluster change #{e.inspect_with_backtrace}")
  end

  # Leader has published a new version to etcd
  #
  def handle_version_change(value)
    version = value.first?.try(&.kv.value) || ""
    logger.debug("v=#{version} l?=#{leader?} message=version change")
    stabilize_channel.send({discovery.nodes.dup, version})
  rescue e
    logger.error("While watching cluster version #{e.inspect_with_backtrace}")
  end

  # The leader publishes a version to the redis channel if...
  # + cluster's version state is consistent
  # + consistent state has not already been confirmed to be consistent
  def handle_readiness_event
    if leader? && cluster_consistent? && previous_node_versions != node_versions
      @previous_node_versions = @node_versions.dup
      redis.publish(@@redis_version_channel, cluster_version)
    end
  rescue e
    logger.error("While handling readiness event #{e.inspect_with_backtrace}")
  end

  getter node_versions = {} of String => String
  getter previous_node_versions = {} of String => String

  # When there's an event under the readiness namespace, node creates a hash from node to version.
  # If all the nodes are at the same version, the cluster is consistent.
  def cluster_consistent?
    # Get values under the "readiness key"
    @node_versions = etcd_client.kv.range_prefix(@@readiness_key).kvs.reduce({} of String => String) do |ready, kv|
      if value = kv.value
        ready[strip_namespace(kv.key, @@readiness_key)] = value
      end
      ready
    end

    node_versions.all? { |_, v| v == cluster_version }
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
end
