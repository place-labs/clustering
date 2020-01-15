require "etcd"
require "hound-dog"
require "redis"
require "uuid"

module Cluster
  getter? is_leader : Bool

  @is_leader = false
  @cluster_version : String = ""
  @version_lock = Mutex.new

  # Only a single etcd lease is required for the node's responsibilities.
  # - Attach the lease to every key.
  # - If node dies, all attached keys disappear simultaneously.
  @lease_id : String? = nil

  abstract def etcd_client

  # Node Metadata
  getter ip : String
  getter port : Int32
  getter service : String = "core"
  getter election_key : String = "leader"
  getter readiness_key : String = "node_version"
  getter cluster_version_key : String = "cluster_version"

  getter discovery = HoundDog::Discovery.new(service: service, ip: ip, port: port)

  private getter election_watcher = etcd_client.watch(filters: [Etcd::Watcher::WatchFilter::NOPUT]).watch(election_key) do |_|
    election
  end

  private getter readiness_watcher = etcd_client.watch.watch(election_key) do |_|
    if cluster_consistent? && is_leader?
    end
  end

  # Performed to align nodes in the cluster
  abstract def stabilize

  def start
    # spawn(same_thread: true) { election_watcher.start }
    # spawn(same_thread: true) { .start }
  end

  def leader
    etcd_client.get(election_key).try(&->HoundDog::Service.node(String))
  end

  # A leader will bump the cluster version on an event, once an event is fired on the discovery channel

  # Node responsibilities
  #############################################################################

  @rebalance_channel = Channel({Array(URI), String}).new(capacity: 1)

  # Triggered when a there's a new cluster version
  #
  def handle_cluster_version_update
    # Node creates a copy of the nodes in the rendezvous hash
    # Updates the version
    # Places copy of nodes and version on the rebalance channel
  end

  def rebalance(version, nodes)
    # Consume the set of nodes, and cluster version
    # Only Rebalance if the version is the same as @cluster_version
    if version_matches(version)
    end
  end

  def perform_rebalance
  end

  def with_version
    version_lock.synchronize do
      yield @cluster_version
    end
  end

  def version_matches(checked)
    with_version { |version| version == checked }
  end

  def set_version(version)
    with_version { |_| @cluster_version = version }
  end

  # Consume rebalance events until fiber channel empty
  # - this ensures that the node will have only the latest version

  # Election
  #############################################################################

  # Try to acquire the leader role
  #
  private def election
    unless discovery.registered
      raise "Node must be registered before participating in election"
    end
    etcd = etcd_client
    lease_id = discovery.lease_id.as(Int64)

    # Determine leader status
    @is_leader = if (kv = etcd.kv.range(election_key).kv.first?)
                   # Check if it is the same as the current node
                   node = HoundDog::Service.node(kv.value.as(String))

                   node[:ip] == ip && node[:port] == port && lease_id == kv.lease
                 else
                   # Attempt to set self as if a leader is not already present
                   etcd.put_not_exists(election_key, lease_id)
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
    @readiness = etcd.kv.prefix_range(@readiness_key).kvs.reduce({} of String => String) do |ready, kv|
      ready[strip_namespace(kv.key, @readiness_key)] = kv.value
      ready
    end

    @readiness.all? { |_, v| v == current_version }
  end

  private def _stabilize(cluster_version)
    stabilize
    set_ready(cluster_version)
  end

  private def set_ready(version : String)
    unless discovery.registered
      raise "Node must be registered before participating in cluster"
    end

    # Set the ready key for this node in etcd
    etcd_client.kv.put(node_ready_key, version, discovery.lease_id.as(Int64))
  end

  # Leader responsibilities
  #############################################################################

  # Handle a node joining/leaving the cluster
  def cluster_change
    # Update the cluster version if leader
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
end
