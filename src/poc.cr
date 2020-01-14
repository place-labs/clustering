require "etcd"
require "hound-dog"
require "redis"
require "uuid"


module 

end

class Node
  getter? is_leader : Bool
  getter election_watcher : Etcd::Watch::Watcher
  getter discovery : HoundDog::Discovery

  @cluster_version : String = ""
  @version_lock = Mutex.new

  @etcd = etcd_client

  def initialize(
    ip : String,
    port : Int32,
    @service : String = "core",
    @election_key : String = "leader",
    @readiness_key : String = "node_version",
    @cluster_version_key : String = "cluster_version",
  )
    @discovery = HoundDog::Discovery.new(service: service, ip: ip, port: port)
    @election_watcher = etcd_client.watch.watch(@election_key) do |event|
      election(event)
    end
  end

  def rebalance
    puts "rebalancing to version #{cluster_version}"
    set_ready(cluster_version)
  end

  # nodes track of all the state.... locally.
  # only leader performs the iteration to check for complete readiness.
  # Whenever there's an event under the readiness namespace, we whack the version and keys into
  # the readiness hash. A simple boost through allows
  # once all the nodes are at the same version
  #                          Node   => Cluster Version
  property readiness = {} of String => String

  def cluster_consistent?
    # Get values under the "readiness key"
    @readiness = @etcd.kv.prefix_range(@readiness_key).kvs.reduce({} of String => String) do |ready, kv|
      ready[strip_namespace(kv.key, @readiness_key)] = kv.value
      ready
    end

    @readiness.all? { |_, v| v == current_version }
  end

  def set_ready(version : String)
    # Set the ready key for this node in etcd
  end

  def start
    # spawn(same_thread: true) { election_watcher.start }
    # spawn(same_thread: true) { .start }
  end

  def leader
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
  def attain_leadership
    # Grab the leader key
    # Check if it is the same as the current node
    # Grab the lease if it is, otherwise create a lease and perform a `put_if`
  end

  # Election callback triggered by delete events on the leadership key
  #
  def watch_for_election
    # # etcd transaction
    # # - either is leader/or not
    # @is_leader = false

    # if is_leader?
    #   spawn(same_thread: true) { retain_leadership }
    # end
  end

  # Leader responsibilities
  #############################################################################

  # Renew the leader lease
  def retain_leadership
    # Loop and keep the leadership lease
  end

  # Handle a node joining/leaving the cluster
  def cluster_change
    # Update the cluster version if leader
  end

  # Helpers
  #############################################################################

  def etcd_client
    Etcd.client(HoundDog.settings.etcd_host, HoundDog.settings.etcd_port)
  end

  def strip_namespace(key, namespace)
    key.lchop("#{namespace}/")
  end
end
