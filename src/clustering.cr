require "etcd"
require "hound-dog"
require "ulid"
require "uri"

class Clustering
  Log = ::Log.for(self)

  # Performed to align nodes in the cluster
  getter stabilize : (Array(HoundDog::Service::Node) ->)?

  # Performed by leader once cluster has stabilized
  private getter on_stable : (String ->)?

  class_getter election_key = ELECTION_KEY
  class_getter meta_namespace = META_NAMESPACE
  class_getter readiness_key = READINESS_KEY
  class_getter service_namespace = SERVICE_NAMESPACE

  private getter etcd_host : String
  private getter etcd_port : Int32

  # Whether node is the cluster leader
  getter? leader : Bool = false

  # The version the current node is stable against
  getter cluster_version : String = ""

  # Provides cluster node discovery
  getter discovery : HoundDog::Discovery

  delegate name, nodes, service, uri, to: discovery

  private getter election_watcher : Etcd::Watch::Watcher
  private getter readiness_watcher : Etcd::Watch::Watcher
  private getter version_watcher : Etcd::Watch::Watcher

  def initialize(
    uri : String | URI,
    name : String = ULID.generate,
    discovery : HoundDog::Discovery? = nil,
    @etcd_host : String = ENV["ETCD_HOST"]? || "localhost",
    @etcd_port : Int32 = ENV["ETCD_PORT"]?.try(&.to_i?) || 2379
  )
    @discovery = discovery || HoundDog::Discovery.new(
      service: SERVICE_NAMESPACE,
      name: name,
      uri: uri
    )

    @election_watcher = etcd_client.watch.watch(ELECTION_KEY, filters: [Etcd::Watch::Filter::NOPUT]) do |e|
      Log.debug { {event: "election", etcd: e.map &.to_s} }
      handle_election
    end

    @readiness_watcher = etcd_client.watch.watch_prefix(READINESS_KEY) do |e|
      Log.debug { {event: "ready", etcd: e.map &.to_s} }
      handle_readiness_event
    end

    @version_watcher = etcd_client.watch.watch(CLUSTER_VERSION_KEY) do |e|
      Log.debug { {event: "version", etcd: e.map &.to_s} }
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
    spawn(same_thread: true) do
      discovery.register do
        cluster_change
      end
    end

    Fiber.yield

    # Ensure the node is registered
    discovery.registration_channel.receive

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

  # Like above.
  # Accepts a block that will be called with cluster nodes
  # during stabilization events
  #
  def start(on_stable : (String -> Nil)? = nil, &stabilize : Array(HoundDog::Service::Node) ->)
    @stabilize = stabilize
    @on_stable = on_stable if on_stable
    start
  end

  # Unregisters node from the cluster and ceases event handling
  #
  def stop
    discovery.unregister
    election_watcher.stop
    version_watcher.stop
    readiness_watcher.stop

    @leader = false
    self
  end

  # Attains the current leader node
  #
  def leader_node
    # Find the uri of the node who is the leader
    etcd_client.kv.get(ELECTION_KEY).try do |uri_string|
      uri = URI.parse(uri_string)
      # Look through cluster nodes for that uri
      nodes.bsearch { |n| n[:uri] == uri }
    end
  end

  # Leader updates the version in etcd when
  # - New leader is elected
  # - There is a change in the number of nodes in a cluster
  private def update_version
    version = ULID.generate
    lease_id = discovery.lease_id.as(Int64)

    etcd_client.kv.put(CLUSTER_VERSION_KEY, version, lease_id)
    Log.info { {version: version, message: "leader set version"} }
  end

  # Node stabilization
  #############################################################################

  private getter stabilize_channel : Channel({Array(HoundDog::Service::Node), String}) = Channel({Array(HoundDog::Service::Node), String}).new

  # Consume stabilization events until fiber channel empty
  #
  # Ensures that the node will have only the latest version
  def consume_stabilization_events
    while discovery.registered?
      message = stabilize_channel.receive?
      break unless message
      nodes, version = message
      next if !version.empty? && version < cluster_version
      _stabilize(version, nodes)
    end
  rescue e
    Log.error(exception: e) { "error while consuming stabilization event" } if watching?
  end

  private def _stabilize(cluster_version : String, nodes : Array(HoundDog::Service::Node))
    Log.info { {node_event: "stablizing", version: cluster_version} }
    stabilize.try &.call(nodes)
    set_ready(cluster_version)
    Log.info { {node_event: "stable", version: cluster_version} }
  end

  private def set_ready(version : String)
    unless discovery.registered?
      Log.warn { "unregistered cluster node setting readiness" }
      return
    end

    @cluster_version = version

    # Set the ready key for this node in etcd
    etcd_client.kv.put(node_ready_key, version, discovery.lease_id.as(Int64))
  end

  # Election
  #############################################################################

  # Try to acquire the leader role
  #
  private def handle_election
    unless discovery.registered?
      Log.warn { "unregistered cluster node participating in election" }
      return
    end

    etcd = etcd_client
    lease_id = discovery.lease_id.as(Int64)

    # Determine leader status
    @leader = if (kv = etcd.kv.range(ELECTION_KEY).kvs.first?)
                leader_uri = URI.parse(kv.value.as(String))

                # Check if it is the same as the current node
                uri == leader_uri && lease_id == kv.lease
              else
                # Attempt to set self as if a leader is not already present
                etcd.kv.put_not_exists(ELECTION_KEY, uri.to_s, lease_id)
              end
    Log.info { {is_leader: leader?, leader: leader_node.to_s} }
    update_version if leader?
  rescue e
    if watching?
      Log.error(exception: e) { "error while participating in election" }
    end
  end

  # Cluster readiness
  #############################################################################

  # Leader publishes a new version upon nodes joining/leaving the cluster
  #
  def cluster_change
    update_version if leader?
  rescue e
    if watching?
      Log.error(exception: e) { "Error during cluster change" }
    end
  end

  # Leader has published a new version to etcd
  #
  def handle_version_change(value)
    version = value.first?.try(&.kv.value) || ""
    Log.debug { {is_leader: leader?, version: version, message: "received version change"} }
    stabilize_channel.send({discovery.nodes.dup, version})
  rescue e
    Log.error(exception: e) { "error while watching cluster version" }
  end

  #################################################################################################

  # The leader calls the `on_stable` callback if...
  # + cluster's version state is consistent
  # + consistent state has not already been confirmed to be consistent
  def handle_readiness_event
    if leader? && cluster_consistent? && previous_node_versions != node_versions
      Log.info { {message: "cluster stable", version: cluster_version} }
      @previous_node_versions = @node_versions.dup
      on_stable.try &.call(cluster_version)
    end
  rescue e
    Log.error(exception: e) { "error while handling readiness event" }
  end

  getter node_versions = {} of String => String
  private getter previous_node_versions = {} of String => String

  # When there's an event under the readiness namespace, node creates a hash from node to version.
  # If all the nodes are at the same version, the cluster is consistent.
  def cluster_consistent?
    # Get values under the "readiness key"
    @node_versions = etcd_client.kv.range_prefix(READINESS_KEY).kvs.reduce({} of String => String) do |ready, kv|
      if value = kv.value
        ready[strip_namespace(kv.key, READINESS_KEY)] = value
      end
      ready
    end

    node_versions.all? { |_, v| v == cluster_version }
  end

  # Constants
  #############################################################################

  private CLUSTER_VERSION_KEY = "#{META_NAMESPACE}/cluster_version"
  private ELECTION_KEY        = "#{META_NAMESPACE}/leader"
  private META_NAMESPACE      = "cluster"
  private READINESS_KEY       = "#{META_NAMESPACE}/node_version"
  private SERVICE_NAMESPACE   = "clustering"

  # Helpers
  #############################################################################

  # Generate a new Etcd client
  #
  def etcd_client
    Etcd::Client.new(host: etcd_host, port: etcd_port)
  end

  private def strip_namespace(key, namespace)
    key.lchop("#{namespace}/")
  end

  private def node_ready_key
    "#{READINESS_KEY}/#{name}"
  end

  private def watching?
    election_watcher.watching? &&
      readiness_watcher.watching? &&
      version_watcher.watching? &&
      discovery.registered?
  end
end

class Etcd::Model::WatchEvent
  def to_s(io)
    io << type.to_s << " " << kv.key
    io << " " << kv.value.as(String) if kv.value
  end
end
