require "uri"
require "../src/clustering"

class ClusteringExample < Clustering
  # NOTE:: rebalance might be important to you, but might not be required for something like mDNS discovery
  # node_info => rebalance_complete
  @@services = Hash(String, Hash(String, Bool)).new { |h, k| h[k] = {} of String => Bool }
  @@node_lock = Mutex.new

  def self.reset
    @@services = Hash(String, Hash(String, Bool)).new { |h, k| h[k] = {} of String => Bool }
  end

  # typically with clustering we want to know the address of the nodes in the
  # cluster, often this can be represented as a URI
  def initialize(service : String, node_info : URI? = nil)
    super(service)

    @watching = false
    @registered = false
    @leader = false

    # for clustering purposes, we need the node_info in a shareable format
    @node_info = node_info.to_s
    @nodes = RendezvousHash.new

    # whilst in this example we have access to the global cluster state,
    # in a real-world implementation you would want to keep the last known state locally
    @local_state = {} of String => Bool
    @cluster_version = 0_u64
  end

  getter? registered : Bool
  getter? watching : Bool
  getter? leader : Bool
  getter node_info : String

  def register : Bool
    raise "no node information provided to join cluster" unless @node_info.presence

    @@node_lock.synchronize do
      return false if watching?

      @@services[service][node_info] = false
      @registered = true
      @watching = true
      @leader = false
    end
    spawn { maintain_registration }
    true
  end

  def unregister : Bool
    @@node_lock.synchronize do
      return false unless registered?

      @@services[service].delete node_info
      @registered = false
      @watching = false
      @leader = false
      @nodes = RendezvousHash.new
    end
    true
  end

  def nodes : RendezvousHash
    # this is a bit of a hack for specs as typically you'd be polling or watching
    # this allows us to emulate both behaviours
    if watching? && @node_info.presence
      @nodes
    else
      # manually obtain the list as we are not actively monitoring the cluster
      RendezvousHash.new(@@node_lock.synchronize { @@services[service].keys })
    end
  end

  def monitor_cluster : Bool
    @@node_lock.synchronize do
      return false if watching?
      spawn { monitor_only }
      @watching = true
    end
    true
  end

  # this would be the method monitoring the active nodes using etcd, zookeeper, mdns, redis, etc
  protected def maintain_registration
    loop do
      break unless registered?

      # obtain cluster state from store
      new_state = @@node_lock.synchronize { @@services[service].dup }
      nodes = new_state.keys

      # Check for changes in the cluster
      cluster_changed = nodes != @local_state.keys
      cluster_readiness_changed = new_state.values != @local_state.values

      # update local state
      @local_state = new_state
      @leader = nodes[0] == @node_info
      @nodes = RendezvousHash.new(nodes)

      if cluster_changed
        Log.trace { "#{@node_info} detected a change in cluster state" }

        # rebalance required, bump cluster version
        version = (@cluster_version += 1)
        rebalance_complete_cb = Proc(Nil).new { node_is_ready version }

        # mark this node as not ready
        @@node_lock.synchronize { @@services[service][@node_info] = false }
        @local_state[@node_info] = false

        rebalance_callbacks.each { |cb| spawn { cb.call(@nodes, rebalance_complete_cb) } }
      elsif @leader && cluster_readiness_changed
        # has the rebalance complete
        # NOTE:: depending on your cluster you might not care about cluster readiness
        check = @local_state.values.uniq!
        if check.size == 1 && check.first
          Log.trace { "#{@node_info} is leader, cluster is ready" }
          cluster_stable_callbacks.each { |cb| spawn { cb.call } }
        else
          Log.trace { "#{@node_info} is leader, cluster not ready" }
        end
      end

      sleep 1
    end
  end

  protected def node_is_ready(version)
    return unless version == @cluster_version
    Log.trace { "#{@node_info} is ready on version #{version}" }
    @@node_lock.synchronize { @@services[service][@node_info] = true }
  end

  # Monitoring when not registered is completely up to the implementor
  protected def monitor_only
    rebalance_complete_cb = Proc(Nil).new { }

    loop do
      break unless watching?

      @nodes = RendezvousHash.new(@@node_lock.synchronize { @@services[service].keys })
      @cluster_version += 1
      rebalance_callbacks.each { |cb| spawn { cb.call(@nodes, rebalance_complete_cb) } }

      sleep 0.1
    end
  end
end
