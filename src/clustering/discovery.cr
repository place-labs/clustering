require "../clustering"

class Clustering::Discovery
  def initialize(@cluster : Clustering, @ttl : Time::Span = 5.seconds)
    @last_updated = Time.unix(0)
    @timer = Time.monotonic - (@ttl + 1.second)
    @mutex = Mutex.new
    @rebalance_callbacks = [] of RendezvousHash ->
    @cluster.on_rebalance { |nodes, _rebalance_complete_cb| @mutex.synchronize { update_node_list(nodes) } }
  end

  getter last_updated : Time
  getter rebalance_callbacks : Array(RendezvousHash ->)
  getter cluster : Clustering

  @nodes : RendezvousHash? = nil

  def on_rebalance(&callback : RendezvousHash ->)
    @rebalance_callbacks << callback
  end

  def nodes : RendezvousHash
    if (nodes = @nodes) && (@cluster.watching? || !expired?)
      # this is up to date as we are listening to cluster.on_rebalance callback
      # or the current list hasn't had its TTL expire
      nodes
    else
      @mutex.synchronize do
        if @nodes.nil? || expired?
          @nodes = @cluster.nodes
          @last_updated = Time.utc
          @timer = Time.monotonic
        end
      end
      @nodes.not_nil!
    end
  end

  protected def update_node_list(nodes : RendezvousHash)
    @nodes = nodes
    @last_updated = Time.utc
    @timer = Time.monotonic
    rebalance_callbacks.each { |callback| spawn { perform(callback, nodes) } }
  end

  protected def perform(callback, nodes)
    callback.call(nodes)
  rescue error
    Log.error(exception: error) { "rebalance callback failed" }
  end

  protected def expired?
    elapsed = Time.monotonic - @timer
    elapsed > @ttl
  end
end
