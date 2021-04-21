require "rendezvous-hash"

abstract class Clustering
  alias RebalanceComplete = ->

  def initialize(@service)
    @rebalance_callbacks = [] of (RendezvousHash, RebalanceComplete) ->
    @cluster_stable_callbacks = [] of ->
  end

  # the name of the service you are clustering
  getter service : String

  getter rebalance_callbacks : Array((RendezvousHash, RebalanceComplete) ->)
  getter cluster_stable_callbacks : Array(->)

  # Called when the cluster has changed
  def on_rebalance(&callback : (RendezvousHash, RebalanceComplete) ->)
    rebalance_callbacks << callback
    callback
  end

  # Called on leader node when the cluster has stabilised
  def on_cluster_stable(&callback : ->)
    cluster_stable_callbacks << callback
    callback
  end

  # registers this node with the cluster as a member
  abstract def register : Bool

  # removes this node from the cluster as a member
  abstract def unregister : Bool

  # is this node registered as part of the cluster
  abstract def registered? : Bool

  # is this class watching for changes to the cluster
  # this should always return true if registered returns true
  abstract def watching? : Bool

  # should return true if the current node is the leader
  abstract def leader? : Bool

  # returns the list of known nodes
  abstract def nodes : RendezvousHash
end

require "./clustering/*"
