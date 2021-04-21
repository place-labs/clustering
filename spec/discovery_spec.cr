require "./helper"

describe Clustering::Discovery do
  channel = Channel(Nil).new
  Spec.before_each { channel = Channel(Nil).new }

  it "should return a rendezvous-hash for the cluster" do
    node1 = ClusteringExample.new("service_name", URI.parse "http://node1/service")
    node1.on_rebalance { |_nodes, rebalance_complete_cb| rebalance_complete_cb.call }
    node1.on_cluster_stable { channel.send(nil) }
    node1.register

    # wait for cluster stablaisation
    channel.receive?
    node1.nodes.nodes.size.should eq 1
    node1.leader?.should be_true

    node2 = ClusteringExample.new("service_name", URI.parse "http://node2/service")
    node2.on_rebalance { |_nodes, rebalance_complete_cb| rebalance_complete_cb.call }
    node2.on_cluster_stable { channel.send(nil) }
    node2.register

    # wait for cluster stablaisation (only the leader will receive this event)
    channel.receive?
    node1.nodes.nodes.size.should eq 2
    node2.nodes.nodes.size.should eq 2
    node1.leader?.should be_true
    node2.leader?.should be_false

    # Get the cluster state
    lookup = Clustering::Discovery.new ClusteringExample.new("service_name"), ttl: 1.second
    hash = lookup.nodes
    hash.nodes.includes?("http://node1/service").should be_true
    hash.nodes.includes?("http://node2/service").should be_true

    node2.unregister
    node1.unregister

    # Should return the old nodes due to TTL
    hash = lookup.nodes
    hash.nodes.includes?("http://node1/service").should be_true
    hash.nodes.includes?("http://node2/service").should be_true

    sleep 1
    hash = lookup.nodes
    hash.nodes.size.should eq 0
  end

  it "should return a rendezvous-hash for the cluster, with watching enabled" do
    watch_channel = Channel(Nil).new

    node1 = ClusteringExample.new("service_name", URI.parse "http://node1/service")
    node1.on_rebalance { |_nodes, rebalance_complete_cb| rebalance_complete_cb.call }
    node1.on_cluster_stable { channel.send(nil) }
    node1.register

    # wait for cluster stablaisation
    channel.receive?
    node1.nodes.nodes.size.should eq 1
    node1.leader?.should be_true

    # Get the cluster state
    cluster = ClusteringExample.new("service_name")
    cluster.monitor_cluster

    # we want to monitor the cluster using callbacks
    lookup = Clustering::Discovery.new cluster, ttl: 1.second
    lookup.on_rebalance do |_nodes|
      watch_channel.send(nil)
    end
    hash = lookup.nodes
    hash.nodes.includes?("http://node1/service").should be_true

    node2 = ClusteringExample.new("service_name", URI.parse "http://node2/service")
    node2.on_rebalance { |_nodes, rebalance_complete_cb| rebalance_complete_cb.call }
    node2.on_cluster_stable { channel.send(nil) }
    node2.register

    # wait for cluster stablaisation (only the leader will receive this event)
    channel.receive?
    node1.nodes.nodes.size.should eq 2
    node2.nodes.nodes.size.should eq 2
    node1.leader?.should be_true
    node2.leader?.should be_false

    # wait for the latest node list on lookup
    loop do
      watch_channel.receive?
      break if lookup.nodes.nodes.size > 1
    end
    hash = lookup.nodes
    hash.nodes.includes?("http://node1/service").should be_true
    hash.nodes.includes?("http://node2/service").should be_true

    node2.unregister
    node1.unregister

    # Should not return old nodes are we are monitoring, so not using TTL
    sleep 0.2
    hash = lookup.nodes
    hash.nodes.size.should eq 0
  end
end
