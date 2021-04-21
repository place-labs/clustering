require "./helper"

describe Clustering do
  channel = Channel(Nil).new
  Spec.before_each { channel = Channel(Nil).new }

  it "should join a cluster of one" do
    rebalanced_called = false

    node1 = ClusteringExample.new("service_name", URI.parse "http://node1/service")
    node1.on_rebalance do |_nodes, rebalance_complete_cb|
      rebalanced_called = true
      rebalance_complete_cb.call
    end
    node1.on_cluster_stable { channel.send(nil) }

    node1.leader?.should be_false
    node1.watching?.should be_false
    node1.registered?.should be_false
    rebalanced_called.should be_false
    node1.nodes.nodes.size.should eq 0

    node1.register

    node1.watching?.should be_true
    node1.registered?.should be_true

    rebalanced_called.should be_false
    node1.leader?.should be_false
    node1.nodes.nodes.size.should eq 0

    # wait for cluster stablaisation
    channel.receive?

    node1.leader?.should be_true
    node1.watching?.should be_true
    node1.registered?.should be_true
    rebalanced_called.should be_true
    node1.nodes.nodes.size.should eq 1

    node1.unregister

    node1.leader?.should be_false
    node1.watching?.should be_false
    node1.registered?.should be_false
    node1.nodes.nodes.size.should eq 0
  end

  it "should join a cluster of one and then rebalance when a new node joins" do
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

    node2.unregister

    # wait for cluster stablaisation
    channel.receive?

    node1.nodes.nodes.size.should eq 1
    node2.nodes.nodes.size.should eq 1
    node1.leader?.should be_true

    node1.unregister

    node1.nodes.nodes.size.should eq 0
    node2.nodes.nodes.size.should eq 0
  end
end
