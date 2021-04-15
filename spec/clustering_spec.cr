require "./helper"
require "hound-dog"
require "mutex"

class Test < Node
  getter received_nodes = [] of Array(HoundDog::Service::Node)
  getter versions = [] of String

  @lock0 = Mutex.new
  @lock1 = Mutex.new

  def add_version(version)
    @lock0.synchronize { versions << version }
  end

  def add_nodes(nodes)
    @lock1.synchronize { received_nodes << nodes }
    true
  end

  def initialize
    super(on_stable: ->(version : String) { add_version(version) }, stabilize: ->(nodes : Array(HoundDog::Service::Node)) { add_nodes(nodes) })
  end
end

describe Clustering do
  it "functions" do
    versions = [] of String

    test_node_1 = Test.new
    test_node_1.start
    sleep 0.1
    test_node_1.leader?.should be_true
    versions << test_node_1.cluster_version

    test_node_2 = Test.new
    test_node_2.start
    sleep 0.1
    test_node_2.leader?.should be_false
    versions << test_node_2.cluster_version

    test_node_3 = Test.new
    test_node_3.start
    sleep 0.1
    test_node_3.leader?.should be_false
    versions << test_node_3.cluster_version

    test_node_4 = Test.new
    test_node_4.start
    sleep 0.1
    test_node_4.leader?.should be_false
    versions << test_node_4.cluster_version

    # Ensure correct amount of cluster changes
    test_node_1.received_nodes.size.should eq 4
    test_node_2.received_nodes.size.should eq 3
    test_node_3.received_nodes.size.should eq 2
    test_node_4.received_nodes.size.should eq 1

    # Ensure ascending versions
    test_node_1.versions.each_cons_pair do |v1, v2|
      v1.should be < v2
    end

    # Ensure correct versions on nodes
    (test_node_1.cluster_version == test_node_2.cluster_version == test_node_3.cluster_version == test_node_4.cluster_version).should be_true

    # Check last published version matches cluster version
    test_node_1.versions.last.should eq test_node_1.cluster_version

    # Check for a consistent version history
    test_node_1.versions.should eq versions

    # Check removing a node, removes from all members
    test_node_3.stop
    sleep 0.1

    test_node_1.discovery.nodes.size.should eq 3
    test_node_2.discovery.nodes.size.should eq 3
    test_node_3.discovery.nodes.size.should eq 3

    test_node_1.stop
    test_node_2.stop
    test_node_3.stop
  end
end
