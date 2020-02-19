require "./helper"
require "hound-dog"

class Test < Node
  getter received_nodes = [] of Array(HoundDog::Service::Node)

  def initialize
    super(stabilize: ->(nodes : Array(HoundDog::Service::Node)) {
      @received_nodes << nodes
      nil
    })
  end
end

describe Clustering do
  it "functions" do
    versions = [] of String
    redis_versions = [] of String
    # Listen for ready events
    spawn(same_thread: true) do
      Redis.new.subscribe(Clustering.redis_version_channel) do |on|
        on.message do |_, message|
          redis_versions << message
        end
      end
    end
    Fiber.yield

    test_node_1 = Test.new
    test_node_1.start
    sleep 0.1
    test_node_1.leader?.should be_true
    versions << test_node_1.cluster_version

    test_node_2 = Test.new
    test_node_2.logger.level = Logger::Severity::ERROR
    test_node_2.start
    sleep 0.1
    test_node_2.leader?.should be_false
    versions << test_node_2.cluster_version

    test_node_3 = Test.new
    test_node_3.logger.level = Logger::Severity::ERROR
    test_node_3.start
    sleep 0.1
    test_node_3.leader?.should be_false
    versions << test_node_3.cluster_version

    test_node_4 = Test.new
    test_node_4.logger.level = Logger::Severity::ERROR
    test_node_4.start
    sleep 0.1
    test_node_4.leader?.should be_false
    versions << test_node_4.cluster_version

    # Ensure correct amount of cluster changes
    test_node_1.received_nodes.size.should eq 4
    test_node_2.received_nodes.size.should eq 3
    test_node_3.received_nodes.size.should eq 2
    test_node_4.received_nodes.size.should eq 1

    # Ensure correct versions on nodes
    (test_node_1.cluster_version == test_node_2.cluster_version == test_node_3.cluster_version == test_node_4.cluster_version).should be_true

    latest_version = redis_versions.last?

    # Check last published version matches cluster version
    latest_version.should_not be_nil
    latest_version.should eq test_node_1.cluster_version

    # Check for a consistent version history
    redis_versions.should eq versions

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
