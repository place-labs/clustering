require "./helper"
require "hound-dog"

class Test < Node
  getter received_nodes = [] of Array(HoundDog::Service::Node)

  def stabilize(nodes)
    received_nodes << nodes
  end
end

describe Coordination do
  it "verks" do
    test_node_1 = Test.new.start
    test_node_1.leader?.should be_true
    sleep 1

    test_node_2 = Test.new.start
    test_node_2.leader?.should be_false
    sleep 1

    test_node_3 = Test.new.start
    test_node_3.leader?.should be_false
    sleep 1

    test_node_4 = Test.new.start
    test_node_4.leader?.should be_false
    sleep 1

    test_node_1.received_nodes.size.should eq 4
    test_node_2.received_nodes.size.should eq 3
    test_node_3.received_nodes.size.should eq 2
    test_node_4.received_nodes.size.should eq 1
  end
end
