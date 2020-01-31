require "./node"

node = Node.new.start

100.times do |n|
  node.logger.tag_info(tick: n, is_leader: node.leader?, cluster_version: node.cluster_version, nodes: node.discovery.nodes)
  sleep 1
end

exit
