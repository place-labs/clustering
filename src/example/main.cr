require "./node"

node = Node.new.start

100.times do |n|
  node.logger.info("t=#{n} l?=#{node.leader?} v=#{node.cluster_version} n=#{node.discovery.nodes}")
  sleep 1
end

exit
