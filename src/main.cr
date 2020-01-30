require "./node"

node = Node.new.start

100.times do |n|
  node.logger.info("tick #{n}")
  sleep 1
end

exit
