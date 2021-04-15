require "./node"

node = Node.new(stabilize: ->(_nodes : Array(HoundDog::Service::Node)) { sleep Random.rand(0..4).milliseconds; true })
node.start

100.times do |n|
  node.tick(n)
end

exit
