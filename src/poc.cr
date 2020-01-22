require "etcd"
require "redis"
require "uuid"

require "./coordination"

class Node
  include Coordination
  getter discovery : HoundDog::Discovery

  def initialize(@ip : String, @port : Int32, @name : String = "node-#{UUID.random}")
    @discovery = HoundDog::Discovery.new(service: "poc", ip: @ip, port: @port)
    super()
  end

  def etcd_client
    Etcd.client(HoundDog.settings.etcd_host, HoundDog.settings.etcd_port)
  end

  getter redis_pool = Redis::PooledClient.new

  def redis_client
    Redis.new(url: ENV["REDIS_URL"]?)
  end

  def stabilize(nodes)
    puts "#{name}: start stabilizing, currently has #{nodes}"
    sleep Random.rand(0..5)
    puts "#{name}: stop stabilizing"
  end
end

node = Node.new(ip: "fake-#{UUID.random}", port: Random.rand(1..65536)).start

(0..100).each do
  puts "#{node.name}: #{node.discovery.nodes}"
  sleep 1
end

exit
