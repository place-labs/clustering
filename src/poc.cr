require "etcd"
require "logger"
require "redis"
require "uuid"

require "./coordination"

class Node
  include Coordination
  getter discovery : HoundDog::Discovery
  getter name : String
  getter logger : Logger = Logger.new(STDOUT, level: Logger::Severity::DEBUG)

  private getter num : Int32 = Random.rand(1..65536)

  def initialize(
    @ip : String = "fake-#{num.to_s.rjust(5, '0')}",
    @port : Int32 = num,
    @name : String = "node-#{num.to_s.rjust(5, '0')}"
  )
    @discovery = HoundDog::Discovery.new(service: "poc", ip: @ip, port: @port)
    super()
  end

  def etcd_client : Etcd::Client
    HoundDog.etcd_client
  end

  getter redis_pool : Redis::PooledClient = Redis::PooledClient.new

  def redis_client : Redis::Client
    Redis.new(url: ENV["REDIS_URL"]?)
  end

  def stabilize(nodes)
    puts "#{name}: start stabilizing, currently has #{nodes}"
    sleep Random.rand(0..5)
    puts "#{name}: stop stabilizing"
  end
end

node = Node.new.start

(0..100).each do
  node.logger.info "#{node.name}: v=#{node.cluster_version} l?=#{node.leader?} #n=#{node.discovery.nodes.size} n=#{node.discovery.nodes.map &.[:ip]}"
  sleep 1
end

exit
