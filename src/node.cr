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

  @num : Int32 = Random.rand(1..65536)

  def initialize(
    ip : String? = nil,
    port : Int32? = nil,
    name : String? = nil
  )
    @ip = ip || "fake-#{@num.to_s.rjust(5, '0')}"
    @port = port || @num
    @name = name || "#{@num.to_s.rjust(5, '0')}"

    @discovery = HoundDog::Discovery.new(service: "poc", ip: @ip, port: @port)
    super()
  end

  def etcd_client : Etcd::Client
    HoundDog.etcd_client
  end

  getter redis_pool : Redis::PooledClient = Redis::PooledClient.new

  def stabilize(nodes)
    puts "#{name}: started stabilizing, i have #{nodes}"
    sleep Random.rand(0..5)
    puts "#{name}: stopped stabilizing"
  end
end
