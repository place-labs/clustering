require "action-controller/logger"
require "etcd"
require "logger"
require "redis"
require "uuid"

require "../clustering"

class Node
  include Clustering
  getter name : String
  @num : Int32 = Random.rand(1..65536)

  getter logger : Logger = Logger.new(STDOUT, level: Logger::Severity::DEBUG)
  getter discovery : HoundDog::Discovery
  getter redis : Redis::PooledClient = Redis::PooledClient.new

  def etcd_client : Etcd::Client
    HoundDog.etcd_client
  end

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

  def stabilize(nodes)
    logger.info "event=STABILIZING nodes=#{nodes}"
    sleep Random.rand(0..4)
    logger.info "event=STABLE nodes=#{nodes}"
  end
end
