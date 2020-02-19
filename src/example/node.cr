require "action-controller/logger"
require "etcd"
require "hound-dog"
require "logger"
require "redis"
require "uuid"

require "../clustering"

class Node
  alias TaggedLogger = ActionController::Logger::TaggedLogger

  @num : Int32 = Random.rand(1..65536)

  getter name : String
  getter clustering : Clustering
  getter discovery : HoundDog::Discovery
  getter logger : TaggedLogger

  delegate start, stop, leader?, cluster_version, to: clustering

  @ip : String
  @port : Int32
  @name : String

  def initialize(
    ip : String? = nil,
    port : Int32? = nil,
    name : String? = nil,
    stabilize : Array(HoundDog::Service::Node) -> Void = ->(_nodes : Array(HoundDog::Service::Node)) {},
    @logger : TaggedLogger = TaggedLogger.new(ActionController::Base.settings.logger)
  )
    @logger.level = Logger::Severity::DEBUG

    @name = name || "#{@num.to_s.rjust(5, '0')}"
    @ip = ip || "fake-#{@name}"
    @port = port || @num

    @discovery = HoundDog::Discovery.new(service: "poc", ip: @ip, port: @port)
    @clustering = Clustering.new(
      ip: @ip,
      port: @port,
      discovery: @discovery,
      logger: @logger,
      stabilize: stabilize,
    )
  end
end
