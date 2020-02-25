require "action-controller/logger"
require "etcd"
require "hound-dog"
require "logger"
require "redis"
require "uuid"

require "../clustering"

# :nodoc:
class Node
  private alias TaggedLogger = ActionController::Logger::TaggedLogger

  @num : Int32 = Random.rand(1..65536)

  getter name : String
  getter clustering : Clustering
  getter discovery : HoundDog::Discovery
  getter logger : TaggedLogger
  getter stabilize : Array(HoundDog::Service::Node) ->
  delegate stop, leader?, cluster_version, to: clustering

  @name : String
  @uri : String

  def initialize(
    name : String? = nil,
    uri : String? = nil,
    @stabilize : Array(HoundDog::Service::Node) -> = ->(_nodes : Array(HoundDog::Service::Node)) {},
    @logger : TaggedLogger = TaggedLogger.new(ActionController::Base.settings.logger)
  )
    @logger.level = Logger::Severity::DEBUG

    @name = name || "#{@num.to_s.rjust(5, '0')}"
    @uri = uri || "https://fake-#{@name}:#{@num}"

    @discovery = HoundDog::Discovery.new(service: "poc", name: @name, uri: @uri)
    @clustering = Clustering.new(
      name: @name,
      uri: @uri,
      discovery: @discovery,
      logger: @logger,
    )
  end

  def start
    clustering.start &stabilize
  end
end
