require "etcd"
require "hound-dog"
require "logger"
require "uuid"

require "../clustering"

# :nodoc:
class Node
  @num : Int32 = Random.rand(1..65536)

  getter name : String
  getter clustering : Clustering
  getter discovery : HoundDog::Discovery
  getter logger : Logger
  getter stabilize : Array(HoundDog::Service::Node) ->
  getter on_stable : String ->
  delegate stop, leader?, cluster_version, to: clustering

  @name : String
  @uri : String

  def initialize(
    name : String? = nil,
    uri : String? = nil,
    @on_stable : String -> = ->(_version : String) {},
    @stabilize : Array(HoundDog::Service::Node) -> = ->(_nodes : Array(HoundDog::Service::Node)) {},
    @logger : Logger = Logger.new(STDOUT, level: Logger::Severity::DEBUG)
  )
    @logger.formatter = Logger::Formatter.new do |severity, _, _, message, io|
      label = severity.unknown? ? "?" : severity.to_s
      io << label[0] << ": " << message
    end

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
    clustering.start(on_stable: @on_stable) do |v|
      stabilize.call(v)
    end
  end
end
