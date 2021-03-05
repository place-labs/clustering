require "etcd"
require "hound-dog"
require "uuid"

require "../clustering"

# :nodoc:
class Node
  Log = ::Log.for(self)

  @num : Int32 = Random.rand(1..65536)

  getter name : String
  getter clustering : Clustering
  getter discovery : HoundDog::Discovery
  getter stabilize : Array(HoundDog::Service::Node) -> Bool
  getter on_stable : String ->
  delegate stop, leader?, cluster_version, to: clustering

  @name : String
  @uri : String

  def initialize(
    name : String? = nil,
    uri : String? = nil,
    @on_stable : String -> = ->(_version : String) {},
    @stabilize : Array(HoundDog::Service::Node) -> Bool = ->(_nodes : Array(HoundDog::Service::Node)) { true }
  )
    @name = name || "#{@num.to_s.rjust(5, '0')}"
    @uri = uri || "https://fake-#{@name}:#{@num}"

    @discovery = HoundDog::Discovery.new(service: "poc", name: @name, uri: @uri)
    @clustering = Clustering.new(
      name: @name,
      uri: @uri,
      discovery: @discovery,
    )
  end

  def tick(n)
    Log.info { "tick=#{n}, is_leader=#{leader?}, version=#{cluster_version}, nodes=#{discovery.nodes}" }
    sleep 1
  end

  def start
    clustering.start(on_stable: @on_stable) do |v|
      stabilize.call(v)
    end
  end
end
