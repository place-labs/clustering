require "spec"
require "../src/example/node"
require "../src/clustering"

Spec.before_each do
  HoundDog.etcd_client do |client|
    client.kv.delete_prefix("cluster")
    client.kv.delete_prefix("service")
  end
  sleep 1
end

Spec.before_suite do
  backend = Log::IOBackend.new
  backend.formatter = Log::Formatter.new do |entry, io|
    io << entry.severity.to_s[0] << ": "
    io << entry.message << " - " unless entry.message.presence.nil?
    io << entry.context.join(", ") { |k, v| "#{k}=#{v.inspect}" }
    io << " " << entry.data.join(", ") { |k, v| "#{k}=#{v.inspect}" }
  end

  Log.setup "*", :trace, backend
end
