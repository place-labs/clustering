require "spec"
require "../src/example/node"
require "../src/clustering"

backend = Log::IOBackend.new
backend.formatter = Log::Formatter.new do |entry, io|
  io << entry.severity.to_s[0] << ": " << entry.message << " - "
  io << entry.context.to_s
end

Log.builder.bind "*", :debug, backend
