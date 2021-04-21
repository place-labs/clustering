require "log"
require "spec"
require "./clustering_example"

::Log.setup("*", :trace)

Spec.before_suite do
  ::Log.builder.bind("*", backend: ::Log::IOBackend.new(STDOUT), level: ::Log::Severity::Trace)
end

Spec.before_each do
  puts "\n---------------------------------\n\n"
  ClusteringExample.reset
end
