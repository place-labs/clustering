require "etcd"

require "./cluster"

class Node
  include Cluster

  def etcd_client
    Etcd.client(HoundDog.settings.etcd_host, HoundDog.settings.etcd_port)
  end
end
