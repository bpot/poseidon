require 'spec_helper'

require 'test_cluster'

class ThreeBrokerCluster
  def initialize
    @zookeeper = ZookeeperRunner.new
    @brokers = (9092..9094).map { |port| BrokerRunner.new(port - 9092, port, 3) }
  end

  def start
    @zookeeper.start
    @brokers.each(&:start)
  end

  def stop
    @zookeeper.stop
    @brokers.each(&:stop)
  end
end

RSpec.configure do |config|
  config.before(:suite) do
    JavaRunner.remove_tmp
    JavaRunner.set_kafka_path!
    $tc = ThreeBrokerCluster.new
    $tc.start
    sleep 5 # wait for cluster to come up
  end

  config.after(:suite) do
    $tc.stop
  end
end
