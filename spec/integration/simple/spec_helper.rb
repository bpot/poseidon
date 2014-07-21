require 'spec_helper'

require 'test_cluster'

RSpec.configure do |config|
  config.before(:each) do
    JavaRunner.remove_tmp
    JavaRunner.set_kafka_path!
    $tc = TestCluster.new
    $tc.start
    sleep 10
  end

  config.after(:each) do
    $tc.stop
  end
end
