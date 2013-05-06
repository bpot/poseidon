require 'spec_helper'

require 'test_cluster'

RSpec.configure do |config|
  config.before(:suite) do
    JavaRunner.remove_tmp
    JavaRunner.set_kafka_path!
    $tc = TestCluster.new
    $tc.start
  end

  config.after(:suite) do
    $tc.stop
  end
end
