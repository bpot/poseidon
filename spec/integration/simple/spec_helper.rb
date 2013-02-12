require 'spec_helper'

require 'test_cluster'

if ENV['KAFKA_PATH']
  JavaRunner.kafka_path = ENV['KAFKA_PATH']
else
  puts "******To run integration specs you must set KAFKA_PATH to kafka src directory. See README*****"
  exit
end

RSpec.configure do |config|
  config.before(:suite) do
    TestCluster.remove_tmp
    $tc = TestCluster.new
    $tc.start
  end

  config.after(:suite) do
    $tc.stop
  end
end
