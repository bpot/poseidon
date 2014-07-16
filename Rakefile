require 'bundler/gem_tasks'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new("spec:unit") do |t|
  t.pattern = 'spec/unit/*_spec.rb'
end

RSpec::Core::RakeTask.new('spec:integration:simple') do |t|
  t.pattern = 'spec/integration/simple/*_spec.rb'
  t.rspec_opts = ["--fail-fast", "-f d"]
end

RSpec::Core::RakeTask.new('spec:integration:multiple_brokers') do |t|
  t.pattern = 'spec/integration/multiple_brokers/*_spec.rb'
  t.rspec_opts = ["--fail-fast", "-f d"]
end

task :spec => 'spec:unit'
task 'spec:all' => ['spec:unit', 'spec:integration:simple', 'spec:integration:multiple_brokers']
task :default => 'spec:unit'
