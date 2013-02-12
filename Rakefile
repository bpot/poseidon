require 'bundler/gem_tasks'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new("spec:unit") do |t|
  t.pattern = 'spec/unit/*_spec.rb'
end

RSpec::Core::RakeTask.new('spec:integration:simple') do |t|
  t.pattern = 'spec/integration/simple/*_spec.rb'
end

task :spec => 'spec:unit'
task :default => 'spec:unit'
