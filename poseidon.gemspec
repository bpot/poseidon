# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'poseidon/version'

Gem::Specification.new do |gem|
  gem.name          = "poseidon"
  gem.version       = Poseidon::VERSION
  gem.authors       = ["Bob Potter"]
  gem.email         = ["bobby.potter@gmail.com"]
  gem.description   = %q{A Kafka (http://kafka.apache.org/) producer and consumer}
  gem.summary       = %q{Poseidon is a producer and consumer implementation for Kafka >= 0.8}
  gem.homepage      = "https://github.com/bpot/poseidon"
  gem.licenses      = ["MIT"]
  gem.required_ruby_version = '>= 1.9.3'

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  # XXX versions
  gem.add_dependency 'activesupport'
  gem.add_dependency 'concurrent-ruby'
  gem.add_dependency 'timestamp'
#  gem.add_development_dependency(%q<lz4-ruby>)
  gem.add_development_dependency(%q<rspec>, '>= 3')
  gem.add_development_dependency(%q<simplecov>)
  gem.add_development_dependency(%q<snappy>)
  gem.add_development_dependency(%q<yard>)
end
