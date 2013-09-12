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
  gem.license       = ['MIT']

  gem.files         = `git ls-files`.split($/)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]

  gem.add_development_dependency(%q<rspec>)
  gem.add_development_dependency(%q<yard>)
  gem.add_development_dependency(%q<simplecov>)
  gem.add_development_dependency(%q<daemon_controller>)
end
