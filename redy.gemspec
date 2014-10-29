# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'redy/version'

Gem::Specification.new do |spec|
  spec.name          = 'redy'
  spec.version       = Redy::VERSION
  spec.authors       = ['Genki Sugawara']
  spec.email         = ['sgwr_dts@yahoo.co.jp']
  spec.summary       = %q{TODO: Write a short summary. Required.}
  spec.description   = %q{TODO: Write a longer description. Optional.}
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_dependency 'aws-sdk-core', '>= 2.0.0.rc15'
  spec.add_dependency 'fluent-logger'
  spec.add_dependency 'msgpack'
  spec.add_dependency 'redis'
  spec.add_dependency 'redis-namespace'
  spec.add_development_dependency 'bundler'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rspec', '>= 3.0.0'
  spec.add_development_dependency 'ddbcli'
  spec.add_development_dependency 'fluentd'
  spec.add_development_dependency 'fluent-plugin-dynamodb-alt', '>= 0.1.5'
end
