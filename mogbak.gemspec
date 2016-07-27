# Ensure we require the local version and not one we might have installed already
require File.join([File.dirname(__FILE__), 'lib', 'mogbak_version.rb'])

Gem::Specification.new do |s|
  s.name = 'mogbak'
  s.license = 'MIT'
  s.version = Mogbak::VERSION
  s.author = 'Jesse Angell'
  s.email = 'jesse.angell@firespring.com'
  s.homepage = 'http://www.github.com/firespring/mogbak'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Backup utility for MogileFS'
  s.description = 'mogbak makes it easy to backup and restore mogilefs domains'
  s.files = `git ls-files`.split("\n")
  s.require_paths << 'lib'
  s.bindir = 'bin'
  s.executables << 'mogbak'
  s.add_development_dependency('awesome_print')

  s.add_runtime_dependency('gli', '~> 2.14.0')
  s.add_runtime_dependency('mysql2', '~> 0.4.4')
  s.add_runtime_dependency('mogilefs-client', '~> 3.9.0')
  s.add_runtime_dependency('sqlite3', '~> 1.3.11')
  s.add_runtime_dependency('activerecord', '~> 5.0.0')
  s.add_runtime_dependency('activerecord-import', '~> 0.15.0')
  s.add_runtime_dependency('composite_primary_keys', '>= 9.0.0')
  s.add_runtime_dependency('aws-sdk', '~> 2')
end
