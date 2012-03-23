# Ensure we require the local version and not one we might have installed already
require File.join([File.dirname(__FILE__),'lib','mogbak_version.rb'])
spec = Gem::Specification.new do |s| 
  s.name = 'mogbak'
  s.version = Mogbak::VERSION
  s.author = 'Jesse Angell'
  s.email = 'jesse.angell@firespring.com'
  s.homepage = 'http://www.github.com/firespring/mogbak'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Utility for backing up and restoring MogileFS domains'
  s.files         = `git ls-files`.split("\n")
  s.require_paths << 'lib'
  s.bindir = 'bin'
  s.executables << 'mogbak'
  s.add_development_dependency('awesome_print')

  s.add_runtime_dependency('gli')
  s.add_runtime_dependency('mysql2')
  s.add_runtime_dependency('mogilefs-client')
  s.add_runtime_dependency('json')
  s.add_runtime_dependency('sqlite3')
  s.add_runtime_dependency('activerecord-import')


end