# Ensure we require the local version and not one we might have installed already
require File.join([File.dirname(__FILE__),'lib','mogbak_version.rb'])
spec = Gem::Specification.new do |s| 
  s.name = 'mogbak'
  s.license = "MIT"
  s.version = Mogbak::VERSION
  s.author = 'Jesse Angell'
  s.email = 'jesse.angell@firespring.com'
  s.homepage = 'http://www.github.com/firespring/mogbak'
  s.platform = Gem::Platform::RUBY
  s.summary = 'Backup utility for MogileFS'
  s.description = 'mogbak makes it easy to backup and restore mogilefs domains'
  s.files         = `git ls-files`.split("\n")
  s.require_paths << 'lib'
  s.bindir = 'bin'
  s.executables << 'mogbak'
  s.add_development_dependency('awesome_print')

  s.add_runtime_dependency('gli', '>= 1.5.1')
  s.add_runtime_dependency('mysql2', '>= 0.3.11')
  s.add_runtime_dependency('mogilefs-client','>= 3.1.1')
  s.add_runtime_dependency('json','>= 1.6.5')
  s.add_runtime_dependency('sqlite3','>=1.3.5')
  s.add_runtime_dependency('activerecord-import','>=0.2.9')


end
