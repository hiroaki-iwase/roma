lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib)

require 'lib/roma/version'

Gem::Specification.new do |s|
  s.authors = ["Junji Torii", "Hiroki Matsue"]
  s.homepage = 'http://code.google.com/p/roma-prj/'
  s.name = "roma"
  s.version = Roma::VERSION
  s.licnese = 'GPL-3.0'
  s.summary = "ROMA server"
  s.description = <<-EOF
    ROMA server
  EOF
  s.files = FileList[
    '[A-Z]*',
    'bin/**/*',
    'lib/**/*',
    'test/**/*.rb',
    'spec/**/*.rb',
    'doc/**/*',
    'examples/**/*',
  ]

  # TODO: Specify Ruby version roma-1.1.0 must support
  # s.required_ruby_version = '>= 2.0.0'

  # Use these for libraries.
  s.require_path = 'lib'

  # Use these for applications.
  s.bindir = "bin"
  s.executables = Dir.entries(base + 'bin').reject{ |d| d =~ /^\.+$/ || d =~ /^sample_/ }

  s.default_executable = "romad"

  s.has_rdoc = true
  s.rdoc_options.concat RDOC_OPTIONS
  s.extra_rdoc_files = ["README", "CHANGELOG"]

  # TODO: for each gem, which version does rom depend on?
  s.add_dependency 'eventmachine'

  s.add_development_dependency 'ffi'
  s.add_development_dependency 'gdbm'
  s.add_development_dependency 'sqlite3'
  s.add_development_dependency 'rroonga'
  s.add_development_dependency 'test-unit'
end
