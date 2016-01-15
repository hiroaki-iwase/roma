#!/usr/bin/env ruby

require 'test/unit'
require 'pathname'

base_path = Pathname(__FILE__).dirname.parent.expand_path

$LOAD_PATH.unshift("#{base_path}/lib")
$LOAD_PATH.unshift("#{base_path}/test")

client_base_path = Pathname(__FILE__).dirname.parent.parent.expand_path
$LOAD_PATH.unshift("#{client_base_path}/roma-ruby-client/lib")

require 'roma-test-utils'

puts "* ROMA Version : #{Roma::Config::VERSION}"
puts "* Ruby Client Version : #{Roma::Client::VERSION::STRING}"

Dir["#{base_path}/test/t_*.rb"].each do |test_file|
  require File.basename(test_file, '*.rb')
end

### optional test
require_relative './optional_test/t_routing_logic.rb'
#require_relative './optional_test/t_mkroute_rich.rb'
#require_relative './optional_test/t_other_database.rb'
#require_relative './optional_test/t_other_cpdb.rb'

exit(Test::Unit::AutoRunner.run)
