require 'singleton'

#Singleton class used to intercept signals.  If a SIGINT or SIGTERM is received a message is outputted and @should_quit
#is set to true
class SignalHandler
  include Singleton
  attr_reader :should_quit

  def handle_signal
    puts "PID #{Process.pid} is gracefully shutting down..."
    @should_quit = true
  end

  def initialize
    @should_quit = false
    Signal.trap("SIGINT") { handle_signal }
    Signal.trap("SIGTERM") { handle_signal }
  end
end