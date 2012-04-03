require 'logger'
#Create a Logger that is a singleton provided by the instance method
class Log
  def self.instance(log_file = nil)
    @@instance ||= create_logger(log_file)
  end

  def self.create_logger(log_file)
    log_file = STDOUT if log_file == nil
    logger = Logger.new(log_file)
    logger.datetime_format = "%Y-%m-%d %H:%M:%S"
    logger.formatter = proc do |severity, datetime, progname, msg|
      "#{datetime}: #{msg}\n"
    end
    logger
  end
end