require 'logger'
#Create a Logger that is a singleton provided by the instance method
class Log
  def self.instance(log_file = nil)
    @@instance ||= create_logger(log_file)
  end

  def self.create_logger(log_file)
    log_file = STDOUT if log_file == nil
    logger = Logger.new(log_file)
    logger.datetime_format = "%Y-%m-%d %H:%M:%S.%6N"
    logger.formatter = proc do |severity, datetime, progname, msg|
      "#{datetime.strftime('%H:%M:%S.%6N')}: #{severity.ljust(6)} #{msg}\n"
    end
    logger
  end
end
