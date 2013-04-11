# Merges a mogile domain with another mogile domain
class Merge
  attr_accessor :source_mogile, :dest_mogile

  # TODO:
  #@param [Hash] o hash containing any settings for the merge
  def initialize(cli_settings={})
    # If a settings file was given, use it. Otherwise default one.
    settings_path = cli_settings[:settings]
    settings_path ||= File.expand_path(File.join(Dir.pwd, "#{self.class.name.downcase}.settings.yml"))

    # First see if a settings file exists. If it does, use it's properties as a starting point.
    settings = load_settings(settings_path)

    # Next merge in all properties from the command line (this way a user can override a specific setting if they want to.)
    merge_cli_settings(settings, cli_settings)

    puts settings[:source][:db_host]

    # Verify mysql settings
    establish_connection(
      settings[:source][:db_host],
      settings[:source][:db_port],
      settings[:source][:db_username],
      settings[:source][:db_passwd],
      settings[:source][:db])

    establish_connection(
      settings[:dest][:db_host],
      settings[:dest][:db_port],
      settings[:dest][:db_username],
      settings[:dest][:db_passwd],
      settings[:dest][:db])

    # Verify mogile settings
    @source_mogile = mogile_tracker_connect(settings[:source][:tracker_ip], settings[:source][:tracker_port], settings[:source][:domain])
    @dest_mogile = mogile_tracker_connect(settings[:dest][:tracker_ip], settings[:dest][:tracker_port], settings[:dest][:domain])

    # Save settings
    save_settings(settings_path, settings)
  end

  def load_settings(filename)
    require('yaml')

    # Check if a settings file exists. If it does, load all relevant properties
    settings = {}
    if File.exists?(filename)
      settings = YAML.load(File.open(filename))

      Log.instance.info("Settings loaded from [ #{filename} ]")
    end

    settings
  end

  def merge_cli_settings(settings, cli_settings)
    # Override any settings we passed in on the command line
    settings[:source] ||= {}
    settings[:source][:db] =           cli_settings[:source_db]           if cli_settings[:source_db]
    settings[:source][:db_host] =      cli_settings[:source_db_host]      if cli_settings[:source_db_host]
    settings[:source][:db_port] =      cli_settings[:source_db_port].to_i if cli_settings[:source_db_port]
    settings[:source][:db_passwd] =    cli_settings[:source_db_passwd]    if cli_settings[:source_db_passwd]
    settings[:source][:db_username] =  cli_settings[:source_db_username]  if cli_settings[:source_db_username]
    settings[:source][:domain] =       cli_settings[:source_domain]       if cli_settings[:source_domain]
    settings[:source][:tracker_ip] =   cli_settings[:source_tracker_ip]   if cli_settings[:source_tracker_ip]
    settings[:source][:tracker_port] = cli_settings[:source_tracker_port] if cli_settings[:source_tracker_port]

    settings[:dest] ||= {}
    settings[:dest][:db] =           cli_settings[:dest_db]           if cli_settings[:dest_db]
    settings[:dest][:db_host] =      cli_settings[:dest_db_host]      if cli_settings[:dest_db_host]
    settings[:dest][:db_port] =      cli_settings[:dest_db_port].to_i if cli_settings[:dest_db_port]
    settings[:dest][:db_passwd] =    cli_settings[:dest_db_passwd]    if cli_settings[:dest_db_passwd]
    settings[:dest][:db_username] =  cli_settings[:dest_db_username]  if cli_settings[:dest_db_username]
    settings[:dest][:domain] =       cli_settings[:dest_domain]       if cli_settings[:dest_domain]
    settings[:dest][:tracker_ip] =   cli_settings[:dest_tracker_ip]   if cli_settings[:dest_tracker_ip]
    settings[:dest][:tracker_port] = cli_settings[:dest_tracker_port] if cli_settings[:dest_tracker_port]
  end

  #Save the settings for the backup into a yaml file (settings.yaml) so that an incremental can be ran without so many parameters
  #@return [Bool] true or false
  def save_settings(filename, settings)
    require 'yaml'

    File.open(filename, 'w') do |file|
      file.write(settings.to_yaml)
      Log.instance.info("Settings written to [ #{filename} ]")
    end

    true
  end

  # Establish a connection to a mysql database
  # @param [String] host
  # @param [String] port
  # @param [String] user
  # @param [String] passwd
  # @param [String] schema
  # @param [String] adapter (optional)
  # @param [String] reconnect (optional)
  def establish_connection(host, port, username, passwd, schema,adapter='mysql2', reconnect=true)
    connection = nil
puts host
    #Verify that we can connect to the mogilefs mysql server
    begin
      pool = ActiveRecord::Base.establish_connection({
        :adapter => adapter,
        :host => host,
        :port => port,
        :username => username,
        :password => passwd,
        :database => schema,
        :reconnect => true})

      # Connections don't seem to actually be verified as valid until '.connection' is called.
      connection = pool.connection
      Log.instance.info("Connected to mysql database [ #{username}@#{host}/#{schema} ].")

    rescue Exception => e
      Log.instance.error("Could not connect to MySQL database: #{e}\n#{e.backtrace}")
      raise 'Could not connect to MySQL database'
    end

    connection
  end

  # Connect to mogile tracker
  # @param [String] ip 
  # @param [String] port 
  def mogile_tracker_connect(ip, port, domain)
    hosts = ["#{ip}:#{port}"]

    mogile = nil
    begin
      mogile = MogileFS::MogileFS.new(:domain => domain, :hosts => hosts)

      # Connections don't seem to actually be verified as valid until we do something with them
      # (this will also validate the domain exists)
      mogile.exist?('foo')
      Log.instance.info("Connected to mogile tracker [ #{hosts.join(',')} -> #{domain} ].")

    rescue Exception => e
      Log.instance.error("Could not connect to MogildFS tracker: #{e}\n#{e.backtrace}")
      raise 'Could not connect to MogileFS tracker'
    end

    mogile
  end
end
