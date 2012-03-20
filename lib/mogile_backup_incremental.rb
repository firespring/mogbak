#Incrementals are a very similiar process to a full backup.  Just need to change the initialize around a bit.
class MogileBackupIncremental < MogileBackupFull

  def initialize(path)

    #If settings file does not exist then this is not a valid mogilefs full backup
    settings_file = "#{path}/settings.yml"
    raise "settings.yml not found in path.  This must not be an existing backup. See: mogbak help create" unless File.exists?(settings_file)

    require 'yaml'
    #Load up the settings file
    settings = YAML::load(File.open(settings_file))
    @db = settings['db']
    @db_host = settings['db_host']
    @db_port = settings['db_port']
    @db_pass = settings['db_pass']
    @db_user = settings['db_user']
    @domain = settings['domain']
    @tracker_ip = settings['tracker_ip']
    @tracker_port = settings['tracker_port']
    @backup_path = path
    $backup_path = @backup_path

    #verify settings
    check_settings

    require ('domain')
    require('file')
    require('bakfile')
    require('fileclass')
  end

end