#Creates a backup profile to be used by Backup
class Create
  attr_accessor :db, :db_host, :db_port, :db_pass, :db_user, :domain, :tracker_host, :tracker_port, :workers
  include Validations

  #Run validations and create the backup profile
  #@param [Hash] o hash containing the settings for the backup profile
  def initialize(o={})
    @db = o[:db] if o[:db]
    @db_host = o[:db_host] if o[:db_host]
    @db_port = o[:db_port] if o[:db_port]
    @db_pass = o[:db_pass] if o[:db_pass]
    @db_user = o[:db_user] if o[:db_user]
    @domain = o[:domain] if o[:domain]
    @tracker_ip = o[:tracker_ip] if o[:tracker_ip]
    @tracker_port = o[:tracker_port] if o[:tracker_port]

    #If settings.yml exists then this is an existing backup and you cannot run a create on top of it
    raise 'Cannot run create on an existing backup.  Try: mogbak backup #{$backup_path} to backup.  If you want
        to change settings on this backup profile you will have to edit #{$backup_path}/settings.yml manually.' if check_settings_file(nil)

    check_backup_path
    create_sqlite_db
    connect_sqlite
    migrate_sqlite
    mogile_db_connect
    mogile_tracker_connect
    check_mogile_domain(@domain)

    #Save settings
    save_settings
  end

  #Save the settings for the backup into a yaml file (settings.yaml) so that an incremental can be ran without so many parameters
  #@return [Bool] true or false
  def save_settings
    require ('yaml')
    settings = {
        'db' => @db,
        'db_host' => @db_host,
        'db_port' => @db_port,
        'db_pass' => @db_pass,
        'db_user' => @db_user,
        'domain' => @domain,
        'tracker_ip' => @tracker_ip,
        'tracker_port' => @tracker_port,
        'backup_path' => $backup_path
    }

    File.open("#{$backup_path}/settings.yml", "w") do |file|
      file.write settings.to_yaml
    end

    true
  end
end