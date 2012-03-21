class MogileBackup
  attr_accessor :db, :db_host, :db_port, :db_pass, :db_user, :domain, :tracker_host, :tracker_port, :backup_path

  def initialize(mode, o={})
    if mode == :create
      @db = o[:db] if o[:db]
      @db_host = o[:db_host] if o[:db_host]
      @db_port = o[:db_port] if o[:db_port]
      @db_pass = o[:db_pass] if o[:db_pass]
      @db_user = o[:db_user] if o[:db_user]
      @domain = o[:domain] if o[:domain]
      @tracker_ip = o[:tracker_ip] if o[:tracker_ip]
      @tracker_port = o[:tracker_port] if o[:tracker_port]
      @backup_path = o[:backup_path] if o[:backup_path]
      $backup_path = @backup_path


      #If settings.yml exists then this is an existing backup and you cannot run a create on top of it
      if File.exists?("#{$backup_path}/settings.yml")
        raise "Cannot run create on an existing backup.  Try: mogbak backup #{$backup_path} to backup.  If you want
        to change settings on this backup profile you will have to edit #{$backup_path}/settings.yml manually."
      end

      #Run other settings checks
      check_settings

      #Save settings
      save_settings
    else
      path = o[:backup_path]
      #If settings file does not exist then this is not a valid mogilefs backup
      settings_file = "#{path}/settings.yml"
      raise "settings.yml not found in path.  This must not be a backup profile. See: mogbak help create" unless File.exists?(settings_file)

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
    end
  end

  #Save the settings for the backup so that an incremental can be ran without so many parameters
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
  end

  def check_settings
    #Error if backup_path is not valid
    raise 'backup_path is not a valid directory' unless File.directory?($backup_path)

    #create the sqlite database
    begin
      if !File.exists?("#{$backup_path}/db.sqlite")
        SQLite3::Database.new("#{$backup_path}/db.sqlite")
      end
    rescue Exception => e
      raise "Could not create #{$backup_path}/db.sqlite - check permissions"
    end

    #connect and run migrations
    begin
      ActiveRecord::Base.establish_connection(:adapter => 'sqlite3', :database => "#{$backup_path}/db.sqlite").connection
    rescue
      raise "Could not open #{$backup_path}/db.sqlite"
    end

    #run migrations
    begin
      ActiveRecord::Migrator.up("db/migrate/")
    rescue
      raise "could not run migrations on #{$backup_path}/db.sqlite"
    end


    #Verify that we can connect to the mogilefs mysql server
    begin
      ActiveRecord::Base.establish_connection({:adapter => "mysql2",
                                               :host => @db_host,
                                               :port => @db_port,
                                               :username => @db_user,
                                               :password => @db_pass,
                                               :database => @db}).connection
    rescue Exception => e
      raise 'Could not connect to MySQL database'
    end

    #open our mogilefs tracker connection
    host = ["#{@tracker_ip}:#{@tracker_port}"]
    $mg = MogileFS::MogileFS.new(:domain => @domain, :hosts => host)

    #Now that database is all setup load the model classes
    require ('domain')
    require('file')
    require('bakfile')
    require('fileclass')

    #check to see if domain exists
    raise 'Domain does not exist in MogileFS.  Cannot backup' unless Domain.find_by_namespace(self.domain)

  end

  def bak_file(file)
    saved = file.save_to_fs

    if saved
      puts "Backed up: FID #{file.fid}"
    else
      puts "Error - will try again on next run: FID #{file.fid}"
    end

    return saved
  end

  def backup(o = {})

    #first we retry files that we haven't been able to backup successfully, if any.
    BakFile.find_each(:conditions => ['saved = ?', false]) do |bak_file|
      file = Fid.find_by_fid(bak_file.fid)
      saved = bak_file(file)
      if saved
        BakFile.transaction do
          bak = BakFile.find_by_fid(bak_file.fid)
          bak.saved = true
          bak.save
        end
      end
    end

    #now back up any new files.  if they fail to be backed up we'll retry them the next time the backup
    #command is ran.
    dmid = Domain.find_by_namespace(self.domain)
    results = Fid.find_each(:conditions => ['dmid = ? AND fid > ?', dmid, BakFile.max_fid]) do |file|
      saved = bak_file(file)
      BakFile.transaction do
        BakFile.create(:fid => file.fid,
                       :domain => file.domain.namespace,
                       :dkey => file.dkey,
                       :length => file.length,
                       :classname => file.classname,
                       :saved => saved)
      end
    end

    if !o[:no_delete]
      #Delete files from the backup that no longer exist in the mogilefs domain
      files_to_delete = Array.new
      BakFile.find_in_batches { |bak_files|

        union = "SELECT #{bak_files.first.fid} as fid"
        bak_files.shift
        bak_files.each do |bakfile|
          union = "#{union} UNION SELECT #{bakfile.fid}"
        end
        connection = ActiveRecord::Base.connection
        files = connection.select_values("SELECT t1.fid FROM (#{union}) as t1 LEFT JOIN file on t1.fid = file.fid WHERE file.fid IS NULL")
        files_to_delete += files
      }
      files_to_delete.each do |del_file|
        BakFile.transaction do
          delete = BakFile.where(:fid => del_file).first.destroy
          if delete
            puts "Deleting from backup: FID #{del_file}"
          else
            puts "Failed to delete from backup: FID #{del_file}"
          end
        end
      end
    end

  end
end