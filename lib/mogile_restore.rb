class MogileRestore
  attr_accessor :domain, :tracker_host, :tracker_port, :backup_path

  def initialize(o={})
    @domain = o[:domain] if o[:domain]
    @tracker_ip = o[:tracker_ip] if o[:tracker_ip]
    @tracker_port = o[:tracker_port] if o[:tracker_port]
    @backup_path = o[:backup_path] if o[:backup_path]
    $backup_path = @backup_path
    path = o[:backup_path]

    #If settings file does not exist then this is not a valid mogilefs backup
    settings_file = "#{path}/settings.yml"
    raise "settings.yml not found in path.  This must not be a backup profile. Cannot restore" unless File.exists?(settings_file)

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

    #open our mogilefs tracker connection
    host = ["#{@tracker_ip}:#{@tracker_port}"]
    $mg = MogileFS::MogileFS.new(:domain => @domain, :hosts => host)

    #Now that database is all setup load the model classes
    require ('domain')
    require('file')
    require('bakfile')
    require('fileclass')
  end

  def restore
    files = BakFile.find_each(:conditions => ['saved = ?', true]) do |file|
      save = file.restore
      if save
        puts "Restored: FID #{file.fid}"
      else
        puts "Error: FID #{file.fid}"
      end
    end
  end
end