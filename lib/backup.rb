#Creates a backup of a MogileFS cluster and is capable of resyncing that backup.
class Backup
  attr_accessor :db, :db_host, :db_port, :db_pass, :db_user, :domain, :tracker_host, :tracker_port, :backup_path, :workers

  #Initialize has two modes,  :create and :backup.  :create is used to initalize the sqlite database for the backup,  ensure
  #the user provided valid settings,  and generate a settings.yml in the backup directory.  This makes it so that users
  #don't have to continue to pass in a long string of options for each backup.
  #@param [Symbol] mode must be either :create or :backup
  #@param [Hash] o hash containing the settings for the backup
  #@return [Bool] object initialized successfully
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
      @workers = o[:workers] if o[:workers]
      @backup_path = path
      $backup_path = @backup_path

      #verify settings
      check_settings
    end
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

  #Validate that all the user provided settings are correct,  also creates a new sqlite database if there isn't one and
  #runs migrations against an existing database.
  #@return [Bool] true or false
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
      ActiveRecord::Base.establish_connection(:adapter => 'sqlite3', :database => "#{$backup_path}/db.sqlite", :timeout => 1000)
    rescue Exception => e
      raise e
      #raise "Could not open #{$backup_path}/db.sqlite"
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
                                               :database => @db,
                                               :reconnect => true})
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

    true
  end

  #Create a backup of a file using a BakFile object
  #@param [BakFile] file file that needs to be backed up
  #@return [Bool] file save result
  def bak_file(file)
    saved = file.bak_it
    if saved
      puts "Backed up: FID #{file.fid}"
    else
      puts "Error - will try again on next run: FID #{file.fid}"
    end

    return saved
  end

  #Launch workers to backup an array of BakFiles
  #@param [Array] files must be an array of BakFiles
  def launch_backup_workers(files)

    #This proc will process the results of the child proc
    parent = Proc.new { |results|
      fids = []

      results.each do |result|
        file = result[:file]
        saved = result[:saved]
        fids << file.fid if saved
      end

      #bulk update all the fids.  much faster then doing it one at a time
      BakFile.update_all({:saved => true}, {:fid => fids})

      #release the connection from the connection pool
      SqliteActiveRecord.clear_active_connections!
    }

    #This proc receives an array of BakFiles,  proccesses them,  and returns a result array to the parent proc
    child = Proc.new { |files|
      result = []
      files.each do |file|
        break if file.nil?
        saved = bak_file(file)
        result << {:saved => saved, :file => file}
      end
      result
    }

    #launch workers using the above procs and files
    Forkinator.hybrid_fork(self.workers.to_i, files, parent, child)
  end

  #Launch workers to delete an array of files
  #param [Array] files must be an array of BakFiles that need to be deleted
  def launch_delete_workers(fids)

    #This proc receives an array of BakFiles, handles them,  and spits them back to the parent.
  child = Proc.new { |fids|
      result = []
      fids.each do |fid|
        break if fid.nil?
        deleted = BakFile.delete_from_fs(fid)
        if deleted
          puts "Deleting from backup: FID #{fid}"
        else
          puts "Failed to delete from backup: FID #{fid}"
        end

        result << fid
      end
      result
    }

    #This proc will process the results of the child proc
    parent = Proc.new { |results|
      fids = []

      results.each do |result|
        fids << result
      end

      BakFile.delete_all({:fid => fids})

      #release the connection from the connection pool
      SqliteActiveRecord.clear_active_connections!
    }

    #launch workers using the above procs and files
    Forkinator.hybrid_fork(self.workers.to_i, fids, parent, child)

  end

  #The real logic for backing the domain up.  It is pretty careful about making sure that it doesn't report a file
  #as backed up unless it actually was.  Supports the ability to remove deleted files from the backup as well.  We grab files
  #from the mogilefs mysql server in groups of 500 * number of workers (default is 1 worker)
  #@param [Hash] o if :no_delete then don't remove deleted files from the backup (intensive process)
  def backup(o = {})

    files = []
    #first we retry files that we haven't been able to backup successfully, if any.
    BakFile.find_each(:conditions => ['saved = ?', false]) do |bak_file|
       files << bak_file
    end

    launch_backup_workers(files)

    #now back up any new files.  if they fail to be backed up we'll retry them the next time the backup
    #command is ran.
    dmid = Domain.find_by_namespace(self.domain)
    results = Fid.find_in_batches(:conditions => ['dmid = ? AND fid > ?', dmid, BakFile.max_fid], :batch_size => 500 * self.workers.to_i, :include => [:domain, :fileclass]) do |batch|

      #Insert all the files into our bak db with :saved false so that we don't think we backed up something that crashed
      files = []
      batch.each do |file|
        files << BakFile.new(:fid => file.fid,
                            :domain => file.domain.namespace,
                            :dkey => file.dkey,
                            :length => file.length,
                            :classname => file.classname,
                            :saved => false)
      end

      #There is no way to do a bulk insert in sqlite so this generates a lot of inserts.  wrapping all of the inserts
      #inside a single transaction makes it much much faster.
      BakFile.transaction do
        BakFile.import files, :validate => false
      end

      #Fire up the workers now that we have work for them to do
      launch_backup_workers(files)

    end

    #Delete files from the backup that no longer exist in the mogilefs domain.  Unfortunently there is no easy way to detect
    #which files have been deleted from the MogileFS domain.  Our only option is to brute force our way through.  This is a bulk
    #query that checks a thousand files in each query against the MogileFS database server.  The query is kind of tricky because
    #I wanted to do this with nothing but SELECT privileges which meant I couldn't create a temporary table (which would require,
    #create temporary table and insert privleges).  You might want to only run this operation every once and awhile if you have a
    #very large domain.  In my testing,  it is able to get through domains with millions of files in a matter of a second.  So
    #all in all it's not so bad
    if !o[:no_delete]
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

      launch_delete_workers(files_to_delete)

    end

  end
end