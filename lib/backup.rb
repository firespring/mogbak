#Used to backup a mogilefs domain using a backup profile.
class Backup
  attr_accessor :db, :db_host, :db_port, :db_pass, :db_user, :domain, :tracker_host, :tracker_port, :workers
  include Validations

  #Run validations and prepare the object for a backup
  #@param [Hash] o hash containing the settings for the backup
  def initialize(o={})

    #Load up the settings file
    check_settings_file
    settings = YAML::load(File.open("#{$backup_path}/settings.yml"))
    @db = settings['db']
    @db_host = settings['db_host']
    @db_port = settings['db_port']
    @db_pass = settings['db_pass']
    @db_user = settings['db_user']
    @domain = settings['domain']
    @tracker_ip = settings['tracker_ip']
    @tracker_port = settings['tracker_port']
    @workers = o[:workers] if o[:workers]


    #run validations and setup
    raise unless check_backup_path
    create_sqlite_db
    connect_sqlite
    migrate_sqlite
    mogile_db_connect
    mogile_tracker_connect
    check_mogile_domain(domain)

    require ('domain')
    require('file')
    require('bakfile')
    require('fileclass')
  end


  #Create a backup of a file using a BakFile object
  #@param [BakFile] file file that needs to be backed up
  #@return [Bool] file save result
  def bak_file(file)
    saved = file.bak_it
    if saved
       Log.instance.info("Backed up: FID #{file.fid}")
    else
       Log.instance.info("Error - will try again on next run: FID #{file.fid}")
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

    #This proc receives an array of BakFiles,  proccesses them,  and returns a result array to the parent proc. We will break
    #from the files if the signal handler says so.
    child = Proc.new { |files|
      result = []
      files.each do |file|
        break if file.nil?
        break if SignalHandler.instance.should_quit
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

    #This proc receives an array of BakFiles, handles them,  and spits them back to the parent, break from the fids if
    #the signal handler says so.
    child = Proc.new { |fids|
      result = []
      fids.each do |fid|
        break if fid.nil?
        break if SignalHandler.instance.should_quit
        deleted = BakFile.delete_from_fs(fid)
        if deleted
          Log.instance.info("Deleting from backup: FID #{fid}")
        else
          Log.instance.info("Failed to delete from backup: FID #{fid}")
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

    #Loop over the main backup logic.  We'll break out at the end unless o[:non_stop] is set
    loop do
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

        #Terminate program if the signal handler says so and this is a clean place to do it
        return true if SignalHandler.instance.should_quit
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

      #Break out of infinite loop unless o[:non_stop] is set
      break unless o[:non_stop]
      sleep 1
    end

  end
end