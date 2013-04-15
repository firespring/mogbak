# Merges a mogile domain with another mogile domain
class Merge
  attr_accessor :source_mogile, :dest_mogile, :added, :updated, :uptodate, :removed, :failures

  # TODO:
  #@param [Hash] o hash containing any settings for the merge
  def initialize(cli_settings={})
    require('sourcedomain')
    require('sourcefile')
    require('destdomain')
    require('destfile')
    require('fileclass')
    require('mirrorfile')

    @start = Time.now
    @added = 0
    @updated = 0
    @uptodate = 0
    @removed = 0
    @failures = 0

    # If a settings file was given, use it. Otherwise default one.
    settings_path = cli_settings[:settings]
    settings_path ||= File.expand_path(File.join(Dir.pwd, "#{self.class.name.downcase}.settings.yml"))

    # First see if a settings file exists. If it does, use it's properties as a starting point.
    settings = load_settings(settings_path)

    # Next merge in all properties from the command line (this way a user can override a specific setting if they want to.)
    merge_cli_settings(settings, cli_settings)

    # Verify mysql settings
    # This will map everything to the dest db connection by default
    establish_connection(
      ActiveRecord::Base,
      settings[:dest][:db_host],
      settings[:dest][:db_port],
      settings[:dest][:db_username],
      settings[:dest][:db_passwd],
      settings[:dest][:db])

    # This will map the sourcedomain class to the source db connection
    establish_connection(
      SourceDomain,
      settings[:source][:db_host],
      settings[:source][:db_port],
      settings[:source][:db_username],
      settings[:source][:db_passwd],
      settings[:source][:db])

    # This will map the sourcefile class to the source db connection
    establish_connection(
      SourceFile,
      settings[:source][:db_host],
      settings[:source][:db_port],
      settings[:source][:db_username],
      settings[:source][:db_passwd],
      settings[:source][:db])

    # Install the schema
    ActiveRecord::Migrator.up(File.expand_path(File.dirname(__FILE__)) + '/../db/migrate')

    # Verify mogile settings
    @dest_mogile = mogile_tracker_connect(
      settings[:dest][:tracker_ip],
      settings[:dest][:tracker_port],
      settings[:dest][:domain])

    @source_mogile = mogile_tracker_connect(
      settings[:source][:tracker_ip],
      settings[:source][:tracker_port],
      settings[:source][:domain])

    # Save settings
    save_settings(settings_path, settings)

#    Log.instance.level = Logger::INFO
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
  # @param [Class] klass
  # @param [String] host
  # @param [String] port
  # @param [String] user
  # @param [String] passwd
  # @param [String] schema
  # @param [String] adapter (optional)
  # @param [String] reconnect (optional)
  def establish_connection(klass, host, port, username, passwd, schema,adapter='mysql2', reconnect=true)
    connection = nil

    #Verify that we can connect to the mogilefs mysql server
    begin
      pool = klass.establish_connection({
        :adapter => adapter,
        :host => host,
        :port => port,
        :username => username,
        :password => passwd,
        :database => schema,
        :reconnect => reconnect})

      # Connections don't seem to actually be verified as valid until '.connection' is called.
      connection = pool.connection
      Log.instance.info("Connected class [ #{klass} ] to mysql database [ #{username}@#{host}/#{schema} ].")

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
      # (Note: this will also validate the domain exists)
      mogile.exist?('foo')
      Log.instance.info("Connected to mogile tracker [ #{hosts.join(',')} -> #{domain} ].")

    rescue Exception => e
      Log.instance.error("Could not connect to MogildFS tracker: #{e}\n#{e.backtrace}")
      raise 'Could not connect to MogileFS tracker'
    end

    mogile
  end

  def process(mirror)
    # Clear out data from previous runs if we are doing a full mirror
    ActiveRecord::Base.connection.execute("TRUNCATE TABLE #{MirrorFile.table_name}") if mirror

    # Find the id of the domain we are mirroring
    source_dmid = SourceDomain.find_by_namespace(@source_mogile.domain)

    # TODO: Figure out if we can get rid of the FID constraint
#    max_fid = 141795
#    max_fid = 205425
#    max_fid = 236605
    max_fid = MirrorFile.max_fid

    Log.instance.debug("Searching for files in domain #{source_dmid} whose fid is larger than #{max_fid}")
    SourceFile.find_in_batches(:conditions => ['dmid = ? AND fid > ?', source_dmid, max_fid], :batch_size => 10, :include => [:domain, :fileclass]) do |batch|
      #Insert all the files into our bak db with :saved false so that we don't think we backed up something that crashed
      #There is no way to do a bulk insert in sqlite so this generates a lot of inserts.  wrapping all of the inserts
      #inside a single transaction makes it much much faster.
      remotefiles = []
      batch.each do |file|
        remotefiles << MirrorFile.new(
          :fid => file.fid,
          :length => file.length,
          :classname => file.classname)

        # Have to set the primary key outside of the init
        remotefiles[-1].dkey = file.dkey
      end
      Log.instance.debug("Bulk inserting files.")
      MirrorFile.import remotefiles

      # Figure out which files need copied over
      # (either because they are missing or because they have been updated)
      files_to_stream = evaluate_missing_local_files(remotefiles)

      if files_to_stream.any?
        # Use between 1 and 5 workers, make sure each has at least 10 things to do.
        num_workers = [1, [5, (files_to_stream.length/10).to_i].min].max
        Log.instance.info("Launching #{num_workers} workers to process #{files_to_stream.length} jobs...")
        do_batch(num_workers, files_to_stream, Proc.new { |data|
          Log.instance.info("Starting child processing to stream missing files...")
          result = []

          data.each do |file|
            break if file.nil?
            break if SignalHandler.instance.should_quit
            result << DestFile.stream(file, @source_mogile, @dest_mogile)
          end
          result
        })
        Log.instance.info("Workers finished.")
      end

      summarize
    end

      # TODO: join mirror_file and dest_file and delete everything from dest_file which isn't in mirror_file
#      SELECT file.dkey from file
#      LEFT OUTER JOIN mirror_file
#        ON (file.dkey=mirror_file.dkey) 
#      WHERE mirror_file.dkey IS NULL
    final_summary
  end

  def evaluate_missing_local_files(files)
    dest_dmid = DestDomain.find_by_namespace(@dest_mogile.domain)

    files_to_copy = []
    files.each do |file|
      break if file.nil?
      break if SignalHandler.instance.should_quit

      destfile = DestFile.find_by_dkey_and_dmid(file.dkey, dest_dmid)
      if destfile
        # File exists!
        # Check that the source and dest file sizes match
        if file.length != destfile.length
          files_to_copy << file
          @updated += 1
          Log.instance.info("key #{file.dkey} is out of date... updating.")
#          DestFile.stream(file, @source_mogile, @dest_mogile)

        else
          @uptodate += 1
          Log.instance.debug("key #{file.dkey} is up to date.")

        end
      else
        # File does not exist. Copy it over.
        files_to_copy << file
        @added += 1
        Log.instance.info("key #{file.dkey} does not exist... creating.")
#        DestFile.stream(file, @source_mogile, @dest_mogile)
      end
    end
    files_to_copy
  end

  def do_batch(num_workers, job_info, proc_code)
    result = []
    threads = []

    #mutex is used to ensure that some operations in the threads don't have the potential of happening at the same time
    #in another thread
    semaphore = Mutex.new

    require('thread')

    #split the jobs up
    jobs = job_info.in_groups(num_workers)

    #spawn the children
    children = []
    num_workers.times { children << make_child(proc_code)}

    #For each worker
    num_workers.times do |i|

      #start a thread
      threads[i] = Thread.new do
        Thread.current.abort_on_exception = true

        child = {}
        semaphore.synchronize { child = children.pop }

        pid = child[:pid]
        njobs = jobs[i - 1]

        #pass jobs to child
        Marshal.dump(njobs, child[:write])

        #wait for child's result
        result.push(*Marshal.load(child[:read]))

        #close the pipe
        child[:write].close

        #wait for process to finish before terminating this thread
        Process.wait(pid)
      end
    end

    threads.compact.each do |t|
      begin
        t.join
      rescue Interrupt
        # no reason to wait on dead threads
      end
    end
  end

  def make_child(child_proc)

    #open pipes for two way communication between the parent and child
    child_read, parent_write = IO.pipe
    parent_read, child_write = IO.pipe

    #fork, code inside this block is only ran inside the child
    pid = Process.fork do
      begin

        #Since we're the child now,  we'll close the parent's r/w pipes as we don't need them
        parent_write.close
        parent_read.close

        #child loops through IO pipe,  listening for data from the parent,  if the parent closes the pipe then we're
        #done
        while !child_read.eof? do
          #rename the process to make it clear that it's a worker in idle status
          $0 = "mogbak [idle]"
          #this call blocks until it receives something from the parent via the pipe
          job = Marshal.load(child_read)
          #since we're working now we'll rename the process
          $0 = "mogbak [working]"
          #call the child proc
          result = child_proc.call(job)
          #hand the child proc response back to the parent
          Marshal.dump(result, child_write)
        end

      #no matter what happens..make sure we get the pipes closed
      ensure
        child_read.close
        child_write.close
      end
    end

    #close the child's handle on the pipes since the parent won't need them
    child_read.close
    child_write.close

    {:write => parent_write, :read => parent_read, :pid => pid}
  end

  def summarize
    Log.instance.info "Summary => Execution time: #{Time.now - @start}, Added: #{@added}, Updated: #{@updated}, Up To Date: #{@uptodate}, Removed: #{@Removed}"
  end

  def final_summary
    puts
    puts <<-eos
    --------------------------------------------------------------------------
      Final Summary:
         Execution time: #{Time.now - @start}
         Added: #{@added}
         Updated: #{@updated}
         Up To Date: #{@uptodate}
         Removed: #{@Removed}
    --------------------------------------------------------------------------
    eos
    puts
    STDOUT.flush
  end
end
