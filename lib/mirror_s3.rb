# Mirrors a mogile domain to an S3 bucket
class MirrorS3
  attr_accessor :dest_s3_client, :source_mogile, :source_mogile_admin, :start, :added, :updated, :uptodate, :removed, :failures

  BYTE_RANGE = %w(TiB GiB MiB KiB B).freeze
  TIME_RANGE = [[60, :seconds], [60, :minutes], [24, :hours], [1000, :days]].freeze

  # Run validations and prepare the object for a backup
  #
  # @param [Hash] cli_settings A hash containing any settings for the mirror
  def initialize(cli_settings = {})
    require 'sourcedomain'
    require 'sourcefile'
    require 'aws-sdk'
    require 'fileclass'
    require 'mirrorfile'

    @start = Time.now
    @uptodate = 0
    @added = 0
    @updated = 0
    @copied_bytes = 0
    @removed = 0
    @freed_bytes = 0
    @failed = 0

    # If a settings file was given, use it. Otherwise default one.
    settings_path = cli_settings[:settings]
    settings_path ||= File.expand_path(File.join(Dir.pwd, "#{self.class.name.underscore}.settings.yml"))

    # First see if a settings file exists. If it does, use it's properties as a starting point.
    settings = load_settings(settings_path)

    # Next merge in all properties from the command line (this way a user can override a specific setting if they want to.)
    merge_cli_settings!(settings, cli_settings)

    # Verify mysql settings
    # This will map everything to the dest db connection by default
    establish_ar_connection(
      ActiveRecord::Base,
      settings[:source][:db_host],
      settings[:source][:db_port],
      settings[:source][:db_username],
      settings[:source][:db_passwd],
      settings[:source][:db]
    )

    # Install the schema
    ActiveRecord::Migrator.up(File.expand_path(File.dirname(__FILE__)) + '/../db/migrate')

    # Verify s3 settings
    @dest_s3_client = Aws::S3::Client.new(
      region: 'us-west-2',
      credentials: Aws::Credentials.new(
        settings[:dest][:access_key_id],
        settings[:dest][:access_key]
      )
    )

    # Verify mogile settings
    @source_mogile = mogile_tracker_connect(
      settings[:source][:tracker_ip],
      settings[:source][:tracker_port],
      settings[:source][:domain]
    )

    @source_mogile_admin = mogile_admin_connect(
      settings[:source][:tracker_ip],
      settings[:source][:tracker_port],
      settings[:source][:domain]
    )

    # Save settings
    save_settings(settings_path, settings)

    # Set logging level to info unless --debug has been specified.
    Log.instance.level = Logger::INFO unless $debug
  end

  # Parse the file as a yaml config file and return a map of it's settings
  #
  # @param [String] filename The filename of the file to parse
  #
  # @return [Map] The map of settings found in the file
  def load_settings(filename)
    require('yaml')

    # Check if a settings file exists. If it does, load all relevant properties
    settings = {}
    if File.exist?(filename)
      settings = YAML.load(File.open(filename))

      Log.instance.info("Settings loaded from [ #{filename} ]")
    end

    settings
  end

  # Merges settings from the command line with settings from the configuration file.
  # This allows a user to use a settings file for the defaults but override settings by using
  # command line options. The settings map will contain the merged properties
  #
  # @param [Map] settings The default settings which were parsed from the configuration file
  # @param [Map] cli_settings The override settings which were parsed from the command line
  def merge_cli_settings!(settings, cli_settings)
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
    settings[:dest][:access_key_id] =  cli_settings[:dest_access_key_id]  if cli_settings[:dest_access_key_id]
    settings[:dest][:access_key] =     cli_settings[:dest_access_key]     if cli_settings[:dest_access_key]
  end

  # Save the settings for the backup into a yaml file (settings.yaml) so that
  # additional mirrors can be ran without so many parameters
  #
  # @param [String] filename The filename of the file to parse
  # @param [Map] settings The map of settings to write to the file
  def save_settings(filename, settings)
    require 'yaml'

    File.open(filename, 'w') do |file|
      file.write(settings.to_yaml)
      Log.instance.info("Settings written to [ #{filename} ]")
    end
  end

  # Establish a connection to an activerecord database
  #
  # @param [Class] klass The class to bind the connection to
  # @param [String] host The database host
  # @param [String] port The database port
  # @param [String] username The database user
  # @param [String] passwd The database password
  # @param [String] schema The database schema
  # @param [String] adapter The database adapter to use (defaults to mysql2)
  # @param [String] reconnect Whether or not to reconnect (defaults to true)
  def establish_ar_connection(klass, host, port, username, passwd, schema, adapter = 'mysql2', reconnect = true)
    #Verify that we can connect to the mogilefs mysql server
    pool = klass.establish_connection(
      adapter: adapter,
      host: host,
      port: port,
      username: username,
      password: passwd,
      database: schema,
      reconnect: reconnect
    )

    # Connections don't seem to actually be verified as valid until '.connection' is called.
    pool.connection
    Log.instance.info("Connected class [ #{klass} ] to mysql database [ #{username}@#{host}/#{schema} ].")

  rescue => e
    Log.instance.error("Could not connect to MySQL database: #{e}\n#{e.backtrace}")
    raise 'Could not connect to MySQL database'
  end

  # Establish a connection to mogile tracker
  #
  # @param [String] ip The IP of the mogile tracker
  # @param [String] port The port of the mogile tracker
  # @param [String] domain The domain to connect to
  #
  # @return [MogileFS] The instantiated mogilefs instance
  def mogile_tracker_connect(ip, port, domain)
    hosts = ["#{ip}:#{port}"]

    mogile = nil
    begin
      mogile = MogileFS::MogileFS.new(domain: domain, hosts: hosts)

      # Connections don't seem to actually be verified as valid until we do something with them
      # (Note: this will also validate the domain exists)
      mogile.exist?('foo')
      Log.instance.info("Connected to mogile tracker [ #{hosts.join(',')} -> #{domain} ].")

    rescue => e
      Log.instance.error("Could not connect to MogildFS tracker at #{ip}: #{e}\n#{e.backtrace}")
      raise 'Could not connect to MogileFS tracker'
    end

    mogile
  end

  # Establish a connection to mogile admin
  #
  # @param [String] ip The IP of the mogile tracker
  # @param [String] port The port of the mogile tracker
  # @param [String] domain The domain to connect to
  #
  # @return [MogileFS] The instantiated mogilefs instance
  def mogile_admin_connect(ip, port, domain)
    hosts = ["#{ip}:#{port}"]

    mogile = nil
    begin
      mogile = MogileFS::Admin.new(domain: domain, hosts: hosts)

      # Connections don't seem to actually be verified as valid until we do something with them
      # (Note: this will also validate the domain exists)
      mogile.get_domains
      Log.instance.info("Connected to mogile admin [ #{hosts.join(',')} -> #{domain} ].")

    rescue => e
      Log.instance.error("Could not connect to MogildFS admin: #{e}\n#{e.backtrace}")
      raise 'Could not connect to MogileFS admin'
    end

    mogile
  end

  # The main processing method for the Mirror class. Processes all necesary information based on
  # user inputs and prints a summary
  #
  # @param [Boolean] incremental Whether the program should perform an incremental mirror or not (defaults to false)
  def mirror(incremental = false)
    # First, make sure the source file classes exist in the destination mogile
    mirror_class_entries

    begin
      # Scan all files greater than max_fid in the source mogile database and copy over any
      # which are missing from the dest mogile database.
      mirror_missing_destination_files unless SignalHandler.instance.should_quit

      # This is only run when incremental is not set because it requires the mirror_files db to express
      # exactly the same state as the remote mogile db. Otherwise this would effectively do nothing.
      remove_superfluous_destination_files unless SignalHandler.instance.should_quit

    ensure
      # Print the overall summary
      final_summary
    end
  end

  # Mirror all classes from the source mogile to the destination mogile
  def mirror_class_entries
    # get_domains returns a hash of each domain's classes and their configuration settings
    source_domain_info = @source_mogile_admin.get_domains[@source_mogile.domain]
    dest_resource = Aws::S3::Resource.new(client: dest_s3_client)

    # Loop through all of the source domain classes and add any which do not exist in the destination mogile.
    source_domain_info.each do |sourceclass, classinfo|
      bucket = dest_resource.bucket(bucket_name(sourceclass))
      unless bucket.exists?
        bucket.create
        Log.instance.info("Added bucket #{bucket.name} to s3")
      end
    end
  end

  # Mirror all files from the source mogile which do not exist in the destination mogile
  def mirror_missing_destination_files
    # Find the id of the domain we are mirroring
    source_domain = SourceDomain.find_by_namespace(@source_mogile.domain)

    # Get the max fid from the mirror db
    # This will only be nonzero if we are doing an incremental
    max_fid = MirrorFile.max_fid

    # Process source files in batches.
    Log.instance.info("Searching for files in domain [ #{source_domain.namespace} ] whose fid is larger than [ #{max_fid} ].")
    SourceFile.where('dmid = ? AND fid > ?', source_domain.dmid, max_fid).includes(:domain, :fileclass).find_in_batches(batch_size: 1000) do |batch|
      # Create an array of MirrorFiles which represents files we have mirrored.
      remotefiles = batch.collect { |file| MirrorFile.new(fid: file.fid, dkey: file.dkey, length: file.length, classname: file.classname) }

      # Insert the mirror files in a batch format.
      Log.instance.debug('Bulk inserting mirror files.')
      MirrorFile.import remotefiles

      # Figure out which files need copied over
      # (either because they are missing or because they have been updated)
      batch_copy_missing_destination_files(remotefiles)

      # Show our progress so people know we are working
      summarize

      # Quit if program exit has been requested.
      return true if SignalHandler.instance.should_quit
    end
  end

  def bucket_name(classname)
    "sbf.mogile.test.#{classname.gsub('_', '.')}"
  end

  # Process a batch of files determining if the file is missing or out of date.
  # If either is true, stream the file into the destination mogile.
  #
  # params [List] files List of files to process
  def batch_copy_missing_destination_files(files)
    dest_resource = Aws::S3::Resource.new(client: dest_s3_client)

    files.each do |file|
      # Quit if no results
      break if file.nil?

      # Quit if program exit has been requested.
      if SignalHandler.instance.should_quit
        MirrorFile.destroy(file.fid)
        next
      end

      destfile = dest_resource.bucket(bucket_name(file.classname)).object(file.dkey)
      if destfile.exists?
        # File exists!
        # Check that the source and dest file sizes match
        if file.length != destfile.content_length
          # File exists but has been modified. Copy it over.
          begin
            Log.instance.debug("Key [ #{file.dkey} ] is out of date. Updating.")
            stream_copy(file, destfile)
            @updated += 1
            @copied_bytes += file.length
          rescue => e
            @failed += 1
            file.destroy
            Log.instance.error("Error updating [ #{file.dkey} ]: #{e.message}\n#{e.backtrace}")
          end
        else
          Log.instance.debug("key [ #{file.dkey} ] is up to date.")
          @uptodate += 1
        end
      else
        # File does not exist. Copy it over.
        begin
          Log.instance.debug("key [ #{file.dkey} ] does not exist... creating.")
          stream_copy(file, destfile)
          @added += 1
          @copied_bytes += file.length
        rescue => e
          @failed += 1
          file.destroy
          Log.instance.error("Error adding [ #{file.dkey} ]: #{e.message}\n#{e.backtrace}")
        end
      end
    end
  end

  # Opens a pipe between the two mogile instances and funnels the file from the source to the destination.
  #
  # @param [SourceFile] source_file The file to be copied
  # @param [SourceFile] dest_file The file to be copied
  def stream_copy(source_file, dest_file)
    # Create a pipe to link the get / store commands
    read_pipe, write_pipe = IO.pipe

    # Fork a child process to write to the pipe
    Process.fork do
      read_pipe.close
      @source_mogile.get_file_data(source_file.dkey, write_pipe)
      write_pipe.close
    end

    # Read info off the pipe that the child is writing to
    write_pipe.close
    dest_file.put(body: read_pipe.read)
    read_pipe.close

    # Wait for the child to exit
    Process.wait

    # Throw an exception if the child process exited non-zero
    if $?.exitstatus.nonzero?
      Log.instance.error("Child exited with a status of [ #{$?.exitstatus} ].")
      raise "Error getting file data from [ #{@source_mogile.domain} ]"
    end
  end

  # Removes all files in the destination which are not in the source. This is done by comparing the
  # files in the destination with the table of files we have mirrored from source.
  def remove_superfluous_destination_files
    dest_resource = Aws::S3::Resource.new(client: dest_s3_client)

    # join mirror_file and src_file and delete everything from dest_file which isn't in mirror_file
    # because mirror_file should represent the current state of the source mogile files
    Log.instance.info('Joining destination and mirror tables to determine files that have been deleted from source repo.')
    MirrorFile.joins('LEFT OUTER JOIN file ON mirror_file.dkey = file.dkey').where('file.dkey IS NULL').order('mirror_file.dkey').find_in_batches(batch_size: 1000) do |batch|
      batch.each do |file|
        # Quit if program exit has been requested.
        break if SignalHandler.instance.should_quit

        # Delete all files from our destination domain which no longer exist in the source domain.
        begin
          Log.instance.debug("key [ #{file.dkey} ] should not exist. Deleting.")
          destfile = dest_resource.bucket(bucket_name(file.classname)).object(file.dkey)
          if destfile.exists?
            destfile.delete
            file.destroy
            @removed += 1
            @freed_bytes += file.length
          end

        rescue => e
          @failed += 1
          Log.instance.error("Error deleting [ #{file.dkey} ]: #{e.message}\n#{e.backtrace}")
        end
      end

      # Print a summary to the user.
      summarize

      # Quit if program exit has been requested.
      return true if SignalHandler.instance.should_quit
    end
  end

  # Uses instance level variables to log a summary of files processed
  def summarize
    Log.instance.info "Summary => Execution time: #{as_time_elapsed(Time.now - @start)}, Up To Date: #{@uptodate}, Failures: #{@failed}, Added: #{@added}, Updated: #{@updated}, Bytes Transferred: #{as_byte_size(@copied_bytes)}, Removed: #{@removed}, Bytes Freed #{as_byte_size(@freed_bytes)}."
  end

  # Prints a more complete, better formatted summary of files processed during the current execution of a mirror
  def final_summary
    puts
    puts <<-eos
    --------------------------------------------------------------------------
      Complete Summary:
         Execution time: #{as_time_elapsed(Time.now - @start)}

         Up To Date: #{@uptodate}

         Failures: #{@failed}

         Added: #{@added}
         Updated: #{@updated}
         Bytes copied: #{as_byte_size(@copied_bytes)}

         Removed: #{@removed}
         Bytes freed: #{as_byte_size(@freed_bytes)}
    --------------------------------------------------------------------------
         eos
         puts
         STDOUT.flush
  end

  # Helper method to print out the number of bytes in a more readible format
  def as_byte_size(bytes)
    bytes = bytes.to_f
    i = BYTE_RANGE.length - 1
    while bytes > 512 && i > 0
      i -= 1
      bytes /= 1024
    end
    ((bytes > 9 || bytes.modulo(1) < 0.1 ? '%d' : '%.1f') % bytes) + ' ' + BYTE_RANGE[i]
  end

  # Helper method to print out the number of seconds of time elapsed in a more readible format
  def as_time_elapsed(secs)
    TIME_RANGE.map do |count, name|
      next unless secs > 0
      secs, n = secs.divmod(count)
      n = if name == :seconds
            n.round(4)
          else
            n.to_i
          end
      "#{n} #{name}"
    end.compact.reverse.join(' ')
  end
end
