#Common methods used for setting up or verifing
module Validations

  #Check that the settings.yml file exists in the backup_path
  #@param [String] raise_msg exception to raise if the file is missing.  if set to nil no exception will be rasied
  #@return [Bool]
  def check_settings_file(raise_msg = 'settings.yml not found in path.  This must not be a backup profile. See: mogbak help create')
    if File.exists?("#{$backup_path}/settings.yml")
      return true
    else
      raise raise_msg if raise_msg
      return false
    end
  end


  #Check that the backup_path is valid
  #@param [String] raise_msg exception to raise if the backup_path is not a valid direcotry. if set to nil no exception will be rasied
  #@return [Bool]
  def check_backup_path(raise_msg = 'backup_path is not a valid directory')
    if !File.directory?($backup_path)
      raise raise_msg if raise_msg
      return false
    end
    true
  end

  #Create database for metadata
  #@param [String] raise_msg exception to raise if database cannot be created.  nil will raise no exception
  #@return [Bool]
  def create_sqlite_db(raise_msg = "Could not create #{$backup_path}/db.sqlite - check permissions")
    begin
      if !File.exists?("#{$backup_path}/db.sqlite")
        SQLite3::Database.new("#{$backup_path}/db.sqlite")
      end
    rescue Exception => e
      raise raise_msg if raise_msg
      return false
    end
    true
  end

  #Connect to sqlite metadata db
  #@param [String] raise_msg exception to raise if we cannot connect. if set to nil no exception will be rasied
  #@return [Bool]
  def connect_sqlite(raise_msg = nil)
    begin
      ActiveRecord::Base.establish_connection(:adapter => 'sqlite3', :database => "#{$backup_path}/db.sqlite", :timeout => 10000)
    rescue Exception => e
      raise raise_msg if raise_msg
      raise e if $debug
      return false
    end
    true
  end

  #Run ActiveRecord migrations on the sqlite database
  #@param [String] raise_msg exception to raise if migrations fail. if set to nil no exception will be rasied
  #@return [Bool]
  def migrate_sqlite(raise_msg = "could not run migrations on #{$backup_path}/db.sqlite")
    #run migrations
    begin
      ActiveRecord::Migrator.up(File.expand_path(File.dirname(__FILE__)) + '/../db/migrate/')
    rescue
      raise raise_msg if raise_msg
      return false
    end
    true
  end

  #Connect to MogileFS mysql server
  #@param [String] raise_msg exception to raise if we cannot connect. if set to nil no exception will be rasied
  #@return [Bool]
  def mogile_db_connect(raise_msg = 'Could not connect to MySQL database')
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
      raise raise_msg if raise_msg
      return false
    end
    true
  end

  #Connect to mogile tracker
  #@param [String] raise_msg exception to raise if we cannot connect. if set to nil no exception will be raised
  #@return [Bool]
  def mogile_tracker_connect(raise_msg = 'Could not connect to MogileFS tracker')
    ips = @tracker_ip.split(",")
    hosts = @tracker_ip.split(",").map! {|ip| ip + ":#{@tracker_port}" }
    begin
    $mg = MogileFS::MogileFS.new(:domain => @domain, :hosts => hosts)
    rescue Exception => e
      if $debug
        raise e
      end
      raise raise_msg if raise_msg
      return false
    end
  end

  #Check if mogile domain is valid
  #@param [String] raise_msg exception to raise if domain does not exist. if set to nil no exception will be raised
  #@return [Bool]
  def check_mogile_domain(domain, raise_msg = 'Domain does not exist in MogileFS')
    require('domain')
    domain = Domain.find_by_namespace(self.domain)
    if !domain
      raise raise_msg if raise_msg
      return false
    end
    true
  end

end
