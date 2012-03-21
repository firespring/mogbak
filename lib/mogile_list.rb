class MogileList
  attr_accessor :backup_path

  def initialize(o={})
    @backup_path = o[:backup_path] if o[:backup_path]
    $backup_path = @backup_path
    path = o[:backup_path]

    #If settings file does not exist then this is not a valid mogilefs backup
    settings_file = "#{path}/settings.yml"
    raise "settings.yml not found in path.  This must not be a backup profile. Cannot list" unless File.exists?(settings_file)

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

    #Now that database is all setup load the model class
    require('bakfile')
  end

  def list
    files = BakFile.find_each(:conditions => ['saved = ?', true]) do |file|
      puts "#{file.fid},#{file.dkey},#{file.length},#{file.classname}"
    end
  end
end