#List the files in a MogileFS backup with their metadata
class List
  attr_accessor :backup_path
  include Validations


  #initialize the list object
  #@param[Hash] o :backup_path is required
  def initialize(o={})

    #If settings file does not exist then this is not a valid mogilefs backup
    check_settings_file('settings.yml not found in path.  This must not be a backup profile. Cannot list')

    connect_sqlite
    migrate_sqlite

    #Now that database is all setup load the model class
    require('bakfile')
  end

  #Outputs a list of files in CSV format
  #fid,key,length,class
  def list
    files = BakFile.find_each(:conditions => ['saved = ?', true]) do |file|
      Log.instance.info("#{file.fid},#{file.dkey},#{file.length},#{file.classname}")
      break if SignalHandler.instance.should_quit
    end
  end
end