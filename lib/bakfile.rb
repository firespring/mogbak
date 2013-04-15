#This is kind of awkward but is the only good way to support multiple database connections using ActiveRecord
class SqliteActiveRecord < ActiveRecord::Base
  establish_connection(:adapter => 'sqlite3', :database => "#{$backup_path}/db.sqlite")
  self.abstract_class = true
end

#Represents files that are either backed up,  about to be backed up,  or failed to be backed up.
class BakFile < SqliteActiveRecord

  #get the max fid that is backed up
  def self.max_fid
    last_backed_file = BakFile.order("fid").last
    if last_backed_file
      max_fid = last_backed_file.fid
    else
      max_fid = 0
    end
    max_fid
  end

  #Restore a file back to a MogileFS domain
  #@return [Bool]
  def restore
    path = PathHelper.path(self.fid)
    begin
      $mg.store_file(self.dkey, self.classname, path)
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end


  #Get a file from MogileFS and save it to the destination path.
  #@return [Bool]
  def bak_it
    begin
      path = PathHelper.path(self.fid)
      $mg.get_file_data(self.dkey, path)
    rescue Exception => e
      if $debug
        raise e
      end
      return false
    end
    true
  end


  #Delete from filesystem using just a fid
  #@return [Bool]
  def self.delete_from_fs(delete_fid)
    begin
      File.delete(PathHelper.path(delete_fid))
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end


  #Delete file from filesystem if someone deletes this object through ActiveRecord somehow.
  before_destroy do
    path = PathHelper.path(self.fid)
    begin
      File.delete(path)
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end
end
