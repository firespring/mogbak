#This is kind of awkward but is the only good way to support multiple database connections using ActiveRecord
class SqliteActiveRecord < ActiveRecord::Base
  establish_connection(adapter: 'sqlite3', database: "#{$backup_path}/db.sqlite")
  self.abstract_class = true
end

#Represents files that are either backed up,  about to be backed up,  or failed to be backed up.
class BakFile < SqliteActiveRecord
  #get the max fid that is backed up
  def self.max_fid
    last_backed_file = BakFile.order('fid').last
    max_fid = if last_backed_file
                last_backed_file.fid
              else
                0
              end
    max_fid
  end

  #Restore a file back to a MogileFS domain
  #@return [Bool]
  def restore
    path = PathHelper.path(fid)
    begin
      $mg.store_file(dkey, classname, path)
    rescue => e
      raise e if $debug
    end
  end

  #Get a file from MogileFS and save it to the destination path.
  #@return [Bool]
  def bak_it
    begin
      path = PathHelper.path(fid)
      $mg.get_file_data(dkey, path)
    rescue => e
      raise e if $debug
      return false
    end
    true
  end

  #Delete from filesystem using just a fid
  #@return [Bool]
  def self.delete_from_fs(delete_fid)
    File.delete(PathHelper.path(delete_fid))
  rescue => e
    raise e if $debug
  end

  #Delete file from filesystem if someone deletes this object through ActiveRecord somehow.
  before_destroy do
    path = PathHelper.path(fid)
    begin
      File.delete(path)
    rescue => e
      raise e if $debug
    end
  end
end
