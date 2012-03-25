class SqliteActiveRecord < ActiveRecord::Base
  establish_connection(:adapter => 'sqlite3', :database => "#{$backup_path}/db.sqlite")
  self.abstract_class = true
end

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

  #restore
  def restore
    path = MogileHelper.path(self.fid)
    begin
      $mg.store_file(self.dkey, self.classname, path)
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end


  #Get a file from MogileFS and save it to the destination path.  TRUE if success, false if there was an error
  def bak_it
    begin
      path = MogileHelper.path(self.fid)
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
  def self.delete_from_fs(delete_fid)
    begin
      File.delete(MogileHelper.path(delete_fid))
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end


  #delete file from filesystem
  before_destroy do
    path = MogileHelper.path(self.fid)
    begin
      File.delete(path)
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end
end