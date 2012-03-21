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
    path = Util.path(self.fid)
    begin
      $mg.store_file(self.dkey, self.classname, path)
    rescue Exception => e
      if $debug
        raise e
      end
    end
  end

  #delete file from filesystem
  before_destroy do
    path = Util.path(self.fid)
    File.delete(path)
  end
end