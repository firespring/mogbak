if !File.exists?('/mnt/mogbak/db.sqlite')
  SQLite3::Database.new('/mnt/mogbak/db.sqlite')
end
ActiveRecord::Base.establish_connection(:adapter => 'sqlite3', :database => '/mnt/mogbak/db.sqlite').connection
ActiveRecord::Migrator.up("db/migrate/")
ActiveRecord::Base.establish_connection({:adapter => "mysql2",
                                         :host => "127.0.0.1",
                                         :username => "mogilefs_bu",
                                         :password => "F5273780u41omoX",
                                         :database => "mogilefs"})

class SqliteActiveRecord < ActiveRecord::Base
  establish_connection(:adapter => 'sqlite3', :database => '/mnt/mogbak/db.sqlite')
  self.abstract_class = true
end

class BakFile < SqliteActiveRecord

  #This produces a hashed path very similar to mogilefs just without the device id.  It also recursively creates the
  #directory inside the backup
  def path
    sfid = "#{self.fid}"
    length = sfid.length
    if length < 10
      length = 10 - length
      pad = ''
      length.times do
        pad = "#{pad}0"
      end
      nfid = pad + sfid
    else
      nfid = fid
    end
    /(?<b>\d)(?<mmm>\d{3})(?<ttt>\d{3})(?<hto>\d{3})/ =~ nfid

    #create the directory
    directory_path = "/mnt/mogbak/#{b}/#{mmm}/#{ttt}"
    FileUtils.mkdir_p(directory_path)

    return "#{directory_path}/#{nfid}.fid"
  end

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

  #delete file from filesystem
  before_destroy do
    File.delete(path)
  end
end