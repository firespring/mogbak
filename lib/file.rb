class Fid < ActiveRecord::Base
  self.primary_key = "fid"
  self.table_name = "file"
  belongs_to :domain,
             :foreign_key => "dmid"
  belongs_to :fileclass,
             :foreign_key => "classid"

  #This produces a hashed path very similar to mogilefs just without the device id.  It also recursively creates the
  #directory inside the backup
  def path
    sfid = "#{self.fid}"
    length = sfid.length
    if length < 10
      nfid = "0" * (10 - length) + sfid
    else
      nfid = fid
    end
    /(?<b>\d)(?<mmm>\d{3})(?<ttt>\d{3})(?<hto>\d{3})/ =~ nfid

    #create the directory
    directory_path = "#{$backup_path}/#{b}/#{mmm}/#{ttt}"
    FileUtils.mkdir_p(directory_path)

    return "#{$backup_path}/#{nfid}.fid"
  end

  #Get a file from MogileFS and save it to the destination path.  TRUE if success, false if there was an error
  def save_to_fs
    begin
      $mg.get_file_data(dkey, path)
    rescue Exception => e
      return false
    end
    true
  end

  #If there is no fileclass then it is the default class
  def classname
    if fileclass
      fileclass.classname
    else
      return 'default'
    end
  end

end