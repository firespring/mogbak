class PathHelper
  #This produces a hashed path very similar to mogilefs just without the device id.  It also recursively creates the
  #directory inside the backup
  def self.path(sfid)
    sfid = sfid.to_s
    length = sfid.length
    nfid = if length < 10
             '0' * (10 - length) + sfid
           else
             fid
           end
    /(?<b>\d)(?<mmm>\d{3})(?<ttt>\d{3})(?<hto>\d{3})/ =~ nfid

    #create the directory
    directory_path = "#{$backup_path}/#{b}/#{mmm}/#{ttt}"
    FileUtils.mkdir_p(directory_path)

    "#{directory_path}/#{nfid}.fid"
  end
end
