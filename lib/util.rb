class Util
  #This produces a hashed path very similar to mogilefs just without the device id.  It also recursively creates the
  #directory inside the backup
  def self.path(sfid)
    sfid = "#{sfid}"
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

    return "#{directory_path}/#{nfid}.fid"
  end
end