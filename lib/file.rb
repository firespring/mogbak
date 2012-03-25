class Fid < ActiveRecord::Base
  self.primary_key = "fid"
  self.table_name = "file"
  belongs_to :domain,
             :foreign_key => "dmid"
  belongs_to :fileclass,
             :foreign_key => "classid"

  #Get a file from MogileFS and save it to the destination path.  TRUE if success, false if there was an error
  def save_to_fs
    begin
      path = MogileHelper.path(self.fid)
      $mg.get_file_data(dkey, path)
    rescue Exception => e
      if $debug
        raise e
      end
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