require "activerecord-import/base"

#Represents files that are mirrored
class MirrorFile < ActiveRecord::Base
  attr_accessor :dmid, :length, :classname

  self.primary_key = :dkey
  self.table_name = :mirror_file

  def self.max_fid
    # TODO: change back when we are ready to go live
#    MirrorFile.order("fid").last.try(:fid) || 0
    MirrorFile.order("fid").last.try(:fid) || 236605
  end

  #Stores a file in a MogileFS domain
  #@return [Bool]
  def mirror(source_mg, dest_mg, dkey)
    begin
      $mg.store_file(dkey, self.classname, path)
    rescue Exception => e
      Log.instance.error("Could not mirror: #{e}\n#{e.backtrace}")
      raise 'Could not mirror'
    end
  end

  #Delete from filesystem using just a fid
  #@return [Bool]
  def self.delete(mg, dkey)
    begin
      mg.delete(dkey)
    rescue Exception => e
      Log.instance.error("Could not delete: #{e}\n#{e.backtrace}")
      raise 'Could not delete'
    end
  end
end
