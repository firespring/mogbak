require "activerecord-import/base"

#Represents files that are mirrored
class MirrorFile < ActiveRecord::Base
  attr_accessor :length, :classname

  self.table_name = :mirror_file

  def self.max_fid
    MirrorFile.order("fid").last.try(:fid) || 0
  end
end
