require "activerecord-import/base"

#Represents files that are mirrored
class MirrorFile < ActiveRecord::Base
  attr_accessor :length, :classname

  self.primary_key = :dkey
  self.table_name = :mirror_file

  def self.max_fid
    # TODO: change back when we are ready to go live
#    MirrorFile.order("fid").last.try(:fid) || 0
#    MirrorFile.order("fid").last.try(:fid) || 236605
#    max_fid = 141795
#    max_fid = 205425
#    max_fid = 236605

    MirrorFile.order("fid").last.try(:fid) || 141795
#    MirrorFile.order("fid").last.try(:fid) || 205425
#    MirrorFile.order("fid").last.try(:fid) || 236874
  end
end
