require "activerecord-import/base"

# Represents files that exist in the source mogile domain.
# This information is updated as we process.
class MirrorFile < ActiveRecord::Base
  attr_accessor :length, :classname

  self.table_name = :mirror_file

  def self.max_fid
    MirrorFile.order("fid").last.try(:fid) || 0
  end
end
