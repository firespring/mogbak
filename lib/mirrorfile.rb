require 'activerecord-import/base'

# Represents files that exist in the source mogile domain.
# This information is updated as we process.
class MirrorFile < ActiveRecord::Base
  self.table_name = :mirror_file

  # Returns the larges fid from the mirror_file table (or 0 if no records are present).
  def self.max_fid
    MirrorFile.order('fid').last.try(:fid) || 0
  end
end
