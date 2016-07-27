#Represents classes in a MogileFS database server
class Fileclass < ActiveRecord::Base
  self.primary_keys = :dmid, :classid
  self.table_name = 'class'
  has_many :fids, class_name: 'Fid', foreign_key: [:dmid, :classid]
end
