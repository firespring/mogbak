#represents files that are in the MogileFS database.  This model is used for talking to the MogileFS database via
#ActiveRecord
class Fid < ActiveRecord::Base
  self.primary_key = 'fid'
  self.table_name = 'file'

  belongs_to :domain, foreign_key: 'dmid'
  belongs_to :fileclass, foreign_key: [:dmid, :classid]

  #If there is no fileclass then it is the default class
  #@return [String] name of class file belongs to
  def classname
    return fileclass.classname if fileclass
    'default'
  end
end
