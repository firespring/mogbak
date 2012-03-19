class Fileclass < ActiveRecord::Base
    self.primary_key = "classid"
    self.table_name = "class"
end