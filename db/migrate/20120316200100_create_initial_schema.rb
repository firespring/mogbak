class CreateInitialSchema < ActiveRecord::Migration[4.2]
  def self.up
    create_table :bak_files do |t|
      t.integer :fid
      t.string :domain, :limit => 255
      t.string :dkey, :limit => 255
      t.integer :length, :limit => 8
      t.string :classname, :limit => 255
      t.boolean :saved, :default => false
    end
    add_index :bak_files, :fid
  end

  def self.down
    drop_table :bak_files
  end
end
