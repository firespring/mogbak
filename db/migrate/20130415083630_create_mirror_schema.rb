class CreateMirrorSchema < ActiveRecord::Migration
  def self.up
    Log.instance.debug("Creating mirror_file table")
    create_table(:mirror_file, {:id => false, :force => true}) do |t|
      t.string :dkey, :limit => 255, :null => false
      t.integer :fid, :null => false
    end

    add_index :mirror_file, :dkey
  end
end
