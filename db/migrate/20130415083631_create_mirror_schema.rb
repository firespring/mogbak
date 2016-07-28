class CreateMirrorSchema < ActiveRecord::Migration[4.2]
  def self.up
    Log.instance.debug('Creating mirror_file table')
    create_table(:mirror_file, id: false, force: true) do |t|
      t.integer :fid, null: false, primary_key: true
      t.string  :dkey, limit: 255, null: false
      t.integer :length, null: false
      t.string  :classname, limit: 255, null: false
    end

    add_index :mirror_file, :dkey, unique: true
  end
end
