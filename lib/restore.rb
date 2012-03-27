#Restore a mogbak backup to a MogileFS domain
class Restore
  attr_accessor :domain, :tracker_host, :tracker_port, :backup_path, :workers
  include Validations

  #
  def initialize(o={})
    @domain = o[:domain] if o[:domain]
    @tracker_ip = o[:tracker_ip] if o[:tracker_ip]
    @tracker_port = o[:tracker_port] if o[:tracker_port]
    @backup_path = o[:backup_path] if o[:backup_path]
    @workers = o[:workers] if o[:workers]


    #If settings file does not exist then this is not a valid mogilefs backup
    check_settings_file('settings.yml not found in path.  This must not be a backup profile. Cannot restore')

    connect_sqlite
    migrate_sqlite
    mogile_tracker_connect

    #Now that database is all setup load the model classes
    require ('domain')
    require('file')
    require('bakfile')
    require('fileclass')
  end

  def output_save(save, fid)
    if save
      puts "Restored: FID #{fid}"
    else
      puts "Error: FID #{fid}"
    end
  end

  def launch_restore_workers(files)
    child = Proc.new { |files|
      results = []
      files.each do |file|
        break if file.nil?
        save = file.restore
        output_save(save, file.fid)
        results << {:restored => save, :fid => file.fid}
      end
      results
    }

    parent = Proc.new { |results|
      SqliteActiveRecord.clear_active_connections!
    }

    Forkinator.hybrid_fork(self.workers.to_i, files, parent, child)

  end

  def restore(dkey = false)
    if dkey
      file = BakFile.find_by_dkey(dkey)
      raise 'file not found in backup' unless file
      save = file.restore
      output_save(save, file.fid)
    else

      BakFile.find_in_batches(:conditions => ['saved = ?', true], :batch_size => 2000) do |batch|
        launch_restore_workers(batch)
      end

    end
  end
end