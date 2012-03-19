class MogileBackup
  attr_accessor :dbhost, :dbport, :dbname, :dbuser, :dbpass, :domain, :path

  def initialize(o={})
    @dbhost = o['dbhost'] if o['dbhost']
    @dbport = o['dbport'] if o['dbport']
    @dbname = o['dbname'] if o['dbname']
    @dbuser = o['dbuser'] if o['dbuser']
    @dbpass = o['dbpass'] if o['dbpass']
    @domain = o[:domain] if o[:domain]
    @path = o[:path] if o[:path]

    require ('domain')
    require('file')
    require('sqlite3')
    require('bakfile')
    require('fileclass')

  end

  def bak_file(file)
    saved = file.save_to_fs

    if saved
      puts "Backed up: FID #{file.fid}"
    else
      puts "Error - will try again on next run: FID #{file.fid}"
    end

    return saved
  end

  def backup
    #get the domain id for the domain
    dmid = Domain.find_by_namespace(self.domain)
    if (dmid)

      #first we retry files that we haven't been able to backup successfully, if any.
      BakFile.find_each(:conditions => ['saved = ?', false]) do |bak_file|
        file = Fid.find_by_fid(bak_file.fid)
        saved = bak_file(file)
        if saved
          BakFile.transaction do
            bak = BakFile.find_by_fid(bak_file.fid)
            bak.saved = true
            bak.save
          end
        end
      end

      #now back up any new files.  if they fail to be backed up we'll retry them the next time the backup
      #command is ran.
      results = Fid.find_each(:conditions => ['dmid = ? AND fid > ?', dmid, BakFile.max_fid]) do |file|
        saved = bak_file(file)
        BakFile.transaction do
          BakFile.create(:fid => file.fid,
                         :domain => file.domain.namespace,
                         :dkey => file.dkey,
                         :length => file.length,
                         :classname => file.classname,
                         :saved => saved)
        end
      end

      #Delete files from the backup that no longer exist in the mogilefs domain
      files_to_delete = Array.new
      BakFile.find_in_batches { |bak_files|

        union = "SELECT #{bak_files.first.fid} as fid"
        bak_files.shift
        bak_files.each do |bakfile|
          union = "#{union} UNION SELECT #{bakfile.fid}"
        end
        connection = ActiveRecord::Base.connection
        files = connection.select_values("SELECT t1.fid FROM (#{union}) as t1 LEFT JOIN file on t1.fid = file.fid WHERE file.fid IS NULL")
        files_to_delete += files
      }
      files_to_delete.each do |del_file|
        BakFile.transaction do
          delete = BakFile.where(:fid => del_file).first.destroy
          if delete
            puts "Deleting from backup: FID #{del_file}"
          else
            puts "Failed to delete from backup: FID #{del_file}"
          end
        end
      end
    else
      raise 'Cannot backup - domain not found'
    end

  end
end