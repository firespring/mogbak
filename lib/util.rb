class Util
  #This produces a hashed path very similar to mogilefs just without the device id.  It also recursively creates the
  #directory inside the backup
  def self.path(sfid)
    sfid = "#{sfid}"
    length = sfid.length
    if length < 10
      nfid = "0" * (10 - length) + sfid
    else
      nfid = fid
    end
    /(?<b>\d)(?<mmm>\d{3})(?<ttt>\d{3})(?<hto>\d{3})/ =~ nfid

    #create the directory
    directory_path = "#{$backup_path}/#{b}/#{mmm}/#{ttt}"
    FileUtils.mkdir_p(directory_path)

    return "#{directory_path}/#{nfid}.fid"
  end




  def self.wait_for_threads(threads)
    threads.compact.each do |t|
      begin
        t.join
      rescue Interrupt
        # thread died, do not stop other threads
      end
    end
  end

  def self.make_child(child_proc)
    child_read, parent_write = IO.pipe
    parent_read, child_write = IO.pipe

    mog_config = ActiveRecord::Base.remove_connection

    pid = Process.fork do
      begin
        $0 = "mogbak [worker]"
        parent_write.close
        parent_read.close

        while !child_read.eof? do
          job = Marshal.load(child_read)
          result = child_proc.call(job)
          Marshal.dump(result, child_write)
        end

      ensure
        child_read.close
        child_write.close
      end
    end

    ActiveRecord::Base.establish_connection(mog_config)


    child_read.close
    child_write.close

    {:write => parent_write, :read => parent_read, :pid => pid}
  end


  def self.hybrid_fork(qty, jobs, parent_proc, child_proc)
    threads = []
    semaphore = Mutex.new

    #split the jobs up
    jobs = jobs.in_groups(qty)

    #spawn the children
    children = []
    qty.times { children << make_child(child_proc)}

    #register signal handler so that children kill if program receives a SIGINT
    #which will happen if the user ctrl c's the parent process
    Signal.trap :SIGINT do
      children.each { |child| Process.kill(:KILL, child[:pid]) if child[:pid]}
      exit 1
    end

    qty.times do |i|

      threads[i] = Thread.new do
        Thread.current.abort_on_exception = true

        child = {}
        semaphore.synchronize { child = children.pop }

        pid = child[:pid]
        njobs = jobs[i - 1]


            Marshal.dump(njobs, child[:write])
            result = Marshal.load(child[:read])
        semaphore.synchronize { parent_proc.call(result) }




        #close the pipe
        child[:write].close

        Process.wait(pid)

        #close db connection
        SqliteActiveRecord.connection.close
      end
    end
    wait_for_threads(threads)

  end



end