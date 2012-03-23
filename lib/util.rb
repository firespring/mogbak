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

    pid = Process.fork do
      begin

        parent_write.close
        parent_read.close

        while !child_read.eof? do
          #puts Marshal.load(child_read)
          #Marshal.dump('Hello parent!', child_write)
          job = Marshal.load(child_read)
          result = child_proc.call(job)
          Marshal.dump(result, child_write)
        end

      ensure
        child_read.close
        child_write.close
      end
    end

    child_read.close
    child_write.close

    {:write => parent_write, :read => parent_read, :pid => pid}
  end


  def self.hybrid_fork(qty, jobs, parent_proc, child_proc)
    threads = []
    semaphore = Mutex.new

    #split the jobs up
    jobs = jobs.in_groups(qty)


    qty.times do |i|
      threads[i] = Thread.new do
        child = make_child(child_proc)
        pid = child[:pid]
        njobs = jobs[i - 1]

        njobs.each do |job|
          break if job == nil
          semaphore.synchronize do
            Marshal.dump(job, child[:write])
            result = Marshal.load(child[:read])
            parent_proc.call(result)
          end

          #parent_proc.call(result)
        end

        #exit
        #parent_proc.call(child[:read], child[:write], semaphore)

        #close the pipe
        child[:write].close

        Process.wait(pid)
      end
    end
    wait_for_threads(threads)
  end



end