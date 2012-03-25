class Forker

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

        parent_write.close
        parent_read.close

        while !child_read.eof? do
          $0 = "mogbak [idle]"
          job = Marshal.load(child_read)
          $0 = "mogbak [working]"
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

    require('thread')
    #add jobs to queue
    queue = Queue.new
    jobs.each { |job| queue << job}

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

        loop do
          job = queue.pop
        end

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