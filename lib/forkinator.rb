#The Forkinator makes it easy to fork workers,  pass a list of jobs for them to work on,  and listen for the results back from
#the child process.  It uses a combination of threading and forking to accomplish this.  Marshal is used to pass objects back
#and forth between the child and parent via IO.pipe.
class Forkinator

  #Wait for threads
  #@param [Array] threads an array containing threads we need to wait on
  def self.wait_for_threads(threads)
    threads.compact.each do |t|
      begin
        t.join
      rescue Interrupt
        # no reason to wait on dead threads
      end
    end
  end

  #Fork a child.  Provide a proc of code to run inside child.  The child proc expects to be sent an array of jobs
  #@param [Proc] child_proc code for the child to run
  #@return [Hash] :write = pipe for writing to the child, :read = pipe for reading from the child, :pid = pid of the child
  def self.make_child(child_proc)

    #open pipes for two way communication between the parent and child
    child_read, parent_write = IO.pipe
    parent_read, child_write = IO.pipe

    #remove our database connection,  we don't want it inside the child,  as it'll get closed when the child shuts down
    mog_config = ActiveRecord::Base.remove_connection

    #fork, code inside this block is only ran inside the child
    pid = Process.fork do
      begin

        #Since we're the child now,  we'll close the parent's r/w pipes as we don't need them
        parent_write.close
        parent_read.close

        #child loops through IO pipe,  listening for data from the parent,  if the parent closes the pipe then we're
        #done
        while !child_read.eof? do
          #rename the process to make it clear that it's a worker in idle status
          $0 = "mogbak [idle]"
          #this call blocks until it receives something from the parent via the pipe
          job = Marshal.load(child_read)
          #since we're working now we'll rename the process
          $0 = "mogbak [working]"
          #call the child proc
          result = child_proc.call(job)
          #hand the child proc response back to the parent
          Marshal.dump(result, child_write)
        end

      #no matter what happens..make sure we get the pipes closed
      ensure
        child_read.close
        child_write.close
      end
    end

    #This is the parent executing this -- reconnect to the database we just dropped above.
    ActiveRecord::Base.establish_connection(mog_config)

    #close the child's handle on the pipes since the parent won't need them
    child_read.close
    child_write.close

    {:write => parent_write, :read => parent_read, :pid => pid}
  end

  #Forks children,  makes threads for two-way communication,  and evenly distributes jobs to each child.
  #@param [Integer] qty number of workers to launch
  #@param [Array] jobs array containing jobs for each child
  #@param [Proc] parent_proc code to be ran in the thread used to communicate with the child
  #@param [Proc] child_proc code to be ran in the forked child
  def self.hybrid_fork(qty, jobs, parent_proc, child_proc)
    threads = []

    #mutex is used to ensure that some operations in the threads don't have the potential of happening at the same time
    #in another thread
    semaphore = Mutex.new

    require('thread')

    #split the jobs up
    jobs = jobs.in_groups(qty)

    #spawn the children
    children = []
    qty.times { children << make_child(child_proc)}

    #For each worker
    qty.times do |i|

      #start a thread
      threads[i] = Thread.new do
        Thread.current.abort_on_exception = true

        child = {}
        semaphore.synchronize { child = children.pop }

        pid = child[:pid]
        njobs = jobs[i - 1]

        #pass jobs to child
        Marshal.dump(njobs, child[:write])

        #wait for result
        result = Marshal.load(child[:read])

        #process result
        semaphore.synchronize { parent_proc.call(result) }

        #close the pipe
        child[:write].close

        #wait for process to finish before terminating this thread
        Process.wait(pid)

        #close db connection
        SqliteActiveRecord.connection.close
      end
    end
    wait_for_threads(threads)
  end
end