require 'file'

class DestFile < Fid
  def self.stream(file, mog_source, mog_dest)
    #TODO: More error handling!

    # Create a pipe to link the get / store commands
    rd, wr = IO.pipe

    # Fork a child process to write to the pipe
    childpid = Process.fork do
      rd.close
      mog_source.get_file_data(file.dkey, dst=wr)
      wr.close
    end

    # Read info off the pipe that the child is writing to
    wr.close
    mog_dest.store_file(file.dkey, file.classname, rd)
    rd.close

    # Wait for the child to exit
    Process.wait
    Log.instance.debug("Child exited with a status of #{$?.exitstatus}")
  end

  #Delete from filesystem using just a fid
  #@return [Bool]
  def self.delete(mg, dkey)
    begin
      mg.delete(dkey)
    rescue Exception => e
      Log.instance.error("Could not delete: #{e}\n#{e.backtrace}")
      raise 'Could not delete'
    end
  end

end
