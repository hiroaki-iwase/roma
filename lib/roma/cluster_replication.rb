require 'thread'
require 'roma/stats'
require 'roma/event/con_pool'
require 'socket'

module Roma

  module ClusterReplication
    
    class StreamWriter

      def initialize(log)
        @log = log
        @do_transmit = false
        @stats = Roma::Stats.instance
        @replica_nodelist = @stats.replica_nodelist
      end

      def transmit(cmd)
        @do_transmit = true
        nid = @replica_nodelist.sample

        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        raise unless con

        con.write(cmd)
      rescue => e
        @log.error("#{e}\n#{$@}")
      ensure
        Roma::Messaging::ConPool.instance.return_connection(nid, con)
        @do_transmit = false
      end

      def close_all
        @fdh.each_value{|fd| fd.close }
      end

    end # class StreamWriter
    
  end # module ClusterReplication

  module ClusterReplicationProcess

    @@cr_queue = Queue.new

    def self.push(cmd)
      @@cr_queue.push(cmd)
    end

    def start_cr_process
      @cr_thread = Thread.new{
        cr_process_loop
      }
      @cr_thread[:name] = 'cluster_replication'
    rescue =>e
      @log.error("#{e}\n#{$@}")
    end

    def stop_cr_process
      until @@cr_queue.empty?
        sleep 0.01
      end
      @cr_thread.exit
      @cr_writer.close_all
    end

    def cr_process_loop
      loop {
        while cmd = @@cr_queue.pop
          @cr_writer.transmit(cmd)
        end
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
      retry
    end
    private :cr_process_loop

  end # module ClusterReplicationProcess

end # module Roma
