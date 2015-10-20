require 'thread'
require 'socket'

module Roma

  module ClusterReplication
    
    class StreamWriter

      def initialize(log)
        @log = log
        @do_transmit = false
        @replica_nodelist = ['192.168.33.13_10001', '192.168.33.13_10002', '192.168.33.13_10003']
      end

      def transmit(cmd)
        @do_transmit = true
        con = get_replica_connection
        con.write(cmd)
      rescue => e
        @log.error("#{e}\n#{$@}")
      ensure
        @do_transmit = false
        con.close
      end

      def get_replica_connection
        # [toDO] コネクションプール使う
        nid = @replica_nodelist.sample
        addr, port = nid.split(/[:_]/)
        TCPSocket.new(addr, port)
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
