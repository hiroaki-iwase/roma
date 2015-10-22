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
        addr, port = @stats.replica_nodelist.sample.split(/[:_]/)
        con = TCPSocket.new(addr, port)
        con.write("mklhash 0\r\n")
        @replica_mklhash = con.gets.chomp
        @log.debug("initial mklhash: [#{@replica_mklhash}]")
      rescue => e
        @log.error("Cluster Replication Error: target cluster is NOT booted.")
        @log.error("#{e}\n#{$@}")
      ensure
        con.close
      end

      def transmit(cmd)
        check_mklhash
        @do_transmit = true
        nid = @stats.replica_nodelist.sample

        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        raise unless con

        con.write(cmd)
      rescue => e
        @log.error("#{e}\n#{$@}")
      ensure
        Roma::Messaging::ConPool.instance.return_connection(nid, con)
        @do_transmit = false
      end

      def check_mklhash
        nid = @stats.replica_nodelist.sample
        addr, port = nid.split(/[:_]/)
        con = TCPSocket.new(addr, port) # [toDO] con pool　利用する
        con.write("mklhash 0\r\n")
        current_mklhash = con.gets.chomp

        unless @replica_mklhash == current_mklhash
          @replica_mklhash = current_mklhash
          con.write("nodelist\r\n")
          @stats.replica_nodelist = con.gets.chomp.split("\s")
          @log.warn("replica cluster's routing was changed.\r\n\tnew replicamklhash: [#{@replica_mklhash}]\r\n\tnew replica nodelist: #{@stats.replica_nodelist}")
        end
      rescue Errno::ECONNREFUSED => e
        @stats.replica_nodelist.delete(nid)
        @stats.replica_nodelist.each{|nid|
          addr, port = nid.split(/[:_]/)
          con = TCPSocket.new(addr, port)
          con.write("mklhash 0\r\n")

          if con
            current_mklhash = con.gets.chomp
            @replica_mklhash = current_mklhash
            con.write("nodelist\r\n") 
            @stats.replica_nodelist = con.gets.chomp.split("\s")
            @log.warn("replica cluster's routing was changed.\r\n\tnew replicamklhash: [#{@replica_mklhash}]\r\n\tnew replica nodelist: #{@stats.replica_nodelist}")
            break
          end
          
          
        }
      rescue => e
        @log.warn("error class => #{e.class}")
      end

      def close_all
        @stats.replica_nodelist.each{|nid|
          delete_connection(nid)
        }
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
