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
        @replica_mklhash = nil
      end

      def transmit(cmd)
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

      def check_replica_mklhash
        nid = @stats.replica_nodelist[0]
        con = Roma::Messaging::ConPool.instance.get_connection(nid)
        con.write("mklhash 0\r\n")
        res = con.gets.chomp
        if @replica_mklhash != res
          @replica_mklhash = res
          @log.debug("replica_mklhash was changed.[#{@replica_mklhash}]")

          con.write("nodelist\r\n")
          new_replica_nodelist = con.gets.chomp.split("\s")
          @stats.replica_nodelist = new_replica_nodelist
          @log.debug("replica_nodelist was changed.#{@stats.replica_nodelist}")
        end
      rescue => e
        @log.error("#{e}\n#{$@}")
      ensure
        Roma::Messaging::ConPool.instance.return_connection(nid, con)
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
      @cr_mklhash_thread = Thread.new{
        cr_mklhash_loop
      }
      @cr_mklhash_thread[:name] = 'cluster_replication_mklhash'
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

    def cr_mklhash_loop
      loop {
        @cr_writer.check_replica_mklhash
        sleep 10
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
      retry
    end
    private :cr_mklhash_loop

  end # module ClusterReplicationProcess

end # module Roma
