# -*- coding: utf-8 -*-
require 'thread'
require 'digest/sha1'

module Roma
  
  class AsyncMessage
    attr_accessor :event
    attr_accessor :args
    attr_accessor :callback

    def initialize(ev,ag=nil,&cb)
      @event = ev
      @args = ag
      @callback = cb
      @retry_count = 0
      @retry_max = 10
      @retry_wait = 0.1
    end

    def retry?
      @retry_max > @retry_count
    end

    def incr_count
      @retry_count += 1
    end

    def wait
      sleep(@retry_wait)
    end
  end

  module AsyncProcess

    @@async_queue = Queue.new

    def self.queue
      @@async_queue
    end

    def start_async_process
      @async_thread = Thread.new{
        async_process_loop
      }
      @async_thread[:name] = __method__
    rescue =>e
      @log.error("#{e}\n#{$@}")
    end

    private

    def stop_async_process
      count = 0
      while @@async_queue.empty? == false && count < 100
        count += 1
        sleep 0.1
      end
      @async_thread.exit
    end

    def async_process_loop
      loop {
        while msg = @@async_queue.pop
          if send("asyncev_#{msg.event}",msg.args)
            msg.callback.call(msg,true) if msg.callback
          else
            if msg.retry?
              t = Thread.new{
                msg.wait
                msg.incr_count
                @@async_queue.push(msg)
              }
              t[:name] = __method__
            else
              @log.error("async process retry out:#{msg.inspect}")
              msg.callback.call(msg,false) if msg.callback
            end
          end
        end
      }
    rescue =>e
      @log.error("#{e}\n#{$@}")
      retry
    end

    def asyncev_broadcast_cmd(args)
      @log.debug("#{__method__} #{args.inspect}")
      cmd, nids, tout = args
      t = Thread::new{
        async_broadcast_cmd("#{cmd}\r\n", nids, tout)
      }
      t[:name] = __method__
      true
    end

    def asyncev_start_join_process(args)
      @log.debug(__method__)
      if @stats.run_join
        @log.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running")
        return true
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_join = true
      t = Thread::new do
        begin
          join_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_join = false
          @stats.join_ap = nil
        end
      end
      t[:name] = __method__
      true      
    end

    def asyncev_start_balance_process(args)
      @log.debug(__method__)
      if @stats.run_join
        @log.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running")
        return true
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_balance = true
      t = Thread::new do
        begin
          balance_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_balance = false
        end
      end
      t[:name] = __method__
      true      
    end

    def asyncev_redundant(args)
      nid,hname,k,d,clk,expt,v = args
      @log.debug("#{__method__} #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async redundant failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rset #{k}\e#{hname} #{d} #{clk} #{expt} #{v.length}\r\n#{v}\r\n",10)
      if res == nil || res.start_with?("ERROR")
        @log.warn("async redundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_zredundant(args)
      nid,hname,k,d,clk,expt,zv = args
      @log.debug("#{__method__} #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async zredundant failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rzset #{k}\e#{hname} #{d} #{clk} #{expt} #{zv.length}\r\n#{zv}\r\n",10)
      if res == nil || res.start_with?("ERROR")
        @log.warn("async zredundant failed:#{k}\e#{hname} #{d} #{clk} #{expt} #{v.length} -> #{nid}")
        return false # retry
      end
      true
    end

    def asyncev_rdelete(args)
      nid,hname,k,clk = args
      @log.debug("#{__method__} #{args.inspect}")
      unless @rttable.nodes.include?(nid)
        @log.warn("async rdelete failed:#{nid} dose not found in routing table.#{k}\e#{hname} #{clk}")
        return true # no retry
      end
      res = async_send_cmd(nid,"rdelete #{k}\e#{hname} #{clk}\r\n",10)
      unless res
        @log.warn("async redundant failed:#{k}\e#{hname} #{clk} -> #{nid}")
        return false # retry
      end
      true      
    end

    def asyncev_reqpushv(args)
      vn, nid, p = args
      @log.debug("#{__method__} #{args.inspect}")
      if @stats.run_iterate_storage
        @log.warn("#{__method__}:already be iterated storage process.")
      else
        @stats.run_iterate_storage = true
        t = Thread::new do
          begin
            sync_a_vnode(vn.to_i, nid, p == 'true')
          rescue =>e
            @log.error("#{__method__}:#{e.inspect} #{$@}")
          ensure
            @stats.run_iterate_storage = false
          end
        end
        t[:name] = __method__
      end
    end

    def asyncev_start_recover_process(args)
      @log.debug("#{__method__} #{args.inspect}")
      if @stats.run_join
        @log.error("#{__method__}:join process running")
        return true
      end
      if @stats.run_recover
        @log.error("#{__method__}:recover process running.")
        return false
      end
      if @stats.run_balance
        @log.error("#{__method__}:balance process running")
        return true
      end
      @stats.run_recover = true
      t = Thread::new do
        begin
          acquired_recover_process
        rescue => e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.run_recover = false
        end
      end
      t[:name] = __method__
    end

    def asyncev_start_release_process(args)
      @log.debug("#{__method__} #{args}")
      if @stats.run_iterate_storage
        @log.warn("#{__method__}:already be iterated storage process.")
      else
        @stats.run_release = true
        @stats.run_iterate_storage = true
        @stats.spushv_protection = true
        t = Thread::new do
          begin
            release_process
          rescue => e
            @log.error("#{__method__}:#{e.inspect} #{$@}")
          ensure
            @stats.run_iterate_storage = false
            @stats.run_release = false
          end
        end
        t[:name] = __method__
      end
    end

    def acquired_recover_process
      @log.info("#{__method__}:start")

      exclude_nodes = @rttable.exclude_nodes_for_recover(@stats.ap_str, @stats.rep_host)
      
      @do_acquired_recover_process = true
      loop do
        break unless @do_acquired_recover_process

        vn, nodes, is_primary = @rttable.select_vn_for_recover(exclude_nodes)
        break unless vn

        if nodes.length != 0
          ret = req_push_a_vnode(vn, nodes[0], is_primary)
          if ret == :rejected
            sleep 1
          elsif ret == false
            break
          end
          sleep 1
        end
      end
      @log.info("#{__method__} has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_acquired_recover_process = false
    end
 
    def join_process
      @log.info("#{__method__}:start")
      count = 0
      nv = @rttable.v_idx.length
      exclude_nodes = @rttable.exclude_nodes_for_join(@stats.ap_str, @stats.rep_host)

      @do_join_process = true
      while (@rttable.vnode_balance(@stats.ap_str) == :less && count < nv) do
        break unless @do_join_process

        vn, nodes, is_primary = @rttable.select_vn_for_join(exclude_nodes)
        unless vn
          @log.warn("#{__method__}:vnode dose not found")
          return false
        end
        ret = req_push_a_vnode(vn, nodes[0], is_primary)      
        if ret == :rejected
          sleep 5
        else
          sleep 1
          count += 1
        end
      end
      @log.info("#{__method__} has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_join_process = false
    end

    def balance_process
      @log.info("#{__method__}:start")
      count = 0
      nv = @rttable.v_idx.length
      exclude_nodes = @rttable.exclude_nodes_for_balance(@stats.ap_str, @stats.rep_host)

      @do_balance_process = true
      while (@rttable.vnode_balance(@stats.ap_str) == :less && count < nv) do
        break unless @do_balance_process

        vn, nodes, is_primary = @rttable.select_vn_for_balance(exclude_nodes)
        unless vn
          @log.warn("#{__method__}:vnode dose not found")
          return false
        end
        ret = req_push_a_vnode(vn, nodes[0], is_primary)      
        if ret == :rejected
          sleep 5
        else
          sleep 1
          count += 1
        end
      end
      @log.info("#{__method__} has done.")
    rescue => e
      @log.error("#{e.inspect} #{$@}")
    ensure
      @do_balance_process = false
    end

    def req_push_a_vnode(vn, src_nid, is_primary)
      con = Roma::Messaging::ConPool.instance.get_connection(src_nid)
      con.write("reqpushv #{vn} #{@stats.ap_str} #{is_primary}\r\n")
      res = con.gets # receive 'PUSHED\r\n' | 'REJECTED\r\n' | 'ERROR\r\n'
      if res == "REJECTED\r\n"
        @log.warn("#{__method__}:request was rejected from #{src_nid}.")
        Roma::Messaging::ConPool.instance.return_connection(src_nid,con)
        return :rejected
      elsif res != "PUSHED\r\n"
        @log.warn("#{__method__}:#{res}")
        return :rejected
      end
      Roma::Messaging::ConPool.instance.return_connection(src_nid,con)
      # waiting for pushv
      count = 0
      while @rttable.search_nodes(vn).include?(@stats.ap_str)==false && count < 300
        sleep 0.1
        count += 1
      end
      if count >= 300
        @log.warn("#{__method__}:request has been time-out.vn=#{vn} nid=#{src_nid}")
        return :timeout
      end
      true
    rescue =>e
      @log.error("#{__method__}:#{e.inspect} #{$@}")
      @rttable.proc_failed(src_nid)
      false
    end
 
    def release_process
      @log.info("#{__method__}:start.")

      if @rttable.can_i_release?(@stats.ap_str, @stats.rep_host)
        @log.error("#{__method__}:Sufficient nodes do not found.")
        return
      end

      @do_release_process = true
      while(@rttable.has_node?(@stats.ap_str)) do
        @rttable.each_vnode do |vn, nids|
          break unless @do_release_process
          if nids.include?(@stats.ap_str)
            
            to_nid, new_nids = @rttable.select_node_for_release(@stats.ap_str, @stats.rep_host, nids)
            unless sync_a_vnode_for_release(vn, to_nid, new_nids)
              @log.warn("#{__method__}:error at vn=#{vn} to_nid=#{to_nid} new_nid=#{new_nids}")
              redo
            end
          end
        end
      end
      @log.info("#{__method__} has done.")
    rescue =>e
      @log.error("#{e}\n#{$@}")
    ensure
      @do_release_process = false
      Roma::Messaging::ConPool.instance.close_all
    end
 
    def sync_a_vnode_for_release(vn, to_nid, new_nids)
      nids = @rttable.search_nodes(vn)
      
      if nids.include?(to_nid)==false || (is_primary && nids[0]!=to_nid)
        @log.debug("#{__method__}:#{vn} #{to_nid}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @rttable.transaction(vn, nids)

        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != "STORED"
            @rttable.rollback(vn)
            @log.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }

        if (clk = @rttable.commit(vn)) == false
          @rttable.rollback(vn)
          @log.error("#{__method__}:routing table commit failed")
          return false
        end

        clk = @rttable.set_route(vn, clk, new_nids)
        if clk.is_a?(Integer) == false
          clk,new_nids = @rttable.search_nodes_with_clk(vn)
        end
        
        cmd = "setroute #{vn} #{clk - 1}"
        new_nids.each{ |nn| cmd << " #{nn}"}
        res = async_broadcast_cmd("#{cmd}\r\n")
        @log.debug("#{__method__}:async_broadcast_cmd(#{cmd}) #{res}")
      end

      return true
    rescue =>e
      @log.error("#{e}\n#{$@}")
      false
    end

    def sync_a_vnode(vn, to_nid, is_primary=nil)
      nids = @rttable.search_nodes(vn)
      
      if nids.include?(to_nid)==false || (is_primary && nids[0]!=to_nid)
        @log.debug("#{__method__}:#{vn} #{to_nid} #{is_primary}")
        # change routing data at the vnode and synchronize a data
        nids << to_nid
        return false unless @rttable.transaction(vn, nids)

        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)

          if res != "STORED"
            @rttable.rollback(vn)
            @log.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }

        if (clk = @rttable.commit(vn)) == false
          @rttable.rollback(vn)
          @log.error("#{__method__}:routing table commit failed")
          return false
        end

        nids = edit_nodes(nids, to_nid, is_primary)
        clk = @rttable.set_route(vn, clk, nids)
        if clk.is_a?(Integer) == false
          clk,nids = @rttable.search_nodes_with_clk(vn)
        end
        
        cmd = "setroute #{vn} #{clk - 1}"
        nids.each{ |nn| cmd << " #{nn}"}
        res = async_broadcast_cmd("#{cmd}\r\n")
        @log.debug("#{__method__}:async_broadcast_cmd(#{cmd}) #{res}")
      else
        # synchronize a data
        @storages.each_key{ |hname|
          res = push_a_vnode_stream(hname, vn, to_nid)
          if res != "STORED"
            @log.error("#{__method__}:push_a_vnode was failed:hname=#{hname} vn=#{vn}:#{res}")
            return false
          end
        }
      end

      return true
    rescue =>e
      @log.error("#{e}\n#{$@}")
      false
    end

    def edit_nodes(nodes, new_nid, is_primary)
      if @rttable.rn == 1
        return [new_nid]
      end
      if nodes.length > @rttable.rn
        nodes.delete(new_nid)
        nodes.delete(nodes.last)
        nodes << new_nid
      end
      if is_primary
        nodes.delete(new_nid)
        nodes.insert(0,new_nid)
      end
      nodes
    end

    def push_a_vnode_stream(hname, vn, nid)
      @log.info("#{__method__}:hname=#{hname} vn=#{vn} nid=#{nid}")

      stop_clean_up

      con = Roma::Messaging::ConPool.instance.get_connection(nid)

      @do_push_a_vnode_stream = true
      
      con.write("spushv #{hname} #{vn}\r\n")

      res = con.gets # READY\r\n or error string
      if res != "READY\r\n"
        con.close
        return res.chomp
      end

      @storages[hname].each_vn_dump(vn){|data|

        unless @do_push_a_vnode_stream
          con.close
          @log.error("#{__method__}:canceled in hname=#{hname} vn=#{vn} nid=#{nid}")
          return "CANCELED"
        end

        con.write(data)
        sleep @stats.stream_copy_wait_param
      }
      con.write("\0"*20) # end of steram

      res = con.gets # STORED\r\n or error string
      Roma::Messaging::ConPool.instance.return_connection(nid,con)
      res.chomp! if res
      res
    rescue =>e
      @log.error("#{e}\n#{$@}")
      e.to_s
    end


    def asyncev_start_storage_clean_up_process(args)
#      @log.info("#{__method__}")
      if @stats.run_storage_clean_up
        @log.error("#{__method__}:already in being")
        return
      end
      @stats.run_storage_clean_up = true
      t = Thread::new{
        begin
          storage_clean_up_process
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
          @stats.last_clean_up = Time.now
          @stats.run_storage_clean_up = false
        end
      }
      t[:name] = __method__
    end

    def storage_clean_up_process
      @log.info("#{__method__}:start")
      me = @stats.ap_str
      vnhash={}
      @rttable.each_vnode do |vn, nids|
        if nids.include?(me)
          if nids[0] == me
            vnhash[vn] = :primary
          else
            vnhash[vn] = :secondary
          end
        end
      end
      t = Time.now.to_i - Roma::Config::STORAGE_DELMARK_EXPTIME
      count = 0
      @storages.each_pair do |hname,st|
        break unless @stats.do_clean_up?
        st.each_clean_up(t, vnhash) do |key, vn|
          # @log.debug("#{__method__}:key=#{key} vn=#{vn}")
          if @stats.run_receive_a_vnode.key?("#{hname}_#{vn}")
            false
          else
            nodes = @rttable.search_nodes_for_write(vn)
            if nodes && nodes.length > 1
              nodes[1..-1].each do |nid|
                res = async_send_cmd(nid,"out #{key}\e#{hname} #{vn}\r\n")
                unless res
                  @log.warn("send out command failed:#{key}\e#{hname} #{vn} -> #{nid}")
                end
                # @log.debug("#{__method__}:res=#{res}")
              end
            end
            count += 1
            @stats.out_count += 1
            true
          end
        end
      end
      if count>0
        @log.info("#{__method__}:#{count} keys deleted.")
      end
    ensure
      @log.info("#{__method__}:stop")
    end

    def asyncev_calc_latency_average(args)
      latency,cmd,denominator = args
      @log.debug(__method__)

      @latency_chk_cnt = {} if !defined?(@latency_chk_cnt)
      @sum = {} if !defined?(@sum)
      @latency_chk_cnt.store(cmd, 0) if !@latency_chk_cnt.key?(cmd)
      @sum.store(cmd, 0) if !@sum.key?(cmd)

      t = Thread::new do
        begin
        @sum[cmd] += latency
          if (@latency_chk_cnt[cmd] += 1) >= denominator
            average = @sum[cmd] / @latency_chk_cnt[cmd]
            @log.info("latency average about [#{cmd}] is #{average} seconds")
            @latency_chk_cnt[cmd] = 0
            @sum[cmd] = 0
          end
        rescue =>e
          @log.error("#{__method__}:#{e.inspect} #{$@}")
        ensure
        end
      end
      t[:name] = __method__
      true
    end

  end # module AsyncProcess

end # module Roma
