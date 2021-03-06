#!/usr/bin/env ruby

require 'roma/client/rclient'
require 'roma/messaging/con_pool'

TCP_SERVER_PORT = 11_219
$dat = {}

def receive_command_server
  $gs = TCPServer.open(TCP_SERVER_PORT)
  loop do
    Thread.new($gs.accept) do |s|
      begin
        while res = s.gets
          if res.start_with?('pushv')
            ss = res.split(' ')
            s.write("READY\r\n")
            len = s.gets.chomp
            $dat[ss[2].to_i] = receive_dump(s, len.to_i)
            s.write("STORED\r\n")
          elsif res.start_with?('spushv')
            ss = res.split(' ')
            s.write("READY\r\n")
            $dat[ss[2].to_i] = receive_stream_dump(s)
            s.write("STORED\r\n")
          elsif res.start_with?('whoami')
            s.write("ROMA\r\n")
          elsif res.start_with?('rbalse')
            s.write("BYE\r\n")
            break
          else
            s.write("STORED\r\n")
          end
        end
      rescue => e
        p e
        p $ERROR_POSITION
      ensure
        s.close if s
      end
    end
  end
rescue => e
  p e
end

def receive_stream_dump(sok)
  ret = {}
  v = nil
  loop do
    context_bin = sok.read(20)
    vn, last, clk, expt, klen = context_bin.unpack('NNNNN')

    break if klen == 0 # end of dump ?
    k = sok.read(klen)
    vlen_bin = sok.read(4)
    vlen, =  vlen_bin.unpack('N')
    if vlen != 0
      v = sok.read(vlen)
    end
    ret[k] = [vn, last, clk, expt, v].pack('NNNNa*')
  end
  ret
rescue => e
  p e
end

def receive_dump(sok, len)
  dmp = ''
  while (dmp.length != len.to_i)
    dmp = dmp + sok.read(len.to_i - dmp.length)
  end
  sok.read(2)
  if sok.gets == "END\r\n"
    return Marshal.load(dmp)
  else
    return nil
  end
rescue => e
  false
end

# test of copying vnode
class CopyDataTest < Test::Unit::TestCase
  include RomaTestUtils

  def setup
    @th = Thread.new { receive_command_server }
    start_roma
    @rc = Roma::Client::RomaClient.new(%w(localhost_11211 localhost_11212))
  end

  def teardown
    stop_roma
    @th.kill
    $gs.close
    Roma::Messaging::ConPool.instance.close_all
  end

  def test_spushv
    # key wihch's vn = 0
    keys = []
    n = 1000
    n.times do |i|
      d = Digest::SHA1.hexdigest(i.to_s).hex % @rc.rttable.hbits
      vn = @rc.rttable.get_vnode_id(d)
      if vn == 0
        keys << i.to_s
      end
    end
    nid = @rc.rttable.search_nodes(0)

    push_a_vnode_stream('roma', 0, nid[0], keys)

    keys.each do |k|
      assert_equal("#{k}-stream", @rc.get(k, true))
      #      puts "#{k} #{@rc.get(k)}"
    end
  end

  def push_a_vnode_stream(hname, vn, nid, keys)
    con = Roma::Messaging::ConPool.instance.get_connection(nid)
    con.write("spushv #{hname} #{vn}\r\n")

    res = con.gets # READY\r\n or error string
    if res != "READY\r\n"
      con.close
      return res.chomp
    end

    keys.each do |k|
      v = k + '-stream'
      data = [vn, Time.now.to_i, 1, 0x7fffffff, k.length, k, v.length, v].pack("NNNNNa#{k.length}Na#{v.length}")
      con.write(data)
    end
    con.write("\0" * 20) # end of steram

    res = con.gets # STORED\r\n or error string
    Roma::Messaging::ConPool.instance.return_connection(nid, con)
    res.chomp! if res
    res
  rescue => e
    "#{e}"
  end
  private :push_a_vnode_stream

  def test_reqpushv
    make_dummy(1000)

    dat = []
    dat[0] = reqpushv('roma', 0)
    assert_not_nil(dat[0])
    dat[0] = reqpushv('roma', 0)
    assert_not_nil(dat[0])  # confirming twice access to same node

    dat[1] = reqpushv('roma', 536_870_912)
    assert_not_nil(dat[1])
    dat[2] = reqpushv('roma', 1_073_741_824)
    assert_not_nil(dat[2])
    dat[3] = reqpushv('roma', 1_610_612_736)
    assert_not_nil(dat[3])
    dat[4] = reqpushv('roma', 2_147_483_648)
    assert_not_nil(dat[4])
    dat[5] = reqpushv('roma', 2_684_354_560)
    assert_not_nil(dat[5])
    dat[6] = reqpushv('roma', 3_221_225_472, true)
    assert_not_nil(dat[6])
    dat[7] = reqpushv('roma', 3_758_096_384, true)
    assert_not_nil(dat[7])

    a = 0
    dat.each { |v| a += v.length }
    assert_equal(1000, a)
  end

  def wait(vn)
    while $dat.key?(vn)
      sleep 0.01
    end
    $dat[vn]
  end

  # set dummy data of n count
  def make_dummy(n)
    n.times do |i|
      assert(@rc.set(i.to_s, i.to_s) == 'STORED')
    end
  end

  def reqpushv(_hname, vn, is_primary = false)
    $dat.delete(vn)
    con = Roma::Messaging::ConPool.instance.get_connection('localhost_11211')
    res = nil
    10.times do
      con.write("reqpushv #{vn} localhost_#{TCP_SERVER_PORT} #{is_primary}\r\n")
      res = con.gets
      break if res == "PUSHED\r\n"
      sleep 0.5
    end
    assert_equal("PUSHED\r\n", res)
    con.close

    until $dat.key?(vn)
      Thread.pass
      sleep 0.01
    end
    $dat[vn]
  rescue => e
    p e
    p $ERROR_POSITION
    return nil
  end

  def receive_dump(sok, len)
    dmp = ''
    while (dmp.length != len.to_i)
      dmp = dmp + sok.read(len.to_i - dmp.length)
    end
    sok.read(2)
    if sok.gets == "END\r\n"
      return Marshal.load(dmp)
    else
      return nil
    end
  rescue => e
    @log.error("#{e}\n#{$ERROR_POSITION}")
    false
  end
end
