require 'sqlite3'
require './basic_storage'

module SQLite3_Ext

  def put(k,v)
    if RUBY_VERSION >= "1.9.2"
      k = k.encode("ascii-8bit") if k.encoding != Encoding::ASCII_8BIT
    end
    if self.execute("select count(*) from t_roma where key=?",k)[0][0].to_i==0
      self.execute("insert into t_roma values (?,?)",k,SQLite3::Blob.new(v))
    else
      self.execute("update t_roma set val=? where key=?",SQLite3::Blob.new(v),k)
    end
  end

  def get(k)
    if RUBY_VERSION >= "1.9.2"
      k = k.encode("ascii-8bit") if k.encoding != Encoding::ASCII_8BIT
    end
    r = self.execute("select * from t_roma where key=?",k)
    return nil if r.length==0
    r[0][1]
  end

  def out(k)
    if RUBY_VERSION >= "1.9.2"
      k = k.encode("ascii-8bit") if k.encoding != Encoding::ASCII_8BIT
    end
    return nil if get(k) == nil
    self.execute("delete from t_roma where key=?",k)
  end

  def rnum
    self.execute("select count(*) from t_roma")[0][0].to_i
  end

  def each
    self.execute("select * from t_roma"){ |r|
      yield r[0],r[1]
    }
  end

  def create_table
    sql = "create table t_roma ( " + 
      "key TEXT PRIMARY KEY," +
      "val BLOB);"
    self.execute( sql )
  end

  def tables
    sql = "SELECT name FROM " +
      "sqlite_master WHERE type='table' UNION ALL SELECT name FROM sqlite_temp_master " +
      "WHERE type='table' ORDER BY name;"
    self.execute( sql ).flatten
  end
end

def open_db(fname)
  hdb = SQLite3::Database.new(fname)
  hdb.extend(SQLite3_Ext)
  hdb.create_table if hdb.tables.length == 0
  p "opened" 
  hdb
end

def close_db(hdb)
   hdb.close
   p "closed"
end


#open
hdb1 = open_db "debug"
p "hdb1 = #{hdb1}"

#put data
for i in 1..10
  hdb1.put("key#{i}","value#{i}")
end

#check data
hdb1.each{|key, value|  p "key:value = #{key}:#{value}" }

#iterate
hdb1.each{|key, value|  hdb1.out(key) }

#re-check data
hdb1.each{|key, value|  p "key:value = #{key}:#{value}" }

#close
close_db hdb1
p "test END"

