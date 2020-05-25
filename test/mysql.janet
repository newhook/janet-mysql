#!/usr/bin/env janet
(import "mysql")
(defn connect [] (mysql/connect {:host "127.0.0.1" :username "root"}))

(defn cleanup []
  (def conn (connect))
(print "drop")
   (mysql/exec conn "drop database if exists janet_tests")
(print "create")
   (mysql/exec conn "create database if not exists janet_tests")
)

(var tests nil)
(defn run-tests
  []
  # Cleanup any previous runs that didn't have time to clean up.
  (cleanup)

  #(set pg-data-dir 
    #(string (sh/$$_ ["mktemp" "-d" "/tmp/janet-pq-test.tmp.XXXXX"])))
  #(print "Launching postgres in " pg-data-dir "...")

  #(sh/$ ["pg_ctl" "-s" "-D" pg-data-dir "initdb" "-o" "--auth=trust"])
  #(sh/$ ["pg_ctl" "-s" "-w" "-D" pg-data-dir  "start" "-l" (string pg-data-dir "/test-log-file.txt")])

      (tests))
  #(defer (cleanup)
      #(tests)))
      
(set tests (fn []
  (print "running tests...")
  (def conn (connect))
  (mysql/select-db conn "janet_tests")

  (defn check-test
    [test-case v]
    (def expected (get test-case :expected (get test-case :val)))
    (unless (deep= v expected)
      (error
        (with-dyns [:out (buffer/new 0)]
          (print "expected:")
          (print (type expected))
          (pp expected)
          (print "got:")
          (print (type v))
          (pp v)
          (string (dyn :out))))))
  
  (defn exec-test
    [test-case]
    (mysql/exec conn (string "create table roundtrip (a text, b " (test-case :coltype) ");"))
    (mysql/exec conn "insert into roundtrip(a, b) values(?, ?);" "t" (test-case :val))
    (def v (mysql/val conn "select b from roundtrip where a = ?;" "t"))
    (check-test test-case v)
    (mysql/exec conn "drop table roundtrip;"))

  (defn stmt-test
    [test-case]
    (mysql/exec conn (string "create table roundtrip (a text, b " (test-case :coltype) ");"))
    (def insert (mysql/prepare conn "insert into roundtrip(a, b) values(?, ?);"))
    (def select (mysql/prepare conn "select b from roundtrip where a = ?;"))
    (mysql/exec insert "t" (test-case :val))
    (def v (mysql/stmt-val select "t"))
    (check-test test-case v)
    #(mysql/exec conn "insert into roundtrip(a, b) values(?, ?);" "t" (test-case :val))
    #(def v (mysql/val conn "select b from roundtrip where a = ?;" "t"))
    (mysql/stmt-close insert)
    (mysql/stmt-close select)
    (mysql/exec conn "drop table roundtrip;"))

  (defn round-trip-test
    [test-case]
    (stmt-test test-case)
    (exec-test test-case))
    
  (print "boolean")
  # nil
  (round-trip-test {:coltype "boolean"})
  # booleans
  (round-trip-test {:coltype "boolean" :val true})
  (round-trip-test {:coltype "boolean" :val false})

  (print "tinyint")
  # tinyint -128 to +127
  (round-trip-test {:coltype "tinyint" :val -128})
  (round-trip-test {:coltype "tinyint" :val 127})
  (round-trip-test {:coltype "tinyint" :val 0})

  (print "smallint")
  # smallint -32768 to +32767
  (round-trip-test {:coltype "smallint" :val -32768})
  (round-trip-test {:coltype "smallint" :val 32767})
  (round-trip-test {:coltype "smallint" :val 0})

  (print "integer")
  # integer -2147483648 to +2147483647
  (round-trip-test {:coltype "integer" :val -2147483648})
  (round-trip-test {:coltype "integer" :val 2147483647})
  (round-trip-test {:coltype "integer" :val 0})
   # bigint    -9223372036854775808 to +9223372036854775807
  #(round-trip-test {:coltype "bigint" :val (int/s64 "-9223372036854775808")})
  #(round-trip-test {:coltype "bigint" :val (int/s64 "9223372036854775807")})

  (print "serial")
  # serial 1 to 2147483647
  (round-trip-test {:coltype "serial" :val 1}) 
  (round-trip-test {:coltype "serial" :val 2147483647})

  (print "real")
  # real
  (round-trip-test {:coltype "real" :val 1})
  (round-trip-test {:coltype "real" :val -1})
  (round-trip-test {:coltype "real" :val 1.123})

  (print "double")
  # double precision
  (round-trip-test {:coltype "double precision" :val 1})
  (round-trip-test {:coltype "double precision" :val -1})
  (round-trip-test {:coltype "double precision" :val 1.123})

  (print "text")
  # text
  (round-trip-test {:coltype "tinytext" :val "hello"})
  (round-trip-test {:coltype "tinytext" :val "😀😀😀"})
  (round-trip-test {:coltype "text" :val "hello"})
  (round-trip-test {:coltype "text" :val "😀😀😀"})
  (round-trip-test {:coltype "mediumtext" :val "hello"})
  (round-trip-test {:coltype "mediumtext" :val "😀😀😀"})
  (round-trip-test {:coltype "longtext" :val "hello"})
  (round-trip-test {:coltype "longtext" :val "😀😀😀"})
  # varchar
  (round-trip-test {:coltype "varchar(128)" :val "hello"})
  (round-trip-test {:coltype "varchar(128)" :val "😀😀😀"})
  # char(n)
  (round-trip-test {:coltype "char(8)" :val "hello" :expected "hello"})
  # blob
  (round-trip-test {:coltype "blob" :val "hello"})
  (round-trip-test {:coltype "blob" :val "😀😀😀"})
  (round-trip-test {:coltype "longblob" :val "hello"})
  (round-trip-test {:coltype "longblob" :val "😀😀😀"})

  (print "dates")
  (mysql/exec conn "SET @@time_zone = '+00:00'")
  (round-trip-test {:coltype "datetime" :val "2020-01-02 04:05:07" :expected {:month 1 :tz 0 :seconds 7 :minutes 5 :year 2020 :day 2 :hours 4 :microseconds 0}})
  (round-trip-test {:coltype "date" :val "2020-01-02" :expected {:month 1 :year 2020 :day 2}})
  (round-trip-test {:coltype "time" :val "04:05:07" :expected { :seconds 7 :minutes 5 :hours 4}})
  (round-trip-test {:coltype "year" :val "2020" :expected 2020})

  # basic commit
  (print "transactions")
  (mysql/exec conn "create table t(a text);")
  (assert 
    (=
      1
      (mysql/txn conn {}
        (mysql/exec conn "insert into t(a) values('aaa')")
        (mysql/val conn "select count(*) from t;"))))
  (assert (= 1 (mysql/val conn "select count(*) from t;")))

  (protect 
    (mysql/txn conn {}
      (mysql/exec conn "insert into t(a) values('aaa')")
      (error "fudge")))

  (assert (= 1 (mysql/val conn "select count(*) from t;")))

  (if false (do
  (mysql/exec conn "create table big_blob(a longblob);")
  # 10 rows each from 1mb to 10mb.
  (for i 0 10
      (do
        (def buf (buffer/new-filled (* 100000 (+ i 1)) 97))
        (mysql/exec conn "insert into big_blob(a) values(?)" buf)
      ))
  (mysql/all conn "select * from big_blob;")
  ))
))

(run-tests)