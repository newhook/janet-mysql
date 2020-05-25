(declare-project
  :name "mysql"
  :author "Matthew Newhook"
  :license "MIT"
  :url "https://github.com/newhook/janet-mysql"
  :repo "git+https://github.com/newhook/janet-mysql.git")


# XXX We should use the shlex module, or make a pkg-config module once post-deps is in a release.
(def- shlike-grammar ~{
  :ws (set " \t\r\n")
  :escape (* "\\" (capture 1))
  :dq-string (accumulate (* "\"" (any (+ :escape (if-not "\"" (capture 1)))) "\""))
  :sq-string (accumulate (* "'" (any (if-not "'" (capture 1))) "'"))
  :token-char (+ :escape (* (not :ws) (capture 1)))
  :token (accumulate (some :token-char))
  :value (* (any (+ :ws)) (+ :dq-string :sq-string :token) (any :ws))
  :main (any :value)
})

(def- peg (peg/compile shlike-grammar))

(defn shsplit
  "Split a string into 'sh like' tokens, returns
   nil if unable to parse the string."
  [s]
  (peg/match peg s))

(defn pkg-config [what]
  (def f (file/popen (string "pkg-config " what)))
  (def v (->>
           (file/read f :all)
           (string/trim)
           shsplit))
  (unless (zero? (file/close f))
    (error "pkg-config failed!"))
  v)

(declare-source
    :source ["mysql.janet"])

(declare-native
    :name "_mysql"
    :lflags (pkg-config "mysqlclient --libs")
    :source ["mysql.c"])
