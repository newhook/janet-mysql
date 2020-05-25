(import _mysql)

(defn connect
  "Open a new connection to the database.\n\n
   
   The parameters are a struct containing the following:
   :host
   :username
   :password
   :database"
[params] (_mysql/connect params))

(defn begin
  [conn] (_mysql/begin conn))

(defn raw-commit
  [conn] (_mysql/commit conn))

(defn raw-rollback
  [conn] (_mysql/rollback conn))

(def raw-exec _mysql/exec)
(def raw-select _mysql/select)

(def stmt-close _mysql/stmt-close)

(defn prepare
  "Prepare a statement for conn.\n\n
   
   If the result is an error, it is thrown.

  Params can be nil|boolean|string|buffer|number|u64|s64."
  [conn query]
  (_mysql/prepare conn query))

(defn exec
  "Execute a query against conn.\n\n
   
   If the result is an error, it is thrown.

   Params can be nil|boolean|string|buffer|number|u64|s64."
  [conn query & params]
  (_mysql/exec conn query ;params))

(defn select
  "Execute a query against conn.\n\n
   
   If the result is an error, it is thrown.

   Params can be nil|boolean|string|buffer|number|u64|s64."
  [conn query & params]
  (_mysql/select conn query ;params))

(defn all
  "Return all results from a query."
  [conn query & params]
  (def rows (_mysql/select conn query ;params))
  (_mysql/rows-unpack rows))

(defn row
  "Run a query like exec, returning the first result"
  [conn query & params]
  (if-let [rows (all conn query ;params)]
    (if (empty? rows)
      nil
      (first rows))))

(defn col
  "Run a query that returns a single column with many rows
   and return an array with all columns unpacked"
  [conn query & params]
  (map |(first (values $)) (all conn query ;params)))

(defn val
  "Run a query returning a single value and return that value or nil."
  [conn query & params]
  (if-let [r (row conn query ;params)
           v (values r)]
    (when (not (empty? v))
      (first v))))

(defn stmt-all
  "Return all results from a query."
  [stmt & params]
  (def rows (_mysql/select stmt ;params))
  (_mysql/rows-unpack rows))

(defn stmt-row
  "Run a query like select, returning the first result"
  [stmt & params]
  (if-let [rows (stmt-all stmt ;params)]
    (if (empty? rows)
      nil
      (first rows))))

(defn stmt-val
  "Run a query returning a single value and return that value or nil."
  [stmt & params]
  (if-let [r (stmt-row stmt ;params)
           v (values r)]
    (when (not (empty? v))
      (first v))))

(def status _mysql/status)

(def close _mysql/close)

(def result-insert-id _mysql/result-insert-id)
(def result-affected-rows _mysql/result-affected-rows)

(def rows-columns _mysql/rows-columns)
(def rows-column-types _mysql/rows-column-types)
(def rows-unpack _mysql/rows-unpack)
(defn in-transaction? [conn] (_mysql/in-transaction))

(defn select-db [conn db] (_mysql/select-db conn db))

(defn rollback
  [conn &opt v]
  (signal 0 [conn [:rollback v]]))
#
(defn commit
  [conn &opt v]
  (signal 0 [conn [:commit v]]))

(defn txn*
  "function form of txn"
  [conn options ftx]
  (def retry (get options :retry false))
  (def mode (get options :mode ""))
  (try
    (do
      (begin conn)
      (def fb (fiber/new ftx :i0123))
      (def v (resume fb))
      (match [v (fiber/status fb)]
        [v :dead]
          (do
            (raw-commit conn)
            v)
        ([[c [action v]] :user0] (= c conn))
          (do 
            (case action
              :commit
                (raw-commit conn)
              :rollback
                (raw-rollback conn)
              (error "misuse of txn*"))
            v)
        (do
          (raw-rollback conn)
          (propagate v fb))))
  ([err f]
      (do
        (raw-rollback conn)
      (propagate err f)))))

(defmacro txn
  `
    Run body in an sql transaction with options.
    
    NOTE: options are not currently supported.

    Valid option table entries are:

    The transaction is rolled back when:

    - An error is raised.
    - If mysql/rollback is called.

    Returns the last form or the value of any inner calls to rollback.

    Examples:

      (pq/txn conn {} ...)
      (pq/txn conn (pq/rollback :foo))
  `
  [conn options & body]
  ~(,txn* ,conn ,options (fn [] ,(tuple 'do ;body))))
