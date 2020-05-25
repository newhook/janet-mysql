# janet-mysql
Bindings to mysql

# quick-examples

Basic usage:

```
(import mysql)
(def conn (mysql/connect {:host "127.0.0.1" :username "root"}))

(mysql/exec conn "create table users(name text, data blob);")
(mysql/exec conn "insert into users(name, data) values(?, ?);" "ac" "some" "data")
(mysql/row conn "select * from users where name = ?;" "ac")
{:name "ac" :data "some" "data"}
(mysql/all conn "select * from users")
[{:name "ac" :data "some" "data"} ...]
(mysql/val conn "select data from users where name = ?;" "ac")
@{"some" "data"}
```

Transactions:

```
(import mysql)
...
(mysql/tx conn
  (unless (mysql/val conn "select ....")
    (mysql/rollback conn))
  (mysql/val conn "select ...."))
```

# Special thanks

[Andrew Chambers](https://github.com/andrewchambers/janet-pq/) - The author of the postgres library from which this library was inspired.