# ocinif
Oracle Call Interface driver using dirty NIFs

Requires Erlang/OTP 20 or later with full dirty nif support.

All remote oci operations are run within the pool of ERL_NIF_DIRTY_JOB_IO_BOUND threads

Status: Early prototype

Todo:

- [ ] Tests

## Development
Currently only builds in windows
### Windows
```sh
rebar3 compile
```

## DB Init SQL (XE)
```cmd
C:\> sqlplus system
```
```sql
EXEC DBMS_XDB.SETLISTENERLOCALACCESS(FALSE);
create user scott identified by tiger;
alter session set "_ORACLE_SCRIPT"=true; -- if 'create user scott...' results into ORA-65096

grant alter system to scott;
grant create session to scott;
grant unlimited tablespace to scott;
grant create table to scott;
grant create cluster to scott;
grant create view to scott;
grant create sequence to scott;
grant create procedure to scott;
grant create trigger to scott;
grant create any directory to scott;
grant drop any directory to scott;
grant create type to scott;
grant create operator to scott;
grant create indextype to scott;

exit
```
```cmd
C:\> sqlplus scott/tiger@192.168.1.49:1521/xe
```
```sql
select * from session_privs;
```
## Increase amount of processes, sessions and transactions

Extensive database usage may exhaust the connection limits that have defaults around 100. If that happens, any attempts to establish further connections may fail, resulting in an error such as "ORA-12516: TNS:listener could not find available handler with matching protocol stack". To prevent this, the limit should be raised so the database can establish more simultaneous connections.

To do this, log into the database as SYSDBA. Normal users aren't privileged enough.

```
sqlplus sys as sysdba
```

Then, set the new limits:

```
alter system set processes=1000 scope=spfile
alter system set sessions=1000 scope=spfile
alter system set transactions=1000 scope=spfile
```

In this example, the limits are set to 1000. The actual values might be even higher because there are constraints regarding the size of those numbers. For instance, "sessions" might be set to 1524 instead.

Then, the database must be restarted for this change to take effect:

```
Windows Menu --> Oracle Database 11g Express Edition --> Stop database
```

This takes a while, as indicated by the console window that opens. When it is done, start the database:

```
Windows Menu --> Oracle Database 11g Express Edition --> Start database
```
Verify that the change took effect using sqlplus:

```
show parameter sessions 
```

A result similar to this should be printed:

```
NAME                                 TYPE        VALUE
------------------------------------ ----------- ------------------------------
java_max_sessionspace_size           integer     0
java_soft_sessionspace_limit         integer     0
license_max_sessions                 integer     0
license_sessions_warning             integer     0
sessions                             integer     1524
shared_server_sessions               integer
```
