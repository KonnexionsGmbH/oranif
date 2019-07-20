# oranif
[![Build Status](https://travis-ci.org/K2InformaticsGmbH/oranif.svg?branch=master)](https://travis-ci.org/K2InformaticsGmbH/oranif)
[![Coverage Status](https://coveralls.io/repos/github/K2InformaticsGmbH/oranif/badge.svg?branch=master)](https://coveralls.io/github/K2InformaticsGmbH/oranif?branch=master)
![GitHub](https://img.shields.io/github/license/K2InformaticsGmbH/oranif.svg)

[Oracle Database Programming Interface for C (ODPI-C)](https://oracle.github.io/odpi/) driver using dirty NIFs. **Requires Erlang/OTP 20 or later with full dirty nif support.**

## Development
Currently builds in Window, Linux and OS X

## Compile (as OSs)

```sh
rebar3 compile
ORANIF_DEBUG=_verbosity_ rebar3 compile # debug log verbosity >= 1
# see dpi_nif.h for ORANIF_DEBUG values and debug log granularities
```

### OSX/Linux

- Requires Oracle Client library installed, see https://oracle.github.io/odpi/doc/installation.html for installation instructions.
- For OSX use `basic` (only `basic-lite` didn't work in our tests).
- Requires a C compiler supporting the c11 standard.
- code coverage
```sh
gcov -o ./ c_src/*.c
```

#### Create Environment variables
```
OTP_ERTS_DIR       = path to erlang run time system
ERL_INTERFACE_DIR  = path to erl_interface or erlang installation
```
Example `.bashrc` snippet:
```sh
...
export OTP_ERTS_DIR=$(find /usr/lib/erlang/ -maxdepth 1 -type d -name erts-*)
export ERL_INTERFACE_DIR=$(find /usr/lib/erlang/lib/ -maxdepth 1 -type d -name erl_interface-*)
...
```

## Usage
`oranif` tries to be as _thin_ as ODPI-C is to OCI. Here are some design notes on how is that attempted:
* ODPI API are named in the pattern of `dpiClass_FUNCTION` which is tranlated to `dpi:class_FUNCTION`. For example:

ODPI-C API|ORANIF
---|---
[`dpiContext_create`](https://oracle.github.io/odpi/doc/functions/dpiContext.html#c.dpiContext_create)|`dpi:context_create`
[`dpiConn_create`](https://oracle.github.io/odpi/doc/functions/dpiConn.html#c.dpiConn_create)|`dpi:conn_create`
[`dpiConn_prepareStmt`](https://oracle.github.io/odpi/doc/functions/dpiConn.html#c.dpiConn_prepareStmt)|`dpi:conn_prepareStmt`
[`dpiStmt_execute`](https://oracle.github.io/odpi/doc/functions/dpiStmt.html#c.dpiStmt_execute)|`dpi:stmt_execute`
**Hence there isn't any need to maintain API documentation in `oranif` simultaneously!** Please refer to [ODPI-C](https://oracle.github.io/odpi/doc/) for API reference.

* Data type mapping
    - all numeric C datatypes are mapped to erlang equivalents (`int`, `unsigned int` etc to `integer()`, ...)
    - strings (`char *`, `const char*`) in input are erlang `binary()`
    - the length of **non** NULL terminated strings are implicitly determined from the corresponding `binary()` argument. All length-of-previous-string type parameters in ODPI-C thus is omitted from interface.
    - all `[OUT]` parameters are returned from function call
    - multiple `[OUT]` parameters are returned as erlang maps where keys are the original parameter name (atom) as in ODPI-C documantation
    - enumerations are passed as atoms [example STMT enum use](https://github.com/K2InformaticsGmbH/oranif/blob/6d581f3793715d7d4563c0e778177f7c1e7b4272/test/oranif_eunit.erl#L731-L745)
    ```erlang
    {'DPI_STMT_TYPE_UNKNOWN', <<"another one bites the dust">>},
    {'DPI_STMT_TYPE_SELECT', <<"select 2 from dual">>},
    {'DPI_STMT_TYPE_UPDATE', <<"update a set b = 5 where c = 3">>},
    {'DPI_STMT_TYPE_DELETE', <<"delete from a where b = 5">>},
    {'DPI_STMT_TYPE_INSERT', <<"insert into a (b) values (5)">>},
    {'DPI_STMT_TYPE_CREATE', <<"create table a (b int)">>},
    {'DPI_STMT_TYPE_DROP', <<"drop table students">>},
    {'DPI_STMT_TYPE_ALTER', <<"alter table a add b int">>},
    {'DPI_STMT_TYPE_BEGIN', <<"begin null end">>},
    {'DPI_STMT_TYPE_DECLARE', <<"declare mambo number(5)">>},
    {'DPI_STMT_TYPE_CALL', <<"call a.b(c)">>},
    {'DPI_STMT_TYPE_MERGE', <<"MERGE INTO a USING b ON (1 = 1)">>},
    {'DPI_STMT_TYPE_EXPLAIN_PLAN', <<"EXPLAIN">>},
    {'DPI_STMT_TYPE_COMMIT', <<"commit">>},
    {'DPI_STMT_TYPE_ROLLBACK', <<"rollback">>}
    ```
    - odpi `struct`s are maps (with keys from the original `struct` member name from ODPI-C documentation). For example [dpiConn_create](https://oracle.github.io/odpi/doc/functions/dpiConn.html#c.dpiConn_create) is called as:
    `dpi:conn_create, (Context, User, Password, Tns, #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{})` where parameter `commonParams` of type [`dpicommoncreateparams`](https://oracle.github.io/odpi/doc/structs/dpiCommonCreateParams.html#dpicommoncreateparams) is passed as a map of `#{struct_field => value}`

### Usage Example (TBD)
```erlang

```

## Testing
There are some eunit tests which can be executed through `rebar3 do clean, compile, eunit` (Oracle Server connect info **MUST** be supplied through `tests/connect.config` first).

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
C:\> sqlplus scott/tiger@host:1521/xe
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
