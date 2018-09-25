# erloci_nif
Oracle OCI driver using dirty NIFs

Requires Erlang/OTP 20 or later with full dirty nif support.

All remote oci operations are run within the pool of ERL_NIF_DIRTY_JOB_IO_BOUND threads

Status: Early prototype

Todo:

- [ ] Tidy up error handling
- [ ] a bunch more OCI operations
- [ ] convert row values to Erlang datatypes where it makes sense (optionally?)
- [ ] cursor support
- [ ] custom datatype support
- [ ] lob support
- [ ] implement cleanup in the destructors for the resource type handles
- [ ] explicit close support to supplement GC driven closes
- [ ] Test Suite
- [ ] Nice front end API
- [ ] much more

Open Questions
- Should we pass bind handles back to Erlang or leave them inside the driver owned by the statement?
    A: Decided to keep them in a map of managed pointers owned by Erlang
- If we keep the bind handles maybe makes sense to also open up the OCIDefine API and drive it from Erlang.
  Now we have a huge ociStmtExecute function that does all the OCIDefine stuff in the driver

## Development
### Windows
```bat
INSTANT_CLIENT_INCLUDE_PATH=path_to_instant_client_installation\sdk\include
INSTANT_CLIENT_LIB_PATH=path_to_instant_client_installation\sdk\lib\msvc
```

# DB Init SQL (XE)
```cmd
C:\> sqlplus system
```
```sql
EXEC DBMS_XDB.SETLISTENERLOCALACCESS(FALSE);
create user scott identified by tiger;

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
