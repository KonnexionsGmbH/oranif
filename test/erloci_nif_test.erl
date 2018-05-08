-module(erloci_nif_test).

-include_lib("eunit/include/eunit.hrl").
-include("test_common.hrl").

-define(TESTTABLE, "erloci_nif_simple_test_1").


-define(DROP,   <<"drop table "?TESTTABLE>>).
-define(CREATE, <<"create table "?TESTTABLE" (pkey integer,"
                  "publisher varchar2(30))">>).
-define(INSERT, <<"insert into "?TESTTABLE
                " (pkey,publisher) values ("
                ":pkey"
                ", :publisher)">>).
-define(SELECT_WITH_ROWID, <<"select "?TESTTABLE".rowid, "?TESTTABLE
                           ".* from "?TESTTABLE>>).
-define(SELECT_ROWID_ASC, <<"select rowid from "?TESTTABLE" order by pkey">>).
-define(SELECT_ROWID_DESC, <<"select rowid from "?TESTTABLE
                           " order by pkey desc">>).
-define(BIND_LIST, [ {<<":pkey">>, 'SQLT_INT'}
                 , {<<":publisher">>, 'SQLT_CHR'}
                 ]).
-define(UPDATE, <<"update "?TESTTABLE" set "
                "pkey = :pkey"
                ", publisher = :publisher"
                " where "?TESTTABLE".rowid = :pri_rowid1">>).
-define(UPDATE_BIND_LIST, [ {<<":pkey">>, 'SQLT_INT'}
                        , {<<":publisher">>, 'SQLT_CHR'}
                        ]).

%%------------------------------------------------------------------------------
%% db_test_
%%------------------------------------------------------------------------------
db_test_() ->
    {timeout, 60, {
       setup,
       fun() ->
               Conf = ?CONN_CONF,
               application:start(erloci),
               #{tns := Tns, user := User, password := Pass,
                 logging := Logging, lang := Lang} = Conf,
               Envhp = erloci_nif_drv:ociEnvNlsCreate(),
               Spoolhp = erloci_nif_drv:ociSpoolHandleCreate(Envhp),
               {ok, SpoolName} = erloci_nif_drv:ociSessionPoolCreate(Envhp, Spoolhp,
                   Tns,
                   2, 10, 1, User, Pass),
                {ok, Authhp} = erloci_nif_drv:ociAuthHandleCreate(Envhp, User, Pass),
                {ok, Svchp} = erloci_nif_drv:ociSessionGet(Envhp, Authhp, SpoolName),
                #{envhp => Envhp,
                  spoolhp => Spoolhp,
                  spool_name => SpoolName,
                  authhp => Authhp,
                  svchp => Svchp,
                  conf => Conf}
       end,
       fun(#{envhp := Envhp} = State) ->
               %DropStmt = OciSession:prep_sql(?DROP),
               %DropStmt:exec_stmt(),
               %DropStmt:close(),
               %OciSession:close(),
               %OciPort:close(),
               application:stop(erloci_nif)
       end,
       {with,
        [fun select_bind/1,
         fun column_types/1,
         fun missing_bind_error/1,
         %fun named_session/1,
         %%fun drop_create/1,
         fun bad_sql_connection_reuse/1,
         fun insert_select_update/1,
         %fun auto_rollback/1,
         %fun commit_rollback/1,
         %fun asc_desc/1,
         %fun lob/1,
         %fun describe/1,
         %fun function/1,
         %fun procedure_scalar/1,
         %fun procedure_cur/1,
         %fun timestamp_interval_datatypes/1,
         %fun stmt_reuse_onerror/1,
         %fun multiple_bind_reuse/1,
         %fun check_ping/1,
         %fun check_session_without_ping/1,
         %fun check_session_with_ping/1,
         %fun urowid/1
        fun select_null/1
        ]}
      }}.

select_null(#{envhp := Envhp, svchp := Svchp}) ->
    ?ELog("+---------------------------------------------+"),
    ?ELog("|            select_bind                      |"),
    ?ELog("+---------------------------------------------+"),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, <<"select * from numbers">>)),
    ?assertMatch({ok, _}, erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT'))),
    {ok, Rows} = erloci_nif_drv:ociStmtFetch(Stmthp, 1),
    ?ELog("ROWS: ~p", [Rows]).


select_bind(#{envhp := Envhp, svchp := Svchp}) ->
    ?ELog("+---------------------------------------------+"),
    ?ELog("|            select_bind                      |"),
    ?ELog("+---------------------------------------------+"),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A or dummy = :B">>)),
    {ok, BindVars1} = erloci_nif_drv:ociBindByName(Stmthp, #{}, <<"A">>, erloci_nif_drv:sql_type('SQLT_CHR'), <<"X">>),
    {ok, BindVars2} = erloci_nif_drv:ociBindByName(Stmthp, BindVars1, <<"B">>, erloci_nif_drv:sql_type('SQLT_CHR'), <<"Y">>),
    ?assertMatch({ok, #{cols := [{<<"DUMMY">>,1,1,0,0}]}}, erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, BindVars2, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT'))),
    ?assertMatch({ok, _Rows}, erloci_nif_drv:ociStmtFetch(Stmthp, 1)).


column_types(#{envhp := Envhp, svchp := Svchp}) ->
    ?ELog("+---------------------------------------------+"),
    ?ELog("|            column_types                     |"),
    ?ELog("+---------------------------------------------+"),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, <<"select * from testtable">>)),
    ?assertMatch({ok, _}, erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT'))),
    ?assertMatch({ok, _}, {ok, _Rows} = erloci_nif_drv:ociStmtFetch(Stmthp, 1)).

missing_bind_error(#{envhp := Envhp, svchp := Svchp}) ->
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A">>)),
    ?assertMatch({error, {1008, _}}, erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT'))).

bad_sql_connection_reuse(#{envhp := Envhp, svchp := Svchp}) ->
        ?ELog("+---------------------------------------------+"),
        ?ELog("|           bad_sql_connection_reuse          |"),
        ?ELog("+---------------------------------------------+"),
        {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
        BadSelect = <<"select 'abc from dual">>,
        ?assertMatch({error, {1756, _}}, erloci_nif_drv:ociStmtPrepare(Stmthp, BadSelect)),
        GoodSelect = <<"select 'abc' from dual">>,
        ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, GoodSelect)).
        %%?assertMatch({cols, [{<<"'ABC'">>,'SQLT_AFC',_,0,0}]}, SelStmt:exec_stmt()),
        %%?assertEqual({{rows, [[<<"abc">>]]}, true}, SelStmt:fetch_rows(2)),
        %%?assertEqual(ok, SelStmt:close()).

insert_select_update(#{envhp := Envhp, svchp := Svchp} = Sess) ->
    ?ELog("+---------------------------------------------+"),
    ?ELog("|            insert_select_update             |"),
    ?ELog("+---------------------------------------------+"),
    RowCount = 6,
    flush_table(Sess).

flush_table(#{envhp := Envhp, svchp := Svchp}) ->
    ?ELog("creating (drop if exists) table ~s", [?TESTTABLE]),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ok = erloci_nif_drv:ociStmtPrepare(Stmthp, ?DROP),
    case erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, #{}, 1, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT')) of
        {ok, _} -> ok;
        {error,{942,_}} -> ok;
        Else -> exit(Else)
    end,
    ok = erloci_nif_drv:ociStmtPrepare(Stmthp, ?CREATE).

