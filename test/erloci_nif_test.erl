-module(erloci_nif_test).

-include_lib("eunit/include/eunit.hrl").
-include("test_common.hrl").


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
        fun column_types/1
         %fun named_session/1,
         %%fun drop_create/1,
         %fun bad_sql_connection_reuse/1,
         %fun insert_select_update/1,
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
        ]}
      }}.

select_bind(#{envhp := Envhp, svchp := Svchp}) ->
    ?ELog("+---------------------------------------------+"),
    ?ELog("|            select_bind                      |"),
    ?ELog("+---------------------------------------------+"),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A or dummy = :B">>)),
    {ok, Stmthp1} = erloci_nif_drv:ociBindByName(Stmthp, <<"A">>, 0, erloci_nif_drv:sql_type('SQLT_CHR'), <<"X">>),
    {ok, Stmthp2} = erloci_nif_drv:ociBindByName(Stmthp1, <<"B">>, 0, erloci_nif_drv:sql_type('SQLT_CHR'), <<"Y">>),
    ?assertMatch(ok, erloci_nif_drv:ociStmtExecute(Svchp, Stmthp2, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT'))),
    ?assertMatch({ok, _}, {ok, _Rows} = erloci_nif_drv:ociStmtFetch(Stmthp2, 1)).


column_types(#{envhp := Envhp, svchp := Svchp}) ->
    ?ELog("+---------------------------------------------+"),
    ?ELog("|            column_types                     |"),
    ?ELog("+---------------------------------------------+"),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci_nif_drv:ociStmtPrepare(Stmthp, <<"select * from testtable">>)),
    ?assertMatch(ok, erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT'))),
    ?assertMatch({ok, _}, {ok, _Rows} = erloci_nif_drv:ociStmtFetch(Stmthp, 1)).