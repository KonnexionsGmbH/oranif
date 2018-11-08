-module(erloci_test).

-include_lib("eunit/include/eunit.hrl").
-include("test_common.hrl").
-include_lib("oci.hrl").

-define(TESTTABLE, "erloci_eunit_test").

-define(DROP, <<"drop table "?TESTTABLE>>).

-define(CREATE,
    <<"create table "?TESTTABLE
      " (pkey integer, publisher varchar2(30), rank float, hero binary_double,"
       " reality raw(10), votes number(1,-10), createdate date default sysdate,"
       " chapters binary_float, votes_first_rank number)">>
).
-define(INSERT,
    <<"insert into "?TESTTABLE" (pkey, publisher, rank, hero, reality, votes,"
                               " createdate, chapters, votes_first_rank) "
      "values (:pkey, :publisher, :rank, :hero, :reality, :votes, :createdate,"
             " :chapters, :votes_first_rank)">>).

-define(SELECT_WITH_ROWID,
    <<"select "?TESTTABLE".rowid, "?TESTTABLE".* from "?TESTTABLE>>
).
-define(SELECT_ROWID_ASC, <<"select rowid from "?TESTTABLE" order by pkey">>).
-define(SELECT_ROWID_DESC,
    <<"select rowid from "?TESTTABLE" order by pkey desc">>
).
-define(BIND_LIST, [{<<":pkey">>, ?SQLT_INT}, {<<":publisher">>, ?SQLT_CHR}]).

-define(UPDATE,
    <<"update "?TESTTABLE" set pkey = :pkey, publisher = :publisher"
      " where "?TESTTABLE".rowid = :pri_rowid1">>
).
-define(UPDATE_BIND_LIST,
    [{<<":pkey">>, ?SQLT_INT}, {<<":publisher">>, ?SQLT_CHR}]
).

-define(SELECT_ROW_COUNT, <<"select count(*) from "?TESTTABLE>>).
-define(SELECT_ALL_ROWS, <<"select * from "?TESTTABLE>>).

% ------------------------------------------------------------------------------
% db_negative_test_
% ------------------------------------------------------------------------------
db_negative_test_() ->
    {timeout, 60, {
        setup, fun setup/0, fun teardown/1,
        {with, [
            fun bad_password/1,
            fun session_ping/1
        ]}
    }}.

bad_password(#{envhp := Envhp, spoolhp := Spoolhp,
               conf := #{user := User, password := Pass}}) ->
    {ok, Authhp} = erloci:ociAuthHandleCreate(Envhp, User, list_to_binary([Pass,"_bad"])),
    Res = erloci:ociSessionGet(Envhp, Authhp, Spoolhp),
    case Res of
        {ok, Svchp} -> ok = erloci:ociSessionRelease(Svchp);
        _ -> ok
    end,
    ok = erloci:ociAuthHandleFree(Authhp),
    ?assertMatch({error, {1017,_}}, Res).

session_ping(#{envhp := Envhp, spoolhp := Spoolhp,
               conf := #{user := User, password := Pass}}) ->
    {ok, Authhp} = erloci:ociAuthHandleCreate(Envhp, User, Pass),
    {ok, Svchp} =  erloci:ociSessionGet(Envhp, Authhp, Spoolhp),
    ?assertEqual(pong, erloci:ociPing(Svchp)),
    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci:ociStmtPrepare(Stmthp, <<"select * from dual">>)),
    ?assertEqual(pong, erloci:ociPing(Svchp)),
    ?assertMatch(
        {ok, #{cols := [{<<"DUMMY">>,?SQLT_CHR,_,_,0}]}},
        erloci:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, ?OCI_DEFAULT)
    ),
    ?assertEqual(pong, erloci:ociPing(Svchp)),
    ?assertEqual({ok, [[<<"X">>]], true}, erloci:ociStmtFetch(Stmthp, 2)),
    ?assertEqual(pong, erloci:ociPing(Svchp)),
    ok = erloci:ociStmtHandleFree(Stmthp),
    ok = erloci:ociSessionRelease(Svchp).

%%------------------------------------------------------------------------------
%% db_test_
%%------------------------------------------------------------------------------
db_test_() ->
    {timeout, 60, {
        setup, fun setup_session/0, fun teardown_session/1,
       {with,
        [fun nls_get_info/1,
         fun set_binary_attr/1,
         fun select_bind/1,
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
         %fun urowid/1,
         fun select_null/1
        ]}
      }}.

nls_get_info(#{envhp := Envhp}) ->
    ?assertMatch({ok, <<"montag">>}, erloci:ociNlsGetInfo(Envhp, ?OCI_NLS_DAYNAME1)),
    CharSetInfo = erloci:ociCharsetAttrGet(Envhp),
    ?assertMatch({ok, {_, _}}, CharSetInfo),
    {ok, {CharsetId, NCharsetId}} = CharSetInfo,
    ?assertEqual({ok, <<"AL32UTF8">>}, erloci:ociNlsCharSetIdToName(Envhp, CharsetId)),
    ?assertEqual({ok, <<"AL32UTF8">>}, erloci:ociNlsCharSetIdToName(Envhp, NCharsetId)).

set_binary_attr(#{envhp := Envhp}) ->
    ?assertMatch(ok, erloci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, <<"GERMAN">>,?OCI_ATTR_ENV_NLS_LANGUAGE)).

select_null(#{envhp := Envhp, svchp := Svchp}) ->
    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci:ociStmtPrepare(Stmthp, <<"select * from numbers">>)),
    ?assertMatch({ok, #{statement := ?OCI_STMT_SELECT}},  erloci:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, ?OCI_DEFAULT)),
    ?assertMatch({ok, _,_},  erloci:ociStmtFetch(Stmthp, 1)).

select_bind(#{envhp := Envhp, svchp := Svchp}) ->
    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A or dummy = :B">>)),
    {ok, BindVars1} = erloci:ociBindByName(Stmthp, #{}, <<"A">>,  ?SQLT_CHR, <<"X">>),
    {ok, BindVars2} = erloci:ociBindByName(Stmthp, BindVars1, <<"B">>,  ?SQLT_CHR, <<"Y">>),
    ?assertMatch({ok, #{statement := ?OCI_STMT_SELECT, cols := [{<<"DUMMY">>,?SQLT_CHR,1,_,0}]}},  erloci:ociStmtExecute(Svchp, Stmthp, BindVars2, 0, 0, ?OCI_DEFAULT)),
    ?assertMatch({ok, [[<<"X">>]], _}, erloci:ociStmtFetch(Stmthp, 1)).

column_types(#{envhp := Envhp, svchp := Svchp}) ->
    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci:ociStmtPrepare(Stmthp, <<"select * from testtable">>)),
    ?assertMatch({ok, _}, erloci:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, ?OCI_DEFAULT)),
    ?assertMatch({ok, _,_}, erloci:ociStmtFetch(Stmthp, 1)).

missing_bind_error(#{envhp := Envhp, svchp := Svchp}) ->
    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ?assertMatch(ok, erloci:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A">>)),
    ?assertMatch({error, {1008, _}},  erloci:ociStmtExecute(Svchp, Stmthp, #{}, 0, 0, ?OCI_DEFAULT)).

bad_sql_connection_reuse(#{envhp := Envhp}) ->
        {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
        BadSelect = <<"select 'abc from dual">>,
        ?assertMatch({error, {1756, _}},  erloci:ociStmtPrepare(Stmthp, BadSelect)),
        GoodSelect = <<"select 'abc' from dual">>,
        ?assertMatch(ok, erloci:ociStmtPrepare(Stmthp, GoodSelect)),
        %%?assertMatch({cols, [{<<"'ABC'">>,?SQLT_AFC,_,0,0}]}, SelStmt:exec_stmt()),
        %%?assertEqual({{rows, [[<<"abc">>]]}, true}, SelStmt:fetch_rows(2)),
        ?assertEqual(ok, erloci:ociStmtHandleFree(Stmthp)).

insert_select_update(#{envhp := Envhp, svchp := Svchp} = Sess) ->
    RowCount = 6,
    flush_table(Sess),

    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ok =  erloci:ociStmtPrepare(Stmthp, ?INSERT),
    lists:foreach(fun(I) ->
        Vars = [{<<"pkey">>,  ?SQLT_INT, I},
                {<<"publisher">>, ?SQLT_CHR,
                            unicode:characters_to_binary(["_püèr_",integer_to_list(I),"_"])},
                {<<"rank">>, ?SQLT_FLT, I+I/2},
                {<<"hero">>, ?SQLT_IBDOUBLE, 9.9945}, %1.0e-307},
                {<<"reality">>, ?SQLT_BIN, list_to_binary([rand:uniform(255) || _I <- lists:seq(1,rand:uniform(5)+5)])},
                {<<"votes">>, ?SQLT_INT, 27},
                {<<"createdate">>, ?SQLT_DAT, oci_util:edatetime_to_ora(os:timestamp())},
                {<<"chapters">>, ?SQLT_IBFLOAT, 9.9945},% 9.999999350456404e-39},
                {<<"votes_first_rank">>, ?SQLT_INT, I}],
        BindVars = lists:foldl(fun({Name, Type, Value}, Acc) ->
                                {ok, BV} = erloci:ociBindByName(Stmthp, Acc, Name, Type, Value),
                                BV
                            end, #{}, Vars),
        Res = erloci:ociStmtExecute(Svchp, Stmthp, BindVars, 1, 0, ?OCI_COMMIT_ON_SUCCESS),
        % io:format("RES: ~p\r\n", [Res]),
        ?assertMatch({ok, _}, Res)
    end, lists:seq(1, RowCount)),

    {ok, Stmthp2} = erloci:ociStmtHandleCreate(Envhp),
    ok =  erloci:ociStmtPrepare(Stmthp2, ?SELECT_ROW_COUNT),
    {ok, _} = erloci:ociStmtExecute(Svchp, Stmthp2, #{}, 0, 0, ?OCI_DEFAULT),
    {ok, [[RowCount1]], _} = erloci:ociStmtFetch(Stmthp2, 1),
    Total = oci_util:from_num(RowCount1),
    ?assertMatch("6", Total),

    {ok, Stmthp3} = erloci:ociStmtHandleCreate(Envhp),
    ok =  erloci:ociStmtPrepare(Stmthp3, ?SELECT_ALL_ROWS),
    {ok, _} = erloci:ociStmtExecute(Svchp, Stmthp3, #{}, 0, 0, ?OCI_DEFAULT),
    {ok, [_R1,_R2,_R3], false} = erloci:ociStmtFetch(Stmthp3, 3),
    {ok, [_R4,_R5], false} = erloci:ociStmtFetch(Stmthp3, 2),
    {ok, [_R6], true} = erloci:ociStmtFetch(Stmthp3, 7).

% ------------------------------------------------------------------------------
% helpers
% ------------------------------------------------------------------------------
setup() ->
    Conf = ?CONN_CONF,
    #{tns := Tns, lang := Lang} = Conf,
    {Language, Country, Charset} = erloci:parse_lang(Lang),
    E1 =  erloci:ociEnvNlsCreate(0,0),
    {ok, CharsetId} = erloci:ociNlsCharSetNameToId(E1, Charset),
    ok = erloci:ociEnvHandleFree(E1),
    Envhp =  erloci:ociEnvNlsCreate(CharsetId,CharsetId),
    ok = erloci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, Language,
                           ?OCI_ATTR_ENV_NLS_LANGUAGE),
    ok = erloci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, Country,
                           ?OCI_ATTR_ENV_NLS_TERRITORY),
    {ok, Spoolhp} =  erloci:ociSessionPoolCreate(Envhp, Tns, 2, 10, 1, null,
                                                 null),
    #{envhp => Envhp, conf => Conf, spoolhp => Spoolhp}.

teardown(#{envhp := Envhp, spoolhp := Spoolhp}) ->
    ok = erloci:ociSessionPoolDestroy(Spoolhp),
    ok = erloci:ociEnvHandleFree(Envhp),
    ok = erloci:ociTerminate().

setup_session() ->
    #{envhp := Envhp, spoolhp := Spoolhp, conf := #{user := User,
      password := Pass}} = Ctx = setup(),
    {ok, Authhp} =  erloci:ociAuthHandleCreate(Envhp, User, Pass),
    {ok, Svchp} =  erloci:ociSessionGet(Envhp, Authhp, Spoolhp),
    Ctx#{authhp => Authhp, svchp => Svchp}.

teardown_session(#{authhp := Authhp, svchp := Svchp} = Ctx) ->
    ok = erloci:ociSessionRelease(Svchp),
    ok = erloci:ociAuthHandleFree(Authhp),
    teardown(Ctx).

flush_table(#{envhp := Envhp, svchp := Svchp}) ->
    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ok = erloci:ociStmtPrepare(Stmthp, ?DROP),
    case  erloci:ociStmtExecute(Svchp, Stmthp, #{}, 1, 0,  ?OCI_DEFAULT) of
        {ok, #{statement := ?OCI_STMT_DROP}} -> ok;
        {error,{942,_}} -> ok;
        Else -> exit(Else)
    end,
    ok = erloci:ociStmtPrepare(Stmthp, ?CREATE),
    {ok, #{statement := ?OCI_STMT_CREATE}} = erloci:ociStmtExecute(Svchp, Stmthp, #{}, 1, 0, ?OCI_DEFAULT).