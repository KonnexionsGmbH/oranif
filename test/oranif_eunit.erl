-module(oranif_eunit).
-include_lib("eunit/include/eunit.hrl").

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

-define(EXEC_STMT(_Conn, _Sql),
    (fun() ->
        __Stmt = dpiCall(Safe, conn_prepareStmt, [_Conn, false, _Sql, <<"">>]),
        R = (catch dpiCall(Safe, stmt_execute, [__Stmt, []])),
        catch dpiCall(Safe, stmt_close, [__Stmt, <<>>]),
        R
    end)()
).

%%% NEW API TESTS

%%%
%%% CONTEXT APIS
%%%

contextCreate_test({Safe}) ->
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assert(is_reference(Context)),
    dpiCall(Safe, context_destroy, [Context]).

contextCreate_NegativeMajType({Safe}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, context_create, [foobar, ?DPI_MINOR_VERSION])),
    ok.

contextCreate_NegativeMinType({Safe}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, foobar])),
    ok.

%% fails due to nonsense major version
contextCreate_NegativeFailCall({Safe}) ->
    ?assertException(error, {error, _},
        dpiCall(Safe, context_create, [1337, ?DPI_MINOR_VERSION])),
    ok.

contextDestroy_test({Safe}) ->
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertEqual(ok, dpiCall(Safe, context_destroy, [Context])),
    ok.

contextDestroy_NegativeContextType({Safe}) ->
   ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, context_destroy, [foobar])),
    ok.

contextDestroy_NegativeContextState({Safe}) ->
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertEqual(ok, dpiCall(Safe, context_destroy, [Context])), %% destroy the context
    ?assertException(error, {error, _File, _Line, _Exception}, %% try to destroy it again
        dpiCall(Safe, context_destroy, [Context])),
    ok.

contextGetClientVersion_test({Safe}) -> 
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),

    #{
        releaseNum := CRNum, versionNum := CVNum, fullVersionNum := CFNum
    } = dpiCall(Safe, context_getClientVersion, [Context]),

    ?assert(is_integer(CRNum)),
    ?assert(is_integer(CVNum)),
    ?assert(is_integer(CFNum)).

contextGetClientVersion_NegativeContextType({Safe}) -> 
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, context_getClientVersion, [foobar])),
    dpiCall(Safe, context_destroy, [Context]),
    ok.

%% fails due to invalid context
contextGetClientVersion_NegativeFailCall({Safe}) -> 
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertEqual(ok, dpiCall(Safe, context_destroy, [Context])), %% context is now invalid
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, context_getClientVersion, [foobar])), %% try to get client version of invalid context
    ok.


%%%
%%% CONN APIS
%%%

connCreate_test({Safe, Context}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(Safe, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    ?assert(is_reference(Conn)),
    dpiCall(Safe, conn_close, [Conn, [], <<>>]).

connCreate_NegativeContextType({Safe, _Context}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [foobar, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeUsernameType({Safe, Context}) ->
    #{tns := Tns, user := _User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, foobar, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativePassType({Safe, Context}) ->
    #{tns := Tns, user := User, password := _Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, User, foobat, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeTNSType({Safe, Context}) ->
    #{tns := _Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, User, Password, foobar,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeParamsType({Safe, Context}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, User, Password, Tns,
            foobar, #{}])),
    ok.

connCreate_NegativeEncodingType({Safe, Context}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, User, Password, Tns,
            #{encoding =>foobar, nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeNencodingType({Safe, Context}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => foobar}, #{}])),
    ok.

%% fails due to invalid user/pass combination
connCreate_NegativeFailCall({Safe, Context}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_create, [Context, <<"Chuck">>, <<"Norris">>, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connPrepareStmt_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"miau">>, <<"foo">>]),
    ?assert(is_reference(Stmt)),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

connPrepareStmt_emptyTag({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"miau">>, <<"">>]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

connPrepareStmt_NegativeConnType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_prepareStmt, [foobar, false, <<"miau">>, <<"">>])),
    ok.

connPrepareStmt_NegativeScrollableType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_prepareStmt, [Conn, "foobar", <<"miau">>, <<>>])),
    ok.

connPrepareStmt_NegativeSQLType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_prepareStmt, [Conn, false, foobar, <<"">>])),
    ok.

connPrepareStmt_NegativeTagType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"miau">>, foobar])),
    ok.

%% fails due to both SQL and Tag being empty
connPrepareStmt_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"">>, <<"">>])),
    ok.
connNewVar_test({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    ?assert(is_reference(Var)),
    ?assert(is_list(Data)),
    [FirstData | _] = Data,
    ?assert(is_reference(FirstData)),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

connNewVar_NegativeConnType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [foobar, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null])),
    ok.

connNewVar_NegativeOraTypeType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, "foobar", 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null])),
    ok.

connNewVar_NegativeDpiTypeType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', "foobar", 100, 0, false, false, null])),
    ok.

connNewVar_NegativeArraySizeType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', foobar, 0, false, false, null])),
    ok.

connNewVar_NegativeSizeType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, foobar, false, false, null])),
    ok.

connNewVar_NegativeSizeIsBytesType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, "foobar", false, null])),
    ok.

connNewVar_NegativeIsArrayType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, "foobar", null])),
    ok.

connNewVar_NegativeObjTypeType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, "foobar"])),
    ok.

%% fails due to array size being 0
connNewVar_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 0, 0, false, false, null])),
    ok.

connCommit_test({Safe, _Context, Conn}) ->
    Result = dpiCall(Safe, conn_commit, [Conn]),
    ?assertEqual(ok, Result),
    ok.
  
connCommit_NegativeConnType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_commit, [foobar])),
    ok.

%% fails due to the reference being wrong
connCommit_NegativeFailCall({Safe, Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_commit, [Context])),
    ok.

connRollback_test({Safe, _Context, Conn}) ->
    Result = dpiCall(Safe, conn_rollback, [Conn]),
    ?assertEqual(ok, Result),
    ok.
  
connRollback_NegativeConnType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_rollback, [foobar])),
    ok.

%% fails due to the reference being wrong
connRollback_NegativeFailCall({Safe, Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_rollback, [Context])),
    ok.

connPing_test({Safe, _Context, Conn}) ->
    Result = dpiCall(Safe, conn_ping, [Conn]),
    ?assertEqual(ok, Result),
    ok.
  
connPing_NegativeConnType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_ping, [foobar])),
    ok.

%% fails due to the reference being wrong
connPing_NegativeFailCall({Safe, Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_ping, [Context])),
    ok.

connClose_test({Safe, Context, _Conn}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(Safe, conn_create, [Context, User, Password, Tns,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    Result = dpiCall(Safe, conn_close, [Conn, [], <<"">>]),
    ?assertEqual(ok, Result),
    ok.

connClose_testWithModes({Safe, Context, _Conn}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(Safe, conn_create, [Context, User, Password, Tns,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    Result = dpiCall(Safe, conn_close, [Conn, ['DPI_MODE_CONN_CLOSE_DEFAULT'], <<"">>]), %% the other two don't work without a session pool
    ?assertEqual(ok, Result),
    ok.
  
connClose_NegativeConnType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_close, [foobar, [], <<"">>])),
    ok.

connClose_NegativeModesType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_close, [Conn, foobar, <<"">>])),
    ok.

connClose_NegativeModeInsideType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_close, [Conn, ["not an atom"], <<"">>])),
    ok.

connClose_NegativeInvalidMode({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_close, [Conn, [foobar], <<"">>])),
    ok.

connClose_NegativeTagType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_close, [Conn, [], foobar])),
    ok.

%% fails due to the reference being wrong
connClose_NegativeFailCall({Safe, Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_close, [Context, [], <<"">>])),
    ok.

connGetServerVersion_test({Safe, _Context, Conn}) ->
    #{
        releaseNum := ReleaseNum, versionNum := VersionNum, fullVersionNum := FullVersionNum,
        portReleaseNum := PortReleaseNum, portUpdateNum := PortUpdateNum,
        releaseString := ReleaseString
    } = dpiCall(Safe, conn_getServerVersion, [Conn]),
    ?assert(is_integer(ReleaseNum)),
    ?assert(is_integer(VersionNum)),
    ?assert(is_integer(FullVersionNum)),
    ?assert(is_integer(PortReleaseNum)),
    ?assert(is_integer(PortUpdateNum)),
    ?assert(is_list(ReleaseString)),
    ok.
  
connGetServerVersion_NegativeConnType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_getServerVersion, [foobar])),
    ok.

%% fails due to the reference being completely wrong (apparently passing a released connection isn't bad enough)
connGetServerVersion_NegativeFailCall({Safe, Context, _Conn}) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(Safe, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    dpiCall(Safe, conn_close, [Conn, [], <<>>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, conn_getServerVersion, [Context])),
    ok.


%%%
%%% STMT APIS
%%%

stmtExecute_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryCols = dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertEqual(1, QueryCols),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtExecute_testWithModes({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryCols = dpiCall(Safe, stmt_execute, [Stmt, ['DPI_MODE_EXEC_DEFAULT']]),
    ?assertEqual(1, QueryCols),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtExecute_NegativeStmtType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_execute, [foobar, []])),
    ok.

stmtExecute_NegativeModesType({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_execute, [Stmt, foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtExecute_NegativeModeInsideType({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_execute, [Stmt, ["not an atom"]])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the SQL being invalid
stmtExecute_NegativeFailCall({Safe, Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"all your base are belong to us">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_execute, [Stmt, []])),
    ok.

stmtFetch_test({Safe, _Context, Conn}) ->
    SQL = <<"select 1337 from dual">>,
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, SQL, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    #{found := Found, bufferRowIndex := BufferRowIndex} = dpiCall(Safe, stmt_fetch, [Stmt]),
    ?assert(is_atom(Found)),
    ?assert(is_integer(BufferRowIndex)),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtFetch_NegativeStmtType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_fetch, [foobar])),
    ok.

%% fails due to the reference being of the wrong type
stmtFetch_NegativeFailCall({Safe, Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi (a) values (1337)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_fetch, [Conn])),
    ok.

stmtGetQueryValue_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_fetch, [Stmt]),
    #{nativeTypeNum := Type, data := Result} =
        dpiCall(Safe, stmt_getQueryValue, [Stmt, 1]),
    ?assert(is_atom(Type)),
    ?assert(is_reference(Result)),
    dpiCall(Safe, data_release, [Result]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetQueryValue_NegativeStmtType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getQueryValue, [foobar, 1])),
    ok.

stmtGetQueryValue_NegativePosType({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getQueryValue, [Stmt, foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the fetch not being done
stmtGetQueryValue_NegativeFailCall({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getQueryValue, [Stmt, 1])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetQueryInfo_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    Info = dpiCall(Safe, stmt_getQueryInfo, [Stmt, 1]),
    ?assert(is_reference(Info)),
    dpiCall(Safe, queryInfo_delete, [Info]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetQueryInfo_NegativeStmtType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getQueryInfo, [foobar, 1])),
    ok.

stmtGetQueryInfo_NegativePosType({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getQueryInfo, [Stmt, foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the SQL being bad
stmtGetQueryInfo_NegativeFailCall({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"bibidi babidi boo">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getQueryInfo, [Stmt, 1])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetNumQueryColumns_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    Count = dpiCall(Safe, stmt_getNumQueryColumns, [Stmt]),
    ?assert(is_integer(Count)),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetNumQueryColumns_NegativeStmtType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getNumQueryColumns, [foobar])),
    ok.


%% fails due to the statement being released too early
stmtGetNumQueryColumns_NegativeFailCall({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"it is showtime">>, <<"">>]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_getNumQueryColumns, [Stmt])),
    ok.

stmtBindValueByPos_test({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok, dpiCall(Safe, stmt_bindValueByPos, [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindValueByPos_NegativeStmtType({Safe, _Context, Conn}) -> 
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
    dpiCall(Safe, stmt_bindValueByPos, [foobar, 1, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    ok.

stmtBindValueByPos_NegativePosType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByPos, [Stmt, foobar, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByPos_NegativeTypeType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByPos, [Stmt, 1, "foobar", BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByPos_NegativeDataType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByPos, [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the position being invalid
stmtBindValueByPos_NegativeFailCall({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByPos, [Stmt, -1, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.


stmtBindValueByName_test({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok, dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindValueByName_NegativeStmtType({Safe, _Context, Conn}) -> 
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
    dpiCall(Safe, stmt_bindValueByName, [foobar, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    ok.

stmtBindValueByName_NegativePosType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByName, [Stmt, foobar, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByName_NegativeTypeType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"A">>, "foobar", BindData])),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByName_NegativeDataType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the name being invalid
stmtBindValueByName_NegativeFailCall({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"B">>, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(Safe, data_release, [BindData]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>),
    %% also freeing the stmt here causes it to abort
    ok.

stmtBindByPos_test({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertEqual(ok, dpiCall(Safe, stmt_bindByPos, [Stmt, 1, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByPos_NegativeStmtType({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByPos, [foobar, 1, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByPos_NegativePosType({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByPos, [Stmt, foobar, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByPos_NegativeVarType({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByPos, [Stmt, 1, foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

%% fails due to the position being invalid
stmtBindByPos_NegativeFailCall({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByPos, [Stmt, -1, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_test({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertEqual(ok, dpiCall(Safe, stmt_bindByName, [Stmt, <<"A">>, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_NegativeStmtType({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByName, [foobar, <<"A">>, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_NegativePosType({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByName, [Stmt, foobar, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_NegativeVarType({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByName, [Stmt, <<"A">>, foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

%% fails due to the position being invalid
stmtBindByName_NegativeFailCall({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_bindByName, [Stmt, <<"B">>, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtDefine_test({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    #{var := Var, data := Data} =
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, dpiCall(Safe, stmt_define, [Stmt, 1, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefine_NegativeStmtType({Safe, _Context, Conn}) -> 
    #{var := Var, data := Data} =
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_define, [foobar, 1, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

stmtDefine_NegativePosType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    #{var := Var, data := Data} =
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_define, [Stmt, foobar, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefine_NegativeVarType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
    dpiCall(Safe, stmt_define, [Stmt, 1, foobar])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the pos being invalid
stmtDefine_NegativeFailCall({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    #{var := Var, data := Data} =
        dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_define, [Stmt, 12345, Var])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefineValue_test({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, dpiCall(Safe, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefineValue_NegativeStmtType({Safe, _Context, Conn}) -> 
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [foobar, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    ok.

stmtDefineValue_NegativePosType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [Stmt, foobar, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeOraTypeType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [Stmt, 1, "foobar", 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeNativeTypeType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', "foobar", 0, false, null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeSizeType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', foobar, false, null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeSizeInBytesType({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, "foobar", null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to invalid position
stmtDefineValue_NegativeFailCall({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, stmt_defineValue, [Stmt, -1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

%%%
%%% Var APIS
%%%

varSetNumElementsInArray_test({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(Safe, var_setNumElementsInArray, [Var, 100])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

varSetNumElementsInArray_NegativeVarType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setNumElementsInArray, [foobar, 100])),
    ok.

varSetNumElementsInArray_NegativeNumElementsType({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setNumElementsInArray, [Var, foobar])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

%% fails due to invalid array size
varSetNumElementsInArray_NegativeFailCall({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setNumElementsInArray, [Var, -1])),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

varSetFromBytes_test({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(Safe, var_setFromBytes, [Var, 0, <<"abc">>])),
    
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

varSetFromBytes_NegativeVarType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setFromBytes, [foobar, 0, <<"abc">>])),
    ok.

varSetFromBytes_NegativePosType({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setFromBytes, [Var, foobar, <<"abc">>])),
    
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

varSetFromBytes_NegativeBinaryType({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setFromBytes, [Var, 0, foobar])),
    
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

%% fails due to position being invalid
varSetFromBytes_NegativeFailCall({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_setFromBytes, [Var, -1, <<"abc">>])),
    
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    dpiCall(Safe, var_release, [Var]),
    ok.

varRelease_test({Safe, _Context, Conn}) ->
    #{var := Var, data := Data} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    [dpiCall(Safe, data_release, [X]) || X <- Data],
    ?assertEqual(ok, dpiCall(Safe, var_release, [Var])),
    ok.

varRelease_NegativeVarType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_release, [foobar])),
    ok.

%% fails due to the reference being wrong
varRelease_NegativeFailCall({Safe, Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, var_release, [Context])),
    ok.

%%%
%%% QuryInfo APIS
%%%

queryInfoGet_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, 1]),
    #{name := Name, nullOk := NullOk,
        typeInfo := #{clientSizeInBytes := ClientSizeInBytes, dbSizeInBytes := DbSizeInBytes,
            defaultNativeTypeNum := DefaultNativeTypeNum, fsPrecision := FsPrecision,
            objectType := ObjectType, ociTypeCode := OciTypeCode,
            oracleTypeNum := OracleTypeNum , precision := Precision,
            scale := Scale, sizeInChars := SizeInChars}} = dpiCall(Safe, queryInfo_get, [QueryInfoRef]),

    ?assert(is_list(Name)),
    ?assert(is_atom(NullOk)),
    ?assert(is_integer(ClientSizeInBytes)),
    ?assert(is_integer(DbSizeInBytes)),
    ?assert(is_atom(DefaultNativeTypeNum)),
    ?assert(is_integer(FsPrecision)),
    ?assert(is_atom(ObjectType)),
    ?assert(is_integer(OciTypeCode)),
    ?assert(is_atom(OracleTypeNum)),
    ?assert(is_integer(Precision)),
    ?assert(is_integer(Scale)),
    ?assert(is_integer(SizeInChars)),
    
    dpiCall(Safe, queryInfo_delete, [QueryInfoRef]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

queryInfoGet_NegativeQueryInfoType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, queryInfo_get, [foobar])),
    ok.

%% fails due to getting a completely wrong reference
queryInfoGet_NegativeFailCall({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, 1]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, queryInfo_get, [Conn])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

queryInfoDelete_test({Safe, _Context, Conn}) ->
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, 1]),
    ?assertEqual(ok, dpiCall(Safe, queryInfo_delete, [QueryInfoRef])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ok.

queryInfoDelete_NegativeQueryInfoType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, queryInfo_delete, [foobar])),
    ok.

%% fails due to getting a completely wrong reference
queryInfoDelete_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, queryInfo_delete, [Conn])),
    ok.

%%%
%%% Data APIS
%%%

dataSetTimestamp_test({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [foobar, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    ok.

dataSetTimestamp_NegativeYearType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, foobar, 2, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeMonthType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, foobar, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeDayType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, foobar, 4, 5, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeHourType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, foobar, 5, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeMinuteType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, foobar, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeSecondType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, 5, foobar, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeFSecondType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, foobar, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeTZHourOffsetType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, foobar, 9])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeTZMinuteOffsetType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, foobar])),
    dpiCall(Safe, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
%% (it doesn't seem to mind the nonsense parameters. Year -1234567? Sure. Timezone of -22398 hours and 3239 minutes? No problem)
dataSetTimestamp_NegativeFailCall({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setTimestamp, [Conn, -1234567, 2, 3, 4, 5, 6, 7, -22398, 3239])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetTimestamp_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(Safe, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataSetIntervalDS_test({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [foobar, 1, 2, 3, 4, 5])),
    ok.

dataSetIntervalDS_NegativeDayType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [Data, foobar, 2, 3, 4, 5])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeHoursType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [Data, 1, foobar, 3, 4, 5])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeMinutesType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [Data, 1, 2, foobar, 4, 5])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeSecondsType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [Data, 1, 2, 3, foobar, 5])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeFSecondsType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [Data, 1, 2, 3, 4, foobar])),
    dpiCall(Safe, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetIntervalDS_NegativeFailCall({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalDS, [Conn, 1, 2, 3, 4, 5])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalDS_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.


dataSetIntervalYM_test({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIntervalYM, [Data, 1, 2])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalYM_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalYM, [foobar, 1, 2])),
    ok.

dataSetIntervalYM_NegativeYearType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalYM, [Data, foobar, 2])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalYM_NegativeMonthType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalYM, [Data, 1, foobar])),
    dpiCall(Safe, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetIntervalYM_NegativeFailCall({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIntervalYM, [Conn, 1, 2])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIntervalYM_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIntervalYM, [Data, 1, 2])),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.


dataSetInt64_test({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setInt64, [Data, 1])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetInt64_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setInt64, [foobar, 1])),
    ok.

dataSetInt64_NegativeAmountType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setInt64, [Data, foobar])),
    dpiCall(Safe, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetInt64_NegativeFailCall({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setInt64, [Conn, 1])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetInt64_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(Safe, data_setInt64, [Data, 1])),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataSetBytes_test({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setBytes, [Data, <<"my string">>])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetBytes_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setBytes, [foobar, <<"my string">>])),
    ok.

dataSetBytes_NegativeBinaryType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setBytes, [Data, foobar])),
    dpiCall(Safe, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetBytes_NegativeFailCall({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setBytes, [Conn, <<"my string">>])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIsNull_testTrue({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIsNull, [Data, true])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIsNull_testFalse({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIsNull, [Data, false])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIsNull_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIsNull, [foobar, 1])),
    ok.

dataSetIsNull_NegativeIsNullType({Safe, _Context, _Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIsNull, [Data, "not an atom"])),
    dpiCall(Safe, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetIsNull_NegativeFailCall({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_setIsNull, [Conn, 1])),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataSetIsNull_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(Safe, data_setIsNull, [Data, true])),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testNull({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(Safe, data_get, [Data])),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testInt64({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(Safe, data_get, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testUint64({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_UINT', 'DPI_NATIVE_TYPE_UINT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(Safe, data_get, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testFloat({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_FLOAT', 'DPI_NATIVE_TYPE_FLOAT', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(Safe, data_get, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testDouble({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(Safe, data_get, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testBinary({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(Safe, var_setFromBytes, [Var, 0, <<"my string">>])),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(Safe, data_get, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testTimestamp({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP', 1, 100,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    #{year := Year, month := Month, day := Day,
        hour := Hour, minute := Minute, second := Second, 
        fsecond := Fsecond, tzHourOffset := TzHourOffset, tzMinuteOffset := TzMinuteOffset} =
        dpiCall(Safe, data_get, [Data]),
    ?assert(is_integer(Year)),
    ?assert(is_integer(Month)),
    ?assert(is_integer(Day)),
    ?assert(is_integer(Hour)),
    ?assert(is_integer(Minute)),
    ?assert(is_integer(Second)),
    ?assert(is_integer(Fsecond)),
    ?assert(is_integer(TzHourOffset)),
    ?assert(is_integer(TzMinuteOffset)),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testIntervalDS({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS', 1, 100,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    #{days := Days, hours := Hours, minutes := Minutes, 
        seconds := Seconds, fseconds := Fseconds} =
        dpiCall(Safe, data_get, [Data]),
    ?assert(is_integer(Days)),
    ?assert(is_integer(Hours)),
    ?assert(is_integer(Minutes)),
    ?assert(is_integer(Seconds)),
    ?assert(is_integer(Fseconds)),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.


dataGet_testIntervalYM({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 100,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    #{years := Years, months := Months} =
        dpiCall(Safe, data_get, [Data]),
    ?assert(is_integer(Years)),
    ?assert(is_integer(Months)),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testStmt({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 100,
            false, false, null
        ]
    ),
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(Safe, var_setFromStmt, [Var, 0, Stmt]),
    ?assert(is_reference( dpiCall(Safe, data_get, [Data]))), %% first-time get
    ?assert(is_reference( dpiCall(Safe, data_get, [Data]))), %% cached re-get
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_testStmtChange({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 100,
            false, false, null
        ]
    ),
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(Safe, var_setFromStmt, [Var, 0, Stmt]),
    ?assert(is_reference( dpiCall(Safe, data_get, [Data]))), %% first-time get
    dpiCall(Safe, var_setFromStmt, [Var, 0, Stmt2]),
    ?assert(is_reference( dpiCall(Safe, data_get, [Data]))), %% "ref cursor changed"
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGet_NegativeDataType({Safe, _Context, _Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_get, [foobar])),
    ok.

%% fails due to completely wrong reference
dataGet_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(Safe, data_get, [Conn])),
    ok.


dataGetInt64_test({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(Safe, data_getInt64, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    ok.

dataGetInt64_NegativeDataType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(Safe, data_getInt64, [foobar]))),
    ok.

%% fails due to completely wrong reference
dataGetInt64_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(Safe, data_getInt64, [Conn]))),
    ok.

dataGetInt64_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(Safe, data_getInt64, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

%% no non-pointer test for this one
dataGetBytes_test({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(Safe, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(Safe, data_getBytes, [Data]))),
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]),
    ok.

dataGetBytes_NegativeDataType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(Safe, data_getBytes, [foobar]))),
    ok.

%% fails due to completely wrong reference
dataGetBytes_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(Safe, data_getBytes, [Conn]))),
    ok.

dataRelease_test({Safe, _Context, Conn}) ->
    Data = dpiCall(Safe, data_ctor, []),
    ?assertEqual(ok,
        (dpiCall(Safe, data_release, [Data]))),
    ok.

dataRelease_NegativeDataType({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(Safe, data_release, [foobar]))),
    ok.

%% fails due to completely wrong reference
dataRelease_NegativeFailCall({Safe, _Context, Conn}) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(Safe, data_release, [Conn]))),
    ok.

dataRelease_viaPointer({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        (dpiCall(Safe, data_release, [Data]))),
    dpiCall(Safe, var_release, [Var]),
    ok.







-define(SLAVE, oranif_slave).

setup_context_only(Safe) ->
    if
        Safe -> ok = dpi:load(?SLAVE);
        true -> ok = dpi:load_unsafe()
    end,
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Context = dpiCall(
        Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    if
        Safe ->
            SlaveNode = list_to_existing_atom(
                re:replace(
                    atom_to_list(node()), ".*(@.*)", atom_to_list(?SLAVE)++"\\1",
                    [{return, list}]
                )
            ),
            pong = net_adm:ping(SlaveNode),
            {Safe, SlaveNode, Context};
        true -> {Safe, Context}
    end.

setup_no_input(Safe) ->
    if
        Safe -> ok = dpi:load(?SLAVE);
        true -> ok = dpi:load_unsafe()
    end,
    #{tns := Tns, user := User, password := Password} = getConfig(),
    if
        Safe ->
            SlaveNode = list_to_existing_atom(
                re:replace(
                    atom_to_list(node()), ".*(@.*)", atom_to_list(?SLAVE)++"\\1",
                    [{return, list}]
                )
            ),
            pong = net_adm:ping(SlaveNode),
            {Safe, SlaveNode};
        true -> {Safe}
    end.

setup(Safe) ->
    if
        Safe -> ok = dpi:load(?SLAVE);
        true -> ok = dpi:load_unsafe()
    end,
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Context = dpiCall(
        Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    Connnnection = dpiCall(
        Safe, conn_create, [
            Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
        ]
    ),
    if
        Safe ->
            SlaveNode = list_to_existing_atom(
                re:replace(
                    atom_to_list(node()), ".*(@.*)", atom_to_list(?SLAVE)++"\\1",
                    [{return, list}]
                )
            ),
            pong = net_adm:ping(SlaveNode),
            {Safe, SlaveNode, Context, Connnnection};
        true -> {Safe, Context, Connnnection}
    end.

cleanup({Safe, _SlaveNode, Context, Connnnection}) ->
    cleanup({Safe, Context, Connnnection});
cleanup({Safe, Context, Connnnection}) ->
    dpiCall(Safe, conn_close, [Connnnection, [], <<>>]),
    dpiCall(Safe, context_destroy, [Context]),
    if Safe -> dpiCall(Safe, unload, []); true -> ok end.

cleanup_context_only({Safe, _SlaveNode, Context}) ->
    cleanup({Safe, Context});
cleanup_context_only({Safe, Context}) ->
    dpiCall(Safe, context_destroy, [Context]),
    if Safe -> dpiCall(Safe, unload, []); true -> ok end.

cleanup_no_input({Safe, _SlaveNode}) ->
    cleanup({Safe});
cleanup_no_input({Safe}) ->
    if Safe -> dpiCall(Safe, unload, []); true -> ok end.

-define(F(__Fn), {??__Fn, fun __Fn/1}).

-define(API_TESTS_NO_INPUT, [
    ?F(contextCreate_test),
    ?F(contextCreate_NegativeMajType),
    ?F(contextCreate_NegativeMinType),
    ?F(contextCreate_NegativeFailCall),
    ?F(contextDestroy_test),
    ?F(contextDestroy_NegativeContextType),
    ?F(contextDestroy_NegativeContextState),
    ?F(contextGetClientVersion_test),
    ?F(contextGetClientVersion_NegativeContextType),
    ?F(contextGetClientVersion_NegativeFailCall)
]).

-define(API_TESTS_CONTEXT_ONLY, [
    ?F(connCreate_test),
    ?F(connCreate_NegativeContextType),
    ?F(connCreate_NegativeUsernameType),
    ?F(connCreate_NegativePassType),
    ?F(connCreate_NegativeTNSType),
    ?F(connCreate_NegativeParamsType),
    ?F(connCreate_NegativeEncodingType),
    ?F(connCreate_NegativeNencodingType),
    ?F(connCreate_NegativeFailCall)
]).

-define(API_TESTS, [
    ?F(connPrepareStmt_test),
    ?F(connPrepareStmt_emptyTag),
    ?F(connPrepareStmt_NegativeConnType),
    ?F(connPrepareStmt_NegativeScrollableType),
    ?F(connPrepareStmt_NegativeSQLType),
    ?F(connPrepareStmt_NegativeTagType),
    ?F(connPrepareStmt_NegativeFailCall),
    ?F(connNewVar_test),
    ?F(connNewVar_NegativeConnType),
    ?F(connNewVar_NegativeOraTypeType),
    ?F(connNewVar_NegativeDpiTypeType),
    ?F(connNewVar_NegativeArraySizeType),
    ?F(connNewVar_NegativeSizeType),
    ?F(connNewVar_NegativeSizeIsBytesType),
    ?F(connNewVar_NegativeIsArrayType),
    ?F(connNewVar_NegativeObjTypeType),
    ?F(connNewVar_NegativeFailCall),
    ?F(connCommit_test),
    ?F(connCommit_NegativeConnType),
    ?F(connCommit_NegativeFailCall),
    ?F(connRollback_test),
    ?F(connRollback_NegativeConnType),
    ?F(connRollback_NegativeFailCall),
    ?F(connPing_test),
    ?F(connPing_NegativeConnType),
    ?F(connPing_NegativeFailCall),
    ?F(connClose_test),
    ?F(connClose_testWithModes),
    ?F(connClose_NegativeConnType),
    ?F(connClose_NegativeModesType),
    ?F(connClose_NegativeModeInsideType),
    ?F(connClose_NegativeInvalidMode),
    ?F(connClose_NegativeTagType),
    ?F(connClose_NegativeFailCall),
    ?F(connGetServerVersion_test),
    ?F(connGetServerVersion_NegativeConnType),
    ?F(connGetServerVersion_NegativeFailCall),
    ?F(stmtExecute_test),
    ?F(stmtExecute_testWithModes),
    ?F(stmtExecute_NegativeStmtType),
    ?F(stmtExecute_NegativeModesType),
    ?F(stmtExecute_NegativeModeInsideType),
    ?F(stmtExecute_NegativeFailCall),
    ?F(stmtFetch_test),
    ?F(stmtFetch_NegativeStmtType),
    ?F(stmtFetch_NegativeFailCall),
    ?F(stmtGetQueryValue_test),
    ?F(stmtGetQueryValue_NegativeStmtType),
    ?F(stmtGetQueryValue_NegativePosType),
    ?F(stmtGetQueryValue_NegativeFailCall),
    ?F(stmtGetQueryInfo_test),
    ?F(stmtGetQueryInfo_NegativeStmtType),
    ?F(stmtGetQueryInfo_NegativePosType),
    ?F(stmtGetQueryInfo_NegativeFailCall),
    ?F(stmtGetNumQueryColumns_test),
    ?F(stmtGetNumQueryColumns_NegativeStmtType),
    ?F(stmtGetNumQueryColumns_NegativeFailCall),
    ?F(stmtBindValueByPos_test),
    ?F(stmtBindValueByPos_NegativeStmtType),
    ?F(stmtBindValueByPos_NegativePosType),
    ?F(stmtBindValueByPos_NegativeTypeType),
    ?F(stmtBindValueByPos_NegativeDataType),
    ?F(stmtBindValueByPos_NegativeFailCall),
    ?F(stmtBindValueByName_test),
    ?F(stmtBindValueByName_NegativeStmtType),
    ?F(stmtBindValueByName_NegativePosType),
    ?F(stmtBindValueByName_NegativeTypeType),
    ?F(stmtBindValueByName_NegativeDataType),
    ?F(stmtBindValueByName_NegativeFailCall),
    ?F(stmtBindByPos_test),
    ?F(stmtBindByPos_NegativeStmtType),
    ?F(stmtBindByPos_NegativePosType),
    ?F(stmtBindByPos_NegativeVarType),
    ?F(stmtBindByPos_NegativeFailCall),
    ?F(stmtBindByName_test),
    ?F(stmtBindByName_NegativeStmtType),
    ?F(stmtBindByName_NegativePosType),
    ?F(stmtBindByName_NegativeVarType),
    ?F(stmtBindByName_NegativeFailCall),
    ?F(stmtDefine_test),
    ?F(stmtDefine_NegativeStmtType),
    ?F(stmtDefine_NegativePosType),
    ?F(stmtDefine_NegativeVarType),
    ?F(stmtDefine_NegativeFailCall),
    ?F(stmtDefineValue_test),
    ?F(stmtDefineValue_NegativeStmtType),
    ?F(stmtDefineValue_NegativePosType),
    ?F(stmtDefineValue_NegativeOraTypeType),
    ?F(stmtDefineValue_NegativeNativeTypeType),
    ?F(stmtDefineValue_NegativeSizeType),
    ?F(stmtDefineValue_NegativeSizeInBytesType),
    ?F(stmtDefineValue_NegativeFailCall),
    ?F(varSetNumElementsInArray_test),
    ?F(varSetNumElementsInArray_NegativeVarType),
    ?F(varSetNumElementsInArray_NegativeNumElementsType),
    ?F(varSetNumElementsInArray_NegativeFailCall),
    ?F(varSetFromBytes_test),
    ?F(varSetFromBytes_NegativeVarType),
    ?F(varSetFromBytes_NegativePosType),
    ?F(varSetFromBytes_NegativeBinaryType),
    ?F(varSetFromBytes_NegativeFailCall),
    ?F(varRelease_test),
    ?F(varRelease_NegativeVarType),
    ?F(varRelease_NegativeFailCall),
    ?F(queryInfoGet_test),
    ?F(queryInfoGet_NegativeQueryInfoType),
    ?F(queryInfoGet_NegativeFailCall),
    ?F(queryInfoDelete_test),
    ?F(queryInfoDelete_NegativeQueryInfoType),
    ?F(queryInfoDelete_NegativeFailCall),
    ?F(dataSetTimestamp_test),
    ?F(dataSetTimestamp_NegativeDataType),
    ?F(dataSetTimestamp_NegativeYearType),
    ?F(dataSetTimestamp_NegativeMonthType),
    ?F(dataSetTimestamp_NegativeDayType),
    ?F(dataSetTimestamp_NegativeHourType),
    ?F(dataSetTimestamp_NegativeMinuteType),
    ?F(dataSetTimestamp_NegativeSecondType),
    ?F(dataSetTimestamp_NegativeFSecondType),
    ?F(dataSetTimestamp_NegativeTZHourOffsetType),
    ?F(dataSetTimestamp_NegativeTZMinuteOffsetType),
    ?F(dataSetTimestamp_NegativeFailCall),
    ?F(dataSetTimestamp_viaPointer),
    ?F(dataSetIntervalDS_test),
    ?F(dataSetIntervalDS_NegativeDataType),
    ?F(dataSetIntervalDS_NegativeDayType),
    ?F(dataSetIntervalDS_NegativeHoursType),
    ?F(dataSetIntervalDS_NegativeMinutesType),
    ?F(dataSetIntervalDS_NegativeSecondsType),
    ?F(dataSetIntervalDS_NegativeFSecondsType),
    ?F(dataSetIntervalDS_NegativeFailCall),
    ?F(dataSetIntervalDS_viaPointer),
    ?F(dataSetIntervalYM_test),
    ?F(dataSetIntervalYM_NegativeDataType),
    ?F(dataSetIntervalYM_NegativeYearType),
    ?F(dataSetIntervalYM_NegativeMonthType),
    ?F(dataSetIntervalYM_NegativeFailCall),
    ?F(dataSetIntervalYM_viaPointer),
    ?F(dataSetInt64_test),
    ?F(dataSetInt64_NegativeDataType),
    ?F(dataSetInt64_NegativeAmountType),
    ?F(dataSetInt64_NegativeFailCall),
    ?F(dataSetInt64_viaPointer),
    ?F(dataSetBytes_test),
    ?F(dataSetBytes_NegativeDataType),
    ?F(dataSetBytes_NegativeBinaryType),
    ?F(dataSetBytes_NegativeFailCall),
    ?F(dataSetIsNull_testTrue),
    ?F(dataSetIsNull_testFalse),
    ?F(dataSetIsNull_NegativeDataType),
    ?F(dataSetIsNull_NegativeIsNullType),
    ?F(dataSetIsNull_NegativeFailCall),
    ?F(dataSetIsNull_viaPointer),
    ?F(dataGet_testNull),
    ?F(dataGet_testInt64),
    ?F(dataGet_testUint64),
    ?F(dataGet_testFloat),
    ?F(dataGet_testDouble),
    ?F(dataGet_testBinary),
    ?F(dataGet_testTimestamp),
    ?F(dataGet_testIntervalDS),
    ?F(dataGet_testIntervalYM),
    ?F(dataGet_testStmt),
    ?F(dataGet_testStmtChange),
    ?F(dataGet_NegativeDataType),
    ?F(dataGet_NegativeFailCall),
    ?F(dataGetInt64_test),
    ?F(dataGetInt64_NegativeDataType),
    ?F(dataGetInt64_NegativeFailCall),
    ?F(dataGetInt64_viaPointer),
    ?F(dataGetBytes_test),
    ?F(dataGetBytes_NegativeDataType),
    ?F(dataGetBytes_NegativeFailCall),
    ?F(dataRelease_test),
    ?F(dataRelease_NegativeDataType),
    ?F(dataRelease_NegativeFailCall),
    ?F(dataRelease_viaPointer)


]).

unsafe_API_no_input_test_() ->
    {
        setup,
        fun() -> setup_no_input(false) end,
        fun cleanup_no_input/1,
        oraniftst_no_input(?API_TESTS_NO_INPUT)
    }.

unsafe_API_context_only_test_() ->
    {
        setup,
        fun() -> setup_context_only(false) end,
         fun cleanup_context_only/1,
        oraniftst_context_only(?API_TESTS_CONTEXT_ONLY)
    }.

unsafe_API_test_() ->
    {
        setup,
        fun() -> setup(false) end,
        fun cleanup/1,
        oraniftst(?API_TESTS)
    }.

oraniftst_no_input(TestFuns) ->
    fun
        ({Safe, SlaveNode}) ->
            [{
                "slave_"++Title,
                fun() ->
                    put(dpi_node, SlaveNode),
                    TestFun(Safe)
                end
            } || {Title, TestFun} <- TestFuns];
        (Ctx) ->
            [{Title, fun() -> TestFun(Ctx) end} || {Title, TestFun} <- TestFuns]
    end.

oraniftst_context_only(TestFuns) ->
    fun
        ({Safe, SlaveNode, Context}) ->
            [{
                "slave_"++Title,
                fun() ->
                    put(dpi_node, SlaveNode),
                    TestFun({Safe, Context})
                end
            } || {Title, TestFun} <- TestFuns];
        (Ctx) ->
            [{Title, fun() -> TestFun(Ctx) end} || {Title, TestFun} <- TestFuns]
    end.

oraniftst(TestFuns) ->
    fun
        ({Safe, SlaveNode, Context, Connnnection}) ->
            [{
                "slave_"++Title,
                fun() ->
                    put(dpi_node, SlaveNode),
                    TestFun({Safe, Context, Connnnection})
                end
            } || {Title, TestFun} <- TestFuns];
        (Ctx) ->
            [{Title, fun() -> TestFun(Ctx) end} || {Title, TestFun} <- TestFuns]
    end.

%-------------------------------------------------------------------------------
% Internal functions
%-------------------------------------------------------------------------------

dpiCall(true, F, A) -> dpi:safe(dpi, F, A);
dpiCall(false, F, A) -> apply(dpi, F, A).

getConfig() ->
    case file:get_cwd() of
        {ok, Cwd} ->
            ConnectConfigFile = filename:join(
                lists:reverse(
                    ["connect.config", "test"
                        | lists:reverse(filename:split(Cwd))]
                )
            ),
            case file:consult(ConnectConfigFile) of
                {ok, [Params]} when is_map(Params) -> Params;
                {ok, Params} ->
                    ?debugFmt("bad config (expected map) ~p", [Params]),
                    error(badconfig);
                {error, Reason} ->
                    ?debugFmt("~p", [Reason]),
                    error(Reason)
            end;
        {error, Reason} ->
            ?debugFmt("~p", [Reason]),
            error(Reason)
    end.

get_column_values(_Safe, _Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_values(Safe, Stmt, ColIdx, Limit) ->
    #{data := Data} = dpiCall(Safe, stmt_getQueryValue, [Stmt, ColIdx]),
    [dpiCall(Safe, data_get, [Data])
     | get_column_values(Safe, Stmt, ColIdx + 1, Limit)].

%% gets a value out of a fetched set, compares it using an assertation,
%% then cleans is up again
assert_getQueryValue(Safe, Stmt, Index, Value) ->
    #{data := QueryValueRef} = dpiCall(Safe, stmt_getQueryValue, [Stmt, Index]),
    ?assertEqual(Value, dpiCall(Safe, data_get, [QueryValueRef])),
	dpiCall(Safe, data_release, [QueryValueRef]),
    ok.


assert_getQueryInfo(Safe, Stmt, Index, Value, Atom) ->
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, Index]),
    ?assertEqual(Value, maps:get(Atom, dpiCall(Safe, queryInfo_get, [QueryInfoRef]))),
	dpiCall(Safe, queryInfo_delete, [QueryInfoRef]),
    ok.

extract_getQueryValue(Safe, Stmt, Index) ->
    #{data := QueryValueRef} = dpiCall(Safe, stmt_getQueryValue, [Stmt, Index]),
    Result = dpiCall(Safe, data_get, [QueryValueRef]),
	dpiCall(Safe, data_release, [QueryValueRef]),
    Result.

extract_getQueryInfo(Safe, Stmt, Index, Atom) ->
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, Index]),
    Result = maps:get(Atom, dpiCall(Safe, queryInfo_get, [QueryInfoRef])),
	dpiCall(Safe, queryInfo_delete, [QueryInfoRef]),
    Result.
