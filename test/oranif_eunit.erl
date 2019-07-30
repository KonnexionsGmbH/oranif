-module(oranif_eunit).
-include_lib("eunit/include/eunit.hrl").

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

-define(EXEC_STMT(_Conn, _Sql),
    (fun() ->
        __Stmt = dpiCall(
            TestCtx, conn_prepareStmt, [_Conn, false, _Sql, <<>>]
        ),
        __StmtExecResult = (catch dpiCall(TestCtx, stmt_execute, [__Stmt, []])),
        catch dpiCall(TestCtx, stmt_close, [__Stmt, <<>>]),
        __StmtExecResult
    end)()
).

%-------------------------------------------------------------------------------
% MACROs
%-------------------------------------------------------------------------------

-define(BAD_INT, -16#FFFFFFFFFFFFFFFF1).
-define(BAD_REF, make_ref()).
-define(W(_Tests), fun(__Ctx) -> _Tests end).
-define(F(__Fn), {??__Fn, fun() -> __Fn(__Ctx) end}).

-define(ASSERT_EX(_Error, _Expern),
    ?assertException(error, {error, _File, _Line, _Error}, _Expern)
).

%-------------------------------------------------------------------------------
% Context APIs
%-------------------------------------------------------------------------------

contextCreate(TestCtx) ->
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?assert(is_reference(Context)),
    dpiCall(TestCtx, context_destroy, [Context]).

contextCreateBadMaj(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve uint major from arg0",
        dpiCall(TestCtx, context_create, [?BAD_INT, ?DPI_MINOR_VERSION])
    ).

contextCreateBadMin(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve uint minor from arg1",
        dpiCall(TestCtx, context_create, [?DPI_MAJOR_VERSION, ?BAD_INT])
    ).

% fails due to nonsense major version
contextCreateFail(TestCtx) ->
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, context_create, [1337, ?DPI_MINOR_VERSION])
    ).

contextDestroy(TestCtx) ->
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, context_destroy, [Context])).

contextDestroyBadContext(TestCtx) ->
   ?ASSERT_EX(
       "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, context_destroy, [?BAD_REF])).

contextDestroyBadContextState(TestCtx) ->
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    % destroy the context
    ?assertEqual(ok, dpiCall(TestCtx, context_destroy, [Context])),
    % try to destroy it again
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, context_destroy, [Context])
    ).

contextGetClientVersion(TestCtx) -> 
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    #{releaseNum := CRNum, versionNum := CVNum, fullVersionNum := CFNum } = 
        dpiCall(TestCtx, context_getClientVersion, [Context]),

    ?assert(is_integer(CRNum)),
    ?assert(is_integer(CVNum)),
    ?assert(is_integer(CFNum)).

contextGetClientVersionBadContext(TestCtx) -> 
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, context_getClientVersion, [?BAD_REF])
    ),
    dpiCall(TestCtx, context_destroy, [Context]).

% fails due to a wrong handle being passed
contextGetClientVersionFail(TestCtx) ->
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, context_getClientVersion, [BindData])
    ),
    dpiCall(TestCtx, data_release, [BindData]).

%-------------------------------------------------------------------------------
% Connection APIs
%-------------------------------------------------------------------------------

connCreate(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    ?assert(is_reference(Conn)),
    dpiCall(TestCtx, conn_close, [Conn, [], <<>>]).

connCreateBadContext(TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve resource context from arg0",
        dpiCall(
            TestCtx, conn_create, [
                ?BAD_REF, User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
            ]
        )
    ).

connCreateBadUsername(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := _User, password := Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve string/binary userName from arg1",
        dpiCall(
            TestCtx, conn_create,
            [Context, badBin, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]
        )
    ).

connCreateBadPass(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := _Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve string/binary password from arg2",
        dpiCall(
            TestCtx, conn_create,
            [Context, User, badBin, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]
        )
    ).

connCreateBadTNS(#{context := Context} = TestCtx) ->
    #{tns := _Tns, user := User, password := Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve string/binary connectString from arg3",
        dpiCall(
            TestCtx, conn_create,
                [Context, User, Password, badBin,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]
        )
    ).

connCreateBadParams(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve map commonParams from arg4",
        dpiCall(
            TestCtx, conn_create, [Context, User, Password, Tns, badMap, #{}]
        )
    ).

connCreateBadEncoding(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve string",
        dpiCall(
            TestCtx, conn_create, [Context, User, Password, Tns,
                #{encoding =>badList, nencoding => "AL32UTF8"}, #{}]
        )
    ).

connCreateBadNencoding(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?ASSERT_EX(
        "Unable to retrieve string",
        dpiCall(
            TestCtx, conn_create,
            [Context, User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => badList}, #{}]
        )
    ).

% fails due to invalid user/pass combination
connCreateFail(#{context := Context} = TestCtx) ->
    #{tns := Tns} = getConfig(),
    ?ASSERT_EX(
        #{},
        dpiCall(
            TestCtx, conn_create,
            [Context, <<"Chuck">>, <<"Norris">>, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]
        )
    ).

connPrepareStmt(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, <<"foo">>]
    ),
    ?assert(is_reference(Stmt)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

connPrepareStmtEmptyTag(#{session := Conn} = TestCtx) ->
    Stmt =
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, <<>>]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

connPrepareStmtBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(
            TestCtx, conn_prepareStmt, [?BAD_REF, false, <<"miau">>, <<>>]
        )
    ).

connPrepareStmtBadScrollable(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve bool/atom scrollable from arg1",
        dpiCall(
            TestCtx, conn_prepareStmt, [Conn, "badAtom", <<"miau">>, <<>>]
        )
    ).

connPrepareStmtBadSQL(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve binary/string sql from arg2",
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, badBinary, <<>>])
    ).

connPrepareStmtBadTag(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve binary/string tag from arg3",
        dpiCall(
            TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, badBinary]
        )
    ).

% fails due to both SQL and Tag being empty
connPrepareStmtFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<>>, <<>>])
    ).

connNewVar(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, 
        [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
            100, 0, false, false, null]
    ),
    ?assert(is_reference(Var)),
    ?assert(is_list(Data)),
    [FirstData | _] = Data,
    ?assert(is_reference(FirstData)),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

connNewVarBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(
            TestCtx, conn_newVar, 
            [?BAD_REF, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE',
                'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]
        )
    ).

connNewVarBadOraType(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        #{},
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, "badAtom", 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false,
                null]
        )
    ).

connNewVarBadDpiType(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', "badAtom", 100, 0, false,
                false, null]
        )
    ).

connNewVarBadArraySize(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve uint size from arg3",
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                ?BAD_INT, 0, false, false, null]
        )
    ).

connNewVarBadSize(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve uint size from arg4",
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 
                100, ?BAD_INT, false, false, null]
        )
    ).

connNewVarBadSizeIsBytes(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve atom sizeIsBytes from arg5",
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, "badAtom", false, null]
        )
    ).

connNewVarBadArray(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve atom isArray from arg6",
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, "badAtom", null]
        )
    ).

connNewVarBadObjType(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve atom objType from arg7",
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, false, "badAtom"]
        )
    ).

% fails due to array size being 0
connNewVarFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        #{},
        dpiCall(
            TestCtx, conn_newVar,
                [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE',
                'DPI_NATIVE_TYPE_DOUBLE', 0, 0, false, false, null]
        )
    ).

connCommit(#{session := Conn} = TestCtx) ->
    Result = dpiCall(TestCtx, conn_commit, [Conn]),
    ?assertEqual(ok, Result).
  
connCommitBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_commit, [?BAD_REF])
    ).

% fails due to the reference being wrong
connCommitFail(#{context := Context} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_commit, [Context])
    ).

connRollback(#{session := Conn} = TestCtx) ->
    Result = dpiCall(TestCtx, conn_rollback, [Conn]),
    ?assertEqual(ok, Result).
  
connRollbackBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_rollback, [?BAD_REF])
    ).

% fails due to the reference being wrong
connRollbackFail(#{context := Context} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_rollback, [Context])
    ).

connPing(#{session := Conn} = TestCtx) ->
    Result = dpiCall(TestCtx, conn_ping, [Conn]),
    ?assertEqual(ok, Result).
  
connPingBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_ping, [?BAD_REF])).

% fails due to the reference being wrong
connPingFail(#{context := Context} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_ping, [Context])
    ).

connClose(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(
        TestCtx, conn_create, [Context, User, Password, Tns,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]
    ),
    Result = dpiCall(TestCtx, conn_close, [Conn, [], <<>>]),
    ?assertEqual(ok, Result).

connCloseWithModes(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(
        TestCtx, conn_create, [Context, User, Password, Tns,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]
    ),
    % the other two don't work without a session pool
    Result = dpiCall(
        TestCtx, conn_close, [Conn, ['DPI_MODE_CONN_CLOSE_DEFAULT'], <<>>]
    ),
    ?assertEqual(ok, Result).
  
connCloseBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_close, [?BAD_REF, [], <<>>])
    ).

connCloseBadModes(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve atom list modes, not a list from arg1",
        dpiCall(TestCtx, conn_close, [Conn, badList, <<>>])
    ).

connCloseBadModesInside(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve mode list value from arg1",
        dpiCall(TestCtx, conn_close, [Conn, ["badAtom"], <<>>])
    ).

connCloseInvalidMode(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve DPI_MODE atom from arg1",
        dpiCall(TestCtx, conn_close, [Conn, [wrongAtom], <<>>])
    ).

connCloseBadTag(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve binary/string tag from arg2",
        dpiCall(TestCtx, conn_close, [Conn, [], badBinary])
    ).

% fails due to the reference being wrong
connCloseFail(#{context := Context} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_close, [Context, [], <<>>])
    ).

connGetServerVersion(#{session := Conn} = TestCtx) ->
    #{
        releaseNum := ReleaseNum, versionNum := VersionNum,
        fullVersionNum := FullVersionNum, portReleaseNum := PortReleaseNum,
        portUpdateNum := PortUpdateNum, releaseString := ReleaseString
    } = dpiCall(TestCtx, conn_getServerVersion, [Conn]),
    ?assert(is_integer(ReleaseNum)),
    ?assert(is_integer(VersionNum)),
    ?assert(is_integer(FullVersionNum)),
    ?assert(is_integer(PortReleaseNum)),
    ?assert(is_integer(PortUpdateNum)),
    ?assert(is_list(ReleaseString)).
  
connGetServerVersionBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_getServerVersion, [?BAD_REF])
    ).

% fails due to the reference being completely wrong (apparently passing a
% released connection isn't bad enough)
connGetServerVersionFail(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    dpiCall(TestCtx, conn_close, [Conn, [], <<>>]),
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_getServerVersion, [Context])
    ).

connSetClientIdentifier(#{session := Conn} = TestCtx) ->
   ?assertEqual(ok,
        dpiCall(
            TestCtx, conn_setClientIdentifier, 
            [Conn, <<"myCoolConnection">>]
        )
    ).

connSetClientIdentifierBadConn(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(
            TestCtx, conn_setClientIdentifier,
            [?BAD_REF, <<"myCoolConnection">>]
        )
    ).

connSetClientIdentifierBadValue(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve string/binary value from arg1",
        dpiCall(
            TestCtx, conn_setClientIdentifier, [Conn, badBinary]
        )
    ).

%-------------------------------------------------------------------------------
% Statement APIs
%-------------------------------------------------------------------------------

stmtExecute(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    QueryCols = dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(1, QueryCols),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtExecuteWithModes(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    QueryCols = dpiCall(
        TestCtx, stmt_execute, [Stmt, ['DPI_MODE_EXEC_DEFAULT']]
    ),
    ?assertEqual(1, QueryCols),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtExecutebadStmt(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_execute, [?BAD_REF, []])
    ).

stmtExecuteBadModes(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve list of atoms from arg1",
        dpiCall(TestCtx, stmt_execute, [Stmt, badList])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtExecuteBadModesInside(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "mode must be a list of atoms",
        dpiCall(TestCtx, stmt_execute, [Stmt, ["badAtom"]])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to the SQL being invalid
stmtExecuteFail(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"all your base are belong to us">>, <<>>]
    ),
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, stmt_execute, [Stmt, []])
    ).

stmtFetch(#{session := Conn} = TestCtx) ->
    SQL = <<"select 1337 from dual">>,
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, SQL, <<>>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    #{found := Found, bufferRowIndex := BufferRowIndex} =
        dpiCall(TestCtx, stmt_fetch, [Stmt]),
    ?assert(is_atom(Found)),
    ?assert(is_integer(BufferRowIndex)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtFetchBadStmt(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_fetch, [?BAD_REF])
    ).

% fails due to the reference being of the wrong type
stmtFetchBadRes(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_fetch, [Conn])
    ).

stmtGetQueryValue(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    dpiCall(TestCtx, stmt_fetch, [Stmt]),
    #{nativeTypeNum := Type, data := Result} =
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, 1]),
    ?assert(is_atom(Type)),
    ?assert(is_reference(Result)),
    dpiCall(TestCtx, data_release, [Result]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetQueryValueBadStmt(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getQueryValue, [?BAD_REF, 1])
    ).

stmtGetQueryValueBadPos(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, ?BAD_INT])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to the fetch not being done
stmtGetQueryValueFail(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, 1])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetQueryInfo(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    Info = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    ?assert(is_reference(Info)),
    dpiCall(TestCtx, queryInfo_delete, [Info]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetQueryInfoBadStmt(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getQueryInfo, [?BAD_REF, 1])
    ).

stmtGetQueryInfoBadPos(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, ?BAD_INT])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to the SQL being bad
stmtGetQueryInfoFail(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"bibidi babidi boo">>, <<>>]
    ),
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetInfoBadStmt(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getInfo, [?BAD_REF])
    ).

% fails due to the ref being wrong
stmtGetInfoFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getInfo, [Conn])
    ).

stmtGetInfoStmtTypes(#{session := Conn} = TestCtx) ->
    lists:foreach(
        fun({Match, StmtStr}) ->
            Stmt = dpiCall(
                TestCtx, conn_prepareStmt, [Conn, false, StmtStr, <<>>]
            ),
            #{
                isDDL := IsDDL, isDML := IsDML,
                isPLSQL := IsPLSQL, isQuery := IsQuery,
                isReturning := IsReturning, statementType := StatementType
            } = dpiCall(TestCtx, stmt_getInfo, [Stmt]),
            dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
            ?assert(is_boolean(IsDDL)),
            ?assert(is_boolean(IsDML)),
            ?assert(is_boolean(IsPLSQL)),
            ?assert(is_boolean(IsQuery)),
            ?assert(is_boolean(IsReturning)),
            ?assertEqual(Match, StatementType) end,
            [
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
            ]
        ).

stmtGetNumQueryColumns(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    Count = dpiCall(TestCtx, stmt_getNumQueryColumns, [Stmt]),
    ?assert(is_integer(Count)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetNumQueryColumnsBadStmt(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getNumQueryColumns, [?BAD_REF])
    ).

% fails due to the statement being released too early
stmtGetNumQueryColumnsFail(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, [Conn, false, <<"it is showtime">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, stmt_getNumQueryColumns, [Stmt])
    ).

stmtBindValueByPos(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(
            TestCtx, stmt_bindValueByPos, 
            [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindValueByPosBadStmt(TestCtx) -> 
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(
            TestCtx, stmt_bindValueByPos,
            [?BAD_REF, 1, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]).

stmtBindValueByPosBadPos(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_bindValueByPos,
            [Stmt, ?BAD_INT, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindValueByPosBadType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, 1, "badAtom", BindData])
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindValueByPosBadData(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg3",
        dpiCall(
            TestCtx, stmt_bindValueByPos, 
            [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', ?BAD_REF]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to the position being invalid
stmtBindValueByPosFail(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_bindValueByPos, 
            [Stmt, -1, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindValueByName(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok, 
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindValueByNameBadStmt(TestCtx) -> 
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [?BAD_REF, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]).

stmtBindValueByNameBadPos(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve string/list name from arg1",
        dpiCall(
            TestCtx, stmt_bindValueByName,
            [Stmt, ?BAD_INT, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindValueByNameBadPosType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(
            TestCtx, stmt_bindValueByName, [Stmt, <<"A">>, "badAtom", BindData])
        ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindValueByNameBadData(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg3",
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', ?BAD_REF]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to bad data handle passing
stmtBindValueByNameFail(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg3",
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', Stmt]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindByPos(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                100, 0, false, false, null]
        ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindByPos, [Stmt, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByPosBadStmt(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
                0, false, false, null]
        ),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_bindByPos, [?BAD_REF, 1, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByPosBadPos(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
            100, 0, false, false, null]
        ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, ?BAD_INT, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByPosBadVar(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg3",
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, 1, ?BAD_REF])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

% fails due to the position being invalid
stmtBindByPosFail(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
            0, false, false, null]
        ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, -1, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByName(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
            0, false, false, null]
        ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"A">>, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByNameBadStmt(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                100, 0, false, false, null]
        ),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_bindByName, [?BAD_REF, <<"A">>, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByNameBadPos(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
                0, false, false, null]
        ),
    ?ASSERT_EX(
        "Unable to retrieve string/list name from arg1",
        dpiCall(TestCtx, stmt_bindByName, [Stmt, badBinary, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtBindByNameBadVar(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg3",
        dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"A">>, ?BAD_REF])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

% fails due to the position being invalid
stmtBindByNameFail(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into test_dpi values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = 
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
                0, false, false, null]
        ),
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"B">>, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>).

stmtDefine(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    #{var := Var, data := Data} =
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, false, null]
        ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_define, [Stmt, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtDefineBadStmt(#{session := Conn} = TestCtx) -> 
    #{var := Var, data := Data} =
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, false, null]
        ),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_define, [?BAD_REF, 1, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

stmtDefineBadPos(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    #{var := Var, data := Data} =
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, false, null]
        ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_define, [Stmt, ?BAD_INT, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtDefineBadVar(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg2",
        dpiCall(TestCtx, stmt_define, [Stmt, 1, ?BAD_REF])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to the pos being invalid
stmtDefineFail(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    #{var := Var, data := Data} =
        dpiCall(
            TestCtx, conn_newVar,
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, false, null]
        ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        #{},
        dpiCall(TestCtx, stmt_define, [Stmt, 12345, Var])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtDefineValue(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, 
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0,
                false, null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtDefineValueBadStmt(TestCtx) -> 
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(
            TestCtx, stmt_defineValue,
            [?BAD_REF, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                0, false, null]
        )
    ).

stmtDefineValueBadPos(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, ?BAD_INT, 'DPI_ORACLE_TYPE_NATIVE_INT',
                'DPI_NATIVE_TYPE_INT64', 0, false, null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).


stmtDefineValueBadOraType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        #{},
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, "badAtom", 'DPI_NATIVE_TYPE_INT64', 0, false, null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).


stmtDefineValueBadNativeType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', "badAtom", 0, false, null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).


stmtDefineValueBadSize(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint size from arg4",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                ?BAD_INT, false, null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).


stmtDefineValueBadSizeInBytes(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve bool/atom sizeIsBytes from arg5",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0,
                "badAtom", null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to invalid position
stmtDefineValueFail(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, -1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0,
                false, null]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtClose(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_close, [Stmt, <<>>])).

stmtCloseBadStmt(TestCtx) -> 
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_close, [?BAD_REF, <<>>])
    ).

stmtCloseBadTag(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve string tag from arg1",
        dpiCall(TestCtx, stmt_close, [Stmt, badBinary])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

% fails due to wrong reference
stmtCloseFail(#{session := Conn} = TestCtx) -> 
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_close, [Conn, <<>>])
    ).

%-------------------------------------------------------------------------------
% Variable APIs
%-------------------------------------------------------------------------------

varSetNumElementsInArray(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setNumElementsInArray, [Var, 100])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varSetNumElementsInArrayBadVar(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_setNumElementsInArray, [?BAD_REF, 100])
    ).

varSetNumElementsInArrayBadNumElements(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint numElements from arg1",
        dpiCall(TestCtx, var_setNumElementsInArray, [Var, ?BAD_INT])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

% fails due to invalid array size
varSetNumElementsInArrayFail(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint numElements from arg1",
        dpiCall(TestCtx, var_setNumElementsInArray, [Var, -1])
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varSetFromBytes(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setFromBytes, [Var, 0, <<"abc">>])),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varSetFromBytesBadVar(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource vat from arg0",
        dpiCall(TestCtx, var_setFromBytes, [?BAD_REF, 0, <<"abc">>])
    ).

varSetFromBytesBadPos(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, var_setFromBytes, [Var, ?BAD_INT, <<"abc">>])
    ),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varSetFromBytesBadBinary(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve binary/string value from arg2",
        dpiCall(TestCtx, var_setFromBytes, [Var, 0, badBinary])
    ),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

% fails due to position being invalid
varSetFromBytesFail(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, var_setFromBytes, [Var, -1, <<"abc">>])
    ),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varRelease(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    ?assertEqual(ok, dpiCall(TestCtx, var_release, [Var])).

varReleaseBadVar(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_release, [?BAD_REF])
    ).

% fails due to the reference being wrong
varReleaseFail(#{context := Context} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_release, [Context])
    ).

%-------------------------------------------------------------------------------
% QueryInfo APIs
%-------------------------------------------------------------------------------

queryInfoGet(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    #{name := Name, nullOk := NullOk,
        typeInfo := #{clientSizeInBytes := ClientSizeInBytes,
            dbSizeInBytes := DbSizeInBytes,
            defaultNativeTypeNum := DefaultNativeTypeNum,
            fsPrecision := FsPrecision,
            objectType := ObjectType, ociTypeCode := OciTypeCode,
            oracleTypeNum := OracleTypeNum , precision := Precision,
            scale := Scale, sizeInChars := SizeInChars
        }
    } = dpiCall(TestCtx, queryInfo_get, [QueryInfoRef]),

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
    
    dpiCall(TestCtx, queryInfo_delete, [QueryInfoRef]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

queryInfoGetBadQueryInfo(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource queryinfo from arg0",
        dpiCall(TestCtx, queryInfo_get, [?BAD_REF])
    ).

queryInfoGetFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource queryinfo from arg0",
        dpiCall(TestCtx, queryInfo_get, [Conn])
    ).

queryInfoDelete(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    ?assertEqual(ok, dpiCall(TestCtx, queryInfo_delete, [QueryInfoRef])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

queryInfoDeleteBadQueryInfo(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource queryInfo from arg0",
        dpiCall(TestCtx, queryInfo_delete, [?BAD_REF])
    ).

% fails due to getting a completely wrong reference
queryInfoDeleteFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource queryInfo from arg0",
        dpiCall(TestCtx, queryInfo_delete, [Conn])
    ).

%-------------------------------------------------------------------------------
% Data APIs
%-------------------------------------------------------------------------------

dataSetTimestamp(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(
            TestCtx, data_setTimestamp,
            [?BAD_REF, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        )
    ).

dataSetTimestampBadYear(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int year from arg1",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, ?BAD_INT, 2, 3, 4, 5, 6, 7, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadMonth(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int month from arg2",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, ?BAD_INT, 3, 4, 5, 6, 7, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadDay(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int day from arg3",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, 2, ?BAD_INT, 4, 5, 6, 7, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadHour(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int hour from arg4",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, 2, 3, ?BAD_INT, 5, 6, 7, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadMinute(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int minute from arg5",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, 2, 3, 4, ?BAD_INT, 6, 7, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadSecond(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int second from arg6",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, ?BAD_INT, 7, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadFSecond(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int fsecond from arg7",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, 2, 3, 4, 5, 6, ?BAD_INT, 8, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadTZHourOffset(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int tzHourOffset from arg8",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, 2, 3, 4, 5, 6, 7, ?BAD_INT, 9]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampBadTZMinuteOffset(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int tzMinuteOffset from arg9",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Data, 1, 2, 3, 4, 5, 6, 7, 8, ?BAD_INT]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to the Data ref passed being completely wrong
% (it doesn't seem to mind the nonsense parameters. Year -1234567? Sure.
% Timezone of -22398 hours and 3239 minutes? No problem)
dataSetTimestampFail(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(
            TestCtx, data_setTimestamp,
            [Conn, -1234567, 2, 3, 4, 5, 6, 7, -22398, 3239]
        )
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetTimestampViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetIntervalDS(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalDSBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIntervalDS, [?BAD_REF, 1, 2, 3, 4, 5])
    ).

dataSetIntervalDSBadDays(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int days from arg1",
        dpiCall(TestCtx, data_setIntervalDS, [Data, ?BAD_INT, 2, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalDSBadHours(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int hours from arg2",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, ?BAD_INT, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalDSBadMinutes(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int minutes from arg3",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, ?BAD_INT, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalDSBadSeconds(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int seconds from arg4",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, ?BAD_INT, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalDSBadFSeconds(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int fseconds from arg5",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, ?BAD_INT])
    ),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to the Data ref passed being completely wrong
dataSetIntervalDSFail(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIntervalDS, [Conn, 1, 2, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalDSViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetIntervalYM(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, 2])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalYMBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIntervalYM, [?BAD_REF, 1, 2])
    ).

dataSetIntervalYMBadYears(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int years from arg1",
        dpiCall(TestCtx, data_setIntervalYM, [Data, ?BAD_INT, 2])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalYMBadMonths(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int months from arg2",
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, ?BAD_INT])
    ),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to the Data ref passed being completely wrong
dataSetIntervalYMFail(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIntervalYM, [Conn, 1, 2])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIntervalYMViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, 2])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetInt64(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setInt64, [Data, 1])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetInt64BadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setInt64, [?BAD_REF, 1])
    ).

dataSetInt64BadAmount(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int amount from arg1",
        dpiCall(TestCtx, data_setInt64, [Data, ?BAD_INT])
    ),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to the Data ref passed being completely wrong
dataSetInt64Fail(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setInt64, [Conn, 1])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetInt64ViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setInt64, [Data, 1])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetBytes(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setBytes, [Data, <<"my string">>])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetBytesBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setBytes, [?BAD_REF, <<"my string">>])
    ).

dataSetBytesBadBinary(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve binary data from arg1",
        dpiCall(TestCtx, data_setBytes, [Data, badBinary])
    ),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to the Data ref passed being completely wrong
dataSetBytesFail(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setBytes, [Conn, <<"my string">>])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIsNullTrue(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIsNull, [Data, true])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIsNullFalse(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIsNull, [Data, false])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIsNullBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIsNull, [?BAD_REF, 1])
    ).

dataSetIsNullBadIsNull(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve bool/atom isNull from arg1",
        dpiCall(TestCtx, data_setIsNull, [Data, "not an atom"])
    ),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to the Data ref passed being completely wrong
dataSetIsNullFail(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIsNull, [Conn, 1])
    ),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIsNullViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIsNull, [Data, true])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetNull(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_get, [Data])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetTInt64(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetUint64(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_UINT', 'DPI_NATIVE_TYPE_UINT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetFloat(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_FLOAT', 'DPI_NATIVE_TYPE_FLOAT', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetDouble(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 1,
            1, true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetBinary(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, 
        dpiCall(TestCtx, var_setFromBytes, [Var, 0, <<"my string">>])
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetRowid(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select rowid from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    dpiCall(TestCtx, stmt_fetch, [Stmt]),
    #{data := Data} = dpiCall(TestCtx, stmt_getQueryValue, [Stmt, 1]),
    ?assert(is_binary(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

dataGetTimestamp(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP',
            1, 100, true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    #{year := Year, month := Month, day := Day, hour := Hour, minute := Minute,
        second := Second, fsecond := Fsecond, tzHourOffset := TzHourOffset,
        tzMinuteOffset := TzMinuteOffset
    } = dpiCall(TestCtx, data_get, [Data]),
    ?assert(is_integer(Year)),
    ?assert(is_integer(Month)),
    ?assert(is_integer(Day)),
    ?assert(is_integer(Hour)),
    ?assert(is_integer(Minute)),
    ?assert(is_integer(Second)),
    ?assert(is_integer(Fsecond)),
    ?assert(is_integer(TzHourOffset)),
    ?assert(is_integer(TzMinuteOffset)),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetIntervalDS(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS',
            1, 100, true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    #{days := Days, hours := Hours, minutes := Minutes, 
        seconds := Seconds, fseconds := Fseconds} =
        dpiCall(TestCtx, data_get, [Data]),
    ?assert(is_integer(Days)),
    ?assert(is_integer(Hours)),
    ?assert(is_integer(Minutes)),
    ?assert(is_integer(Seconds)),
    ?assert(is_integer(Fseconds)),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).


dataGetIntervalYM(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 100, true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    #{years := Years, months := Months} =
        dpiCall(TestCtx, data_get, [Data]),
    ?assert(is_integer(Years)),
    ?assert(is_integer(Months)),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetStmt(#{session := Conn} = TestCtx) ->
    CreateStmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure ORANIF_TEST_1
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select 1 from dual;
            end ORANIF_TEST_1;">>,
        <<>>
    ]),
    dpiCall(TestCtx, stmt_execute, [CreateStmt, []]),
    dpiCall(TestCtx, stmt_close, [CreateStmt, <<>>]),

    #{var := VarStmt, data := [DataStmt]} = dpiCall(TestCtx, conn_newVar, [
        Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0,
        false, false, null
    ]),
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false, <<"begin ORANIF_TEST_1(:cursor); end;">>, <<>>
    ]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"cursor">>, VarStmt]),

    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    % first-time get
    ?assert(is_reference(dpiCall(TestCtx, data_get, [DataStmt]))),
    % cached re-get
    ?assert(is_reference(dpiCall(TestCtx, data_get, [DataStmt]))),
    dpiCall(TestCtx, data_release, [DataStmt]),
    dpiCall(TestCtx, var_release, [VarStmt]).

dataGetStmtChange(#{session := Conn} = TestCtx) ->
    CreateStmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure ORANIF_TEST_1
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select 1 from dual;
            end ORANIF_TEST_1;">>,
        <<>>
    ]),
    dpiCall(TestCtx, stmt_execute, [CreateStmt, []]),
    dpiCall(TestCtx, stmt_close, [CreateStmt, <<>>]),

    CreateStmt2 = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure ORANIF_TEST_2
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select 2 from dual;
            end ORANIF_TEST_2;">>,
        <<>>
    ]),
    dpiCall(TestCtx, stmt_execute, [CreateStmt2, []]),
    dpiCall(TestCtx, stmt_close, [CreateStmt2, <<>>]),

    #{var := VarStmt, data := [DataStmt]} = dpiCall(TestCtx, conn_newVar, [
        Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0,
        false, false, null
    ]),
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false, <<"begin ORANIF_TEST_1(:cursor); end;">>, <<>>
    ]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"cursor">>, VarStmt]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),

    % first-time get
    ?assert(is_reference(dpiCall(TestCtx, data_get, [DataStmt]))),

    Stmt2 = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false, <<"begin ORANIF_TEST_2(:cursor); end;">>, <<>>
    ]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt2, <<"cursor">>, VarStmt]),
    dpiCall(TestCtx, stmt_execute, [Stmt2, []]),

    % "ref cursor changed"
    ?assert(is_reference(dpiCall(TestCtx, data_get, [DataStmt]))),
    dpiCall(TestCtx, data_release, [DataStmt]),
    dpiCall(TestCtx, var_release, [VarStmt]),

    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    dpiCall(TestCtx, stmt_close, [Stmt2, <<>>]).

dataGetBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_get, [?BAD_REF])
    ).

% fails due to completely wrong reference
dataGetFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_get, [Conn])
    ).

dataGetUnsupportedType(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, 
        [Conn, 'DPI_ORACLE_TYPE_CLOB', 'DPI_NATIVE_TYPE_LOB',
            1, 0, false, false, null]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?ASSERT_EX(
        "Unsupported nativeTypeNum",
        dpiCall(TestCtx, data_get, [Data])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetInt64(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_getInt64, [Data]))),
    dpiCall(TestCtx, data_release, [Data]).

dataGetInt64BadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getInt64, [?BAD_REF])
    ).

% fails due to completely wrong reference
dataGetInt64Fail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getInt64, [Conn])
    ).

dataGetInt64Null(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_getInt64, [Data])),
    dpiCall(TestCtx, data_release, [Data]).

dataGetInt64ViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_getInt64, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

% no non-pointer test for this one
dataGetBytes(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(TestCtx, data_getBytes, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]).

dataGetBytesBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getBytes, [?BAD_REF])
    ).

dataGetBytesNull(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_getBytes, [Data])),
    dpiCall(TestCtx, data_release, [Data]).

% fails due to completely wrong reference
dataGetBytesFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getBytes, [Conn])
    ).

dataRelease(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok, dpiCall(TestCtx, data_release, [Data])).

dataReleaseBadData(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_release, [?BAD_REF])
    ).

% fails due to completely wrong reference
dataReleaseFail(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_release, [Conn])
    ).

dataReleaseViaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_release, [Data])
    ),
    dpiCall(TestCtx, var_release, [Var]).

%-------------------------------------------------------------------------------
% eunit infrastructure callbacks
%-------------------------------------------------------------------------------
-define(SLAVE, oranif_slave).

setup(#{safe := false}) ->
    ok = dpi:load_unsafe(),
    #{safe => false};
setup(#{safe := true}) ->
    SlaveNode = dpi:load(?SLAVE),
    pong = net_adm:ping(SlaveNode),
    #{safe => true, node => SlaveNode}.

setup_context(TestCtx) ->
    SlaveCtx = setup(TestCtx),
    SlaveCtx#{
        context => dpiCall(
            SlaveCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
        )
    }.

setup_connecion(TestCtx) ->
    ContextCtx = #{context := Context} = setup_context(TestCtx),
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ContextCtx#{
        session => dpiCall(
            ContextCtx, conn_create, [
                Context, User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
            ]
        )
    }.

cleanup(#{session := Connnnection} = Ctx) ->
    dpiCall(Ctx, conn_close, [Connnnection, [], <<>>]),
    cleanup(maps:without([session], Ctx));
cleanup(#{context := Context} = Ctx) ->
    dpiCall(Ctx, context_destroy, [Context]),
    cleanup(maps:without([context], Ctx));
cleanup(_) -> ok.

%-------------------------------------------------------------------------------
% Internal functions
%-------------------------------------------------------------------------------

-define(NO_CONTEXT_TESTS, [
    ?F(contextCreate),
    ?F(contextCreateBadMaj),
    ?F(contextCreateBadMin),
    ?F(contextCreateFail),
    ?F(contextDestroy),
    ?F(contextDestroyBadContext),
    ?F(contextDestroyBadContextState),
    ?F(contextGetClientVersion),
    ?F(contextGetClientVersionBadContext),
    ?F(contextGetClientVersionFail)
]).

-define(AFTER_CONTEXT_TESTS, [
    ?F(connCreate),
    ?F(connCreateBadContext),
    ?F(connCreateBadUsername),
    ?F(connCreateBadPass),
    ?F(connCreateBadTNS),
    ?F(connCreateBadUsername),
    ?F(connCreateBadParams),
    ?F(connCreateBadEncoding),
    ?F(connCreateBadNencoding),
    ?F(connCreateFail)
]).

-define(AFTER_CONNECTION_TESTS, [
    ?F(connPrepareStmt),
    ?F(connPrepareStmtEmptyTag),
    ?F(connPrepareStmtBadConn),
    ?F(connPrepareStmtBadScrollable),
    ?F(connPrepareStmtBadSQL),
    ?F(connPrepareStmtBadTag),
    ?F(connPrepareStmtFail),
    ?F(connNewVar),
    ?F(connNewVarBadConn),
    ?F(connNewVarBadOraType),
    ?F(connNewVarBadDpiType),
    ?F(connNewVarBadArraySize),
    ?F(connNewVarBadSize),
    ?F(connNewVarBadSizeIsBytes),
    ?F(connNewVarBadArray),
    ?F(connNewVarBadObjType),
    ?F(connNewVarFail),
    ?F(connCommit),
    ?F(connCommitBadConn),
    ?F(connCommitFail),
    ?F(connRollback),
    ?F(connRollbackBadConn),
    ?F(connRollbackFail),
    ?F(connPing),
    ?F(connPingBadConn),
    ?F(connPingFail),
    ?F(connClose),
    ?F(connCloseWithModes),
    ?F(connCloseBadConn),
    ?F(connCloseBadModes),
    ?F(connCloseBadModesInside),
    ?F(connCloseInvalidMode),
    ?F(connCloseBadTag),
    ?F(connCloseFail),
    ?F(connGetServerVersion),
    ?F(connGetServerVersionBadConn),
    ?F(connGetServerVersionFail),
    ?F(connSetClientIdentifier),
    ?F(connSetClientIdentifierBadConn),
    ?F(connSetClientIdentifierBadValue),
    ?F(stmtExecute),
    ?F(stmtExecuteWithModes),
    ?F(stmtExecutebadStmt),
    ?F(stmtExecuteBadModes),
    ?F(stmtExecuteBadModesInside),
    ?F(stmtExecuteFail),
    ?F(stmtFetch),
    ?F(stmtFetchBadStmt),
    ?F(stmtFetchBadRes),
    ?F(stmtGetQueryValue),
    ?F(stmtGetQueryValueBadStmt),
    ?F(stmtGetQueryValueBadPos),
    ?F(stmtGetQueryValueFail),
    ?F(stmtGetQueryInfo),
    ?F(stmtGetQueryInfoBadStmt),
    ?F(stmtGetQueryInfoBadPos),
    ?F(stmtGetQueryInfoFail),
    ?F(stmtGetInfoBadStmt),
    ?F(stmtGetInfoFail),
    ?F(stmtGetInfoStmtTypes),
    ?F(stmtGetNumQueryColumns),
    ?F(stmtGetNumQueryColumnsBadStmt),
    ?F(stmtGetNumQueryColumnsFail),
    ?F(stmtBindValueByPos),
    ?F(stmtBindValueByPosBadStmt),
    ?F(stmtBindValueByPosBadPos),
    ?F(stmtBindValueByPosBadType),
    ?F(stmtBindValueByPosBadData),
    ?F(stmtBindValueByPosFail),
    ?F(stmtBindValueByName),
    ?F(stmtBindValueByNameBadStmt),
    ?F(stmtBindValueByNameBadPos),
    ?F(stmtBindValueByNameBadPosType),
    ?F(stmtBindValueByNameBadData),
    ?F(stmtBindValueByNameFail),
    ?F(stmtBindByPos),
    ?F(stmtBindByPosBadStmt),
    ?F(stmtBindByPosBadPos),
    ?F(stmtBindByPosBadVar),
    ?F(stmtBindByPosFail),
    ?F(stmtBindByName),
    ?F(stmtBindByNameBadStmt),
    ?F(stmtBindByNameBadPos),
    ?F(stmtBindByNameBadVar),
    ?F(stmtBindByNameFail),
    ?F(stmtDefine),
    ?F(stmtDefineBadStmt),
    ?F(stmtDefineBadPos),
    ?F(stmtDefineBadVar),
    ?F(stmtDefineFail),
    ?F(stmtDefineValue),
    ?F(stmtDefineValueBadStmt),
    ?F(stmtDefineValueBadPos),
    ?F(stmtDefineValueBadOraType),
    ?F(stmtDefineValueBadNativeType),
    ?F(stmtDefineValueBadSize),
    ?F(stmtDefineValueBadSizeInBytes),
    ?F(stmtDefineValueFail),
    ?F(stmtClose),
    ?F(stmtCloseBadStmt),
    ?F(stmtCloseBadTag),
    ?F(stmtCloseFail),
    ?F(varSetNumElementsInArray),
    ?F(varSetNumElementsInArrayBadVar),
    ?F(varSetNumElementsInArrayBadNumElements),
    ?F(varSetNumElementsInArrayFail),
    ?F(varSetFromBytes),
    ?F(varSetFromBytesBadVar),
    ?F(varSetFromBytesBadPos),
    ?F(varSetFromBytesBadBinary),
    ?F(varSetFromBytesFail),
    ?F(varRelease),
    ?F(varReleaseBadVar),
    ?F(varReleaseFail),
    ?F(queryInfoGet),
    ?F(queryInfoGetBadQueryInfo),
    ?F(queryInfoGetFail),
    ?F(queryInfoDelete),
    ?F(queryInfoDeleteBadQueryInfo),
    ?F(queryInfoDeleteFail),
    ?F(dataSetTimestamp),
    ?F(dataSetTimestampBadData),
    ?F(dataSetTimestampBadYear),
    ?F(dataSetTimestampBadMonth),
    ?F(dataSetTimestampBadDay),
    ?F(dataSetTimestampBadHour),
    ?F(dataSetTimestampBadMinute),
    ?F(dataSetTimestampBadSecond),
    ?F(dataSetTimestampBadFSecond),
    ?F(dataSetTimestampBadTZHourOffset),
    ?F(dataSetTimestampBadTZMinuteOffset),
    ?F(dataSetTimestampFail),
    ?F(dataSetTimestampViaPointer),
    ?F(dataSetIntervalDS),
    ?F(dataSetIntervalDSBadData),
    ?F(dataSetIntervalDSBadDays),
    ?F(dataSetIntervalDSBadHours),
    ?F(dataSetIntervalDSBadMinutes),
    ?F(dataSetIntervalDSBadSeconds),
    ?F(dataSetIntervalDSBadFSeconds),
    ?F(dataSetIntervalDSFail),
    ?F(dataSetIntervalDSViaPointer),
    ?F(dataSetIntervalYM),
    ?F(dataSetIntervalYMBadData),
    ?F(dataSetIntervalYMBadYears),
    ?F(dataSetIntervalYMBadMonths),
    ?F(dataSetIntervalYMFail),
    ?F(dataSetIntervalYMViaPointer),
    ?F(dataSetInt64),
    ?F(dataSetInt64BadData),
    ?F(dataSetInt64BadAmount),
    ?F(dataSetInt64Fail),
    ?F(dataSetInt64ViaPointer),
    ?F(dataSetBytes),
    ?F(dataSetBytesBadData),
    ?F(dataSetBytesBadBinary),
    ?F(dataSetBytesFail),
    ?F(dataSetIsNullTrue),
    ?F(dataSetIsNullFalse),
    ?F(dataSetIsNullBadData),
    ?F(dataSetIsNullBadIsNull),
    ?F(dataSetIsNullFail),
    ?F(dataSetIsNullViaPointer),
    ?F(dataGetNull),
    ?F(dataGetTInt64),
    ?F(dataGetUint64),
    ?F(dataGetFloat),
    ?F(dataGetDouble),
    ?F(dataGetBinary),
    ?F(dataGetRowid),
    ?F(dataGetTimestamp),
    ?F(dataGetIntervalDS),
    ?F(dataGetIntervalYM),
    ?F(dataGetStmt),
    ?F(dataGetStmtChange),
    ?F(dataGetBadData),
    ?F(dataGetFail),
    ?F(dataGetUnsupportedType),
    ?F(dataGetInt64),
    ?F(dataGetInt64BadData),
    ?F(dataGetInt64Fail),
    ?F(dataGetInt64Null),
    ?F(dataGetInt64ViaPointer),
    ?F(dataGetBytes),
    ?F(dataGetBytesBadData),
    ?F(dataGetBytesFail),
    ?F(dataGetBytesNull),
    ?F(dataRelease),
    ?F(dataReleaseBadData),
    ?F(dataReleaseFail),
    ?F(dataReleaseViaPointer)
]).

dpiCall(#{safe := true, node := Node}, F, A) ->
    case dpi:safe(Node, dpi, F, A) of
        {error, _, _, _} = Error -> error(Error);
        Result -> Result
    end;
dpiCall(#{safe := false}, F, A) -> apply(dpi, F, A).

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

%-------------------------------------------------------------------------------
% Unit Tests
%-------------------------------------------------------------------------------

unsafe_no_context_test_() ->
    {
        setup,
        fun() -> setup(#{safe => false}) end,
        fun cleanup/1,
        ?W(?NO_CONTEXT_TESTS)
    }.

unsafe_context_test_() ->
    {
        setup,
        fun() -> setup_context(#{safe => false}) end,
        fun cleanup/1,
        ?W(?AFTER_CONTEXT_TESTS)
    }.

unsafe_session_test_() ->
    {
        setup,
        fun() -> setup_connecion(#{safe => false}) end,
        fun cleanup/1,
        ?W(?AFTER_CONNECTION_TESTS)
    }.

no_context_test_() ->
    {
        setup,
        fun() -> setup(#{safe => true}) end,
        fun cleanup/1,
        ?W(?NO_CONTEXT_TESTS)
    }.

context_test_() ->
    {
        setup,
        fun() -> setup_context(#{safe => true}) end,
        fun cleanup/1,
        ?W(?AFTER_CONTEXT_TESTS)
    }.

session_test_() ->
    {
        setup,
        fun() -> setup_connecion(#{safe => true}) end,
        fun cleanup/1,
        ?W(?AFTER_CONNECTION_TESTS)
    }.
