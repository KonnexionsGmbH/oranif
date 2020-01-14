-module(cover_tests).
-include_lib("eunit/include/eunit.hrl").

%-------------------------------------------------------------------------------
% MACROs
%-------------------------------------------------------------------------------

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

-define(BAD_INT, -16#FFFFFFFFFFFFFFFF1).
-define(BAD_FLOAT, notEvenAFloatAtAll).
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
    ?ASSERT_EX(
        "Unable to retrieve uint major from arg0",
        dpiCall(TestCtx, context_create, [?BAD_INT, ?DPI_MINOR_VERSION])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint minor from arg1",
        dpiCall(TestCtx, context_create, [?DPI_MAJOR_VERSION, ?BAD_INT])
    ),
    % fails due to nonsense major version
    ?ASSERT_EX(
        #{message := "DPI-1020: version 1337.0 is not supported by ODPI-C"
                     " library version 3.0"},
        dpiCall(TestCtx, context_create, [1337, ?DPI_MINOR_VERSION])
    ),
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?assert(is_reference(Context)),
    dpiCall(TestCtx, context_destroy, [Context]).

contextDestroy(TestCtx) ->
   ?ASSERT_EX(
       "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, context_destroy, [?BAD_REF])
    ),
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    % destroy the context
    ?assertEqual(ok, dpiCall(TestCtx, context_destroy, [Context])),
    % try to destroy it again
    ?ASSERT_EX(
        #{message := "DPI-1002: invalid dpiContext handle"},
        dpiCall(TestCtx, context_destroy, [Context])
    ).

contextGetClientVersion(TestCtx) -> 
    ?ASSERT_EX(
        "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, context_getClientVersion, [?BAD_REF])
    ),
    % fails due to a wrong handle being passed
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, context_getClientVersion, [BindData])
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    #{releaseNum := CRNum, versionNum := CVNum, fullVersionNum := CFNum} =
        dpiCall(TestCtx, context_getClientVersion, [Context]),
    ?assert(is_integer(CRNum)),
    ?assert(is_integer(CVNum)),
    ?assert(is_integer(CFNum)),
    dpiCall(TestCtx, context_destroy, [Context]).

%-------------------------------------------------------------------------------
% Connection APIs
%-------------------------------------------------------------------------------

connCreate(TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    CP = #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
    ?ASSERT_EX(
        "Unable to retrieve resource context from arg0",
        dpiCall(TestCtx, conn_create, [?BAD_REF, User, Password, Tns, CP, #{}])
    ),
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?ASSERT_EX(
        "Unable to retrieve string/binary userName from arg1",
        dpiCall(TestCtx, conn_create, [Context, badBin, Password, Tns, CP, #{}])
    ),
    ?ASSERT_EX(
        "Unable to retrieve string/binary password from arg2",
        dpiCall(TestCtx, conn_create, [Context, User, badBin, Tns, CP, #{}])
    ),
    ?ASSERT_EX(
        "Unable to retrieve string/binary connectString from arg3",
        dpiCall(
            TestCtx, conn_create, [Context, User, Password, badBin, CP, #{}]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve map commonParams from arg4",
        dpiCall(
            TestCtx, conn_create, [Context, User, Password, Tns, badMap, #{}]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve string",
        dpiCall(
            TestCtx, conn_create,
            [Context, User, Password, Tns, CP#{encoding => badList}, #{}]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve string",
        dpiCall(
            TestCtx, conn_create,
            [Context, User, Password, Tns, CP#{nencoding => badList}, #{}]
        )
    ),
    ?ASSERT_EX(
        #{message := "ORA-01017: invalid username/password; logon denied"},
        dpiCall(TestCtx, conn_create, [Context, <<"C">>, <<"N">>, Tns, CP, #{}])
    ),
    Conn = dpiCall(
        TestCtx, conn_create, [Context, User, Password, Tns, CP, #{}]
    ),
    ?assert(is_reference(Conn)),
    dpiCall(TestCtx, conn_close, [Conn, [], <<>>]),
    dpiCall(TestCtx, context_destroy, [Context]).

connPrepareStmt(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(
            TestCtx, conn_prepareStmt, [?BAD_REF, false, <<"miau">>, <<>>]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve bool/atom scrollable from arg1",
        dpiCall(
            TestCtx, conn_prepareStmt, [Conn, "badAtom", <<"miau">>, <<>>]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve binary/string sql from arg2",
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, badBinary, <<>>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve binary/string tag from arg3",
        dpiCall(
            TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, badBinary]
        )
    ),
    % fails due to both SQL and Tag being empty
    ?ASSERT_EX(
        #{message := "ORA-24373: invalid length specified for statement"},
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<>>, <<>>])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, <<"foo">>]
    ),
    ?assert(is_reference(Stmt)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

connNewVar(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                ?BAD_REF, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE',
                'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiOracleType type",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                Conn, 'BAD_DPI_ORACLE_TYPE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0,
                false, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'BAD_DPI_NATIVE_TYPE', 100, 0, false, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint size from arg3",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                ?BAD_INT, 0, false, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint size from arg4",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 
                100, ?BAD_INT, false, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve atom sizeIsBytes from arg5",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, "badAtom", false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve atom isArray from arg6",
        dpiCall(
            TestCtx, conn_newVar, 
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, "badAtom", null
            ]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve atom objType from arg7",
        dpiCall(
            TestCtx, conn_newVar,
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                100, 0, false, false, "badAtom"
            ]
        )
    ),
    % fails due to array size being 0
    ?ASSERT_EX(
        #{message := "DPI-1031: array size cannot be zero"},
        dpiCall(
            TestCtx, conn_newVar,
            [
                Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
                0, 0, false, false, null
            ]
        )
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
            100, 0, false, false, null
        ]
    ),
    ?assert(is_reference(Var)),
    ?assert(is_list(Data)),
    [FirstData | _] = Data,
    ?assert(is_reference(FirstData)),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

connNewTempLob(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_newTempLob, [?BAD_REF, 'DPI_ORACLE_TYPE_CLOB'])
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiOracleType type",
        dpiCall(TestCtx, conn_newTempLob, [Conn, 'BAD_DPI_ORACLE_TYPE'])
    ),
    % fails due to the type being wrong
    ?ASSERT_EX(
        #{message := "DPI-1021: Oracle type 2008 is invalid"},
        dpiCall(
            TestCtx, conn_newTempLob, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE']
        )
    ),
    Lob = dpiCall(TestCtx, conn_newTempLob, [Conn, 'DPI_ORACLE_TYPE_CLOB']),
    ?assert(is_reference(Lob)),
    dpiCall(TestCtx, lob_release, [Lob]),
    ok.

connCommit(#{context := Context, session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_commit, [?BAD_REF])
    ),
    % fails due to the reference being wrong
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_commit, [Context])
    ),
    Result = dpiCall(TestCtx, conn_commit, [Conn]),
    ?assertEqual(ok, Result).
  
connRollback(#{context := Context, session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_rollback, [?BAD_REF])
    ),
    % fails due to the reference being wrong
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_rollback, [Context])
    ),
    Result = dpiCall(TestCtx, conn_rollback, [Conn]),
    ?assertEqual(ok, Result).
  
connPing(#{context := Context, session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_ping, [?BAD_REF])
    ),
    % fails due to the reference being wrong
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_ping, [Context])
    ),
    Result = dpiCall(TestCtx, conn_ping, [Conn]),
    ?assertEqual(ok, Result).
  
connClose(#{context := Context, session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_close, [?BAD_REF, [], <<>>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve atom list modes, not a list from arg1",
        dpiCall(TestCtx, conn_close, [Conn, badList, <<>>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve mode list value from arg1",
        dpiCall(TestCtx, conn_close, [Conn, ["badAtom"], <<>>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve DPI_MODE atom from arg1",
        dpiCall(TestCtx, conn_close, [Conn, [wrongAtom], <<>>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve binary/string tag from arg2",
        dpiCall(TestCtx, conn_close, [Conn, [], badBinary])
    ),
    % fails due to the reference being wrong
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_close, [Context, [], <<>>])
    ),
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn1 = dpiCall(
        TestCtx, conn_create,
        [
            Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
        ]
    ),
    % the other two don't work without a session pool
    Result = dpiCall(
        TestCtx, conn_close, [Conn1, ['DPI_MODE_CONN_CLOSE_DEFAULT'], <<>>]
    ),
    ?assertEqual(ok, Result).

connGetServerVersion(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_getServerVersion, [?BAD_REF])
    ),
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

connSetClientIdentifier(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource connection from arg0",
        dpiCall(TestCtx, conn_setClientIdentifier, [?BAD_REF, <<"myCoolConn">>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve string/binary value from arg1",
        dpiCall(TestCtx, conn_setClientIdentifier, [Conn, badBinary])
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, conn_setClientIdentifier, [Conn, <<"myCoolConn">>])
    ).

%-------------------------------------------------------------------------------
% Statement APIs
%-------------------------------------------------------------------------------

stmtExecuteMany_varGetReturnedData(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_executeMany, [?BAD_REF, [], 0])
    ),
    StmtDrop = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"drop table oranif_test">>, <<>>]
    ),
    catch dpiCall(TestCtx, stmt_execute, [StmtDrop, []]),
    catch dpiCall(TestCtx, stmt_close, [StmtDrop, <<>>]),
    StmtCreate = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"create table oranif_test (col1 varchar2(100))">>, <<>>]
    ),
    0 = dpiCall(TestCtx, stmt_execute, [StmtCreate, []]),
    ok = dpiCall(TestCtx, stmt_close, [StmtCreate, <<>>]),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [
            Conn, false,
            <<
                "insert into oranif_test values(:col1)"
                " returning rowid into :rid"
            >>,
            <<>>
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve list of atoms from arg1",
        dpiCall(TestCtx, stmt_executeMany, [Stmt, badList, 0])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint32 numIters from arg2",
        dpiCall(TestCtx, stmt_executeMany, [Stmt, [], ?BAD_INT])
    ),
    ?ASSERT_EX(
        "mode must be a list of atoms",
        dpiCall(TestCtx, stmt_executeMany, [Stmt, ["badAtom"], 0])
    ),
    #{var := Var} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 10,
            10, true, false, null
        ]
    ),
    #{var := VarRowId} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_ROWID', 'DPI_NATIVE_TYPE_ROWID',
            10, 0, false, false, null
        ]
    ),
    dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"col1">>, Var]),
    dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"rid">>, VarRowId]),
    Data = lists:seq($0, $z),
    DataLen = length(Data),
    Indices = lists:seq(0, 9),
    rand:seed(exsplus, {0, 0, 0}),
    [dpiCall(
        TestCtx, var_setFromBytes,
        [
            Var, Idx,
            << <<(lists:nth(rand:uniform(DataLen), Data))>>
                || _ <- lists:seq(1, 10) >>
        ]
    ) || Idx <- Indices],
    ?assertEqual(
        ok,
        dpiCall(
            TestCtx, stmt_executeMany,
            [Stmt, ['DPI_MODE_EXEC_COMMIT_ON_SUCCESS'], 10]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_getReturnedData, [?BAD_REF, 0])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, var_getReturnedData, [VarRowId, ?BAD_INT])
    ),
    [begin
        Result = dpiCall(TestCtx, var_getReturnedData, [VarRowId, Idx]),
        ?assertMatch(#{numElements := 1, data := [_]}, Result),
        [D] = maps:get(data, Result),
        ?assert(byte_size(dpiCall(TestCtx, data_get, [D])) > 0)
    end || Idx <- Indices],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, var_release, [VarRowId]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtExecute(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_execute, [?BAD_REF, []])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve list of atoms from arg1",
        dpiCall(TestCtx, stmt_execute, [Stmt, badList])
    ),
    ?ASSERT_EX(
        "mode must be a list of atoms",
        dpiCall(TestCtx, stmt_execute, [Stmt, ["badAtom"]])
    ),
    ?assertEqual(
        1,
        dpiCall(
            TestCtx, stmt_execute, [Stmt, ['DPI_MODE_EXEC_DEFAULT']]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    % fails due to the SQL being invalid
    Stmt1 = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"all your base are belong to us">>, <<>>]
    ),
    ?ASSERT_EX(
        #{message := "ORA-00900: invalid SQL statement"},
        dpiCall(TestCtx, stmt_execute, [Stmt1, []])
    ),
    dpiCall(TestCtx, stmt_close, [Stmt1, <<>>]).

stmtFetch(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_fetch, [?BAD_REF])
    ),
    % fails due to the reference being of the wrong type
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_fetch, [Conn])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    #{found := Found, bufferRowIndex := BufferRowIndex} =
        dpiCall(TestCtx, stmt_fetch, [Stmt]),
    ?assert(is_atom(Found)),
    ?assert(is_integer(BufferRowIndex)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetQueryValue(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getQueryValue, [?BAD_REF, 1])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, ?BAD_INT])
    ),
    % fails due to the fetch not being done
    ?ASSERT_EX(
        #{message := "DPI-1029: no row currently fetched"},
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, 1])
    ),
    dpiCall(TestCtx, stmt_fetch, [Stmt]),
    #{nativeTypeNum := Type, data := Result} = dpiCall(
        TestCtx, stmt_getQueryValue, [Stmt, 1]
    ),
    ?assert(is_atom(Type)),
    ?assert(is_reference(Result)),
    dpiCall(TestCtx, data_release, [Result]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetQueryInfo(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getQueryInfo, [?BAD_REF, 1])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, ?BAD_INT])
    ),
    #{
        name := Name, nullOk := NullOk, typeInfo := #{
            clientSizeInBytes := ClientSizeInBytes,
            dbSizeInBytes := DbSizeInBytes,
            defaultNativeTypeNum := DefaultNativeTypeNum,
            fsPrecision := FsPrecision,
            objectType := ObjectType, ociTypeCode := OciTypeCode,
            oracleTypeNum := OracleTypeNum , precision := Precision,
            scale := Scale, sizeInChars := SizeInChars
        }
    } = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
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
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtGetInfo(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getInfo, [?BAD_REF])
    ),
    % fails due to the ref being wrong
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getInfo, [Conn])
    ),
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
            ?assertEqual(Match, StatementType)
        end,
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
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_getNumQueryColumns, [?BAD_REF])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"select 1337 from dual">>, <<>>]
    ),
    Count = dpiCall(TestCtx, stmt_getNumQueryColumns, [Stmt]),
    ?assert(is_integer(Count)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    % fails due to the statement already released
    ?ASSERT_EX(
        #{message := "DPI-1039: statement was already closed"},
        dpiCall(TestCtx, stmt_getNumQueryColumns, [Stmt])
    ).

stmtBindValueByPos(#{session := Conn} = TestCtx) -> 
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(
            TestCtx, stmt_bindValueByPos,
            [?BAD_REF, 1, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into dual values (:A)">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_bindValueByPos,
            [Stmt, ?BAD_INT, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, 1, "badAtom", BindData])
    ),
    ?assertEqual(ok,
        dpiCall(
            TestCtx, stmt_bindValueByPos, 
            [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg3",
        dpiCall(
            TestCtx, stmt_bindValueByPos, 
            [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', ?BAD_REF]
        )
    ),
    % fails due to the position being invalid
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_bindValueByPos, 
            [Stmt, -1, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindValueByName(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into dual values (:A)">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg3",
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', ?BAD_REF]
        )
    ),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [?BAD_REF, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve string/list name from arg1",
        dpiCall(
            TestCtx, stmt_bindValueByName,
            [Stmt, ?BAD_INT, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(
            TestCtx, stmt_bindValueByName, [Stmt, <<"A">>, "badAtom", BindData]
        )
    ),
    % fails due to bad data handle passing
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg3",
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', ?BAD_REF]
        )
    ),
    ?assertEqual(ok, 
        dpiCall(
            TestCtx, stmt_bindValueByName, 
            [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData]
        )
    ),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindByPos(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt, 
        [Conn, false, <<"insert into dual values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, 
        [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
            0, false, false, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_bindByPos, [?BAD_REF, 1, Var])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, ?BAD_INT, Var])
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg3",
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, 1, ?BAD_REF])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindByPos, [Stmt, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtBindByName(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"insert into dual values (:A)">>, <<>>]
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100,
            0, false, false, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_bindByName, [?BAD_REF, <<"A">>, Var])
    ),
    ?ASSERT_EX(
        "Unable to retrieve string/list name from arg1",
        dpiCall(TestCtx, stmt_bindByName, [Stmt, badBinary, Var])
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg3",
        dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"A">>, ?BAD_REF])
    ),
    % fails due to the position being invalid
    ?ASSERT_EX(
        #{message := "ORA-01036: illegal variable name/number"},
        dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"B">>, Var])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"A">>, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtDefine(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
            100, 0, false, false, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_define, [?BAD_REF, 1, Var])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, stmt_define, [Stmt, ?BAD_INT, Var])
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg2",
        dpiCall(TestCtx, stmt_define, [Stmt, 1, ?BAD_REF])
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    % fails due to the pos being invalid
    ?ASSERT_EX(
        #{message := "DPI-1028: query position 12345 is invalid"},
        dpiCall(TestCtx, stmt_define, [Stmt, 12345, Var])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_define, [Stmt, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtDefineValue(#{session := Conn} = TestCtx) -> 
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(
            TestCtx, stmt_defineValue,
            [
                ?BAD_REF, 1, 'DPI_ORACLE_TYPE_NATIVE_INT',
                'DPI_NATIVE_TYPE_INT64', 0, false, null
            ]
        )
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(
            TestCtx, stmt_defineValue,
            [
                Stmt, ?BAD_INT, 'DPI_ORACLE_TYPE_NATIVE_INT',
                'DPI_NATIVE_TYPE_INT64', 0, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiOracleType type",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, badAtom, 'DPI_NATIVE_TYPE_INT64', 0, false, null]
        )
    ),
    ?ASSERT_EX(
        "wrong or unsupported dpiNativeType type",
        dpiCall(
            TestCtx, stmt_defineValue,
            [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', badAtom, 0, false, null]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint size from arg4",
        dpiCall(
            TestCtx, stmt_defineValue,
            [
                Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                ?BAD_INT, false, null
            ]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve bool/atom sizeIsBytes from arg5",
        dpiCall(
            TestCtx, stmt_defineValue,
            [
                Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                0, "badAtom", null
            ]
        )
    ),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, 
        dpiCall(
            TestCtx, stmt_defineValue,
            [
                Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
                0, false, null
            ]
        )
    ),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

stmtClose(#{session := Conn} = TestCtx) ->
    % fails due to wrong reference
    ?ASSERT_EX(
        "Unable to retrieve resource statement from arg0",
        dpiCall(TestCtx, stmt_close, [?BAD_REF, <<>>])
    ),
    Stmt = dpiCall(
        TestCtx, conn_prepareStmt,
        [Conn, false, <<"select 1 from dual">>, <<>>]
    ),
    ?ASSERT_EX(
        "Unable to retrieve string tag from arg1",
        dpiCall(TestCtx, stmt_close, [Stmt, badBinary])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_close, [Stmt, <<>>])).

%-------------------------------------------------------------------------------
% Variable APIs
%-------------------------------------------------------------------------------

varSetNumElementsInArray(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_setNumElementsInArray, [?BAD_REF, 100])
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint numElements from arg1",
        dpiCall(TestCtx, var_setNumElementsInArray, [Var, ?BAD_INT])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setNumElementsInArray, [Var, 100])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varSetFromBytes(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_setFromBytes, [?BAD_REF, 0, <<"abc">>])
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint pos from arg1",
        dpiCall(TestCtx, var_setFromBytes, [Var, ?BAD_INT, <<"abc">>])
    ),
    ?ASSERT_EX(
        "Unable to retrieve binary/string value from arg2",
        dpiCall(TestCtx, var_setFromBytes, [Var, 0, badBinary])
    ),
    ?ASSERT_EX(
        #{message :=
            "DPI-1009: zero-based position 1000 is not valid with max array"
            " size of 100"
        },
        dpiCall(TestCtx, var_setFromBytes, [Var, 1000, <<"abc">>])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setFromBytes, [Var, 0, <<"abc">>])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]).

varRelease(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource var from arg0",
        dpiCall(TestCtx, var_release, [?BAD_REF])
    ),
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    ?assertEqual(ok, dpiCall(TestCtx, var_release, [Var])).

%-------------------------------------------------------------------------------
% Data APIs
%-------------------------------------------------------------------------------

dataSetTimestamp(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(
            TestCtx, data_setTimestamp, [?BAD_REF, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        )
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int year from arg1",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, ?BAD_INT, 2, 3, 4, 5, 6, 7, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int month from arg2",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, ?BAD_INT, 3, 4, 5, 6, 7, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int day from arg3",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, ?BAD_INT, 4, 5, 6, 7, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int hour from arg4",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, ?BAD_INT, 5, 6, 7, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int minute from arg5",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, ?BAD_INT, 6, 7, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int second from arg6",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, ?BAD_INT, 7, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int fsecond from arg7",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, ?BAD_INT, 8, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int tzHourOffset from arg8",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, ?BAD_INT, 9]
        )
    ),
    ?ASSERT_EX(
        "Unable to retrieve int tzMinuteOffset from arg9",
        dpiCall(
            TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, ?BAD_INT]
        )
    ),
    ?assertEqual(
        ok,
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setTimestamp, [Data1, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    ),
    dpiCall(TestCtx, data_release, [Data1]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetIntervalDS(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIntervalDS, [?BAD_REF, 1, 2, 3, 4, 5])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int days from arg1",
        dpiCall(TestCtx, data_setIntervalDS, [Data, ?BAD_INT, 2, 3, 4, 5])
    ),
    ?ASSERT_EX(
        "Unable to retrieve int hours from arg2",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, ?BAD_INT, 3, 4, 5])
    ),
    ?ASSERT_EX(
        "Unable to retrieve int minutes from arg3",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, ?BAD_INT, 4, 5])
    ),
    ?ASSERT_EX(
        "Unable to retrieve int seconds from arg4",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, ?BAD_INT, 5])
    ),
    ?ASSERT_EX(
        "Unable to retrieve int fseconds from arg5",
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, ?BAD_INT])
    ),
    ?assertEqual(
        ok,
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalDS, [Data1, 1, 2, 3, 4, 5])
    ),
    dpiCall(TestCtx, data_release, [Data1]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetIntervalYM(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIntervalYM, [?BAD_REF, 1, 2])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int years from arg1",
        dpiCall(TestCtx, data_setIntervalYM, [Data, ?BAD_INT, 2])
    ),
    ?ASSERT_EX(
        "Unable to retrieve int months from arg2",
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, ?BAD_INT])
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, 2])
    ),
    dpiCall(TestCtx, data_release, [Data]),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setIntervalYM, [Data1, 1, 2])),
    dpiCall(TestCtx, data_release, [Data1]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetInt64(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setInt64, [?BAD_REF, 1])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve int amount from arg1",
        dpiCall(TestCtx, data_setInt64, [Data, ?BAD_INT])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setInt64, [Data, 1])),
    dpiCall(TestCtx, data_release, [Data]),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setInt64, [Data1, 1])),
    dpiCall(TestCtx, data_release, [Data1]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetDouble(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setDouble, [?BAD_REF, 1])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve double amount from arg1",
        dpiCall(TestCtx, data_setDouble, [Data, ?BAD_FLOAT])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setDouble, [Data, 1.0])),
    dpiCall(TestCtx, data_release, [Data]),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setDouble, [Data1, 1.0])),
    dpiCall(TestCtx, data_release, [Data1]),
    dpiCall(TestCtx, var_release, [Var]).

dataSetBytes(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setBytes, [?BAD_REF, <<"my string">>])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve binary data from arg1",
        dpiCall(TestCtx, data_setBytes, [Data, badBinary])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setBytes, [Data, <<"my string">>])),
    dpiCall(TestCtx, data_release, [Data]).

dataSetIsNull(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_setIsNull, [?BAD_REF, 1])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?ASSERT_EX(
        "Unable to retrieve bool/atom isNull from arg1",
        dpiCall(TestCtx, data_setIsNull, [Data, "not an atom"])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setIsNull, [Data, true])),
    ?assertEqual(ok, dpiCall(TestCtx, data_setIsNull, [Data, false])),
    dpiCall(TestCtx, data_release, [Data]),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM',
            1, 1, true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_setIsNull, [Data1, true])),
    dpiCall(TestCtx, data_release, [Data1]),
    dpiCall(TestCtx, var_release, [Var]).

dataGet(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_get, [?BAD_REF])
    ),
    Types = [
        {null, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM'},
        {int, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64'},
        {int, 'DPI_ORACLE_TYPE_NATIVE_UINT', 'DPI_NATIVE_TYPE_UINT64'},
        {float, 'DPI_ORACLE_TYPE_NATIVE_FLOAT', 'DPI_NATIVE_TYPE_FLOAT'},
        {float, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE'},
        {double, 'DPI_ORACLE_TYPE_NATIVE_FLOAT', 'DPI_NATIVE_TYPE_FLOAT'},
        {double, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE'},
        {ts, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP'},
        {intvlds, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS'},
        {intvlym, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM'},
        {unsupported, 'DPI_ORACLE_TYPE_CLOB', 'DPI_NATIVE_TYPE_LOB'}
    ],
    lists:foreach(
        fun({Test, OraType, NativeType}) ->
            #{var := Var, data := [Data]} = dpiCall(
                TestCtx, conn_newVar,
                [Conn, OraType, NativeType, 1, 0, false, false, null]
            ),
            if Test == null -> dpiCall(TestCtx, data_setIsNull, [Data, true]);
            true -> dpiCall(TestCtx, data_setIsNull, [Data, false])
            end,
            case Test of
                null -> ?assertEqual(null, dpiCall(TestCtx, data_get, [Data]));
                int ->
                    ?assert(
                        is_integer(dpiCall(TestCtx, data_getInt64, [Data]))
                    ),
                    ?assert(is_integer(dpiCall(TestCtx, data_get, [Data])));
                float -> ?assert(is_float(dpiCall(TestCtx, data_get, [Data])));
                double ->
                    ?assert(
                        is_float(dpiCall(TestCtx, data_getDouble, [Data]))
                    );
                ts ->
                    #{
                        year := Year, month := Month, day := Day, hour := Hour,
                        minute := Minute, second := Second, fsecond := Fsecond,
                        tzHourOffset := TzHourOffset,
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
                    ?assert(is_integer(TzMinuteOffset));
                intvlds ->
                    #{
                        days := Days, hours := Hours, minutes := Minutes, 
                        seconds := Seconds, fseconds := Fseconds
                    } = dpiCall(TestCtx, data_get, [Data]),
                    ?assert(is_integer(Days)),
                    ?assert(is_integer(Hours)),
                    ?assert(is_integer(Minutes)),
                    ?assert(is_integer(Seconds)),
                    ?assert(is_integer(Fseconds));
                intvlym ->
                    #{
                        years := Years,
                        months := Months
                    } = dpiCall(TestCtx, data_get, [Data]),
                    ?assert(is_integer(Years)),
                    ?assert(is_integer(Months));
                unsupported ->
                    ?ASSERT_EX(
                        "Unsupported nativeTypeNum",
                        dpiCall(TestCtx, data_get, [Data])
                    )
            end,
            dpiCall(TestCtx, data_release, [Data]),
            dpiCall(TestCtx, var_release, [Var])
        end,
        Types
    ).

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

dataGetStmt(#{session := Conn} = TestCtx) ->
    SQL = <<"
        DECLARE
            p_cursor SYS_REFCURSOR;
        BEGIN
            IF :choice > 0 THEN
                OPEN p_cursor FOR SELECT 1 FROM dual;
                :cursor := p_cursor;
            ELSE
                OPEN p_cursor FOR SELECT 2 FROM dual;
                :cursor := p_cursor;
            END IF;
        END;
    ">>,
    #{var := VarChoice, data := [DataChoice]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0,
            false, false, null
        ]
    ),
    #{var := VarStmt, data := [DataStmt]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0,
            false, false, null
        ]
    ),
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, SQL, <<>>]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"choice">>, VarChoice]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"cursor">>, VarStmt]),

    % first-time get
    ok = dpiCall(TestCtx, data_setInt64, [DataChoice, 0]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assert(is_reference(dpiCall(TestCtx, data_get, [DataStmt]))),

    % cached re-get
    ok = dpiCall(TestCtx, data_setInt64, [DataChoice, 1]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assert(is_reference(dpiCall(TestCtx, data_get, [DataStmt]))),

    dpiCall(TestCtx, data_release, [DataChoice]),
    dpiCall(TestCtx, var_release, [VarChoice]),
    dpiCall(TestCtx, data_release, [DataStmt]),
    dpiCall(TestCtx, var_release, [VarStmt]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]).

dataGetInt64(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getInt64, [?BAD_REF])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_getInt64, [Data])),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_getInt64, [Data]))),
    dpiCall(TestCtx, data_release, [Data]).

dataGetDouble(TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getDouble, [?BAD_REF])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_getDouble, [Data])),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(TestCtx, data_getDouble, [Data]))),
    dpiCall(TestCtx, data_release, [Data]).

% no non-pointer test for this one
dataGetBytes(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data/ptr from arg0",
        dpiCall(TestCtx, data_getBytes, [?BAD_REF])
    ),
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(TestCtx, data_getBytes, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    Data1 = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data1, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_getBytes, [Data1])),
    dpiCall(TestCtx, data_release, [Data1]).

dataRelease(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_release, [?BAD_REF])
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource data from arg0",
        dpiCall(TestCtx, data_release, [Conn])
    ),
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok, dpiCall(TestCtx, data_release, [Data])),
    #{var := Var, data := [Data1]} = dpiCall(
        TestCtx, conn_newVar,
        [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, data_release, [Data1])),
    dpiCall(TestCtx, var_release, [Var]).

resourceCounting(#{context := Context, session := Conn} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Indices = lists:seq(1, 5),
    #{
        context     := ICtxs,
        variable    := IVars,
        connection  := IConns,
        data        := IDatas,
        statement   := IStmts,
        datapointer := IDataPtrs
    } = InitialRC = dpiCall(TestCtx, resource_count, []),
    Resources = [{
        dpiCall(
            TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
        ),
        dpiCall(
            TestCtx, conn_create, [
                Context, User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
            ]
        ),
        dpiCall(
            TestCtx, conn_prepareStmt,
            [Conn, false, <<"select * from dual">>, <<>>]
        ),
        dpiCall(
            TestCtx, conn_newVar, 
            [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE',
            1, 0, false, false, null]
        ),
        dpiCall(TestCtx, data_ctor, [])
    } || _ <- Indices],

    #{
        context     := Ctxs,
        variable    := Vars,
        connection  := Conns,
        data        := Datas,
        statement   := Stmts,
        datapointer := DataPtrs
    } = dpiCall(TestCtx, resource_count, []),
    ?assertEqual(5, Ctxs - ICtxs),
    ?assertEqual(5, Vars - IVars),
    ?assertEqual(5, Conns - IConns),
    ?assertEqual(5, Stmts - IStmts),
    ?assertEqual(5, Datas - IDatas),
    ?assertEqual(5, DataPtrs - IDataPtrs),

    lists:foreach(
        fun({Ctx, LConn, Stmt, #{var := Var}, Data}) ->
            ok = dpiCall(TestCtx, var_release, [Var]),
            ok = dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
            ok = dpiCall(TestCtx, conn_close, [LConn, [], <<>>]),
            ok = dpiCall(TestCtx, context_destroy, [Ctx]),
            ok = dpiCall(TestCtx, data_release, [Data])
        end,
        Resources
    ),
    ?assertEqual(InitialRC, dpiCall(TestCtx, resource_count, [])).

%-------------------------------------------------------------------------------
% LOB APIs
%-------------------------------------------------------------------------------
lobSetReadFromBytes(#{session := Conn} = TestCtx) ->
    ?ASSERT_EX(
        "Unable to retrieve resource lob from arg0",
        dpiCall(TestCtx, lob_setFromBytes, [?BAD_REF, <<"abc">>])
    ),
    Lob = dpiCall(TestCtx, conn_newTempLob, [Conn, 'DPI_ORACLE_TYPE_BLOB']),
    ?ASSERT_EX(
        "Unable to retrieve binary value from arg1",
        dpiCall(TestCtx, lob_setFromBytes, [Lob, badBinary])
    ),
    ?ASSERT_EX(
        "Unable to retrieve resource lob from arg0",
        dpiCall(TestCtx, lob_setFromBytes, [Conn, <<"abc">>])
    ),
    ?assertEqual(ok, dpiCall(TestCtx, lob_setFromBytes, [Lob, <<"abc">>])),
    ?ASSERT_EX(
        "Unable to retrieve resource lob from arg0",
        dpiCall(TestCtx, lob_readBytes, [?BAD_REF, 1, 3])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint64 offset from arg1",
        dpiCall(TestCtx, lob_readBytes, [Lob, ?BAD_INT, 3])
    ),
    ?ASSERT_EX(
        "Unable to retrieve uint64 length from arg2",
        dpiCall(TestCtx, lob_readBytes, [Lob, 1, ?BAD_INT])
    ),
    ?ASSERT_EX(
        #{message := "ORA-24801: illegal parameter value in OCI lob function"},
        dpiCall(TestCtx, lob_readBytes, [Lob, 0, 3])
    ),

    ?assertEqual(<<"abc">>, dpiCall(TestCtx, lob_readBytes, [Lob, 1, 3])),
    dpiCall(TestCtx, lob_release, [Lob]).


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
    maps:fold(
        fun(K, V, _) ->
            if V > 0 -> ?debugFmt("~p ~p = ~p", [?FUNCTION_NAME, K, V]);
            true -> ok
            end
        end,
        noacc,
        dpiCall(SlaveCtx, resource_count, [])
    ),
    SlaveCtx#{
        context => dpiCall(
            SlaveCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
        )
    }.

setup_connecion(TestCtx) ->
    ContextCtx = #{context := Context} = setup_context(TestCtx),
    #{tns := Tns, user := User, password := Password} = getConfig(),
    maps:fold(
        fun
            (_K, 0, _) -> ok;
            (context, 1, _) -> ok;
            (K, V, _) -> ?assertEqual({K, 0}, {K, V})
        end,
        noacc,
        dpiCall(ContextCtx, resource_count, [])
    ),
    ContextCtx#{
        session => dpiCall(
            ContextCtx, conn_create,
            [
                Context, User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
            ]
        )
    }.

cleanup(#{session := Connnnection} = Ctx) ->
    dpiCall(Ctx, conn_close, [Connnnection, [], <<>>]),
    maps:fold(
        fun
            (_K, 0, _) -> ok;
            (context, 1, _) -> ok;
            (K, V, _) -> ?debugFmt("~p ~p = ~p", [?FUNCTION_NAME, K, V])
        end,
        noacc,
        dpiCall(Ctx, resource_count, [])
    ),
    cleanup(maps:without([session], Ctx));
cleanup(#{context := Context} = Ctx) ->
    dpiCall(Ctx, context_destroy, [Context]),
    maps:fold(
        fun
            (_K, 0, _) -> ok;
            (K, V, _) -> ?assertEqual({K, 0}, {K, V})
        end,
        noacc,
        dpiCall(Ctx, resource_count, [])
    ),
    cleanup(maps:without([context], Ctx));
cleanup(#{safe := true, node := SlaveNode}) ->
    unloaded = dpi:unload(SlaveNode);
cleanup(_) -> ok.

%-------------------------------------------------------------------------------
% Internal functions
%-------------------------------------------------------------------------------

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

-define(NO_CONTEXT_TESTS, [
    ?F(contextCreate),
    ?F(contextDestroy),
    ?F(contextGetClientVersion),
    ?F(connCreate)
]).

-define(AFTER_CONNECTION_TESTS, [
    ?F(connPrepareStmt),
    ?F(connNewVar),
    ?F(connNewTempLob),
    ?F(connCommit),
    ?F(connRollback),
    ?F(connPing),
    ?F(connClose),
    ?F(connGetServerVersion),
    ?F(connSetClientIdentifier),
    ?F(stmtExecute),
    ?F(stmtExecuteMany_varGetReturnedData),
    ?F(stmtFetch),
    ?F(stmtGetQueryValue),
    ?F(stmtGetQueryInfo),
    ?F(stmtGetInfo),
    ?F(stmtGetNumQueryColumns),
    ?F(stmtBindValueByPos),
    ?F(stmtBindValueByName),
    ?F(stmtBindByPos),
    ?F(stmtBindByName),
    ?F(stmtDefine),
    ?F(stmtDefineValue),
    ?F(stmtClose),
    ?F(varSetNumElementsInArray),
    ?F(varSetFromBytes),
    ?F(varRelease),
    ?F(dataSetTimestamp),
    ?F(dataSetIntervalDS),
    ?F(dataSetIntervalYM),
    ?F(dataSetInt64),
    ?F(dataSetDouble),
    ?F(dataSetBytes),
    ?F(dataSetIsNull),
    ?F(dataGet),
    ?F(dataGetBinary),
    ?F(dataGetRowid),
    ?F(dataGetStmt),
    ?F(dataGetInt64),
    ?F(dataGetDouble),
    ?F(dataGetBytes),
    ?F(dataRelease),
    ?F(resourceCounting),
    ?F(lobSetReadFromBytes)
]).

unsafe_no_context_test_() ->
    {
        setup,
        fun() -> setup(#{safe => false}) end,
        fun cleanup/1,
        ?W(?NO_CONTEXT_TESTS)
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

session_test_() ->
    {
        setup,
        fun() -> setup_connecion(#{safe => true}) end,
        fun cleanup/1,
        ?W(?AFTER_CONNECTION_TESTS)
    }.

load_test() ->
    % This is a place holder to trigger the upgrade and unload calbacks of the
    % NIF code. This doesn't test anything only ensures code coverage.
    ?assertEqual(ok, dpi:load_unsafe()),
    c:c(dpi),

    % triggering upgrade callback
    ?assertEqual(ok, dpi:load_unsafe()),

    % at this point, both old and current dpi code might be "bad"

    % delete the old code, triggers unload callback
    code:purge(dpi),

    % make the new code old
    code:delete(dpi),

    % delete that old code, too. Now all the code is gone, triggering unload
    % callback again
    code:purge(dpi).

slave_reuse_test() ->

    % single load / unload test
    Node = dpi:load(?SLAVE),
    ?assertEqual([Node], nodes(hidden)),
    ?assertEqual([self()], reg_pids(Node)),
    ?assertEqual(unloaded, dpi:unload(Node)),
    ?assertEqual([], reg_pids(Node)),

    % multiple load / unload test
    RxTO = 1000,

    % - first process which creates the slave node
    Self = self(),
    Pid1 = spawn(fun() -> slave_client_proc(Self) end),
    Pid1 ! load,
    ?assertEqual(ok, receive {Pid1, loaded} -> ok after RxTO -> timeout end),
    ?assertEqual([Node], nodes(hidden)),

    % - create three more processes sharing the same slave node
    Pids0 = [spawn(fun() -> slave_client_proc(Self) end) || _ <- lists:seq(1, 3)],
    ok = lists:foreach(fun(Pid) -> Pid ! load end, Pids0),

    ?assertEqual(done, 
        (fun
            WaitLoad([]) -> done;
            WaitLoad(Workers) when length(Workers) > 0 ->
                receive {Pid, loaded} -> WaitLoad(Workers -- [Pid])
                after RxTO -> timeout
                end
        end)(Pids0)
    ),

    Pids = [P1, P2, P3, P4] = lists:usort([Pid1 | Pids0]),
    ?assertEqual(Pids, lists:usort(reg_pids(Node))),

    % slave is still running after first process calls dpi:unload/1
    P1 ! {unload, Node},
    ?assertEqual(ok, receive {P1, unloaded} -> ok after RxTO -> timeout end),
    ?assertEqual(lists:usort(Pids -- [P1]), lists:usort(reg_pids(Node))),
    ?assertEqual([Node], nodes(hidden)),

    % slave is still running after second process exists without
    % calling dpi:unload/1 (crash simulation)
    P2 ! exit,
    ?assertEqual(ok, receive {P2, exited} -> ok after RxTO -> timeout end),
    ?assertEqual(lists:usort(Pids -- [P1, P2]), lists:usort(reg_pids(Node))),
    ?assertEqual([Node], nodes(hidden)),

    % slave is still running after third process calls dpi:unload/1
    P3 ! {unload, Node},
    ?assertEqual(ok, receive {P3, unloaded} -> ok after RxTO -> timeout end),
    ?assertEqual(
        lists:usort(Pids -- [P1, P2, P3]),
        lists:usort(reg_pids(Node))
    ),
    ?assertEqual([Node], nodes(hidden)),

    % slave is still running after last process exists without
    % calling dpi:unload/1 (last process crash simulation)
    P4 ! exit,
    ?assertEqual(ok, receive {P4, exited} -> ok after RxTO -> timeout end),
    ?assertEqual([], reg_pids(Node)), % global register is empty
    lists:foreach( % all processes are also dead
        fun(Pid) -> ?assertEqual(false, is_process_alive(Pid)) end,
        Pids
    ),
    ?assertEqual([Node], nodes(hidden)),

    % console cleanup simulation after last process carsh
    ?assertEqual(unloaded, dpi:unload(Node)),
    ?assertEqual([], reg_pids(Node)),
    ?assertEqual([], nodes(hidden)).
    
slave_client_proc(TestPid) ->
    receive
        load ->
            dpi:load(?SLAVE),
            TestPid ! {self(), loaded},
            slave_client_proc(TestPid);
        {unload, Node} ->
            ok = dpi:unload(Node),
            TestPid ! {self(), unloaded};
        exit ->
            TestPid ! {self(), exited}
    end.

reg_pids(Node) ->
    lists:filtermap(
        fun
            ({dpi, N, SN, _} = Name) when N == Node, SN == node() ->
                case global:whereis_name(Name) of
                    Pid when is_pid(Pid) -> {true, Pid};
                    _ -> false
                end;
            (_) -> false
        end,
        global:registered_names()
    ).
