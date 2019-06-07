-module(errbug_eunit).
-include_lib("eunit/include/eunit.hrl").

getNamePassTns()->
    {<<"scott">>,
    <<"regit">>,
    <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
    "(PROTOCOL=tcp)(HOST=192.168.1.43)(PORT=1521)))"
    "(CONNECT_DATA=(SERVICE_NAME=XE)))">>}.

execDefault() ->    % version that is not IO-bound
    ok = dpi:load(miau),
    Result = dpi:safe(fun errorRoutine/1, [fun dpi:stmt_execute_default/2]),
    ?debugFmt("Result default: ~p ~n",[Result]),
    Result.

execIob() ->        % IO-bound version (mangled error)
    ok = dpi:load(miau2),
    Result = dpi:safe(fun errorRoutine/1, [fun dpi:stmt_execute_iob/2]),
    ?debugFmt("Result IOB: ~p ~n",[Result]),
    Result.

execNoExceptions() -> % IO-bound, but returns the error properly
    ok = dpi:load(miau2),
    Result = dpi:safe(fun errorRoutine/1, [fun dpi:stmt_execute_no_exceptions/3]),
    ?debugFmt("Result NoExecptions: ~p ~n",[Result]),
    Result.

errorRoutine(ExecFun)->
    Context = dpi:context_create(3, 0),
    try
        {User, Password, Tns} = getNamePassTns(),
        Conn = dpi:conn_create(Context, User, Password, Tns, #{}, #{}), 
        #{var := SomeVar, data := SomeData} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, undefined),
        dpi:var_release(SomeVar),
        [dpi:data_release(X) || X <- SomeData],
        SQL = <<"select 'miaumiau' from unexistingTable">>,
        Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
        {arity, Arity} = erlang:fun_info(ExecFun,arity),
        Result = case Arity of 2 -> ExecFun(Stmt, []);
                               3 -> ExecFun(Stmt, [], Context);
                               _Else -> {error, invalidArity}
        end,
        case is_map(Result) of true -> {maps:get(fnName, Result), maps:get(message, Result)};
        Else -> Else end

    catch
        _:_->
            DpiError = dpi:context_getError(Context),
            {maps:get(fnName, DpiError), maps:get(message, DpiError)}
    end.


eunit_test_() ->
     
[   
     ?_assert({"dpiStmt_execute", "ORA-00942: table or view does not exist"} == execDefault()),
     ?_assert({"dpiConn_newVar", []} == execIob()),
     ?_assert({"dpiStmt_execute", "ORA-00942: table or view does not exist"} == execNoExceptions())
].
