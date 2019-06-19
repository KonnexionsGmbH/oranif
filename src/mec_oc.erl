-module(mec_oc).

-ifdef(CONSOLE).

werl.exe -name a@127.0.0.1 -setcookie a -pa _build/default/lib/oranif/ebin &

f().
Tns = <<"(DESCRIPTION=(CONNECT_TIMEOUT=4)(TRANSPORT_CONNECT_TIMEOUT=3)(ENABLE=BROKEN)(ADDRESS=(PROTOCOL=TCP)(HOST=127.0.0.1)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=XE)))">>.
User = <<"scott">>.
Password = <<"regit">>.
SlaveName = dpi_test_slave.
TestSql = <<"begin TESTNEWPROC(:SQLT_INT_PERSID, :SQLT_OUT_CURSOR); end;">>.
BindVars = [
    {<<":SQLT_INT_PERSID">>,in,'DPI_ORACLE_TYPE_NUMBER'},
    {<<":SQLT_OUT_CURSOR">>,out,'DPI_ORACLE_TYPE_STMT'}
].
BindValues = [4].

f(Conn).
Conn = mec_oc:connect(User, Password, Tns, SlaveName).
mec_oc:test_ref_cursor(Conn, TestSql, 1000, BindVars, BindValues).
-endif.

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

% public safe apis (when NIF is loaded in slave)
-export([connect/4, connect_internal/3, test_ref_cursor/5, test_ref_cursor_internal/4, get_mem_internal/0, close/1, close_internal/2]).

connect(User, Password, ConStr, SlaveName)
    when is_binary(User), is_binary(Password), is_binary(ConStr)
->
    ok = dpi:load(SlaveName),
    dpi:safe(fun ?MODULE:connect_internal/3, [User, Password, ConStr]).
connect_internal(User, Password, ConStr) ->
    try
        Ctx = dpi:context_create(?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION),
        try
            CommonParams = #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
            Conn = dpi:conn_create(Ctx, User, Password, ConStr, CommonParams, #{}),
            #{context => Ctx, conn => Conn}
        catch
            _Class:{error, _, _, #{message := Message}} -> {error, Message};
            _Class:Exception -> {error, Exception}
        end
    catch
        Class1:Exception1 -> {error, #{class => Class1, exception => Exception1,
            message => "Bad Context"}
        }
    end.

close(#{conn := Conn, context := Ctx}) ->
    dpi:safe(fun ?MODULE:close_internal/2, [Conn, Ctx]).
close_internal(Conn, Context)->
    try
        ok = dpi:conn_release(Conn),
        ok = dpi:context_destroy(Context)
    catch
        _Class:{error, _, _, #{message := Message}} -> {error, Message};
        _Class:Exception -> {error, Exception}
    end.

test_ref_cursor(State, Sql, Count, Vars, Values) ->
	StartMem = dpi:safe(fun ?MODULE:get_mem_internal/0, []),
	test_ref_cursor(State, Sql, Count, Vars, Values, StartMem),
	close(State),
	Mem = dpi:safe(fun ?MODULE:get_mem_internal/0, []),
	Incr = (Mem - StartMem) / StartMem,
	io:format("{end, ~.2f%}~n", [Incr]).

test_ref_cursor(_State, _Sql, Count, _Vars, _Values, _StartMem) when Count =< 0 -> ok;
test_ref_cursor(State, Sql, Count, Vars, Values, StartMem) ->
    case dpi:safe(fun ?MODULE:test_ref_cursor_internal/4, [State, Sql, Vars, Values]) of
		{[R|_], _} when is_map(R) -> ok;
		Result ->
			 io:format("ERROR : ~p Result ~p~n", [Count, Result])
	end,
	if Count rem 100 == 0 ->
			Mem = dpi:safe(fun ?MODULE:get_mem_internal/0, []),
			Incr = (Mem - StartMem) / StartMem,
			io:format("{~p, ~.2f%} ", [Count, Incr]);
		true -> ok
	end,
    test_ref_cursor(State, Sql, Count - 1, Vars, Values, StartMem).
test_ref_cursor_internal(#{conn := Conn}, Sql, Vars, Values) ->
    try
		Stmt = dpi:conn_prepareStmt(Conn, false, Sql, <<>>),
        OutVars = bind_vars(Conn, Stmt, Vars, Values),
        dpi:stmt_execute(Stmt, []),
        [{_Name, _Type, Var, Data}] = OutVars,
        RefCursor = dpi:data_get(Data),
        Info = query_info_stmt_internal(RefCursor),
        Row1 = fetch_stmt_internal(RefCursor),
        Row2 = fetch_stmt_internal(RefCursor),
        Row3 = fetch_stmt_internal(RefCursor),
        dpi:var_release(Var),
        dpi:data_release(Data),
        dpi:stmt_release(Stmt),
        {Info, [Row1, Row2, Row3]}
    catch
        _Class:{error, _, _, #{message := Message}} -> {error, Message};
        _Class:Exception -> {error, Exception}
    end.

bind_vars(_Conn, _Stmt, [], _Values) -> [];
bind_vars(Conn, Stmt, [{Name, in, Type} | RestVars], [Value | RestVals]) ->
    Data = dpi:data_ctor(),
    dpi:data_setInt64(Data, Value), % Test with int but helper function is needed.
    dpi:stmt_bindValueByName(Stmt, Name, native_type(Type), Data),
    dpi:data_release(Data),
    bind_vars(Conn, Stmt, RestVars, RestVals);
bind_vars(Conn, Stmt, [{Name, out, Type} | RestVars], Values) ->
    #{var := Var, data := [Data]} = dpi:conn_newVar(Conn, Type, native_type(Type), 1, 0, false, false, null),
    dpi:stmt_bindByName(Stmt, Name, Var),
    [{Name, Type, Var, Data} | bind_vars(Conn, Stmt, RestVars, Values)].

query_info_stmt_internal(Stmt) ->
    try
        NumCols = dpi:stmt_getNumQueryColumns(Stmt),
        get_column_info(Stmt, 1, NumCols)
    catch
        _Class:{error, _, _, #{message := Message}} -> {error, Message};
        _Class:Exception -> {error, Exception}
    end.

fetch_stmt_internal(Stmt) ->
    try
        case dpi:stmt_fetch(Stmt) of
            #{found := true} ->
                NumCols = dpi:stmt_getNumQueryColumns(Stmt),
                {true, get_column_values(Stmt, 1, NumCols)};
            #{found := false} ->
                {false, []}
        end
    catch
        _Class:{error, _, _, #{message := Message}} -> {error, Message};
        _Class:Exception -> {error, Exception}
    end.

get_column_values(_Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_values(Stmt, ColIdx, Limit) ->
    #{data := Data} = dpi:stmt_getQueryValue(Stmt, ColIdx),
    Value = dpi:data_get(Data),
    dpi:data_release(Data),
    [Value | get_column_values(Stmt, ColIdx + 1, Limit)].

get_column_info(_Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_info(Stmt, ColIdx, Limit) ->
    QueryInfoRef = dpi:stmt_getQueryInfo(Stmt, ColIdx),
    QueryInfo = dpi:queryInfo_get(QueryInfoRef),
    dpi:queryInfo_delete(QueryInfoRef),
    [QueryInfo | get_column_info(Stmt, ColIdx + 1, Limit)].

native_type('DPI_ORACLE_TYPE_NUMBER') -> 'DPI_NATIVE_TYPE_INT64';
native_type('DPI_ORACLE_TYPE_VARCHAR') -> 'DPI_NATIVE_TYPE_BYTES';
native_type('DPI_ORACLE_TYPE_STMT') -> 'DPI_NATIVE_TYPE_STMT'.

get_mem_internal() -> get_mem_internal(os:type()).
get_mem_internal({win32, _}) ->
	list_to_integer(
            re:replace(
              os:cmd("wmic process where processid=" ++ os:getpid() ++
                     " get workingsetsize | findstr /v \"WorkingSetSize\""),
              "[[:space:]]*", "", [global, {return,list}]));
get_mem_internal({unix, _}) ->
	SysData = memsup:get_system_memory_data(),
	TotalMemory = proplists:get_value(total_memory, SysData),
    erlang:round(
		TotalMemory
				* list_to_float(
					re:replace(
						os:cmd("ps -p "++os:getpid()++" -o pmem="),
						"[[:space:]]*", "", [global, {return,list}])
					) / 100).
