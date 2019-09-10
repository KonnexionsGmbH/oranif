-module(leak_SUITE).

% rebar3 ct --readable=false --suite=test/leak_SUITE

-export([
    all/0, suite/0%,
    %init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    %end_per_testcase/2
]).
-export([
    leak/1, connect_internal/3, close_internal/2, get_mem_internal/0,
    test_ref_cursor_internal/4
]).

-define(TNS,
	<<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
	"(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))"
	"(CONNECT_DATA=(SERVICE_NAME=XE)))">>
).
-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).
-define(TEST_SQL,
    <<"begin TESTNEWPROC(:SQLT_INT_PERSID, :SQLT_OUT_CURSOR); end;">>
).

all() ->
    [leak].

suite() ->
    [
        {timetrap, infinity},
        {require, tns},
        {require, user},
        {require, password}
    ].

leak(_Config) ->
    SlaveNode = dpi:load(oranif_test_slave),
    Conn = connect(<<"scott">>, <<"regit">>, ?TNS, SlaveNode),
    BindVars = [
        {<<":SQLT_INT_PERSID">>,in,'DPI_ORACLE_TYPE_NUMBER'},
        {<<":SQLT_OUT_CURSOR">>,out,'DPI_ORACLE_TYPE_STMT'}
    ],
    BindValues = [4],
    test_ref_cursor(Conn, ?TEST_SQL, 1000, BindVars, BindValues),
    close(Conn),
    dpi:unload(SlaveNode).

connect(User, Password, ConStr, Node) ->
    Conn = dpi:safe(Node, fun ?MODULE:connect_internal/3, [User, Password, ConStr]),
	Conn#{node => Node}.
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
        Class1:Exception1 ->
            {
                error,
                #{
                    class => Class1,
                    exception => Exception1,
                    message => "Bad Context"
                }
        }
    end.

close(#{node := Node, conn := Conn, context := Ctx}) ->
    dpi:safe(Node, fun ?MODULE:close_internal/2, [Conn, Ctx]).
close_internal(Conn, Context)->
    try
        ok = dpi:conn_close(Conn, [], <<>>),
        ok = dpi:context_destroy(Context)
    catch
        _Class:Exception ->	{error, Exception}
    end.

test_ref_cursor(#{node := Node, conn := Conn} = State, Sql, Count, Vars, Values) ->
	io:format("Resources ~p~n", [dpi:safe(Node, dpi, resource_count, [])]),
	Stmt = dpi:safe(Node, dpi, conn_prepareStmt, [Conn, false, Sql, <<>>]),
	StartMem = dpi:safe(Node, fun ?MODULE:get_mem_internal/0, []),
	test_ref_cursor(State, Stmt, Count, Vars, Values, StartMem),
	dpi:safe(Node, dpi, stmt_close, [Stmt, <<>>]),
	Mem = dpi:safe(Node, fun ?MODULE:get_mem_internal/0, []),
	Incr = (Mem - StartMem) / StartMem,
	io:format("{end, ~.2f%}~nResources ~p~n", [Incr, dpi:safe(Node, dpi, resource_count, [])]),
	close(State).

test_ref_cursor(_State, _Stmt, Count, _Vars, _Values, _StartMem) when Count =< 0 -> ok;
test_ref_cursor(#{node := Node} = State, Stmt, Count, Vars, Values, StartMem) ->
    case dpi:safe(Node, fun ?MODULE:test_ref_cursor_internal/4, [State, Stmt, Vars, Values]) of
		{ok, _} -> ok;
		Result ->
			 io:format("ERROR : ~p Result ~p~n", [Count, Result])
	end,
	if Count rem 100 == 0 ->
			Mem = dpi:safe(Node, fun ?MODULE:get_mem_internal/0, []),
			Incr = (Mem - StartMem) / StartMem,
			io:format("{~p, ~.2f%} ", [Count, Incr]);
		true -> ok
	end,
    test_ref_cursor(State, Stmt, Count - 1, Vars, Values, StartMem).
test_ref_cursor_internal(#{conn := Conn}, Stmt, Vars, Values) ->
    try
        OutVars = bind_vars(Conn, Stmt, Vars, Values),
        dpi:stmt_execute(Stmt, []),
		{value, {_Name, _Type, Var, Data}, OutVars1}
         = lists:keytake(<<":SQLT_OUT_CURSOR">>, 1, OutVars),
        RefCursor = dpi:data_get(Data),
        Info = query_info_stmt_internal(RefCursor),
        Row1 = fetch_stmt_internal(RefCursor),
        Row2 = fetch_stmt_internal(RefCursor),
        Row3 = fetch_stmt_internal(RefCursor),
        dpi:var_release(Var),
		dpi:data_release(Data),
		[dpi:var_release(V) || {_, _, V, _} <- OutVars1],
        {ok, {Info, [Row1, Row2, Row3]}}
    catch
        _Class:{error, _, _, #{message := Message}} -> {error, Message};
        _Class:Exception -> {error, Exception}
    end.

bind_vars(_Conn, _Stmt, [], _Values) -> [];
bind_vars(Conn, Stmt, [{Name, _, Type} | RestVars], Values) ->
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
    QueryInfo = dpi:stmt_getQueryInfo(Stmt, ColIdx),
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
