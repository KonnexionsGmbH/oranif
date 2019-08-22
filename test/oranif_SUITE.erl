-module(oranif_SUITE).

% rebar3 ct --readable=false --config=test/default.cfg

-export([
    all/0, suite/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2
]).
-export([benchmark/1]).

-include_lib("common_test/include/ct.hrl").

-define(L(_F, _A),
    %ct:pal(info, ?MAX_IMPORTANCE, ?MODULE_STRING":~p:~p:~p "_F, [?FUNCTION_NAME, ?LINE, self() | _A])
    io:format(user, "===> ["?MODULE_STRING":~p:~p:~p] "_F"~n", [?FUNCTION_NAME, ?LINE, self() | _A])
).
-define(L(_S), ?L(_S, [])).

-define(SQL_DROP, <<"DROP TABLE oraniftest">>).
-define(SQL_CREATE,
    <<
        "CREATE TABLE oraniftest"
        " (thread VARCHAR2(1000) NOT NULL, something INTEGER)"
    >>
).
-define(SQL_INSERT,
    <<
        "INSERT INTO oraniftest values"
        " (:thread_varchar2, :something_integer)"
    >>
).
-define(SQL_SELECT,
    <<
        "SELECT * FROM oraniftest WHERE thread = :thread_varchar2"
    >>
).

all() ->
    [benchmark].

suite() ->
    [
        {timetrap, infinity},
        {require, tns},
        {require, user},
        {require, password}
    ].

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

init_per_suite(Config) ->
    Result = dpi:load_unsafe(),
    ?L("Result ~p", [Result]),
    ok = Result,
    Context = dpi:context_create(?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION),
    init_sut(
        [
            {dpiCtx,    Context},
            {tns,       l2b(ct:get_config(tns))},
            {user,      l2b(ct:get_config(user))},
            {password,  l2b(ct:get_config(password))}
        | Config]
    ).

init_per_testcase(Test, Config) ->
    Connection = dpi:conn_create(
        ?config(dpiCtx, Config), ?config(user, Config),
        ?config(password, Config), ?config(tns, Config),
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
        #{}
    ),
    [{test, Test}, {dpiConn, Connection} | Config].

benchmark(Config) ->
    {DurationConnect, Connections} = timer:tc(fun connect/1, [Config]),
    TotalConnections = length(Connections) * 1000000,
    ?L(
        "Connections ~p @ ~p connect per second",
        [length(Connections), TotalConnections div DurationConnect]
    ),
    {DurationInsert, {Inserts, Threads}} = timer:tc(fun insert/1, [Connections]),
    InsertCount = lists:sum(Inserts),
    ?L(
        "Inserts ~p @ ~p inserts per second",
        [InsertCount, (InsertCount * 1000000) div DurationInsert]
    ),
    {DurationUpdate, Updates} = timer:tc(fun update/2, [Connections, Threads]),
    UpdateCount = lists:sum(Updates),
    ?L(
        "Updates ~p @ ~p Updates per second",
        [UpdateCount, (UpdateCount * 1000000) div DurationUpdate]
    ),
    {DurationSelect, Selects} = timer:tc(fun select/2, [Connections, Threads]),
    SelectCount = lists:sum(Selects),
    ?L(
        "Selects ~p @ ~p selects per second",
        [SelectCount, (SelectCount * 1000000) div DurationSelect]
    ),
    {DurationDisconnect, ok} = timer:tc(fun disconnect/1, [Connections]),
    ?L("~p disconnect per second", [TotalConnections div DurationDisconnect]),
    ok.

end_per_testcase(_Test, Config) ->
    ok = dpi:conn_close(?config(dpiConn, Config), [], <<>>),
    ok.

end_per_suite(Config) ->
    cleanup_sut(Config),
    ok = dpi:context_destroy(?config(dpiCtx, Config)),
    false = code:purge(dpi),
    true = code:delete(dpi),
    false = code:purge(dpi).

connect(Config) ->
    try dpi:conn_create(
        ?config(dpiCtx, Config), ?config(user, Config),
        ?config(password, Config), ?config(tns, Config),
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
        #{}
    ) of
        Connection -> [Connection | connect(Config)]
    catch
        error:{error, _, _, #{code := 12520}} -> [];
        Class:Exception ->
            ?L("~p:~p", [Class, Exception]),
            []
    end.

insert(Connections) ->
    process_flag(trap_exit, true),
    Self = self(),
    Pids = [
        spawn_link(
            fun() ->
                Self ! insert_many(_Bulksize = 1000, Connection)
            end
        ) || Connection <- Connections
    ],
    {
        wait(Pids),
        [list_to_binary(io_lib:format("~.36B", [erlang:phash2(Pid)]))
         || Pid <- Pids]
    }.

insert_many(BulkSize, Connection) ->
    Indices = lists:seq(0, BulkSize - 1),
    Stmt = dpi:conn_prepareStmt(Connection, false, ?SQL_INSERT, <<>>),
    #{var := VarThread} = dpi:conn_newVar(
        Connection, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES',
        BulkSize, 1000, true, false, null
    ),
    #{var := VarSomething, data := DataListSomething} = dpi:conn_newVar(
        Connection, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
        BulkSize, 0, true, false, null
    ),
    dpi:stmt_bindByName(Stmt, <<"thread_varchar2">>, VarThread),
    dpi:stmt_bindByName(Stmt, <<"something_integer">>, VarSomething),
    SelfHashBin = list_to_binary(io_lib:format("~.36B", [erlang:phash2(self())])),
    [dpi:var_setFromBytes(VarThread, Idx, SelfHashBin) || Idx <- Indices],
    InsertFailed = insert_many(Stmt, DataListSomething, 1000000),
    dpi:var_release(VarThread),
    dpi:var_release(VarSomething),
    dpi:stmt_close(Stmt, <<>>),
    1000000 - InsertFailed.

insert_many(_Stmt, _DataListSomething, 0) -> 0;
insert_many(Stmt, DataListSomething, Count) ->
    try
        NewCount = lists:foldl(
            fun(DataSomething, LCount) ->
                dpi:data_setInt64(DataSomething, LCount),
                LCount - 1
            end,
            Count,
            DataListSomething
        ),
        dpi:stmt_executeMany(Stmt, [], length(DataListSomething)),
        insert_many(Stmt, DataListSomething, NewCount)
    catch
        error:{error, _, _, #{code := 1653}} -> Count;
        Class:Exception ->
            ?L("~p:~p", [Class, Exception]),
            Count
    end.

select(Connections, Threads) ->
    process_flag(trap_exit, true),
    Self = self(),
    Pids = [spawn_link(
        fun() ->
            Self ! select_all(Connection, Thread)
        end
    ) || {Connection, Thread} <- lists:zip(Connections, Threads)],
    wait(Pids).

select_all(Connection, Thread) when is_binary(Thread) ->
    Stmt = dpi:conn_prepareStmt(Connection, false, ?SQL_SELECT, <<>>),
    #{var := VarThread} = dpi:conn_newVar(
        Connection, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES',
        1, 1000, true, false, null
    ),
    dpi:stmt_bindByName(Stmt, <<"thread_varchar2">>, VarThread),
    dpi:var_setFromBytes(VarThread, 0, Thread),
    dpi:stmt_execute(Stmt, []),
    Selected = select_all(Stmt, 0),
    dpi:var_release(VarThread),
    dpi:stmt_close(Stmt, <<>>),
    Selected;
select_all(Stmt, Count) when is_integer(Count) ->
    case dpi:stmt_fetch(Stmt) of
        #{found := true} ->
            #{data := First} = dpi:stmt_getQueryValue(Stmt, 1),
            #{data := Second} = dpi:stmt_getQueryValue(Stmt, 2),
            _FirstValue = dpi:data_get(First),
            _SecondValue = dpi:data_get(Second),
            dpi:data_release(First),
            dpi:data_release(Second),
            select_all(Stmt, Count + 1);
        _ -> Count
    end.

-define(SQL_UPDATE,
    <<
        "UPDATE oraniftest SET something = something + :something_integer"
        " WHERE thread = :thread_varchar2"
    >>
).

update(Connections, Threads) ->
    process_flag(trap_exit, true),
    Self = self(),
    Pids = [spawn_link(
        fun() ->
            Self ! update_all(Connection, Thread)
        end
    ) || {Connection, Thread} <- lists:zip(Connections, Threads)],
    wait(Pids).

update_all(Connection, Thread) when is_binary(Thread) ->
    Stmt = dpi:conn_prepareStmt(Connection, false, ?SQL_UPDATE, <<>>),
    #{var := VarThread} = dpi:conn_newVar(
        Connection, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES',
        1, 1000, true, false, null
    ),
    dpi:stmt_bindByName(Stmt, <<"thread_varchar2">>, VarThread),
    #{var := VarSomething, data := [DataSomething]} = dpi:conn_newVar(
        Connection, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64',
        1, 0, true, false, null
    ),
    dpi:stmt_bindByName(Stmt, <<"thread_varchar2">>, VarThread),
    dpi:stmt_bindByName(Stmt, <<"something_integer">>, VarSomething),
    dpi:var_setFromBytes(VarThread, 0, Thread),
    dpi:data_setInt64(DataSomething, 1),
    0 = dpi:stmt_execute(Stmt, []),
    Rows = dpi:stmt_getRowCount(Stmt),
    dpi:var_release(VarThread),
    dpi:var_release(VarSomething),
    dpi:stmt_close(Stmt, <<>>),
    Rows.

disconnect([]) -> ok;
disconnect([Connection | Connections]) ->
    catch dpi:conn_commit(Connection),
    catch dpi:conn_close(Connection, [], <<>>),
    disconnect(Connections).

l2b(B) when is_binary(B) -> B;
l2b(L) when is_list(L) -> list_to_binary(L).

wait(Pids) -> wait(Pids, []).
wait([], Acc) ->
    io:format(user, "~n", []),
    Acc;
wait(Pids, Acc) ->
    receive
        {'EXIT', Pid, R} ->
            if R /= normal -> ?L("~p exit ~p", [Pid, R]); true -> ok end,
            wait(Pids -- [Pid], Acc);
        Result ->
            wait(Pids, [Result | Acc])
    after
        10000 ->
            io:format(user, ".", []),
            wait(Pids, Acc)
    end.

init_sut(Config) ->
    Conn = dpi:conn_create(
        ?config(dpiCtx, Config), ?config(user, Config),
        ?config(password, Config), ?config(tns, Config),
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
        #{}
    ),

    try
        Stmt = dpi:conn_prepareStmt(Conn, false, ?SQL_DROP, <<>>),
        0 = dpi:stmt_execute(Stmt, []),
        ok = dpi:stmt_close(Stmt, <<>>),
        ?L("~s", [?SQL_DROP])
    catch
        error:{error, _, _, #{code := 942}} -> ok;
        Class:Exception ->
            ?L("~s : ~p:~p", [?SQL_DROP, Class, Exception])
    end,

    Stmt1 = dpi:conn_prepareStmt(Conn, false, ?SQL_CREATE, <<>>),
    0 = dpi:stmt_execute(Stmt1, []),
    ok = dpi:stmt_close(Stmt1, <<>>),
    ?L("~s", [?SQL_CREATE]),

    ok = dpi:conn_commit(Conn),
    ok = dpi:conn_close(Conn, [], <<>>),
    
    ?L("DONE"),
    Config.

cleanup_sut(Config) ->
    timer:sleep(5000),
    try
        Conn = dpi:conn_create(
            ?config(dpiCtx, Config), ?config(user, Config),
            ?config(password, Config), ?config(tns, Config),
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
            #{}
        ),
        Stmt = dpi:conn_prepareStmt(Conn, false, ?SQL_DROP, <<>>),
        0 = dpi:stmt_execute(Stmt, []),
        ok = dpi:stmt_close(Stmt, <<>>),
        ok = dpi:conn_commit(Conn),
        ok = dpi:conn_close(Conn, [], <<>>),
        ?L("~s", [?SQL_DROP])
    catch
        Class:Exception ->
            ?L("~p~n~p", [Class, Exception]),
            timer:sleep(1000),
            cleanup_sut(Config)
    end.
