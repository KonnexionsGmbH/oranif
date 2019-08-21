-module(oranif_SUITE).

-export([
    all/0, suite/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2,
    end_per_testcase/2
]).
-export([benchmark/1]).

-include_lib("common_test/include/ct.hrl").

-define(L(_F, _A),
    %ct:pal(info, ?MODULE_STRING":~p:~p:~p "_F, [?FUNCTION_NAME, ?LINE, self() | _A])
    io:format(user, "===> ["?MODULE_STRING":~p:~p:~p] "_F"~n", [?FUNCTION_NAME, ?LINE, self() | _A])
).
-define(L(_S), ?L(_S, [])).

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
    ok = dpi:load_unsafe(),
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
    [{dpiConn, Connection} | Config].

benchmark(Config) ->
    {DurationConnect, Connections} = timer:tc(fun connect/1, [Config]),
    %{DurationInsert, _} = timer:tc(fun insert/1, [Connections]),
    %{DurationSelect, _} = timer:tc(fun select/1, [Connections]),
    {DurationDisconnect, ok} = timer:tc(fun disconnect/1, [Connections]),
    TotalConnecions = length(Connections) * 1000000,
    ?L(
        "~nConnecions ~p @ ~p connect per second, ~p disconnect per second",
        [
            length(Connections), TotalConnecions div DurationConnect,
            TotalConnecions div DurationDisconnect
        ]
    ),
    ok.

end_per_testcase(Test, Config) ->
    ok = dpi:conn_close(?config(dpiConn, Config), [], <<>>),
    ?L("~p pid ~p", [Test, self()]),
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
        _:_ -> []
    end.

disconnect([]) -> ok;
disconnect([Connection | Connections]) ->
    catch dpi:conn_commit(Connection),
    catch dpi:conn_close(Connection, [], <<>>),
    disconnect(Connections).

l2b(B) when is_binary(B) -> B;
l2b(L) when is_list(L) -> list_to_binary(L).

-define(SQL_DROP, <<"DROP TABLE oraniftest">>).
-define(SQL_CREATE,
    <<
        "CREATE TABLE oraniftest ("
        " thread VARCHAR2(100) NOT NULL PRIMARY KEY, int1 INTEGER,"
        " str1 VARCHAR2(1000)"
        ")"
    >>
).

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
        _:_ -> ok
    end,

    Stmt1 = dpi:conn_prepareStmt(Conn, false, ?SQL_CREATE, <<>>),
    0 = dpi:stmt_execute(Stmt1, []),
    ok = dpi:stmt_close(Stmt1, <<>>),
    ?L("~s", [?SQL_CREATE]),

    ok = dpi:conn_commit(Conn),
    ok = dpi:conn_close(Conn, [], <<>>),
    
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
