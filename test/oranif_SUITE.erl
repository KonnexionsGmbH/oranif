%% TODO set traviy integration test
% Port tests from https://github.com/K2InformaticsGmbH/odpi/tree/master/samples
-module(oranif_SUITE).

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([load/1]).

-include_lib("common_test/include/ct.hrl").
-include("test_common.hrl").

-define(value(Key, Config), proplists:get_value(Key, Config)).
-define(TAB, "oranif_load").

%10 10 100
%select table_name from all_tables where table_name like 'ERL%';
-define(CONNECTIONS, 10).
-define(STATEMENTS, 1).
-define(ROWS_PER_TABLE, 10).
-define(RUNS, 10).

-define(CONN_DELAY, 1000).      %% ms to wait after every connection that has been made
-define(ROW_DELAY, 1000).         %% ms to wait after every inserted row
%-define(DELAYS, true).

-ifdef(DELAYS).
-define(SLEEP(X), timer:sleep(X)).
-else.
-define(SLEEP(X), no_op).
-endif.

-define(CONNIDLIST, lists:seq(1, ?CONNECTIONS)).
-define(STMTIDLIST, lists:seq(1, ?STATEMENTS)).

callS(Conn, SQL) -> 
    Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
    
    try dpi:stmt_execute(Stmt, []) of
    Ret -> 
        dpi:stmt_release(Stmt),
        Ret
    catch _:_ ->
        dpi:stmt_release(Stmt)
       % dpi:context_getError(Context)
    end.

-define(CALL(SQL), callS(Conn, SQL)).


edatetime_to_ora({Meg,Mcr,Mil} = Now)
    when is_integer(Meg)
    andalso is_integer(Mcr)
    andalso is_integer(Mil) ->
    edatetime_to_ora(calendar:now_to_datetime(Now));
edatetime_to_ora({{FullYear,Month,Day},{Hour,Minute,Second}}) ->
    Century = (FullYear div 100) + 100,
    Year = (FullYear rem 100) + 100,
    << Century:8, Year:8, Month:8, Day:8, Hour:8, Minute:8, Second:8 >>.

getConfig() ->
    case file:get_cwd() of
        {ok, Cwd} ->
             io:format(user, "Got CWD ~p ~n", [Cwd]),
            ConnectConfigFile = filename:join(
                lists:reverse(
                    ["connect.config", "../../../../test"
                        | lists:reverse(filename:split(Cwd))]
                )
            ),
            case file:consult(ConnectConfigFile) of
                {ok, [Params]} when is_map(Params) -> Params;
                {ok, Params} ->
                    io:format(user, "bad config (expected map) ~p", [Params]),
                    error(badconfig);
                {error, Reason} ->
                    io:format(user, "Error i: ~p file ~p ~n", [Reason, ConnectConfigFile]),
                    error(Reason)
            end;
        {error, Reason} ->
            io:format(user, "Error o: ~p ~n", [Reason]),
            error(Reason)
    end.

all() -> [load].

suite() ->
     [
      {timetrap, infinity}
     ].

init_per_suite(ConfigData) ->
    %application:start(erloci),                     TODO: put the load here
    %{ok, Result} = dpi:load(),
    #{tns := Tns, user := User, password := Pswd,
        logging := Logging, lang := Lang} = getConfig(),
    Tables = [{C,
        [lists:flatten([?TAB, "_", integer_to_list(C), "_", integer_to_list(S)])
            || S <- ?STMTIDLIST]}
        || C <- ?CONNIDLIST],
    io:format(user, "Building ~p rows to bind for ~p tables ~n", [?ROWS_PER_TABLE, length(Tables)]),
    %io:format(user, "---~p---~n", [?LINE]),
    Binds = [{ I
     , list_to_binary(["_publisher_",integer_to_list(I),"_"])
     , I+I/2
     , list_to_binary(["_hero_",integer_to_list(I),"_"])
     , list_to_binary(["_reality_",integer_to_list(I),"_"])
     , I
     , edatetime_to_ora(os:timestamp())
     , I
     } || I <- lists:seq(1, ?ROWS_PER_TABLE)],
     %io:format(user, "---~p---~n", [?LINE]),
    io:format(user, "Starting ~p processes~n", [length(Tables)]),
    [{tables, Tables}, {binds, Binds}, {config, {Tns, User, Pswd, Lang, Logging}} | ConfigData].

end_per_suite(ConfigData) ->
    ok = dpi:load(slave),
    io:format(user, "End per suite---~p---~n", [?LINE]),
    Tables = lists:merge([Tabs || {_, Tabs} <- ?value(tables, ConfigData)]),
    {Tns, User, Pswd, Lang, Logging} = ?value(config, ConfigData),
    Context = dpi:safe(dpi, context_create, [3, 0]),
    Conn = dpi:safe(dpi, conn_create, [Context, User, Pswd, Tns, #{}, #{}]),
    [dpi:safe(fun tab_drop/2, [Conn, Table]) || Table <- Tables],
    ok = dpi:safe(dpi, conn_release, [Conn]),
    ok = dpi:safe(dpi, context_destroy, [Context]),
    io:format(user, "Finishing...~n", []).

load(ConfigData) ->
    io:format(user, "---~p---~n", [?LINE]),
    ok = dpi:load(slave),
    io:format(user, "---~p---~n", [?LINE]),
    Tables = ?value(tables, ConfigData),
    Binds = ?value(binds, ConfigData),
    {Tns, User, Pswd, Lang, Logging} = ?value(config, ConfigData),
    RowsPerProcess = length(Binds),
    Context = dpi:safe(dpi, context_create, [3, 0]),
    This = self(),
    [begin
    [begin
        ?SLEEP(?CONN_DELAY),
        %spawn_link(fun() -> dpi:safe(fun connection/9, [Context, C, proplists:get_value(C, Tables, []), Tns, User, Pswd, This, RowsPerProcess, Binds]) end)
        dpi:safe(fun connection/9, [Context, C, proplists:get_value(C, Tables, []), Tns, User, Pswd, This, RowsPerProcess, Binds])
        %dpi:save(connection(Context, C, proplists:get_value(C, Tables, []), Tns, User, Pswd, myeh, RowsPerProcess, Binds);
        
    end
        || C <- ?CONNIDLIST],
    collect_processes(lists:sort(Tables), [])
    end || Run <- lists:seq(1, ?RUNS)],
    io:format(user, "Closing port ~p~n", [a_OciPort]),
    ok = dpi:safe(dpi, context_destroy, [Context]),
    CloseResult = close,
    close = CloseResult.

connection(Context, Cid, Tables, Tns, User, Pswd, Master, RowsPerProcess, Binds) ->
    io:format(user, "---~p--- CID ~p ~n", [?LINE, Cid]),
    Conn = dpi:conn_create(Context, User, Pswd, Tns, #{}, #{}), 
     io:format(user, "---~p---~n", [?LINE]),
    [begin
         tab_drop(Conn, Table),
         tab_create(Conn, Table)
         %io:format(user, "---~p--- ~n", [?LINE])
     end
        || Table <- Tables],
    This = self(),
    
    [link(spawn(fun() ->
        table(Conn, Cid, Tid, This, RowsPerProcess, Binds, Context)
           end))
        || Tid <- ?STMTIDLIST],
    collect_processes(lists:sort(Tables), []),
    %io:format(user, "---~p---~n", [?LINE]),
    %ok = OciSession:close(),
    dpi:conn_close(Conn, [], <<"">>),
    Master ! {Cid, Tables}.

table(Conn, Cid, Tid, Master, RowsPerProcess, Binds, Context) ->
    %{ok, _Result} = dpi:load(),
    %io:format(user, "Table ---~p--- ~n", [?LINE]),
    Table = lists:flatten([?TAB, "_", integer_to_list(Cid), "_", integer_to_list(Tid)]),
    %io:format(user, "Binds: ~p~n", [Binds]),
    %io:format(user, "START BINDAGE: ~p~n", [mooo]),
    tab_load(Conn, Table, RowsPerProcess, Binds, Context),
    tab_access(Conn, Table, 10, Context),
    tab_drop(Conn, Table),
    %io:format(user, "Table done ---~p--- ~n", [?LINE]),
    Master ! Table.

collect_processes(Tables, Acc) ->
    %io:format(user, "collecting---~p---- ~n", [?LINE]),
    receive
        Table ->
            case lists:sort([Table | Acc]) of
                Tables -> ok;
                NewAcc ->
                    io:format(user, "Expecting ~p", [Tables -- NewAcc]),
                    timer:sleep(1000),
                    collect_processes(Tables, NewAcc)
            end
    end.

-define(B(__L), list_to_binary(__L)).
-define(CREATE(__T), ?B([
    "create table "
    , __T
    , " (pkey integer,"
    , "publisher varchar2(30),"
    , "rank float,"
    , "hero varchar2(30),"
    , "reality varchar2(30),"
    , "votes number(1,-10),"
    %, "createdate date default sysdate,"
    , "createdate varchar2(30),"
    , "chapters int,"
    , "votes_first_rank number)"])
).
-define(INSERT(__T), ?B([
    "insert into "
    , __T
    , " (pkey,publisher,rank,hero,reality,votes,createdate,votes_first_rank) values ("
    , ":pkey"
    , ", :publisher"
    , ", :rank"
    , ", :hero"
    , ", :reality"
    , ", :votes"
    , ", :createdate"
    , ", :votes_first_rank)"])
).
-define(BIND_LIST, [
    {<<":pkey">>, 'SQLT_INT'}
    , {<<":publisher">>, 'SQLT_CHR'}
    , {<<":rank">>, 'SQLT_FLT'}
    , {<<":hero">>, 'SQLT_CHR'}
    , {<<":reality">>, 'SQLT_CHR'}
    , {<<":votes">>, 'SQLT_INT'}
    , {<<":createdate">>, 'SQLT_CHR'} % SQLT_DAT
    , {<<":votes_first_rank">>, 'SQLT_INT'}
]
).
-define(SELECT_WITH_ROWID(__T), ?B([
    "select ", __T, ".rowid, ", __T, ".* from ", __T])
).
tab_drop(Conn, Table) when is_list(Table) ->
    io:format(user, "Tab Drop ~p---~p---~n", [ Table, ?LINE]),
    ?CALL(?B(["drop table ", Table])),
    io:format(user, "[~s] Dropped ~n", [Table]).

tab_create(Conn, Table) when is_list(Table) ->
    io:format(user, "Tab Create ~p---~p---~n", [ Table, ?LINE]),
    ?CALL(?CREATE(Table)),
    io:format(user, "[~s] Created ~n", [Table]).

%continue here with the bound shite
tab_load(Conn, Table, RowCount, Binds, Context) ->
    io:format(user, "Tab Load ~p---~p---~n", [ Table, ?LINE]),
    
    
    DotheThing = fun(Bind) ->
        %{_,_,MicroStart} = os:timestamp(),
        
        {Pkey, Publisher, Rank, Hero, Reality, Votes, Createdate, VotesFirstRank} = Bind,
        Stmt = dpi:conn_prepareStmt(Conn, false, ?INSERT(Table), <<"">>),
        %{_,_,MicroStmt} = os:timestamp(),
        BindData = dpi:data_ctor(),
        dpi:data_setInt64(BindData, Pkey),
        ok = dpi:stmt_bindValueByPos(Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData),
        %{_,_,MicroBind1} = os:timestamp(),
        BindData2 = dpi:data_ctor(),
        dpi:data_setBytes(BindData2, Publisher),
        ok = dpi:stmt_bindValueByPos(Stmt, 2, 'DPI_NATIVE_TYPE_BYTES', BindData2),
        %{_,_,MicroBind2} = os:timestamp(),
        BindData3 = dpi:data_ctor(),
        dpi:data_setDouble(BindData3, Rank),
        ok = dpi:stmt_bindValueByPos(Stmt, 3, 'DPI_NATIVE_TYPE_DOUBLE', BindData3),
        %{_,_,MicroBind3} = os:timestamp(),
        BindData4 = dpi:data_ctor(),
        dpi:data_setBytes(BindData4, Hero),
        ok = dpi:stmt_bindValueByPos(Stmt, 4, 'DPI_NATIVE_TYPE_BYTES', BindData4),
        %{_,_,MicroBind4} = os:timestamp(),
        BindData5 = dpi:data_ctor(),
        dpi:data_setBytes(BindData5, Reality),
        ok = dpi:stmt_bindValueByPos(Stmt, 5, 'DPI_NATIVE_TYPE_BYTES', BindData5),
        %{_,_,MicroBind5} = os:timestamp(),
        BindData6 = dpi:data_ctor(),
        dpi:data_setInt64(BindData6, Votes),
        ok = dpi:stmt_bindValueByPos(Stmt, 6, 'DPI_NATIVE_TYPE_INT64', BindData6),
        %{_,_,MicroBind6} = os:timestamp(),
        BindData7 = dpi:data_ctor(),
        dpi:data_setBytes(BindData7, Createdate),
        ok = dpi:stmt_bindValueByPos(Stmt, 7, 'DPI_NATIVE_TYPE_BYTES', BindData7), % DAT
        %{_,_,MicroBind7} = os:timestamp(),
        BindData8 = dpi:data_ctor(),
        dpi:data_setInt64(BindData8, VotesFirstRank),
        ok = dpi:stmt_bindValueByPos(Stmt, 8, 'DPI_NATIVE_TYPE_INT64', BindData8),
        %{_,_,MicroBind8} = os:timestamp(),
        try dpi:stmt_execute(Stmt, []) of
        _ -> 
        dpi:conn_commit(Conn),
        io:format("Commit passed ~p~n",[2323]),    
        ok
        %dpi:stmt_release(Stmt)
        catch _Class:Error ->
       io:format(user, "ERROR: ~p ~n", [Error]),
        dpi:stmt_execute(gffsasf, [])

        end,

        dpi:data_release(BindData),
        dpi:data_release(BindData2),
        dpi:data_release(BindData3),
        dpi:data_release(BindData4),
        dpi:data_release(BindData5),
        dpi:data_release(BindData6),
        dpi:data_release(BindData7),
        dpi:data_release(BindData8),
        %{_,_,MicroExecuted} = os:timestamp(),
        dpi:stmt_release(Stmt),
        ?SLEEP(?ROW_DELAY),
       % {_,_,MicroReleased} = os:timestamp(),
       % io:format(user, "Micros Start ~p Stmt ~p [1] ~p [2] ~p [3] ~p [4] ~p [5] ~p [6] ~p [7] ~p [8] ~p Executed ~p Released ~p~n", 
    %[MicroStart, MicroStmt-MicroStart, MicroBind1-MicroStmt, MicroBind2-MicroBind1, MicroBind3-MicroBind2, MicroBind4-MicroBind3,
    %MicroBind5-MicroBind4, MicroBind6-MicroBind5, MicroBind7-MicroBind6, MicroBind8-MicroBind7, MicroExecuted-MicroBind8, MicroReleased-MicroExecuted]),
       % io:format(user, "Released Stmt ~p ~n", [?LINE]),
     ok end,

    [DotheThing(X)|| X <- Binds],
    
    dpi:conn_commit(Conn),

    io:format(user, "[~s] Loaded ~p rows", [Table, RowCount]).

tab_access(Conn, Table, Count, Context) ->
    io:format(user, "Tab Access ~p---~p---~n", [ Table, ?LINE]),
    Stmt = dpi:conn_prepareStmt(Conn, false, ?SELECT_WITH_ROWID(Table), <<"">>),
    Cols = try
    dpi:stmt_execute(Stmt, []) of
        _ -> ok
    catch _:_ -> ok
    end,
    %io:format(user, "Error: ~p ~n", [dpi:context_getError(Context)]),
    %#{data := TblCount, nativeTypeNum := _Type} = dpi:stmt_getQueryValue(Stmt, 1),


    io:format(user, "[~s]  Loading rows @ ~p per fetch", [Table, Count]),
    io:format(user, "[~s] Selected columns ~p", [Table, Cols]),
    FetchEm = fun FetchEm(Statement)->
        #{found := Found, bufferRowIndex := BRI} = dpi:stmt_fetch(Statement),
        %io:format(user, "Fetchem ~p ~p ~n", [Found, BRI]),
        case Found of true ->
        FetchEm(Statement);
        _else -> ok end end,

    FetchEm(Stmt),
    dpi:stmt_release(Stmt),
    ok.

load_rows_to_end(Table, {{rows, Rows}, true}, _, _, Total) ->
    Loaded = length(Rows),
    io:format(user, "[~s] Loaded ~p / ~p rows - Finished", [Table, Loaded, Total + Loaded]);
load_rows_to_end(Table, {error, Error}, SelStmt, Count, Total) ->
    io:format(user, "[~s] Loaded ~p error - ~p", [Table, Total, Error]),
    load_rows_to_end(Table, SelStmt:fetch_rows(Count), SelStmt, Count, Total);
load_rows_to_end(Table, {{rows, Rows}, false}, SelStmt, Count, Total) ->
    Loaded = length(Rows),
    io:format(user, "[~s] Loaded ~p / ~p", [Table, Loaded, Total + Loaded]),
    load_rows_to_end(Table, SelStmt:fetch_rows(Count), SelStmt, Count, Total + Loaded).
