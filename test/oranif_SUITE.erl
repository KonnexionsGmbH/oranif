%% TODO set traviy integration test
% Port tests from https://github.com/K2InformaticsGmbH/odpi/tree/master/samples
-module(oranif_SUITE).

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([shortInserts/1, balancedInserts/1, heavyInserts/1, simultaneousInserts/1, longInserts/1]).
-export([heavyData/1, longData/1]).
-export([insertStress/1, dataMemStress/1]).

-include_lib("common_test/include/ct.hrl").
-include("test_common.hrl").

-define(value(Key, Config), proplists:get_value(Key, Config)).
-define(B(__L), list_to_binary(__L)).

call(Conn, SQL) -> call(Conn, SQL, true).
call(Conn, SQL, PrintError) -> 
    Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
    
    try dpi:stmt_execute(Stmt, []) of
    Ret -> 
        %io:format(user, "oi ~p ~n", [Ret]),
        dpi:stmt_release(Stmt),
        Ret
    catch A:B ->
        case PrintError of 
            true -> io:format(user, "Error in call of SQL [~p]! ~p ~p ~n", [SQL, A, B]), throw(fatal_exception);
            false -> no_op
        end,
        dpi:stmt_release(Stmt)
    end.

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

%all() -> [shortInserts, balancedInserts, heavyInserts, simultaneousInserts, longInserts].
all() -> [insertStress, dataMemStress].
-define(F(Fn, Runs, Conn, Stmt, Job), Fn(ConfigData) -> runFrame(ConfigData, Runs, Conn, Stmt, fun Job/5, false)).
-define(N(Fn, Runs, Stmt, Job), Fn(ConfigData) -> runFrame(ConfigData, Runs, undefined, Stmt, fun Job/5, true)).

%% defines the test cases. For each one, the amount of runs and the amount of
%% connections per run as well as the job that each connection runs can be
%% specified. "Stmt" refers to how often something is done in each job, for
%% instance an insert job could insert that many rows. But it's up to the
%% implemntation of each individual job to do anything meaningful with it

% Function Name            Runs      Conn      Stmt       Job
?F(shortInserts,           10,       3,        10,        insertIntoTable).
?F(balancedInserts,        50,       10,       1500,      insertIntoTable).
?F(heavyInserts,           20,       3,        5000,      insertIntoTable).
?F(simultaneousInserts,    20,       100,      500,       insertIntoTable).
?F(longInserts,            200,      3,        500,       insertIntoTable).

?F(heavyData,              20,       5,        2000,      bindData).
?F(longData,               200,      5,        500,       bindData).

%% "Native" test cases that only run on one slave node that is persistent
%% as such, there's no "Conn" parameter because there's only one slave

?N(insertStress,           200,               5000,      insertIntoTable).
?N(dataMemStress,          500,               2000,      bindData).

suite() ->
     [
      {timetrap, infinity}
     ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%% MANDATORY FUNCTIONS %%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% extracts the parameters required to establish the connection from the file
init_per_suite(ConfigData) ->

    #{tns := Tns, user := User, password := Pswd,
        logging := Logging, lang := Lang} = getConfig(),

    [{config, {Tns, User, Pswd, Lang, Logging}} | ConfigData].

end_per_suite(ConfigData) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DISTRIBUTED INFRASTRUCTURE %%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% takes all the parameters, then runs executes the right amount of runs
runFrame(ConfigData, Runs, Connections, Statements, SlaveJob, Native ) ->
    io:format(user, "AYYYYYYYYYYYYYYYYYYYYYYYYY ~p ~n", [?LINE]),
    [
    begin
        io:format(user, "Start Run ~p/~p ~n", [X, Runs]),
        (case Native of false -> fun run/4; true -> fun native/4 end)
            (ConfigData, Connections, Statements, SlaveJob)
    end
    || X <- lists:seq(1, Runs)].

%% Performs a run by creating the right amount of nodes and passing to each of
%% them them the data required to run the job, then waits for each node's
%% message
run(ConfigData, Connections, Statements, SlaveJob) ->
    {Tns, User, Pswd, Lang, Logging} = ?value(config, ConfigData),
    Master = self(),
    [spawn_link(fun() -> spawnJob(list_to_atom(lists:flatten(io_lib:format("miau~p", [X]))),
        lists:flatten(io_lib:format("bobbytables~p", [X])), User, Pswd, Tns, Master, Connections, Statements, SlaveJob) end) || X <- lists:seq(1, Connections)],
    multiReceive(Connections, Connections).


%% Performs a run by creating the right amount of nodes and passing to each of
%% them them the data required to run the job, then waits for each node's
%% message
native(ConfigData, _, Statements, SlaveJob) ->
    {Tns, User, Pswd, Lang, Logging} = ?value(config, ConfigData),
    ok = dpi:load(nativeSlave),
    ok = dpi:safe(SlaveJob, ["bobbytables", User, Pswd, Tns, Statements]).

%% loads a slave node and tells it to run the job, then when it is done, sends
%% its own master the message
spawnJob(SlaveName, TableName, User, Pswd, Tns, Master, Connections, Statements, SlaveJob) ->
    ok = dpi:load(SlaveName),
    ok = dpi:safe(SlaveJob, [TableName, User, Pswd, Tns, Statements]),
    Master ! aye,
    ok.

%% receives a given amount of messages, used to "wait" for each slave to finish
multiReceive(Count, Connections) ->
    receive
            A -> ok %io:format(user, "Got em: ~p Count ~p ~n", [A, Count])
    end,
    case Count of 1 ->  io:format(user, "Got all of them. ~p ~n", [Connections]);
                  Else -> multiReceive(Count-1, Connections)
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%% JOBS %%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% all jobs take 5 parameters: [TableName, User, Pswd, Tns, Statements]
%% each job should do something useful with the [Statements] parameter
%% each job should return ok iff it ran successfully


%% this job makes a table, inserts [Statements] rows, then tries to find all of
%% those rows again and finally drops the table.
insertIntoTable(TableName, User, Pswd, Tns, Statements) ->
    Context = dpi:context_create(3, 0),
    Conn = dpi:conn_create(Context, User, Pswd, Tns, #{}, #{}),
    call(Conn, ?B(["drop table ", TableName]), false),
    call(Conn, ?B(["create table ", TableName, " (a int)"])),
    [call(Conn, ?B(["insert into ", TableName, "(a) values (", integer_to_list(X), ")"])) || X <- lists:seq(1, Statements)],
    [
        begin
        Stmt = dpi:conn_prepareStmt(Conn, false, ?B(["select a from ", TableName, " where a = ", integer_to_list(X), ""]), <<"">>),
        Query_cols = dpi:stmt_execute(Stmt, []),
        dpi:stmt_fetch(Stmt),
        #{nativeTypeNum := Type, data := Result} = dpi:stmt_getQueryValue(Stmt, 1),
        true = (1.0 * X == dpi:data_get(Result)),
        Type = 'DPI_NATIVE_TYPE_DOUBLE',
        Query_cols = 1,
        dpi:data_release(Result),
        dpi:stmt_release(Stmt)
        end
    
        || X <- lists:seq(1, Statements)],
    dpi:conn_commit(Conn),
    call(Conn, ?B(["drop table ", TableName])),
    dpi:conn_release(Conn),
    dpi:context_destroy(Context),
    ok.


%% this job makes a table, inserts [Statements] rows, then tries to find all of
%% those rows again and finally drops the table.
bindData(TableName, User, Pswd, Tns, Statements) ->
    Context = dpi:context_create(3, 0),
    Conn = dpi:conn_create(Context, User, Pswd, Tns, #{}, #{}),
    call(Conn, ?B(["drop table ", TableName]), false),
    call(Conn, ?B(["create table ", TableName, " (a int, b int, c int)"])),
    [
        begin
        Stmt = dpi:conn_prepareStmt(Conn, false, ?B(["insert into ", TableName, " values (:A, :B, :C)"]), <<"">>),
        BindData = dpi:data_ctor(),
        dpi:data_setInt64(BindData, X*2),
        dpi:stmt_bindValueByPos(Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData),
        dpi:data_setInt64(BindData, X*3),     % data can be recycled
        dpi:stmt_bindValueByPos(Stmt, 2, 'DPI_NATIVE_TYPE_INT64', BindData),
        dpi:data_setInt64(BindData, X*5),
        dpi:stmt_bindValueByPos(Stmt, 3, 'DPI_NATIVE_TYPE_INT64', BindData),
        dpi:data_release(BindData),
        dpi:stmt_execute(Stmt, []),
        dpi:stmt_release(Stmt)
        end
     || X <- lists:seq(1, Statements)],

    [
        begin
        Stmt2 = dpi:conn_prepareStmt(Conn, false, ?B(["select a, b, c from ", TableName, " where a = ", integer_to_list(2*X)]) , <<"">>),
        3 = dpi:stmt_execute(Stmt2, []),
        dpi:stmt_fetch(Stmt2),
        %ok = assert_getQueryValue(Stmt2, 1, 2.0 * X),
        %ok = assert_getQueryValue(Stmt2, 2, 3.0 * X),
        %ok = assert_getQueryValue(Stmt2, 3, 5.0 * X),
        dpi:stmt_release(Stmt2)
        end
    
    || X <- lists:seq(1, Statements)],

    dpi:conn_commit(Conn),
    call(Conn, ?B(["drop table ", TableName])),
    dpi:conn_release(Conn),
    dpi:context_destroy(Context),
    ok.

assert_getQueryValue(Stmt, Index, Value) ->
    QueryValueRef = maps:get(data, (dpi:stmt_getQueryValue(Stmt, Index))),
    Value = dpi:data_get(QueryValueRef),
	dpi:data_release(QueryValueRef),
    
    ok.
