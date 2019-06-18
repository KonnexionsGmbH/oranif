%% TODO set traviy integration test
% Port tests from https://github.com/K2InformaticsGmbH/odpi/tree/master/samples
-module(oranif_SUITE).

-export([all/0, suite/0, init_per_suite/1, end_per_suite/1]).
-export([shortInserts/1, balancedInserts/1, heavyInserts/1, longInserts/1, longInserts/1]).

-include_lib("common_test/include/ct.hrl").
-include("test_common.hrl").

-define(value(Key, Config), proplists:get_value(Key, Config)).
-define(TAB, "oranif_load").
-define(B(__L), list_to_binary(__L)).

callS(Conn, SQL) -> callS(Conn, SQL, true).
callS(Conn, SQL, PrintError) -> 
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

-define(CALL(SQL), callS(Conn, SQL)).
-define(CALL(SQL, PrintError), callS(Conn, SQL, PrintError)).


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

%all() -> [shortInserts, balancedInserts, heavyInserts, longInserts, longInserts].
all() -> [shortInserts].
-define(F(Fn, Runs, Conn, Stmt, Job), Fn(ConfigData) -> runFrame(ConfigData, Runs, Conn, Stmt, fun Job/5)).

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
runFrame(ConfigData, Runs, Connections, Statements, SlaveJob ) ->
    io:format(user, "AYYYYYYYYYYYYYYYYYYYYYYYYY ~p ~n", [?LINE]),
    [
    begin
        io:format(user, "Start Run ~p/~p ~n", [X, Runs]),
        run(ConfigData, Connections, Statements, SlaveJob)
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
    ?CALL(?B(["drop table ", TableName]), false),
    ?CALL(?B(["create table ", TableName, " (a int)"])),
    [?CALL(?B(["insert into ", TableName, "(a) values (", integer_to_list(X), ")"])) || X <- lists:seq(1, Statements)],
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
    ?CALL(?B(["drop table ", TableName])),
    dpi:conn_release(Conn),
    dpi:context_destroy(Context),
    ok.
