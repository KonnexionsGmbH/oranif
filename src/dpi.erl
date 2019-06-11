-module(dpi).
-compile({parse_transform, dpi_transform}).
-behavior(gen_server).

-export([load/0, unload/0]).

-export([load_unsafe/0]).

% gen_server apis
-export([handle_call/3, handle_cast/2, init/1, fa/2]).

-include("dpiContext.hrl").
-include("dpiConn.hrl").
-include("dpiStmt.hrl").
-include("dpiQueryInfo.hrl").
-include("dpiData.hrl").
-include("dpiVar.hrl").

%===============================================================================
%   Slave Node APIs
%===============================================================================

load() ->
    case get(dpi_slave) of
        undefined ->
            case is_alive() of
                false -> {error, not_distributed};
                true ->
                    case start_slave() of
                        {ok, Slave} ->
                            put(dpi_slave, Slave),
                            ok = rpc_call(
                                Slave, code, add_paths, [code:get_path()]
                            ),
                            gen_server_rpc_start(Slave);
                        {error, {already_running, Slave}} ->
                            put(dpi_slave, Slave),
                            gen_server_rpc_start(Slave);
                        Error -> Error
                    end
            end;
        Slave ->
            gen_server_rpc_start(Slave)
    end.

unload() ->
    Slave = erase(dpi_slave),
    slave:stop(Slave).

%===============================================================================
%   gen_server mandatory callbacks
%===============================================================================

init(_) ->
    case load_unsafe() of
        ok -> {ok, []};
        {error, Error} -> {stop, {error, Error}}
    end.

%% takes the function, arguments and OP, returns result of the odpi query
handle_call({Fun, Args, Op}, _From, State) ->
    Result = (catch fa(Fun, Args)),
    NewState = process_res(Op, State, Args, Result),
    {reply, Result, NewState}.

handle_cast(Request, State) ->
    {stop, {unimplemented, cast, Request}, State}.

%===============================================================================
%   NIF test / debug interface (DO NOT use in production)
%===============================================================================

load_unsafe() ->
    PrivDir = case code:priv_dir(?MODULE) of
		{error, _} ->
		    EbinDir = filename:dirname(code:which(?MODULE)),
		    AppPath = filename:dirname(EbinDir),
		    filename:join(AppPath, "priv");
		Path -> Path
	end,
    case erlang:load_nif(filename:join(PrivDir, "dpi_nif"), 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Error} -> {error, Error}
    end.

%===============================================================================
%   local helper functions
%===============================================================================

start_slave() ->
    [_,SlaveHost] = string:tokens(atom_to_list(node()), "@"),
    ExtraArgs =
        case {init:get_argument(pa), init:get_argument(boot)} of
            {error, error} -> {error, bad_config};
            {error, {ok, [[Boot]]}} ->
                [_ | T] = lists:reverse(filename:split(Boot)),
                StartClean = filename:join(lists:reverse(["start_clean" | T])),
                case filelib:is_regular(StartClean ++ ".boot") of
                    true -> " -boot \"" ++ StartClean ++ "\"";
                    false ->
                    % {error, "Start clean boot not found"}
                        io:format(user, "[ERROR] REVISIT ~p:~p boot file not found!!~n", [?MODULE, ?LINE]),
                        []
                end;
            {{ok, _}, _} -> []
        end,
    case ExtraArgs of
        {error, _} = Error -> Error;
        ExtraArgs ->
            slave:start(
                SlaveHost, 'dpi_slave',
                lists:concat([
                    " -hidden ",
                    "-setcookie ", erlang:get_cookie(),
                    ExtraArgs
                ])
            )
    end.

rpc_call(undefined, _Mod, _Fun, _Args) -> {error, slave_down};
rpc_call(Node, Mod, Fun, Args) ->
    case (catch rpc:call(Node, Mod, Fun, Args)) of
        {badrpc, {'EXIT', Error}} ->
            error(Error);
        {badrpc, nodedown} ->
            erase(dpi_slave),
            error({slave_down_internal, Node, Mod, Fun, Args});
        Result ->
            Result
    end.

process_res(create, State, _Args , Result) -> State ++ refList(Result);
process_res(delete, State, Args, _Result) -> State -- refList(Args);
process_res(_, _, _, _) -> error.

refList(Input) when is_map(Input)-> refList(maps:values(Input));
refList(Input) when is_reference(Input)-> [Input];
refList(Input) when is_list(Input)->
    lists:filter(
        fun is_reference/1,
        lists:flatten([refList(X) || X <- Input])
    );
refList(_Input) -> [].

%% starts the gen_server and "standardizes" the return value.
%% on success, it will always be {ok, Pid}, even if it already exsits,
%% in which case there would be a different value that is however transformed
%% to {ok, Pid} instead. This way the calling function doesn't have to handle
%% this extra case every time
gen_server_rpc_start(Slave) ->
    Res = rpc_call(
        Slave, gen_server, start_link,
        [{local, ?MODULE}, ?MODULE, [], []]
    ),
    case Res of
        {ok, Pid} -> {ok, Pid};
        {error, {already_started, Pid}} -> {ok, Pid};
        Else -> error({start_link_load, Else})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% This function stub MUST be present and always at the end of file
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
fa(Fun, Args) -> error({unsupported, Fun, Args}).
