-module(dpi).
-compile({parse_transform, dpi_transform}).

-export([load/1, unload/0]).

-export([load_unsafe/0]).
-export([safe/1, safe/2, safe/3]).

-include("dpiContext.hrl").
-include("dpiConn.hrl").
-include("dpiStmt.hrl").
-include("dpiQueryInfo.hrl").
-include("dpiData.hrl").
-include("dpiVar.hrl").
-include("dpiObjectType.hrl").

%===============================================================================
%   Slave Node APIs
%===============================================================================

load(SlaveNodeName) when is_atom(SlaveNodeName) ->
    case get(dpi_node) of
        undefined ->
            case is_alive() of
                false -> {error, not_distributed};
                true ->
                    case start_slave(SlaveNodeName) of
                        {ok, SlaveNode} ->
                            put(dpi_node, SlaveNode),
                            ok = rpc_call(
                                SlaveNode, code, add_paths, [code:get_path()]
                            ),
                            rpc_call(
                                SlaveNode, dpi, load_unsafe, []
                            ),
                            ok;
                        {error, {already_running, SlaveNode}} ->
                            put(dpi_node, SlaveNode),
                            ok;
                        Error -> Error
                    end
            end;
        _Slave ->
            ok
    end.

unload() ->
    SlaveNode = erase(dpi_node),
    slave:stop(SlaveNode).

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

start_slave(SlaveNodeName) when is_atom(SlaveNodeName) ->
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
                SlaveHost, SlaveNodeName,
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
            erase(Node),
            error({slave_down_internal, Node, Mod, Fun, Args});
        Result ->
            Result
    end.

-spec safe(atom(), atom(), list()) -> term().
safe(Module, Fun, Args) when is_atom(Module), is_atom(Fun), is_list(Args) ->
    rpc:call(get(dpi_node), Module, Fun, Args).

-spec safe(function(), list()) -> term().
safe(Fun, Args) when is_function(Fun), is_list(Args) ->
    rpc:call(get(dpi_node), erlang, apply, [Fun, Args]).

-spec safe(function()) -> term().
safe(Fun) when is_function(Fun)->
    rpc:call(get(dpi_node), erlang, apply, [Fun, []]).
