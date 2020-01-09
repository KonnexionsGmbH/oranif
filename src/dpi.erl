-module(dpi).
-compile({parse_transform, dpi_transform}).

-export([load/1, unload/1]).

-export([load_unsafe/0]).
-export([safe/2, safe/3, safe/4]).

-export([resource_count/0]).

-include("dpiContext.hrl").
-include("dpiConn.hrl").
-include("dpiStmt.hrl").
-include("dpiData.hrl").
-include("dpiVar.hrl").
-include("dpiLob.hrl").

%===============================================================================
%   Slave Node APIs
%===============================================================================

-spec load(atom()) -> node().
load(SlaveNodeName) when is_atom(SlaveNodeName) ->
    case is_alive() of
        false -> {error, not_distributed};
        true ->
            case start_slave(SlaveNodeName) of
                {ok, SlaveNode} ->
                    case slave_call(
                        SlaveNode, code, add_paths, [code:get_path()]
                    ) of
                        ok ->
                            case slave_call(SlaveNode, dpi, load_unsafe, []) of
                                ok ->
                                    case reg(SlaveNode) of
                                        SlaveNode -> SlaveNode;
                                        Error ->
                                            slave:stop(SlaveNode),
                                            Error
                                    end;
                                Error -> Error
                            end;
                        Error -> Error
                    end;
                {error, {already_running, SlaveNode}} ->
                    reg(SlaveNode);
                Error -> Error
            end
    end.

-spec unload(atom()) -> ok | unloaded.
unload(SlaveNode) when is_atom(SlaveNode) ->
    UnloadingPid = self(),
    case lists:foldl(
        fun
            ({?MODULE, SN, N, _} = Name, Acc)
                when SN == SlaveNode, N == node()
            ->
                Pid = global:whereis_name(Name),
                case
                    is_pid(Pid) andalso
                    Pid /= UnloadingPid andalso
                    rpc:call(node(Pid), erlang, is_process_alive, [Pid])
                of
                    true -> [Pid | Acc];
                    _ ->
                        ok = global:unregister_name(Name),
                        Acc
                end;
            (_, Acc) -> Acc
        end, [], global:registered_names()
    ) of
        [] ->
          slave:stop(SlaveNode),
          unloaded;
        _ -> ok
    end.

%===============================================================================
%   NIF test / debug interface (DO NOT use in production)
%===============================================================================

load_unsafe() ->
    case erlang:load_nif(find_nif(), 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Error} -> {error, Error}
    end.

find_nif() ->
    find_nif(case code:priv_dir(oranif) of
        Path when is_list(Path) -> Path;
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            Path = filename:join(AppPath, "priv"),
            io:format(
                user, "{~p,~p,~p} priv ~s~n",
                [?MODULE, ?FUNCTION_NAME, ?LINE, Path]
            ),
            Path
    end).
find_nif(PrivDir) ->
    case filelib:wildcard("dpi_nif.*", PrivDir) of
        [] ->
            Source = filename:dirname(
                proplists:get_value(source, dpi:module_info(compile))
            ),
            find_nif(filename:join([Source, "..", "priv"]));
        _ -> filename:join(PrivDir, "dpi_nif")
    end.

%===============================================================================
%   local helper functions
%===============================================================================

reg(SlaveNode) ->
    Name = {?MODULE, SlaveNode, node(), make_ref()},
    case global:register_name(Name, self()) of
        yes -> SlaveNode;
        no -> {error, "failed to register process globally"}
    end.

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

slave_call(SlaveNode, Mod, Fun, Args) ->
    case rpc:call(SlaveNode, Mod, Fun, Args) of
        {badrpc, nodedown} -> {error, slave_down};
        {badrpc, {'EXIT', {Error, _}}} -> Error;
        Result -> Result
    end.

-spec safe(atom(), atom(), atom(), list()) -> term().
safe(SlaveNode, Module, Fun, Args) when is_atom(Module), is_atom(Fun), is_list(Args) ->
    slave_call(SlaveNode, Module, Fun, Args).

-spec safe(atom(), function(), list()) -> term().
safe(SlaveNode, Fun, Args) when is_function(Fun), is_list(Args) ->
    slave_call(SlaveNode, erlang, apply, [Fun, Args]).

-spec safe(atom(), function()) -> term().
safe(SlaveNode, Fun) when is_function(Fun)->
    slave_call(SlaveNode, erlang, apply, [Fun, []]).

resource_count() -> ?NIF_NOT_LOADED.
