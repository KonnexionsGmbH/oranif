-module(oci).
%% Low level API to the nif

-export([init/0, ociEnvNlsCreate/2, ociEnvHandleFree/1, ociAuthHandleFree/1,
         ociTerminate/0, ociNlsGetInfo/2, ociCharsetAttrGet/1, ociAttrSet/5,
         ociNlsCharSetIdToName/2, ociNlsCharSetNameToId/2, ociPing/1,
         ociAttrGet/3, ociSessionPoolCreate/7, ociSessionPoolDestroy/1,
         ociAuthHandleCreate/3, ociSessionGet/3, ociSessionRelease/1,
         ociStmtHandleCreate/1, ociStmtPrepare/2, ociStmtExecute/6,
         ociStmtHandleFree/1, ociBindByName/5, ociStmtFetch/2]).

% lib API
-export([parse_lang/1]).

-on_load(init/0).

-define(NOT_LOADED, not_loaded(?LINE)).

init() ->
    PrivDir =
        case code:priv_dir(?MODULE) of
            {error, _} ->
                EbinDir = filename:dirname(code:which(?MODULE)),
                AppPath = filename:dirname(EbinDir),
                filename:join(AppPath, "priv");
            Path ->
                Path
        end,
    check_lib(PrivDir),
    case erlang:load_nif(filename:join(PrivDir, "ocinif"), 0) of
        ok ->                  ok;
        {error,{reload, _}} -> ok;
        {error, Error} ->      error(Error)
    end.

%%--------------------------------------------------------------------
%% Create an OCI Env
%% One of these is sufficient for the whole system
%% returns {ok, Envhp}
%%--------------------------------------------------------------------
-spec ociEnvNlsCreate(ClientCharset :: pos_integer(),
                    NationalCharset :: pos_integer()) -> {ok, Envhp :: reference()}
                                                     | {error, binary()}.
ociEnvNlsCreate(_ClientCharset, _NationalCharset) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Free the OCI Env handle
%%--------------------------------------------------------------------
-spec ociEnvHandleFree(Envhp :: reference()) -> ok.
ociEnvHandleFree(_Envhp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Free the OCI Env handle
%%--------------------------------------------------------------------
-spec ociAuthHandleFree(Authhp :: reference()) -> ok.
ociAuthHandleFree(_Authhp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Terminate the instance of OCI releasing all shared memory held by OCI.
%%--------------------------------------------------------------------
-spec ociTerminate() -> ok.
ociTerminate() ->
    ?NOT_LOADED.

-spec ociNlsGetInfo(Envhp :: reference(),
                    Item :: integer()) -> {ok, Info :: binary()}
                                          | {error, binary()}.
ociNlsGetInfo(_Envhp, _Item) ->
    ?NOT_LOADED.

-spec ociCharsetAttrGet(Envhp :: reference()) ->
    {ok, {Charset :: integer(), NCharset :: integer()}}
    | {error, binary()}.
ociCharsetAttrGet(_Envhp) ->
    ?NOT_LOADED.

-spec ociNlsCharSetIdToName(Envhp :: reference(), CharsetId :: integer()) ->
        {ok, CharsetName :: binary()}
        | {error, binary()}.
ociNlsCharSetIdToName(_Envhp, _CharsetId) ->
    ?NOT_LOADED.

-spec ociNlsCharSetNameToId(Envhp :: reference(), CharsetName :: binary()) ->
    {ok, CharsetId :: integer()}
    | {error, binary()}.
ociNlsCharSetNameToId(Envhp, CharsetName) ->
    ociNlsCharSetNameToId_ll(Envhp, <<CharsetName/binary, 0>>).

ociNlsCharSetNameToId_ll(_Envhp, _CharsetName) ->
    ?NOT_LOADED.

-spec ociAttrSet(Handle :: reference(),
                 HandleType :: integer(),
                 CDataTpe :: integer(),
                 Value :: integer() | binary(),
                 AttrType :: integer()) ->
                     ok | {error, binary()}.
ociAttrSet(_Handle, _HandleType, _CDataTpe, _Value, _AttrType) ->
    ?NOT_LOADED.

-spec ociAttrGet(Handle :: reference(),
                 HandleType :: integer(),
                 AttrType :: integer()) ->
                     ok | {error, binary()}.
ociAttrGet(_Handle, _HandleType, _AttrType) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Ping the database on the referenced Session
%%--------------------------------------------------------------------
-spec ociPing(Svchp :: reference()) -> pong | pang.
ociPing(_Svchp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Create an Auth Handle based on supplied username / password
%% Used as an argument to ociSessionGet
%% returns {ok, Authhp}
%%--------------------------------------------------------------------
-spec ociAuthHandleCreate(Envhp :: reference(),
                          UserName :: binary(),
                          Password :: binary()) -> {ok, Authhp :: reference()}
                                                   | {error, binary()}.
ociAuthHandleCreate(_Envhp, _UserName, _Password) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Create a session pool. All operations to the database must use one
%% of the connections to this pool.
%% Username and Password here are used for all sessions in this pool 
%% (i.e. the pool uses OCI_SPC_HOMOGENEOUS)
%% returns {ok, PoolName}
%%--------------------------------------------------------------------
-spec ociSessionPoolCreate(Envhp :: reference(),
                           DataBase :: binary(),
                           SessMin :: pos_integer(),
                           SessMax :: pos_integer(),
                           SessInc :: pos_integer(),
                           UserName :: null | binary(),
                           Password :: null | binary()) ->
        {ok, Spoolhp :: reference()} | {error, binary()}.
ociSessionPoolCreate(_Envhp, _DataBase, _SessMin,
                     _SessMax, _SessInc, _UserName, _Password) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Destroy a session pool. Any work on outstanding sessions will cause
%% this to return an error return
%%--------------------------------------------------------------------
-spec ociSessionPoolDestroy(Spoolhp :: reference()) -> ok.
ociSessionPoolDestroy(_Spoolhp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Fetch a session from the session pool.
%% Returns {ok, Svchp :: reference()}
%%--------------------------------------------------------------------
-spec ociSessionGet(Envhp :: reference(),
                    Authhp :: reference(),
                    Spoolhp :: reference()) -> {ok, Svchp :: reference()}
                                             | {error, binary()}.
ociSessionGet(_Envhp, _Authhp, _Spoolhp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Returns a session to the session pool.
%%--------------------------------------------------------------------
-spec ociSessionRelease(Svchp :: reference()) -> ok | {error, binary()}.
ociSessionRelease(_Svchp) ->
    ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Create a statement Handle. Can be re-used for multiple statements.
%%--------------------------------------------------------------------
-spec ociStmtHandleCreate(Envhp :: reference()) -> {ok, Stmthp :: reference()}
                                                   | {error, binary()}.
ociStmtHandleCreate(_Envhp) ->
     ?NOT_LOADED.

%%--------------------------------------------------------------------
%% Free a statement handle.
%%--------------------------------------------------------------------
-spec ociStmtHandleFree(Stmthp :: reference()) -> ok.
ociStmtHandleFree(_Stmthp) ->
    ?NOT_LOADED.

-spec ociStmtPrepare(Stmthp :: reference(),
                     Stmt :: binary()) -> ok | {error, binary()}.
ociStmtPrepare(_Stmthp, _Stmt) ->
     ?NOT_LOADED.

-spec ociStmtExecute(Svchp :: reference(),
                     Stmthp :: reference(),
                     BindVars :: map(),
                     Iters :: pos_integer(),
                     RowOff :: pos_integer(),
                     Mode :: integer()) -> ok | {error, binary()}.
ociStmtExecute(_Svchp, _Stmthp, _BindVars, _Iters, _RowOff, _Mode) ->
    ?NOT_LOADED.

-spec ociBindByName(Stmthp :: reference(),
                    BindVars :: map(),
                    BindVarName :: binary(),
                    SqlType :: integer(),
                    BindVarValue :: binary() | null) -> {ok, Bindvars2 :: map()}
                                                 | {error, binary()}.
ociBindByName(_Stmthp, _BindVars, _BindVarName, _SqlType, _BindVarValue) ->
    ?NOT_LOADED.

-spec ociStmtFetch(Stmthp :: {reference(), map()},
                   NumRows :: pos_integer()) -> {ok, [term]}
                                                | {error, binary()}.
ociStmtFetch(_Stmthp, _NumRows) ->
    ?NOT_LOADED.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).

%%--------------------------------------------------------------------
%% Parses a TNS style language/country/charset string
%%--------------------------------------------------------------------
parse_lang(Lang) when is_list(Lang) ->
    case string:tokens(Lang, "._") of
        [Language, Country, Charset] ->
            {list_to_binary(Language),
                list_to_binary(Country),
                list_to_binary(Charset)};
        Else ->
            Else
    end;
parse_lang(Lang) when is_binary(Lang) ->
    case binary:split(Lang, [<<".">>, <<"_">>], [global]) of
        [Language, Country, Charset] ->
            {Language, Country, Charset};
        Else ->
                Else
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
check_lib(PrivDir) ->
    {Envs, Seperator, LibFiles} =
        case os:type() of
            {unix, darwin} ->
                {["DYLD_LIBRARY_PATH", "PATH"], ":", ["libocci.dylib"]};
            {win32, nt} ->
                {["PATH"], ";", ["oci.dll", "oraons.dll", "oraociei12.dll"]};
            {unix, linux}->
                {["LD_LIBRARY_PATH", "PATH"], ":", ["libocci.so"]}
        end,
    Paths = [
        PrivDir |
            re:split(
                string:join(
                    lists:foldl(
                        fun(E, Acc) ->
                            case os:getenv(E) of
                                Path when is_list(Path) -> [Path | Acc];
                                _ -> Acc
                            end
                        end, [], Envs
                    ), Seperator
                ), Seperator, [{return, list}]
            )
    ],
    case lists:flatten(
            [filelib:wildcard(L, P) || L <- LibFiles, P <- Paths]
    ) of
        [] -> error({"OCI runtime libraries missing", Envs, LibFiles, Paths});
        _ ->
            if Envs == ["PATH"] ->
                    os:putenv(
                        "PATH",
                        lists:flatten([os:getenv("PATH"), Seperator, PrivDir])
                    );
                true -> ok
            end
    end.
