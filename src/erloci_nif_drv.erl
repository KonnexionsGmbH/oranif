-module(erloci_nif_drv).
%% Low level API to the nif

-export([init/0, ociEnvNlsCreate/2, ociEnvHandleFree/1, ociTerminate/0,
        ociNlsGetInfo/2, ociCharsetAttrGet/1,
        ociNlsCharSetIdToName/2, ociNlsCharSetNameToId/2,
        ociAttrSet/5, ociAttrGet/4,
        ociSessionPoolCreate/7, ociSessionPoolDestroy/1,
        ociAuthHandleCreate/3,
        ociSessionGet/3, ociSessionRelease/1,
        ociStmtHandleCreate/1, ociStmtPrepare/2, ociStmtExecute/6,
        ociStmtHandleFree/1,
        ociBindByName/6, ociStmtFetch/2]).

-on_load(init/0).

-define(NOT_LOADED, not_loaded(?LINE)).

init() ->
    PrivDir = case code:priv_dir(?MODULE) of
                  {error, _} ->
                      EbinDir = filename:dirname(code:which(?MODULE)),
                      AppPath = filename:dirname(EbinDir),
                      filename:join(AppPath, "priv");
                  Path ->
                      Path
              end,
    case erlang:load_nif(filename:join(PrivDir, "erloci_nif"), 0) of
        ok ->                  ok;
        {error,{reload, _}} -> ok;
        Error ->               Error
    end.

-spec ociEnvNlsCreate(ClientCharset :: pos_integer(),
                    NationalCharset :: pos_integer()) -> {ok, Envhp :: reference()}
                                                     | {error, binary()}.
ociEnvNlsCreate(_ClientCharset, _NationalCharset) ->
    ?NOT_LOADED.

-spec ociEnvHandleFree(Envhp :: reference()) -> ok.
ociEnvHandleFree(_Envhp) ->
    ?NOT_LOADED.

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
                 CDataTpe :: integer(),
                 AttrType :: integer()) ->
                     ok | {error, binary()}.
ociAttrGet(_Handle, _HandleType, _CDataTpe, _AttrType) ->
    ?NOT_LOADED.

-spec ociAuthHandleCreate(Envhp :: reference(),
                          UserName :: binary(),
                          Password :: binary()) -> {ok, Authhp :: reference()}
                                                   | {error, binary()}.
ociAuthHandleCreate(_Envhp, _UserName, _Password) ->
    ?NOT_LOADED.

-spec ociSessionPoolCreate(Envhp :: reference(),
                           DataBase :: binary(),
                           SessMin :: pos_integer(),
                           SessMax :: pos_integer(),
                           SessInc :: pos_integer(),
                           UserName :: binary(),
                           Password :: binary()) -> {ok, Spoolhp :: reference()}
                                                    | {error, binary()}.
ociSessionPoolCreate(_Envhp, _DataBase, _SessMin,
                     _SessMax, _SessInc, _UserName, _Password) ->
    ?NOT_LOADED.

-spec ociSessionPoolDestroy(Spoolhp :: reference()) -> ok.
ociSessionPoolDestroy(_Spoolhp) ->
    ?NOT_LOADED.
-spec ociSessionGet(Envhp :: reference(),
                    Authhp :: reference(),
                    Spoolhp :: reference()) -> {ok, Svchp :: reference()}
                                             | {error, binary()}.
ociSessionGet(_Envhp, _Authhp, _Spoolhp) ->
    ?NOT_LOADED.

-spec ociSessionRelease(Svchp :: reference()) -> ok
                                                | {error, binary()}.
ociSessionRelease(_Svchp) ->
    ?NOT_LOADED.

-spec ociStmtHandleCreate(Envhp :: reference()) -> {ok, Stmthp :: reference()}
                                                   | {error, binary()}.
ociStmtHandleCreate(_Envhp) ->
     ?NOT_LOADED.

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
                    Ind :: integer(),
                    SqlType :: integer(),
                    BindVarValue :: binary()) -> {ok, Bindvars2 :: map()}
                                                 | {error, binary()}.
ociBindByName(_Stmthp, _BindVars, _BindVarName, _Ind, _SqlType, _BindVarValue) ->
    ?NOT_LOADED.

-spec ociStmtFetch(Stmthp :: {reference(), map()},
                   NumRows :: pos_integer()) -> {ok, [term]}
                                                | {error, binary()}.
ociStmtFetch(_Stmthp, _NumRows) ->
    ?NOT_LOADED.

not_loaded(Line) ->
    erlang:nif_error({not_loaded, [{module, ?MODULE}, {line, Line}]}).
