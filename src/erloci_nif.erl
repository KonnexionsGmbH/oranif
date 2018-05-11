-module(erloci_nif).

%% External API to OCI Driver

-export([ociEnvNlsCreate/2, ociEnvHandleFree/1, ociTerminate/0,
        ociNlsGetInfo/2, ociCharsetAttrGet/1,
        ociAttrSet/5, ociAttrGet/4,
        ociSessionPoolCreate/7, ociSessionPoolDestroy/1,
        ociAuthHandleCreate/3,
        ociSessionGet/3, ociSessionRelease/1,
        ociStmtHandleCreate/1, ociStmtPrepare/2, ociStmtExecute/6,
        ociBindByName/5, ociStmtFetch/2,
        ociStmtHandleFree/1]).

-export([parse_lang/1]).

%%--------------------------------------------------------------------
%% Create an OCI Env
%% One of these is sufficient for the whole system
%% returns {ok, Envhp}
%%--------------------------------------------------------------------
-spec ociEnvNlsCreate(ClientCharset :: integer(),
                     NationalCharset :: integer()) -> {ok, Envhp :: reference()}
                                                     | {error, binary()}.
ociEnvNlsCreate(ClientCharset, NationalCharset) ->
    erloci_nif_drv:ociEnvNlsCreate(ClientCharset, NationalCharset).

%%--------------------------------------------------------------------
%% Free the OCI Env handle
%%--------------------------------------------------------------------
-spec ociEnvHandleFree(Envhp :: reference()) -> ok.
ociEnvHandleFree(Envhp) ->
    erloci_nif_drv:ociEnvHandleFree(Envhp).

%%--------------------------------------------------------------------
%% Terminate the instance of OCI releasing all shared memory held by OCI.
%%--------------------------------------------------------------------
-spec ociTerminate() -> ok.
ociTerminate() ->
    erloci_nif_drv:ociTerminate().

-spec ociNlsGetInfo(Envhp :: reference(),
                    Item :: atom()) -> {ok, Info :: binary()}
                                          | {error, binary()}.
ociNlsGetInfo(Envhp, Item) ->
    ItemId = erloci_nif_int:nls_info_to_int(Item),
    erloci_nif_drv:ociNlsGetInfo(Envhp, ItemId).

-spec ociCharsetAttrGet(Envhp :: reference()) ->
    {ok, {Charset :: integer(), NCharset :: integer()}}
    | {error, binary()}.
ociCharsetAttrGet(Envhp) ->
    case erloci_nif_drv:ociCharsetAttrGet(Envhp) of
        {ok, {CharsetId, NCharsetId}} ->
            {ok, Name} = erloci_nif_drv:ociNlsCharSetIdToName(Envhp, CharsetId),
            {ok, NName} = erloci_nif_drv:ociNlsCharSetIdToName(Envhp, NCharsetId),
            {ok, #{charset => {CharsetId, Name},
                   ncharset => {NCharsetId, NName}}};
        Err ->
            Err
    end.

-spec ociAttrSet(Handle :: reference(),
                HandleType :: atom(),
                CDataType :: atom(),
                Value :: integer() | binary(),
                AttrType :: atom()) ->
                    ok | {error, binary()}.
ociAttrSet(Handle, HandleType, CDataTpe, Value, AttrType) ->
    HandleTypeInt = erloci_nif_int:handle_type_to_int(HandleType),
    CDataTypeInt = erloci_nif_int:c_type(CDataTpe),
    AttrTypeInt = erloci_nif_int:attr_name_to_int(AttrType),
    erloci_nif_drv:ociAttrSet(Handle, HandleTypeInt, CDataTypeInt, Value, AttrTypeInt).

-spec ociAttrGet(Handle :: reference(),
                HandleType :: atom(),
                CDataTpe :: atom(),
                AttrType :: atom()) ->
        {ok, Value :: integer() | binary()} | {error, binary()}.
ociAttrGet(Handle, HandleType, CDataTpe, AttrType) ->
    HandleTypeInt = erloci_nif_int:handle_type_to_int(HandleType),
    CDataTypeInt = erloci_nif_int:c_type(CDataTpe),
    AttrTypeInt = erloci_nif_int:attr_name_to_int(AttrType),
erloci_nif_drv:ociAttrGet(Handle, HandleTypeInt, CDataTypeInt, AttrTypeInt).

%%--------------------------------------------------------------------
%% Create an Auth Handle based on supplied username / password
%% Used as an argument to ociSessionGet
%% returns {ok, Authhp}
%%--------------------------------------------------------------------
-spec ociAuthHandleCreate(Envhp :: reference(),
                          UserName :: binary(),
                          Password :: binary()) -> {ok, Authhp :: reference()}
                                                   | {error, binary()}.
ociAuthHandleCreate(Envhp, UserName, Password) ->
    erloci_nif_drv:ociAuthHandleCreate(Envhp, UserName, Password).

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
                           UserName :: binary(),
                           Password :: binary()) -> {ok, Spoolhp :: reference()}
                                                    | {error, binary()}.
ociSessionPoolCreate(Envhp, DataBase, SessMin,
                     SessMax, SessInc, UserName, Password) ->
    erloci_nif_drv:ociSessionPoolCreate(Envhp, DataBase, SessMin,
                                        SessMax, SessInc, UserName, Password).

%%--------------------------------------------------------------------
%% Destroy a session pool. Any work on outstanding sessions will cause
%% this to return an error return
%%--------------------------------------------------------------------
-spec ociSessionPoolDestroy(Spoolhp :: reference()) -> ok.
ociSessionPoolDestroy(Spoolhp) ->
    erloci_nif_drv:ociSessionPoolDestroy(Spoolhp).

%%--------------------------------------------------------------------
%% Fetch a session from the session pool.
%% Returns {ok, Svchp :: reference()}
%%--------------------------------------------------------------------
-spec ociSessionGet(Envhp :: reference(),
                    Authhp :: reference(),
                    Spoolhp :: reference()) -> {ok, Svchp :: reference()}
                                             | {error, binary()}.
ociSessionGet(Envhp, Authhp, Spoolhp) ->
    erloci_nif_drv:ociSessionGet(Envhp, Authhp, Spoolhp).

%%--------------------------------------------------------------------
%% Returns a session to the session pool.
%%--------------------------------------------------------------------
-spec ociSessionRelease(Svchp :: reference()) -> ok
                                                 | {error, binary()}.
ociSessionRelease(Svchp) ->
    erloci_nif_drv:ociSessionRelease(Svchp).

%%--------------------------------------------------------------------
%% Create a statement Handle. Can be re-used for multiple statements.
%%--------------------------------------------------------------------
-spec ociStmtHandleCreate(Envhp :: reference()) -> {ok, Stmthp :: reference()}
                                                   | {error, binary()}.
ociStmtHandleCreate(Envhp) ->
    erloci_nif_drv:ociStmtHandleCreate(Envhp).

%%--------------------------------------------------------------------
%% Free a statement handle.
%%--------------------------------------------------------------------
-spec ociStmtHandleFree(Stmthp :: reference()) -> ok.
ociStmtHandleFree(Stmthp) ->
    erloci_nif_drv:ociStmtHandleFree(Stmthp).

-spec ociStmtPrepare(Stmthp :: reference(),
                    Stmt :: binary()) -> ok | {error, binary()}.
ociStmtPrepare(Stmthp, Stmt) ->
        erloci_nif_drv:ociStmtPrepare(Stmthp, Stmt).
%%
-spec ociStmtExecute(Svchp :: reference(),
                    Stmthp :: reference(),
                    BindVars :: map(),
                    Iters :: pos_integer(),
                    RowOff :: pos_integer(),
                    Mode :: atom()) -> ok | {error, binary()}.
ociStmtExecute(Svchp, Stmthp, BindVars, Iters, RowOff, Mode) ->
    ModeInt = erloci_nif_int:oci_mode(Mode),
    case erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, BindVars, Iters, RowOff, ModeInt) of
        {ok, #{statement := Stmt} = Map} ->
            {ok, maps:put(statement, erloci_nif_int:int_to_stmt_type(Stmt), Map)};
        Else ->
            Else
    end.

-spec ociBindByName(Stmthp :: reference(),
                    BindVars :: map(),
                    BindVarName :: binary(),
                    SqlType :: atom(),
                    BindVarValue :: binary() | 'NULL') ->
                                        {ok, BindVars2 :: map()}
                                        | {error, binary()}.
ociBindByName(Stmthp, BindVars, BindVarName, SqlType, BindVarValue) when
                                                    is_reference(Stmthp),
                                                    is_map(BindVars),
                                                    is_binary(BindVarName),
                                                    is_atom(SqlType),
                                                    is_binary(BindVarValue) ->
    IntType = erloci_nif_int:sql_type_to_int(SqlType),
    case BindVarValue of
        'NULL' ->
            erloci_nif_drv:ociBindByName(Stmthp, BindVars, BindVarName, -1, IntType, <<>>);
        _ ->
            erloci_nif_drv:ociBindByName(Stmthp, BindVars, BindVarName, 0, IntType, BindVarValue)
    end.

-spec ociStmtFetch(Stmthp :: {reference(), map()},
  NumRows :: pos_integer()) -> {ok, [term]}
                               | {error, binary()}.
ociStmtFetch(Stmthp, NumRows) ->
        erloci_nif_drv:ociStmtFetch(Stmthp, NumRows).

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