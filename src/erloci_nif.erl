-module(erloci_nif).

%% External API to OCI Driver

-export([ociEnvNlsCreate/0,
        ociSessionPoolCreate/7, ociAuthHandleCreate/3, ociSessionGet/3,
        ociStmtHandleCreate/1, ociStmtPrepare/2, ociStmtExecute/6,
        ociBindByName/5, ociStmtFetch/2]).

%%--------------------------------------------------------------------
%% Create an OCI Env
%% One of these is sufficient for the whole system
%% returns {ok, Envhp}
%%--------------------------------------------------------------------
-spec ociEnvNlsCreate() -> {ok, Envhp :: reference()} | {error, binary()}.
ociEnvNlsCreate() ->
    erloci_nif_drv:ociEnvNlsCreate().

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
%% Create a statement Handle. Can be re-used for multiple statements.
%%--------------------------------------------------------------------
-spec ociStmtHandleCreate(Envhp :: reference()) -> {ok, Stmthp :: reference()}
                                                   | {error, binary()}.
ociStmtHandleCreate(Envhp) ->
    erloci_nif_drv:ociStmtHandleCreate(Envhp).

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
    ModeInt = erloci_nif_drv:oci_mode(Mode),
    erloci_nif_drv:ociStmtExecute(Svchp, Stmthp, BindVars, Iters, RowOff, ModeInt).

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
    IntType = erloci_nif_drv:sql_type_to_int(SqlType),
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
