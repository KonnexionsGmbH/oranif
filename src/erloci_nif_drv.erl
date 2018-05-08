-module(erloci_nif_drv).

-export([init/0, ociEnvNlsCreate/0,
        ociSessionPoolCreate/7, ociAuthHandleCreate/3, ociSessionGet/3,
        ociStmtHandleCreate/1, ociStmtPrepare/2, ociStmtExecute/6,
        ociBindByName/5, ociStmtFetch/2]).

-export([sql_type/1, oci_mode/1]).

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

%%--------------------------------------------------------------------
%% Create an OCI Env
%% One of these is sufficient for the whole system
%% returns {ok, Envhp}
%%--------------------------------------------------------------------
-spec ociEnvNlsCreate() -> {ok, Envhp :: reference()} | {error, binary()}.
ociEnvNlsCreate() ->
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
                           UserName :: binary(),
                           Password :: binary()) -> {ok, Spoolhp :: reference()}
                                                    | {error, binary()}.
ociSessionPoolCreate(_Envhp, _DataBase, _SessMin,
                     _SessMax, _SessInc, _UserName, _Password) ->
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
%% Create a statement Handle. Can be re-used for multiple statements.
%%--------------------------------------------------------------------
-spec ociStmtHandleCreate(Envhp :: reference()) -> {ok, Stmthp :: reference()}
                                                   | {error, binary()}.
ociStmtHandleCreate(_Envhp) ->
     ?NOT_LOADED.

-spec ociStmtPrepare(Stmthp :: reference(),
                     Stmt :: binary()) -> ok | {error, binary()}.
ociStmtPrepare(_Stmthp, _Stmt) ->
     ?NOT_LOADED.

%% 
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
                    BindVarValue :: binary() | 'NULL') ->
                                        {ok, BindVars2 :: map()}
                                        | {error, binary()}.
ociBindByName(Stmthp, BindVars, BindVarName, SqlType, BindVarValue) ->
    case BindVarValue of
        'NULL' ->
            ociBindByName(Stmthp, BindVars, BindVarName, -1, SqlType, <<>>);
        _ ->
            ociBindByName(Stmthp, BindVars, BindVarName, 0, SqlType, BindVarValue)
    end.

%% Bind named placeholder to value low level interface to nif
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

%% From oci.h
oci_mode('OCI_DEFAULT') ->           0;
oci_mode('OCI_DESCRIBE_ONLY') ->     16#00000010; %  only describe the statement
oci_mode('OCI_COMMIT_ON_SUCCESS') -> 16#00000020.  % commit, if successful exec

sql_type('SQLT_CHR') ->           1;
sql_type('SQLT_NUM') ->           2;
sql_type('SQLT_INT') ->           3;
sql_type('SQLT_FLT') ->           4;
sql_type('SQLT_STR') ->           5;
sql_type('SQLT_VNU') ->           6;
sql_type('SQLT_PDN') ->           7;
sql_type('SQLT_LNG') ->           8;
sql_type('SQLT_VCS') ->           9;
sql_type('SQLT_NON') ->           10;
sql_type('SQLT_RID') ->           11;
sql_type('SQLT_DAT') ->           12;
sql_type('SQLT_VBI') ->           15;
sql_type('SQLT_BFLOAT') ->        21;
sql_type('SQLT_BDOUBLE') ->       22;
sql_type('SQLT_BIN') ->           23;
sql_type('SQLT_LBI') ->           24;
sql_type('SQLT_UIN') ->           68;
sql_type('SQLT_SLS') ->           91;
sql_type('SQLT_LVC') ->           94;
sql_type('SQLT_LVB') ->           95;
sql_type('SQLT_AFC') ->           96;
sql_type('SQLT_AVC') ->           97;
sql_type('SQLT_IBFLOAT') ->       100;
sql_type('SQLT_IBDOUBLE') ->      101;
sql_type('SQLT_CUR') ->           102;
sql_type('SQLT_RDD') ->           104;
sql_type('SQLT_LAB') ->           105;
sql_type('SQLT_OSL') ->           106;

sql_type('SQLT_NTY') ->           108;
sql_type('SQLT_REF') ->           110;
sql_type('SQLT_CLOB') ->          112;
sql_type('SQLT_BLOB') ->          113;
sql_type('SQLT_BFILEE') ->        114;
sql_type('SQLT_CFILEE') ->        115;
sql_type('SQLT_RSET') ->          116;
sql_type('SQLT_NCO') ->           122;
sql_type('SQLT_VST') ->           155;
sql_type('SQLT_ODT') ->           156;

sql_type('SQLT_DATE') ->          184;
sql_type('SQLT_TIME') ->          185;
sql_type('SQLT_TIME_TZ') ->       186;
sql_type('SQLT_TIMESTAMP') ->     187;
sql_type('SQLT_TIMESTAMP_TZ') ->  188;
sql_type('SQLT_INTERVAL_YM') ->   189;
sql_type('SQLT_INTERVAL_DS') ->   190;
sql_type('SQLT_TIMESTAMP_LTZ') -> 232;

sql_type('SQLT_PNTY') ->          241;

%% some pl/sql specific types
sql_type('SQLT_REC') ->           250;  % pl/sql 'record' (or %rowtype)
sql_type('SQLT_TAB') ->           251;  % pl/sql 'indexed table'
sql_type('SQLT_BOL') ->           252;  % pl/sql 'boolean'


sql_type('SQLT_FILE') ->          sql_type('SQLT_BFILEE');
sql_type('SQLT_CFILE') ->         sql_type('SQLT_CFILEE');
sql_type('SQLT_BFILE') ->         sql_type('SQLT_BFILEE').
