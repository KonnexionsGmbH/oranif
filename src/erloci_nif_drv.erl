-module(erloci_nif_drv).
%% Low level API to the nif

-export([init/0, ociEnvNlsCreate/0,
        ociSessionPoolCreate/7, ociAuthHandleCreate/3, ociSessionGet/3,
        ociStmtHandleCreate/1, ociStmtPrepare/2, ociStmtExecute/6,
        ociBindByName/6, ociStmtFetch/2]).

-export([sql_type_to_int/1, int_to_sql_type/1,
        stmt_type_to_int/1, int_to_stmt_type/1,
        oci_mode/1]).

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

-spec ociEnvNlsCreate() -> {ok, Envhp :: reference()} | {error, binary()}.
ociEnvNlsCreate() ->
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

-spec ociSessionGet(Envhp :: reference(),
                    Authhp :: reference(),
                    Spoolhp :: reference()) -> {ok, Svchp :: reference()}
                                             | {error, binary()}.
ociSessionGet(_Envhp, _Authhp, _Spoolhp) ->
     ?NOT_LOADED.

-spec ociStmtHandleCreate(Envhp :: reference()) -> {ok, Stmthp :: reference()}
                                                   | {error, binary()}.
ociStmtHandleCreate(_Envhp) ->
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

%% From oci.h
%%--------------------------- OCI Statement Types -----------------------------

stmt_type_to_int('unknowN') -> 0;      % Unknown statement
stmt_type_to_int('select')  -> 1;      % select statement
stmt_type_to_int('update')  -> 2;      % update statement
stmt_type_to_int('delete')  -> 3;      % delete statement
stmt_type_to_int('insert')  -> 4;      % Insert Statement
stmt_type_to_int('create')  -> 5;      % create statement
stmt_type_to_int('drop')    -> 6;      % drop statement
stmt_type_to_int('alter')   -> 7;      % alter statement
stmt_type_to_int('begin')   -> 8;      % begin ... (pl/sql statement)
stmt_type_to_int('declare') -> 9;      % declare .. (pl/sql statement)
stmt_type_to_int('call')    -> 10.     % corresponds to kpu call

int_to_stmt_type(0) -> 'unknown';   % Unknown statement
int_to_stmt_type(1) -> 'select';    % select statement
int_to_stmt_type(2) -> 'update';    % update statement
int_to_stmt_type(3) -> 'delete';    % delete statement
int_to_stmt_type(4) -> 'insert';    % Insert Statement
int_to_stmt_type(5) -> 'create';    % create statement
int_to_stmt_type(6) -> 'drop';      % drop statement
int_to_stmt_type(7) -> 'alter';     % alter statement
int_to_stmt_type(8) -> 'begin';     % begin ... (pl/sql statement)
int_to_stmt_type(9) -> 'declare';   % declare .. (pl/sql statement)
int_to_stmt_type(10) -> 'call'.     % corresponds to kpu call

%---------------------------------------------------------------------------*/

oci_mode('OCI_DEFAULT') ->           0;
oci_mode('OCI_DESCRIBE_ONLY') ->     16#00000010; %  only describe the statement
oci_mode('OCI_COMMIT_ON_SUCCESS') -> 16#00000020.  % commit, if successful exec

sql_type_to_int('SQLT_CHR') ->           1;
sql_type_to_int('SQLT_NUM') ->           2;
sql_type_to_int('SQLT_INT') ->           3;
sql_type_to_int('SQLT_FLT') ->           4;
sql_type_to_int('SQLT_STR') ->           5;
sql_type_to_int('SQLT_VNU') ->           6;
sql_type_to_int('SQLT_PDN') ->           7;
sql_type_to_int('SQLT_LNG') ->           8;
sql_type_to_int('SQLT_VCS') ->           9;
sql_type_to_int('SQLT_NON') ->           10;
sql_type_to_int('SQLT_RID') ->           11;
sql_type_to_int('SQLT_DAT') ->           12;
sql_type_to_int('SQLT_VBI') ->           15;
sql_type_to_int('SQLT_BFLOAT') ->        21;
sql_type_to_int('SQLT_BDOUBLE') ->       22;
sql_type_to_int('SQLT_BIN') ->           23;
sql_type_to_int('SQLT_LBI') ->           24;
sql_type_to_int('SQLT_UIN') ->           68;
sql_type_to_int('SQLT_SLS') ->           91;
sql_type_to_int('SQLT_LVC') ->           94;
sql_type_to_int('SQLT_LVB') ->           95;
sql_type_to_int('SQLT_AFC') ->           96;
sql_type_to_int('SQLT_AVC') ->           97;
sql_type_to_int('SQLT_IBFLOAT') ->       100;
sql_type_to_int('SQLT_IBDOUBLE') ->      101;
sql_type_to_int('SQLT_CUR') ->           102;
sql_type_to_int('SQLT_RDD') ->           104;
sql_type_to_int('SQLT_LAB') ->           105;
sql_type_to_int('SQLT_OSL') ->           106;

sql_type_to_int('SQLT_NTY') ->           108;
sql_type_to_int('SQLT_REF') ->           110;
sql_type_to_int('SQLT_CLOB') ->          112;
sql_type_to_int('SQLT_BLOB') ->          113;
sql_type_to_int('SQLT_BFILEE') ->        114;
sql_type_to_int('SQLT_CFILEE') ->        115;
sql_type_to_int('SQLT_RSET') ->          116;
sql_type_to_int('SQLT_NCO') ->           122;
sql_type_to_int('SQLT_VST') ->           155;
sql_type_to_int('SQLT_ODT') ->           156;

sql_type_to_int('SQLT_DATE') ->          184;
sql_type_to_int('SQLT_TIME') ->          185;
sql_type_to_int('SQLT_TIME_TZ') ->       186;
sql_type_to_int('SQLT_TIMESTAMP') ->     187;
sql_type_to_int('SQLT_TIMESTAMP_TZ') ->  188;
sql_type_to_int('SQLT_INTERVAL_YM') ->   189;
sql_type_to_int('SQLT_INTERVAL_DS') ->   190;
sql_type_to_int('SQLT_TIMESTAMP_LTZ') -> 232;

sql_type_to_int('SQLT_PNTY') ->          241;

%% some pl/sql specific types
sql_type_to_int('SQLT_REC') ->           250;  % pl/sql 'record' (or %rowtype)
sql_type_to_int('SQLT_TAB') ->           251;  % pl/sql 'indexed table'
sql_type_to_int('SQLT_BOL') ->           252;  % pl/sql 'boolean'


sql_type_to_int('SQLT_FILE') ->          sql_type_to_int('SQLT_BFILEE');
sql_type_to_int('SQLT_CFILE') ->         sql_type_to_int('SQLT_CFILEE');
sql_type_to_int('SQLT_BFILE') ->         sql_type_to_int('SQLT_BFILEE').

%% Convert internal SQL type code to atom
int_to_sql_type(1) ->   'SQLT_CHR';
int_to_sql_type(2) ->   'SQLT_NUM';
int_to_sql_type(3) ->   'SQLT_INT';
int_to_sql_type(4) ->   'SQLT_FLT';
int_to_sql_type(5) ->   'SQLT_STR';
int_to_sql_type(6) ->   'SQLT_VNU';
int_to_sql_type(7) ->   'SQLT_PDN';
int_to_sql_type(8) ->   'SQLT_LNG';
int_to_sql_type(9) ->   'SQLT_VCS';
int_to_sql_type(10) ->   'SQLT_NON';
int_to_sql_type(11) ->   'SQLT_RID';
int_to_sql_type(12) ->   'SQLT_DAT';
int_to_sql_type(15) ->   'SQLT_VBI';
int_to_sql_type(21) -> 'SQLT_BFLOAT';
int_to_sql_type(22) -> 'SQLT_BDOUBLE';
int_to_sql_type(23) ->  'SQLT_BIN';
int_to_sql_type(24) ->  'SQLT_LBI';
int_to_sql_type(68) ->  'SQLT_UIN';
int_to_sql_type(91) ->  'SQLT_SLS';
int_to_sql_type(94) ->  'SQLT_LVC';
int_to_sql_type(95) ->  'SQLT_LVB';
int_to_sql_type(96) ->  'SQLT_AFC';
int_to_sql_type(97) ->  'SQLT_AVC';
int_to_sql_type(100) -> 'SQLT_IBFLOAT';
int_to_sql_type(101) -> 'SQLT_IBDOUBLE';
int_to_sql_type(102) -> 'SQLT_CUR';
int_to_sql_type(104) -> 'SQLT_RDD';
int_to_sql_type(105) -> 'SQLT_LAB';
int_to_sql_type(106) -> 'SQLT_OSL';

int_to_sql_type(108) -> 'SQLT_NTY';
int_to_sql_type(110) -> 'SQLT_REF';
int_to_sql_type(112) -> 'SQLT_CLOB';
int_to_sql_type(113) -> 'SQLT_BLOB';
int_to_sql_type(114) -> 'SQLT_BFILEE';
int_to_sql_type(115) -> 'SQLT_CFILEE';
int_to_sql_type(116) -> 'SQLT_RSET';
int_to_sql_type(122) -> 'SQLT_NCO';
int_to_sql_type(155) -> 'SQLT_VST';
int_to_sql_type(156) -> 'SQLT_ODT';

int_to_sql_type(184) -> 'SQLT_DATE';
int_to_sql_type(185) -> 'SQLT_TIME';
int_to_sql_type(186) -> 'SQLT_TIME_TZ';
int_to_sql_type(187) -> 'SQLT_TIMESTAMP';
int_to_sql_type(188) -> 'SQLT_TIMESTAMP_TZ';
int_to_sql_type(189) -> 'SQLT_INTERVAL_YM';
int_to_sql_type(190) -> 'SQLT_INTERVAL_DS';
int_to_sql_type(232) -> 'SQLT_TIMESTAMP_LTZ';

int_to_sql_type(241) -> 'SQLT_PNTY';

%% some pl/sql specific types
int_to_sql_type(250) -> 'SQLT_REC';             % pl/sql 'record' (or %rowtype)
int_to_sql_type(251) -> 'SQLT_TAB';             % pl/sql 'indexed table'
int_to_sql_type(252) -> 'SQLT_BOL'.             % pl/sql 'boolean'
