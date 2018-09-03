-module(erloci_int).

-export([sql_type_to_int/1, int_to_sql_type/1,
        stmt_type_to_int/1, int_to_stmt_type/1,
        nls_info_to_int/1,
        handle_type_to_int/1,
        attr_name_to_int/1,
        c_type/1,
        oci_mode/1]).

c_type(text) -> 0;
c_type(ub1) -> 1;
c_type(ub2) -> 2;
c_type(ub4) -> 3;
c_type(sb1) -> 4;
c_type(sb2) -> 5;
c_type(sb4) -> 6.

%% From oci.h
%%--------------------------- OCI Statement Types -----------------------------

stmt_type_to_int('unknown') -> 0;      % Unknown statement
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

%%---------------------------------------------------------------------------

%%-----------------------------Handle Types----------------------------------
                                           % handle types range from 1 - 49
handle_type_to_int('OCI_HTYPE_FIRST')         -> 1; % start value of handle type
handle_type_to_int('OCI_HTYPE_ENV')           -> 1;         % environment handle
handle_type_to_int('OCI_HTYPE_ERROR')         -> 2;         % error handle
handle_type_to_int('OCI_HTYPE_SVCCTX')        -> 3;         % service handle
handle_type_to_int('OCI_HTYPE_STMT')          -> 4;         % statement handle
handle_type_to_int('OCI_HTYPE_BIND')          -> 5;         % bind handle
handle_type_to_int('OCI_HTYPE_DEFINE')        -> 6;         % define handle
handle_type_to_int('OCI_HTYPE_DESCRIBE')      -> 7;         % describe handle
handle_type_to_int('OCI_HTYPE_SERVER')        -> 8;         % server handle
handle_type_to_int('OCI_HTYPE_SESSION')       -> 9;      % authentication handle
handle_type_to_int('OCI_HTYPE_AUTHINFO')      ->
    handle_type_to_int('OCI_HTYPE_SESSION');            % SessionGet auth handle
handle_type_to_int('OCI_HTYPE_TRANS')         -> 10;        % transaction handle
handle_type_to_int('OCI_HTYPE_COMPLEXOBJECT') -> 11; % complex object retrieval handle
handle_type_to_int('OCI_HTYPE_SECURITY')      -> 12;       % security handle
handle_type_to_int('OCI_HTYPE_SUBSCRIPTION')  -> 13;       % subscription handle
handle_type_to_int('OCI_HTYPE_DIRPATH_CTX')   -> 14;       % direct path context
handle_type_to_int('OCI_HTYPE_DIRPATH_COLUMN_ARRAY') -> 15; % direct path column array
handle_type_to_int('OCI_HTYPE_DIRPATH_STREAM')       -> 16; % direct path stream
handle_type_to_int('OCI_HTYPE_PROC')                 -> 17; % process handle
handle_type_to_int('OCI_HTYPE_DIRPATH_FN_CTX')       -> 18; % direct path function context
handle_type_to_int('OCI_HTYPE_DIRPATH_FN_COL_ARRAY') -> 19; % dp object column array
handle_type_to_int('OCI_HTYPE_XADSESSION')   -> 20;      % access driver session
handle_type_to_int('OCI_HTYPE_XADTABLE')     -> 21;      % access driver table
handle_type_to_int('OCI_HTYPE_XADFIELD')     -> 22;      % access driver field
handle_type_to_int('OCI_HTYPE_XADGRANULE')   -> 23;      % access driver granule
handle_type_to_int('OCI_HTYPE_XADRECORD')    -> 24;      % access driver record
handle_type_to_int('OCI_HTYPE_XADIO')        -> 25;      % access driver I/O
handle_type_to_int('OCI_HTYPE_CPOOL')        -> 26;     % connection pool handle
handle_type_to_int('OCI_HTYPE_SPOOL')        -> 27;      % session pool handle
handle_type_to_int('OCI_HTYPE_ADMIN')        -> 28;      % admin handle
handle_type_to_int('OCI_HTYPE_EVENT')        -> 29;      % HA event handle
handle_type_to_int('OCI_HTYPE_LAST')         -> 29. % last value of a handle type

%%---------------------------------------------------------------------------

%%--------------------- NLS service type and constance ----------------------
nls_info_to_int('OCI_NLS_DAYNAME1') ->      1;                    % Native name for Monday
nls_info_to_int('OCI_NLS_DAYNAME2') ->      2;                   % Native name for Tuesday
nls_info_to_int('OCI_NLS_DAYNAME3') ->      3;                 % Native name for Wednesday
nls_info_to_int('OCI_NLS_DAYNAME4') ->      4;                  % Native name for Thursday
nls_info_to_int('OCI_NLS_DAYNAME5') ->      5;                    % Native name for Friday
nls_info_to_int('OCI_NLS_DAYNAME6') ->      6;              % Native name for for Saturday
nls_info_to_int('OCI_NLS_DAYNAME7') ->      7;                % Native name for for Sunday
nls_info_to_int('OCI_NLS_ABDAYNAME1') ->    8;        % Native abbreviated name for Monday
nls_info_to_int('OCI_NLS_ABDAYNAME2') ->    9;       % Native abbreviated name for Tuesday
nls_info_to_int('OCI_NLS_ABDAYNAME3') ->    10;    % Native abbreviated name for Wednesday
nls_info_to_int('OCI_NLS_ABDAYNAME4') ->    11;     % Native abbreviated name for Thursday
nls_info_to_int('OCI_NLS_ABDAYNAME5') ->    12;       % Native abbreviated name for Friday
nls_info_to_int('OCI_NLS_ABDAYNAME6') ->    13; % Native abbreviated name for for Saturday
nls_info_to_int('OCI_NLS_ABDAYNAME7') ->    14;   % Native abbreviated name for for Sunday
nls_info_to_int('OCI_NLS_MONTHNAME1') ->    15;                  % Native name for January
nls_info_to_int('OCI_NLS_MONTHNAME2') ->    16;                 % Native name for February
nls_info_to_int('OCI_NLS_MONTHNAME3') ->    17;                    % Native name for March
nls_info_to_int('OCI_NLS_MONTHNAME4') ->    18;                    % Native name for April
nls_info_to_int('OCI_NLS_MONTHNAME5') ->    19;                      % Native name for May
nls_info_to_int('OCI_NLS_MONTHNAME6') ->    20;                     % Native name for June
nls_info_to_int('OCI_NLS_MONTHNAME7') ->    21;                     % Native name for July
nls_info_to_int('OCI_NLS_MONTHNAME8') ->    22;                   % Native name for August
nls_info_to_int('OCI_NLS_MONTHNAME9') ->    23;                % Native name for September
nls_info_to_int('OCI_NLS_MONTHNAME10') ->   24;                  % Native name for October
nls_info_to_int('OCI_NLS_MONTHNAME11') ->   25;                 % Native name for November
nls_info_to_int('OCI_NLS_MONTHNAME12') ->   26;                 % Native name for December
nls_info_to_int('OCI_NLS_ABMONTHNAME1') ->  27;      % Native abbreviated name for January
nls_info_to_int('OCI_NLS_ABMONTHNAME2') ->  28;     % Native abbreviated name for February
nls_info_to_int('OCI_NLS_ABMONTHNAME3') ->  29;        % Native abbreviated name for March
nls_info_to_int('OCI_NLS_ABMONTHNAME4') ->  30;        % Native abbreviated name for April
nls_info_to_int('OCI_NLS_ABMONTHNAME5') ->  31;          % Native abbreviated name for May
nls_info_to_int('OCI_NLS_ABMONTHNAME6') ->  32;         % Native abbreviated name for June
nls_info_to_int('OCI_NLS_ABMONTHNAME7') ->  33;         % Native abbreviated name for July
nls_info_to_int('OCI_NLS_ABMONTHNAME8') ->  34;       % Native abbreviated name for August
nls_info_to_int('OCI_NLS_ABMONTHNAME9') ->  35;    % Native abbreviated name for September
nls_info_to_int('OCI_NLS_ABMONTHNAME10') -> 36;      % Native abbreviated name for October
nls_info_to_int('OCI_NLS_ABMONTHNAME11') -> 37;     % Native abbreviated name for November
nls_info_to_int('OCI_NLS_ABMONTHNAME12') -> 38;     % Native abbreviated name for December
nls_info_to_int('OCI_NLS_YES') ->           39;   % Native string for affirmative response
nls_info_to_int('OCI_NLS_NO') ->            40;                 % Native negative response
nls_info_to_int('OCI_NLS_AM') ->            41;           % Native equivalent string of AM
nls_info_to_int('OCI_NLS_PM') ->            42;           % Native equivalent string of PM
nls_info_to_int('OCI_NLS_AD') ->            43;           % Native equivalent string of AD
nls_info_to_int('OCI_NLS_BC') ->            44;           % Native equivalent string of BC
nls_info_to_int('OCI_NLS_DECIMAL') ->       45;                        % decimal character
nls_info_to_int('OCI_NLS_GROUP') ->         46;                          % group separator
nls_info_to_int('OCI_NLS_DEBIT') ->         47;                   % Native symbol of debit
nls_info_to_int('OCI_NLS_CREDIT') ->        48;                  % Native sumbol of credit
nls_info_to_int('OCI_NLS_DATEFORMAT') ->    49;                       % Oracle date format
nls_info_to_int('OCI_NLS_INT_CURRENCY') ->  50;            % International currency symbol
nls_info_to_int('OCI_NLS_LOC_CURRENCY') ->  51;                   % Locale currency symbol
nls_info_to_int('OCI_NLS_LANGUAGE') ->      52;                            % Language name
nls_info_to_int('OCI_NLS_ABLANGUAGE') ->    53;           % Abbreviation for language name
nls_info_to_int('OCI_NLS_TERRITORY') ->     54;                           % Territory name
nls_info_to_int('OCI_NLS_CHARACTER_SET') -> 55;                       % Character set name
nls_info_to_int('OCI_NLS_LINGUISTIC_NAME') ->    56;                     % Linguistic name
nls_info_to_int('OCI_NLS_CALENDAR') ->      57;                            % Calendar name
nls_info_to_int('OCI_NLS_DUAL_CURRENCY') -> 78;                     % Dual currency symbol
nls_info_to_int('OCI_NLS_WRITINGDIR') ->    79;               % Language writing direction
nls_info_to_int('OCI_NLS_ABTERRITORY') ->   80;                   % Territory Abbreviation
nls_info_to_int('OCI_NLS_DDATEFORMAT') ->   81;               % Oracle default date format
nls_info_to_int('OCI_NLS_DTIMEFORMAT') ->   82;               % Oracle default time format
nls_info_to_int('OCI_NLS_SFDATEFORMAT') ->  83;       % Local string formatted date format
nls_info_to_int('OCI_NLS_SFTIMEFORMAT') ->  84;       % Local string formatted time format
nls_info_to_int('OCI_NLS_NUMGROUPING') ->   85;                   % Number grouping fields
nls_info_to_int('OCI_NLS_LISTSEP') ->       86;                           % List separator
nls_info_to_int('OCI_NLS_MONDECIMAL') ->    87;               % Monetary decimal character
nls_info_to_int('OCI_NLS_MONGROUP') ->      88;                 % Monetary group separator
nls_info_to_int('OCI_NLS_MONGROUPING') ->   89;                 % Monetary grouping fields
nls_info_to_int('OCI_NLS_INT_CURRENCYSEP') -> 90;       % International currency separator
nls_info_to_int('OCI_NLS_CHARSET_MAXBYTESZ') -> 91;     % Maximum character byte size     
nls_info_to_int('OCI_NLS_CHARSET_FIXEDWIDTH') -> 92;    % Fixed-width charset byte size   
nls_info_to_int('OCI_NLS_CHARSET_ID') ->    93;                         % Character set id
nls_info_to_int('OCI_NLS_NCHARSET_ID') ->   94.                        % NCharacter set id


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

%% Convert internal SQL type code to atom   Implemented?
int_to_sql_type(1) ->   'SQLT_CHR';         % y
int_to_sql_type(2) ->   'SQLT_NUM';         % y
int_to_sql_type(3) ->   'SQLT_INT';         % y
int_to_sql_type(4) ->   'SQLT_FLT';         % y
int_to_sql_type(5) ->   'SQLT_STR';
int_to_sql_type(6) ->   'SQLT_VNU';
int_to_sql_type(7) ->   'SQLT_PDN';
int_to_sql_type(8) ->   'SQLT_LNG';         % y
int_to_sql_type(9) ->   'SQLT_VCS';
int_to_sql_type(10) ->   'SQLT_NON';
int_to_sql_type(11) ->   'SQLT_RID';
int_to_sql_type(12) ->   'SQLT_DAT';        % y
int_to_sql_type(15) ->   'SQLT_VBI';
int_to_sql_type(21) -> 'SQLT_BFLOAT';       % y
int_to_sql_type(22) -> 'SQLT_BDOUBLE';      % y
int_to_sql_type(23) ->  'SQLT_BIN';         % y
int_to_sql_type(24) ->  'SQLT_LBI';
int_to_sql_type(68) ->  'SQLT_UIN';
int_to_sql_type(91) ->  'SQLT_SLS';
int_to_sql_type(94) ->  'SQLT_LVC';
int_to_sql_type(95) ->  'SQLT_LVB';
int_to_sql_type(96) ->  'SQLT_AFC';         % y
int_to_sql_type(97) ->  'SQLT_AVC'; 
int_to_sql_type(100) -> 'SQLT_IBFLOAT';     % y
int_to_sql_type(101) -> 'SQLT_IBDOUBLE';    % y
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
int_to_sql_type(116) -> 'SQLT_RSET';        % y
int_to_sql_type(122) -> 'SQLT_NCO';
int_to_sql_type(155) -> 'SQLT_VST';
int_to_sql_type(156) -> 'SQLT_ODT';

int_to_sql_type(184) -> 'SQLT_DATE';        % y
int_to_sql_type(185) -> 'SQLT_TIME';
int_to_sql_type(186) -> 'SQLT_TIME_TZ';
int_to_sql_type(187) -> 'SQLT_TIMESTAMP';   % y
int_to_sql_type(188) -> 'SQLT_TIMESTAMP_TZ'; % y
int_to_sql_type(189) -> 'SQLT_INTERVAL_YM';  % y
int_to_sql_type(190) -> 'SQLT_INTERVAL_DS';  % y
int_to_sql_type(232) -> 'SQLT_TIMESTAMP_LTZ'; % y

int_to_sql_type(241) -> 'SQLT_PNTY';

%% some pl/sql specific types
int_to_sql_type(250) -> 'SQLT_REC';             % pl/sql 'record' (or %rowtype)
int_to_sql_type(251) -> 'SQLT_TAB';             % pl/sql 'indexed table'
int_to_sql_type(252) -> 'SQLT_BOL'.             % pl/sql 'boolean'


%%============================Attribute Types===============================
%%   Note: All attributes are global.  New attibutes should be added to the end
%%   of the list. Before you add an attribute see if an existing one can be 
%%   used for your handle. Note: this does not mean reuse an existing number;
%%   it means use an attribute name from another handle for your handle.%%
%%   If you see any holes please use the holes first. However, be very
%%   careful, as this file is not fully ordered.
%%==========================================================================


attr_name_to_int('OCI_ATTR_FNCODE') ->  1;                          % the OCI function code
attr_name_to_int('OCI_ATTR_OBJECT') ->   2; % is the environment initialized in object mode
attr_name_to_int('OCI_ATTR_NONBLOCKING_MODE') ->  3;                    % non blocking mode
attr_name_to_int('OCI_ATTR_SQLCODE') ->  4;                                  % the SQL verb
attr_name_to_int('OCI_ATTR_ENV') ->  5;                            % the environment handle
attr_name_to_int('OCI_ATTR_SERVER') -> 6;                               % the server handle
attr_name_to_int('OCI_ATTR_SESSION') -> 7;                        % the user session handle
attr_name_to_int('OCI_ATTR_TRANS') ->   8;                         % the transaction handle
attr_name_to_int('OCI_ATTR_ROW_COUNT') ->   9;                  % the rows processed so far
attr_name_to_int('OCI_ATTR_SQLFNCODE') -> 10;               % the SQL verb of the statement
attr_name_to_int('OCI_ATTR_PREFETCH_ROWS') ->  11;    % sets the number of rows to prefetch
attr_name_to_int('OCI_ATTR_NESTED_PREFETCH_ROWS') -> 12; % the prefetch rows of nested table
attr_name_to_int('OCI_ATTR_PREFETCH_MEMORY') -> 13;         % memory limit for rows fetched
attr_name_to_int('OCI_ATTR_NESTED_PREFETCH_MEMORY') -> 14;   % memory limit for nested rows
attr_name_to_int('OCI_ATTR_CHAR_COUNT') ->  15;
                                                % this specifies the bind and define size in characters
attr_name_to_int('OCI_ATTR_PDSCL') ->   16;                          % packed decimal scale
attr_name_to_int('OCI_ATTR_FSPRECISION') -> attr_name_to_int('OCI_ATTR_PDSCL');
                                                % fs prec for datetime data types
attr_name_to_int('OCI_ATTR_PDPRC') ->   17;                         % packed decimal format
attr_name_to_int('OCI_ATTR_LFPRECISION') -> attr_name_to_int('OCI_ATTR_PDPRC');
                                                % fs prec for datetime data types
attr_name_to_int('OCI_ATTR_PARAM_COUNT') -> 18;       % number of column in the select list
attr_name_to_int('OCI_ATTR_ROWID') ->   19;                                     % the rowid
attr_name_to_int('OCI_ATTR_CHARSET') ->  20;                      % the character set value
attr_name_to_int('OCI_ATTR_NCHAR') ->   21;                                    % NCHAR type
attr_name_to_int('OCI_ATTR_USERNAME') -> 22;                           % username attribute
attr_name_to_int('OCI_ATTR_PASSWORD') -> 23;                           % password attribute
attr_name_to_int('OCI_ATTR_STMT_TYPE') ->   24;                            % statement type
attr_name_to_int('OCI_ATTR_INTERNAL_NAME') ->   25;             % user friendly global name
attr_name_to_int('OCI_ATTR_EXTERNAL_NAME') ->   26;      % the internal name for global txn
attr_name_to_int('OCI_ATTR_XID') ->     27;           % XOPEN defined global transaction id
attr_name_to_int('OCI_ATTR_TRANS_LOCK') -> 28;                                            %
attr_name_to_int('OCI_ATTR_TRANS_NAME') -> 29;    % string to identify a global transaction
attr_name_to_int('OCI_ATTR_HEAPALLOC') -> 30;                % memory allocated on the heap
attr_name_to_int('OCI_ATTR_CHARSET_ID') -> 31;                           % Character Set ID
attr_name_to_int('OCI_ATTR_ENV_CHARSET_ID') ->   attr_name_to_int('OCI_ATTR_CHARSET_ID');   % charset id in env
attr_name_to_int('OCI_ATTR_CHARSET_FORM') -> 32;                       % Character Set Form
attr_name_to_int('OCI_ATTR_MAXDATA_SIZE') -> 33;       % Maximumsize of data on the server 
attr_name_to_int('OCI_ATTR_CACHE_OPT_SIZE') -> 34;              % object cache optimal size
attr_name_to_int('OCI_ATTR_CACHE_MAX_SIZE') -> 35;   % object cache maximum size percentage
attr_name_to_int('OCI_ATTR_PINOPTION') -> 36;             % object cache default pin option
attr_name_to_int('OCI_ATTR_ALLOC_DURATION') -> 37;
                                                % object cache default allocation duration
attr_name_to_int('OCI_ATTR_PIN_DURATION') -> 38;        % object cache default pin duration
attr_name_to_int('OCI_ATTR_FDO') ->          39;       % Format Descriptor object attribute
attr_name_to_int('OCI_ATTR_POSTPROCESSING_CALLBACK') -> 40;
                                                % Callback to process outbind data
attr_name_to_int('OCI_ATTR_POSTPROCESSING_CONTEXT') -> 41;
                                                % Callback context to process outbind data
attr_name_to_int('OCI_ATTR_ROWS_RETURNED') -> 42;
                                                % Number of rows returned in current iter - for Bind handles
attr_name_to_int('OCI_ATTR_FOCBK') ->        43;              % Failover Callback attribute
attr_name_to_int('OCI_ATTR_IN_V8_MODE') ->   44; % is the server/service context in V8 mode
attr_name_to_int('OCI_ATTR_LOBEMPTY') ->     45;                              % empty lob ?
attr_name_to_int('OCI_ATTR_SESSLANG') ->     46;                  % session language handle

attr_name_to_int('OCI_ATTR_VISIBILITY') ->             47;                     % visibility
attr_name_to_int('OCI_ATTR_RELATIVE_MSGID') ->         48;            % relative message id
attr_name_to_int('OCI_ATTR_SEQUENCE_DEVIATION') ->     49;             % sequence deviation

attr_name_to_int('OCI_ATTR_CONSUMER_NAME') ->          50;                  % consumer name
%% complex object retrieval parameter attributes
attr_name_to_int('OCI_ATTR_COMPLEXOBJECTCOMP_TYPE') ->         50;

attr_name_to_int('OCI_ATTR_DEQ_MODE') ->               51;                   % dequeue mode
attr_name_to_int('OCI_ATTR_COMPLEXOBJECTCOMP_TYPE_LEVEL') ->   51;

attr_name_to_int('OCI_ATTR_NAVIGATION') ->             52;                     % navigation
attr_name_to_int('OCI_ATTR_COMPLEXOBJECT_LEVEL') ->            52;

attr_name_to_int('OCI_ATTR_WAIT') ->                   53;                           % wait
attr_name_to_int('OCI_ATTR_COMPLEXOBJECT_COLL_OUTOFLINE') ->   53;

attr_name_to_int('OCI_ATTR_DEQ_MSGID') ->              54;             % dequeue message id

attr_name_to_int('OCI_ATTR_PRIORITY') ->               55;                       % priority
attr_name_to_int('OCI_ATTR_DELAY') ->                  56;                          % delay
attr_name_to_int('OCI_ATTR_EXPIRATION') ->             57;                     % expiration
attr_name_to_int('OCI_ATTR_CORRELATION') ->            58;                 % correlation id
attr_name_to_int('OCI_ATTR_ATTEMPTS') ->               59;                  % # of attempts
attr_name_to_int('OCI_ATTR_RECIPIENT_LIST') ->         60;                 % recipient list
attr_name_to_int('OCI_ATTR_EXCEPTION_QUEUE') ->        61;           % exception queue name
attr_name_to_int('OCI_ATTR_ENQ_TIME') ->               62; % enqueue time (only OCIAttrGet)
attr_name_to_int('OCI_ATTR_MSG_STATE') ->              63;% message state (only OCIAttrGet') ->)
                                                   % NOTE: 64-66 used below
attr_name_to_int('OCI_ATTR_AGENT_NAME') ->             64;                     % agent name
attr_name_to_int('OCI_ATTR_AGENT_ADDRESS') ->          65;                  % agent address
attr_name_to_int('OCI_ATTR_AGENT_PROTOCOL') ->         66;                 % agent protocol
attr_name_to_int('OCI_ATTR_USER_PROPERTY') ->          67;                  % user property
attr_name_to_int('OCI_ATTR_SENDER_ID') ->              68;                      % sender id
attr_name_to_int('OCI_ATTR_ORIGINAL_MSGID') ->         69;            % original message id

attr_name_to_int('OCI_ATTR_QUEUE_NAME') ->             70;                     % queue name
attr_name_to_int('OCI_ATTR_NFY_MSGID') ->              71;                     % message id
attr_name_to_int('OCI_ATTR_MSG_PROP') ->               72;             % message properties

attr_name_to_int('OCI_ATTR_NUM_DML_ERRORS') ->         73;       % num of errs in array DML
attr_name_to_int('OCI_ATTR_DML_ROW_OFFSET') ->         74;        % row offset in the array

attr_name_to_int('OCI_ATTR_DATEFORMAT') ->             75;     % default date format string
attr_name_to_int('OCI_ATTR_BUF_ADDR') ->               76;                 % buffer address
attr_name_to_int('OCI_ATTR_BUF_SIZE') ->               77;                    % buffer size

%% For values 78 - 80, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_NUM_ROWS') ->               81; % number of rows in column array
                                  % NOTE that OCI_ATTR_NUM_COLS is a column
                                  % array attribute too.

attr_name_to_int('OCI_ATTR_COL_COUNT') ->              82;        % columns of column array
                                                     % processed so far.
attr_name_to_int('OCI_ATTR_STREAM_OFFSET') ->          83;  % str off of last row processed
attr_name_to_int('OCI_ATTR_SHARED_HEAPALLOC') ->       84;    % Shared Heap Allocation Size

attr_name_to_int('OCI_ATTR_SERVER_GROUP') ->           85;              % server group name

attr_name_to_int('OCI_ATTR_MIGSESSION') ->             86;   % migratable session attribute

attr_name_to_int('OCI_ATTR_NOCACHE') ->                87;                 % Temporary LOBs

attr_name_to_int('OCI_ATTR_MEMPOOL_SIZE') ->           88;                      % Pool Size
attr_name_to_int('OCI_ATTR_MEMPOOL_INSTNAME') ->       89;                  % Instance name
attr_name_to_int('OCI_ATTR_MEMPOOL_APPNAME') ->        90;               % Application name
attr_name_to_int('OCI_ATTR_MEMPOOL_HOMENAME') ->       91;            % Home Directory name
attr_name_to_int('OCI_ATTR_MEMPOOL_MODEL') ->          92;     % Pool Model (proc,thrd,both)
attr_name_to_int('OCI_ATTR_MODES') ->                  93;                          % Modes

attr_name_to_int('OCI_ATTR_SUBSCR_NAME') ->            94;           % name of subscription
attr_name_to_int('OCI_ATTR_SUBSCR_CALLBACK') ->        95;            % associated callback
attr_name_to_int('OCI_ATTR_SUBSCR_CTX') ->             96;    % associated callback context
attr_name_to_int('OCI_ATTR_SUBSCR_PAYLOAD') ->         97;             % associated payload
attr_name_to_int('OCI_ATTR_SUBSCR_NAMESPACE') ->       98;           % associated namespace

attr_name_to_int('OCI_ATTR_PROXY_CREDENTIALS') ->      99;         % Proxy user credentials
attr_name_to_int('OCI_ATTR_INITIAL_CLIENT_ROLES') ->  100;       % Initial client role list

%% Only Columns
attr_name_to_int('OCI_ATTR_DISP_NAME') ->      100;                      % the display name

attr_name_to_int('OCI_ATTR_UNK') ->              101;                   % unknown attribute
attr_name_to_int('OCI_ATTR_ENCC_SIZE') ->      101;                   % encrypted data size
attr_name_to_int('OCI_ATTR_NUM_COLS') ->         102;                   % number of columns
attr_name_to_int('OCI_ATTR_COL_ENC') ->        102;                 % column is encrypted ?
attr_name_to_int('OCI_ATTR_LIST_COLUMNS') ->     103;        % parameter of the column list
attr_name_to_int('OCI_ATTR_COL_ENC_SALT') ->   103;          % is encrypted column salted ?
attr_name_to_int('OCI_ATTR_RDBA') ->             104;           % DBA of the segment header
attr_name_to_int('OCI_ATTR_COL_PROPERTIES') -> 104;          % column properties
attr_name_to_int('OCI_ATTR_CLUSTERED') ->        105;      % whether the table is clustered
attr_name_to_int('OCI_ATTR_PARTITIONED') ->      106;    % whether the table is partitioned
attr_name_to_int('OCI_ATTR_INDEX_ONLY') ->       107;     % whether the table is index only
attr_name_to_int('OCI_ATTR_LIST_ARGUMENTS') ->   108;      % parameter of the argument list
attr_name_to_int('OCI_ATTR_LIST_SUBPROGRAMS') -> 109;    % parameter of the subprogram list
attr_name_to_int('OCI_ATTR_REF_TDO') ->          110;          % REF to the type descriptor
attr_name_to_int('OCI_ATTR_LINK') ->             111;              % the database link name
attr_name_to_int('OCI_ATTR_MIN') ->              112;                       % minimum value
attr_name_to_int('OCI_ATTR_MAX') ->              113;                       % maximum value
attr_name_to_int('OCI_ATTR_INCR') ->             114;                     % increment value
attr_name_to_int('OCI_ATTR_CACHE') ->            115;   % number of sequence numbers cached
attr_name_to_int('OCI_ATTR_ORDER') ->            116;     % whether the sequence is ordered
attr_name_to_int('OCI_ATTR_HW_MARK') ->          117;                     % high-water mark
attr_name_to_int('OCI_ATTR_TYPE_SCHEMA') ->      118;                  % type's schema name
attr_name_to_int('OCI_ATTR_TIMESTAMP') ->        119;             % timestamp of the object
attr_name_to_int('OCI_ATTR_NUM_ATTRS') ->        120;                % number of sttributes
attr_name_to_int('OCI_ATTR_NUM_PARAMS') ->       121;                % number of parameters
attr_name_to_int('OCI_ATTR_OBJID') ->            122;       % object id for a table or view
attr_name_to_int('OCI_ATTR_PTYPE') ->            123;           % type of info described by
attr_name_to_int('OCI_ATTR_PARAM') ->            124;                % parameter descriptor
attr_name_to_int('OCI_ATTR_OVERLOAD_ID') ->      125;     % overload ID for funcs and procs
attr_name_to_int('OCI_ATTR_TABLESPACE') ->       126;                    % table name space
attr_name_to_int('OCI_ATTR_TDO') ->              127;                       % TDO of a type
attr_name_to_int('OCI_ATTR_LTYPE') ->            128;                           % list type
attr_name_to_int('OCI_ATTR_PARSE_ERROR_OFFSET') -> 129;                % Parse Error offset
attr_name_to_int('OCI_ATTR_IS_TEMPORARY') ->     130;          % whether table is temporary
attr_name_to_int('OCI_ATTR_IS_TYPED') ->         131;              % whether table is typed
attr_name_to_int('OCI_ATTR_DURATION') ->         132;         % duration of temporary table
attr_name_to_int('OCI_ATTR_IS_INVOKER_RIGHTS') -> 133;                  % is invoker rights
attr_name_to_int('OCI_ATTR_OBJ_NAME') ->         134;           % top level schema obj name
attr_name_to_int('OCI_ATTR_OBJ_SCHEMA') ->       135;                         % schema name
attr_name_to_int('OCI_ATTR_OBJ_ID') ->           136;          % top level schema object id
attr_name_to_int('OCI_ATTR_LIST_PKG_TYPES') ->   137;  % parameter of the package type list

%% For values 137 - 141, see DirPathAPI attribute section in this file


attr_name_to_int('OCI_ATTR_TRANS_TIMEOUT') ->              142;       % transaction timeout
attr_name_to_int('OCI_ATTR_SERVER_STATUS') ->              143;% state of the server handle
attr_name_to_int('OCI_ATTR_STATEMENT') ->                  144; % statement txt in stmt hdl

%% For value 145, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_DEQCOND') ->                    146;         % dequeue condition
attr_name_to_int('OCI_ATTR_RESERVED_2') ->                 147;                  % reserved

  
attr_name_to_int('OCI_ATTR_SUBSCR_RECPT') ->               148; % recepient of subscription
attr_name_to_int('OCI_ATTR_SUBSCR_RECPTPROTO') ->          149;    % protocol for recepient

%% For values 150 - 151, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_LDAP_HOST') ->       153;              % LDAP host to connect to
attr_name_to_int('OCI_ATTR_LDAP_PORT') ->       154;              % LDAP port to connect to
attr_name_to_int('OCI_ATTR_BIND_DN') ->         155;                              % bind DN
attr_name_to_int('OCI_ATTR_LDAP_CRED') ->       156;       % credentials to connect to LDAP
attr_name_to_int('OCI_ATTR_WALL_LOC') ->        157;               % client wallet location
attr_name_to_int('OCI_ATTR_LDAP_AUTH') ->       158;           % LDAP authentication method
attr_name_to_int('OCI_ATTR_LDAP_CTX') ->        159;        % LDAP adminstration context DN
attr_name_to_int('OCI_ATTR_SERVER_DNS') ->      160;      % list of registration server DNs

attr_name_to_int('OCI_ATTR_DN_COUNT') ->        161;             % the number of server DNs
attr_name_to_int('OCI_ATTR_SERVER_DN') ->       162;                  % server DN attribute

attr_name_to_int('OCI_ATTR_MAXCHAR_SIZE') ->               163;     % max char size of data

attr_name_to_int('OCI_ATTR_CURRENT_POSITION') ->           164; % for scrollable result sets

%% Added to get attributes for ref cursor to statement handle
attr_name_to_int('OCI_ATTR_RESERVED_3') ->                 165;                  % reserved
attr_name_to_int('OCI_ATTR_RESERVED_4') ->                 166;                  % reserved

%% For value 167, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_DIGEST_ALGO') ->                168;          % digest algorithm
attr_name_to_int('OCI_ATTR_CERTIFICATE') ->                169;               % certificate
attr_name_to_int('OCI_ATTR_SIGNATURE_ALGO') ->             170;       % signature algorithm
attr_name_to_int('OCI_ATTR_CANONICAL_ALGO') ->             171;    % canonicalization algo.
attr_name_to_int('OCI_ATTR_PRIVATE_KEY') ->                172;               % private key
attr_name_to_int('OCI_ATTR_DIGEST_VALUE') ->               173;              % digest value
attr_name_to_int('OCI_ATTR_SIGNATURE_VAL') ->              174;           % signature value
attr_name_to_int('OCI_ATTR_SIGNATURE') ->                  175;                 % signature

%% attributes for setting OCI stmt caching specifics in svchp
attr_name_to_int('OCI_ATTR_STMTCACHESIZE') ->              176;     % size of the stm cache

%% --------------------------- Connection Pool Attributes ------------------
attr_name_to_int('OCI_ATTR_CONN_NOWAIT') ->               178;

attr_name_to_int('OCI_ATTR_CONN_BUSY_COUNT') ->            179;
attr_name_to_int('OCI_ATTR_CONN_OPEN_COUNT') ->            180;
attr_name_to_int('OCI_ATTR_CONN_TIMEOUT') ->               181;
attr_name_to_int('OCI_ATTR_STMT_STATE') ->                 182;
attr_name_to_int('OCI_ATTR_CONN_MIN') ->                   183;
attr_name_to_int('OCI_ATTR_CONN_MAX') ->                   184;
attr_name_to_int('OCI_ATTR_CONN_INCR') ->                  185;

%% For value 187, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_NUM_OPEN_STMTS') ->             188;     % open stmts in session
attr_name_to_int('OCI_ATTR_RESERVED_189') ->               189;
attr_name_to_int('OCI_ATTR_DESCRIBE_NATIVE') -> attr_name_to_int('OCI_ATTR_RESERVED_189');

attr_name_to_int('OCI_ATTR_BIND_COUNT') ->                 190;   % number of bind postions
attr_name_to_int('OCI_ATTR_HANDLE_POSITION') ->            191; % pos of bind/define handle
attr_name_to_int('OCI_ATTR_RESERVED_5') ->                 192;                 % reserverd
attr_name_to_int('OCI_ATTR_SERVER_BUSY') ->                193; % call in progress on server

%% For value 194, see DirPathAPI attribute section in this file

%% notification presentation for recipient
attr_name_to_int('OCI_ATTR_SUBSCR_RECPTPRES') ->           195;

attr_name_to_int('OCI_ATTR_TRANSFORMATION') ->             196; % AQ message transformation

attr_name_to_int('OCI_ATTR_ROWS_FETCHED') ->               197; % rows fetched in last call

%% --------------------------- Snapshot attributes -------------------------
attr_name_to_int('OCI_ATTR_SCN_BASE') ->                   198;             % snapshot base
attr_name_to_int('OCI_ATTR_SCN_WRAP') ->                   199;             % snapshot wrap

%% --------------------------- Miscellanous attributes ---------------------
attr_name_to_int('OCI_ATTR_RESERVED_6') ->                 200;                  % reserved
attr_name_to_int('OCI_ATTR_READONLY_TXN') ->               201;           % txn is readonly
attr_name_to_int('OCI_ATTR_RESERVED_7') ->                 202;                  % reserved
attr_name_to_int('OCI_ATTR_ERRONEOUS_COLUMN') ->           203; % position of erroneous col
attr_name_to_int('OCI_ATTR_RESERVED_8') ->                 204;                  % reserved
attr_name_to_int('OCI_ATTR_ASM_VOL_SPRT') ->               205;     % ASM volume supported?

%% For value 206, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_INST_TYPE') ->                  207;      % oracle instance type
attr_name_to_int('OCI_ATTR_SPOOL_STMTCACHESIZE') ->        208;   %Stmt cache size of pool 
attr_name_to_int('OCI_ATTR_ENV_UTF16') ->                  209;     % is env in utf16 mode?

%%Only Stored Procs
attr_name_to_int('OCI_ATTR_RESERVED_9') ->                 210;                  % reserved
attr_name_to_int('OCI_ATTR_OVERLOAD') ->                  210;% is this position overloaded
attr_name_to_int('OCI_ATTR_RESERVED_10') ->                211;                  % reserved
attr_name_to_int('OCI_ATTR_LEVEL') ->                     211; % level for structured types
attr_name_to_int('OCI_ATTR_HAS_DEFAULT') ->               212;        % has a default value
attr_name_to_int('OCI_ATTR_IOMODE') ->                    213;              % in, out inout

%% For values 212 and 213, see ALSO DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_RESERVED_12') ->                214;                  % reserved
attr_name_to_int('OCI_ATTR_RADIX') ->                     214;            % returns a radix
attr_name_to_int('OCI_ATTR_RESERVED_13') ->                215;                  % reserved
attr_name_to_int('OCI_ATTR_NUM_ARGS') ->                  215;  % total number of arguments
attr_name_to_int('OCI_ATTR_IS_EXTERNAL') ->                216; % whether table is external
attr_name_to_int('OCI_ATTR_TYPECODE') ->                  216;       % object or collection


%% -------------------------- Statement Handle Attributes ------------------

attr_name_to_int('OCI_ATTR_RESERVED_15') ->                217;                  % reserved
attr_name_to_int('OCI_ATTR_COLLECTION_TYPECODE') ->       217;     % varray or nested table
attr_name_to_int('OCI_ATTR_STMT_IS_RETURNING') ->          218; % stmt has returning clause
attr_name_to_int('OCI_ATTR_VERSION') ->                   218;      % user assigned version
attr_name_to_int('OCI_ATTR_RESERVED_16') ->                219;                  % reserved
attr_name_to_int('OCI_ATTR_IS_INCOMPLETE_TYPE') ->        219; % is this an incomplete type
attr_name_to_int('OCI_ATTR_RESERVED_17') ->                220;                  % reserved
attr_name_to_int('OCI_ATTR_IS_SYSTEM_TYPE') ->            220;              % a system type
attr_name_to_int('OCI_ATTR_RESERVED_18') ->                221;                  % reserved
attr_name_to_int('OCI_ATTR_IS_PREDEFINED_TYPE') ->        221;          % a predefined type

%% --------------------------- session attributes ---------------------------
attr_name_to_int('OCI_ATTR_RESERVED_19') ->                222;                  % reserved
attr_name_to_int('OCI_ATTR_IS_TRANSIENT_TYPE') ->         222;           % a transient type
attr_name_to_int('OCI_ATTR_RESERVED_20') ->                223;                  % reserved
attr_name_to_int('OCI_ATTR_IS_SYSTEM_GENERATED_TYPE') ->  223;      % system generated type
attr_name_to_int('OCI_ATTR_CURRENT_SCHEMA') ->             224;            % Current Schema
attr_name_to_int('OCI_ATTR_HAS_NESTED_TABLE') ->          224; % contains nested table attr

%% ------------------------- notification subscription ----------------------
attr_name_to_int('OCI_ATTR_SUBSCR_QOSFLAGS') ->            225;                 % QOS flags
attr_name_to_int('OCI_ATTR_HAS_LOB') ->                   225;        % has a lob attribute
attr_name_to_int('OCI_ATTR_SUBSCR_PAYLOADCBK') ->          226;          % Payload callback
attr_name_to_int('OCI_ATTR_HAS_FILE') ->                  226;       % has a file attribute
attr_name_to_int('OCI_ATTR_SUBSCR_TIMEOUT') ->             227;                   % Timeout
attr_name_to_int('OCI_ATTR_COLLECTION_ELEMENT') ->        227; % has a collection attribute
attr_name_to_int('OCI_ATTR_SUBSCR_NAMESPACE_CTX') ->       228;         % Namespace context
attr_name_to_int('OCI_ATTR_NUM_TYPE_ATTRS') ->            228;  % number of attribute types
attr_name_to_int('OCI_ATTR_SUBSCR_CQ_QOSFLAGS') ->         229;
                              % change notification (CQ) specific QOS flags
attr_name_to_int('OCI_ATTR_LIST_TYPE_ATTRS') ->           229;    % list of type attributes
attr_name_to_int('OCI_ATTR_SUBSCR_CQ_REGID') ->            230;
                                      % change notification registration id
attr_name_to_int('OCI_ATTR_NUM_TYPE_METHODS') ->          230;     % number of type methods
attr_name_to_int('OCI_ATTR_SUBSCR_NTFN_GROUPING_CLASS') -> 231;       % ntfn grouping class
attr_name_to_int('OCI_ATTR_LIST_TYPE_METHODS') ->         231;       % list of type methods
attr_name_to_int('OCI_ATTR_SUBSCR_NTFN_GROUPING_VALUE') -> 232;       % ntfn grouping value
attr_name_to_int('OCI_ATTR_MAP_METHOD') ->                232;         % map method of type
attr_name_to_int('OCI_ATTR_SUBSCR_NTFN_GROUPING_TYPE') ->  233;        % ntfn grouping type
attr_name_to_int('OCI_ATTR_ORDER_METHOD') ->              233;       % order method of type
attr_name_to_int('OCI_ATTR_SUBSCR_NTFN_GROUPING_START_TIME') ->   234;% ntfn grp start time
attr_name_to_int('OCI_ATTR_NUM_ELEMS') ->                        234;  % number of elements
attr_name_to_int('OCI_ATTR_SUBSCR_NTFN_GROUPING_REPEAT_COUNT') -> 235; % ntfn grp rep count
attr_name_to_int('OCI_ATTR_ENCAPSULATION') ->                    235; % encapsulation level
attr_name_to_int('OCI_ATTR_AQ_NTFN_GROUPING_MSGID_ARRAY') ->      236; % aq grp msgid array
attr_name_to_int('OCI_ATTR_IS_SELFISH') ->                       236;      % method selfish
attr_name_to_int('OCI_ATTR_AQ_NTFN_GROUPING_COUNT') ->            237;  % ntfns recd in grp
attr_name_to_int('OCI_ATTR_IS_VIRTUAL') ->                       237;             % virtual
attr_name_to_int('OCI_ATTR_IS_INLINE') ->                 238;                     % inline
attr_name_to_int('OCI_ATTR_IS_CONSTANT') ->               239;                   % constant
attr_name_to_int('OCI_ATTR_HAS_RESULT') ->                240;                 % has result
attr_name_to_int('OCI_ATTR_IS_CONSTRUCTOR') ->            241;                % constructor
attr_name_to_int('OCI_ATTR_IS_DESTRUCTOR') ->             242;                 % destructor
attr_name_to_int('OCI_ATTR_IS_OPERATOR') ->               243;                   % operator
attr_name_to_int('OCI_ATTR_IS_MAP') ->                    244;               % a map method
attr_name_to_int('OCI_ATTR_IS_ORDER') ->                  245;               % order method
attr_name_to_int('OCI_ATTR_IS_RNDS') ->                   246;  % read no data state method
attr_name_to_int('OCI_ATTR_IS_RNPS') ->                   247;      % read no process state
attr_name_to_int('OCI_ATTR_IS_WNDS') ->                   248; % write no data state method
attr_name_to_int('OCI_ATTR_IS_WNPS') ->                   249;     % write no process state

attr_name_to_int('OCI_ATTR_DESC_PUBLIC') ->               250;              % public object

 %% Object Cache Enhancements : attributes for User Constructed Instances    
attr_name_to_int('OCI_ATTR_CACHE_CLIENT_CONTEXT') ->      251;
attr_name_to_int('OCI_ATTR_UCI_CONSTRUCT') ->             252;
attr_name_to_int('OCI_ATTR_UCI_DESTRUCT') ->              253;
attr_name_to_int('OCI_ATTR_UCI_COPY') ->                  254;
attr_name_to_int('OCI_ATTR_UCI_PICKLE') ->                255;
attr_name_to_int('OCI_ATTR_UCI_UNPICKLE') ->              256;
attr_name_to_int('OCI_ATTR_UCI_REFRESH') ->               257;

%% for type inheritance
attr_name_to_int('OCI_ATTR_IS_SUBTYPE') ->                258;

attr_name_to_int('OCI_ATTR_SUPERTYPE_SCHEMA_NAME') ->     259;
attr_name_to_int('OCI_ATTR_SUPERTYPE_NAME') ->            260;

%% for schemas
attr_name_to_int('OCI_ATTR_LIST_OBJECTS') ->              261;  % list of objects in schema

%% for database
attr_name_to_int('OCI_ATTR_NCHARSET_ID') ->               262;                % char set id
attr_name_to_int('OCI_ATTR_ENV_NCHARSET_ID') ->  attr_name_to_int('OCI_ATTR_NCHARSET_ID'); % ncharset id in env
attr_name_to_int('OCI_ATTR_LIST_SCHEMAS') ->              263;            % list of schemas
attr_name_to_int('OCI_ATTR_MAX_PROC_LEN') ->              264;       % max procedure length
attr_name_to_int('OCI_ATTR_MAX_COLUMN_LEN') ->            265;     % max column name length
attr_name_to_int('OCI_ATTR_CURSOR_COMMIT_BEHAVIOR') ->    266;     % cursor commit behavior
attr_name_to_int('OCI_ATTR_MAX_CATALOG_NAMELEN') ->       267;         % catalog namelength
attr_name_to_int('OCI_ATTR_CATALOG_LOCATION') ->          268;           % catalog location
attr_name_to_int('OCI_ATTR_SAVEPOINT_SUPPORT') ->         269;          % savepoint support
attr_name_to_int('OCI_ATTR_NOWAIT_SUPPORT') ->            270;             % nowait support
attr_name_to_int('OCI_ATTR_AUTOCOMMIT_DDL') ->            271;             % autocommit DDL
attr_name_to_int('OCI_ATTR_LOCKING_MODE') ->              272;               % locking mode

%% for externally initialized context
attr_name_to_int('OCI_ATTR_APPCTX_SIZE') ->               273; % count of context to be init
attr_name_to_int('OCI_ATTR_APPCTX_LIST') ->               274; % count of context to be init
attr_name_to_int('OCI_ATTR_APPCTX_NAME') ->               275; % name  of context to be init
attr_name_to_int('OCI_ATTR_APPCTX_ATTR') ->               276; % attr  of context to be init
attr_name_to_int('OCI_ATTR_APPCTX_VALUE') ->              277; % value of context to be init

%% for client id propagation
attr_name_to_int('OCI_ATTR_CLIENT_IDENTIFIER') ->         278;   % value of client id to set

%% for inheritance - part 2
attr_name_to_int('OCI_ATTR_IS_FINAL_TYPE') ->             279;            % is final type ?
attr_name_to_int('OCI_ATTR_IS_INSTANTIABLE_TYPE') ->      280;     % is instantiable type ?
attr_name_to_int('OCI_ATTR_IS_FINAL_METHOD') ->           281;          % is final method ?
attr_name_to_int('OCI_ATTR_IS_INSTANTIABLE_METHOD') ->    282;   % is instantiable method ?
attr_name_to_int('OCI_ATTR_IS_OVERRIDING_METHOD') ->      283;     % is overriding method ?

attr_name_to_int('OCI_ATTR_DESC_SYNBASE') ->              284;   % Describe the base object


attr_name_to_int('OCI_ATTR_CHAR_USED') ->                 285;      % char length semantics
attr_name_to_int('OCI_ATTR_CHAR_SIZE') ->                 286;                % char length

%% SQLJ support
attr_name_to_int('OCI_ATTR_IS_JAVA_TYPE') ->              287; % is java implemented type ?

attr_name_to_int('OCI_ATTR_EDITION') ->                   288;                % ORA_EDITION

%% N-Tier support
attr_name_to_int('OCI_ATTR_DISTINGUISHED_NAME') ->        300;        % use DN as user name

attr_name_to_int('OCI_ATTR_BIND_ROWCBK') ->               301;          % bind row callback
attr_name_to_int('OCI_ATTR_KERBEROS_TICKET') ->          301;    % Kerberos ticket as cred.
attr_name_to_int('OCI_ATTR_BIND_ROWCTX') ->               302;  % ctx for bind row callback
attr_name_to_int('OCI_ATTR_ORA_DEBUG_JDWP') ->           302;    % ORA_DEBUG_JDWP attribute
attr_name_to_int('OCI_ATTR_SKIP_BUFFER') ->               303;   % skip buffer in array ops
attr_name_to_int('OCI_ATTR_RESERVED_14') ->              303;                    % reserved

attr_name_to_int('OCI_ATTR_CQ_QUERYID') ->               304;

attr_name_to_int('OCI_ATTR_EVTCBK') ->                    304;                % ha callback
attr_name_to_int('OCI_ATTR_EVTCTX') ->                    305;        % ctx for ha callback

%% ------------------ User memory attributes (all handles) -----------------
attr_name_to_int('OCI_ATTR_USER_MEMORY') ->               306;     % pointer to user memory

%% For values 303 - 307, see ALSO DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_ACCESS_BANNER') ->             307;              % access banner
attr_name_to_int('OCI_ATTR_AUDIT_BANNER') ->              308;               % audit banner
attr_name_to_int('OCI_ATTR_SPOOL_TIMEOUT') ->              308;           % session timeout
attr_name_to_int('OCI_ATTR_SPOOL_GETMODE') ->              309;          % session get mode
attr_name_to_int('OCI_ATTR_SPOOL_BUSY_COUNT') ->           310;        % busy session count
attr_name_to_int('OCI_ATTR_SPOOL_OPEN_COUNT') ->           311;        % open session count
attr_name_to_int('OCI_ATTR_SPOOL_MIN') ->                  312;         % min session count
attr_name_to_int('OCI_ATTR_SPOOL_MAX') ->                  313;         % max session count
attr_name_to_int('OCI_ATTR_SPOOL_INCR') ->                 314;   % session increment count

%%---------------------------- For XML Types -------------------------------
%% For table, view and column
attr_name_to_int('OCI_ATTR_IS_XMLTYPE') ->          315;         % Is the type an XML type?
attr_name_to_int('OCI_ATTR_XMLSCHEMA_NAME') ->      316;               % Name of XML Schema
attr_name_to_int('OCI_ATTR_XMLELEMENT_NAME') ->     317;              % Name of XML Element
attr_name_to_int('OCI_ATTR_XMLSQLTYPSCH_NAME') ->   318;    % SQL type's schema for XML Ele
attr_name_to_int('OCI_ATTR_XMLSQLTYPE_NAME') ->     319;     % Name of SQL type for XML Ele
attr_name_to_int('OCI_ATTR_XMLTYPE_STORED_OBJ') ->  320;       % XML type stored as object?


%%---------------------------- For Subtypes -------------------------------
%% For type
attr_name_to_int('OCI_ATTR_HAS_SUBTYPES') ->        321;                    % Has subtypes?
attr_name_to_int('OCI_ATTR_NUM_SUBTYPES') ->        322;               % Number of subtypes
attr_name_to_int('OCI_ATTR_LIST_SUBTYPES') ->       323;                 % List of subtypes


%% XML flag
attr_name_to_int('OCI_ATTR_XML_HRCHY_ENABLED') ->   324;               % hierarchy enabled?

%% Method flag
attr_name_to_int('OCI_ATTR_IS_OVERRIDDEN_METHOD') -> 325;           % Method is overridden?


%% For values 326 - 335, see DirPathAPI attribute section in this file

%%------------- Attributes for 10i Distributed Objects ----------------------
attr_name_to_int('OCI_ATTR_OBJ_SUBS') ->                   336; % obj col/tab substitutable

%% For values 337 - 338, see DirPathAPI attribute section in this file

%%---------- Attributes for 10i XADFIELD (NLS language, territory -----------
attr_name_to_int('OCI_ATTR_XADFIELD_RESERVED_1') ->        339;                  % reserved
attr_name_to_int('OCI_ATTR_XADFIELD_RESERVED_2') ->        340;                  % reserved

%%------------- Kerberos Secure Client Identifier ---------------------------
attr_name_to_int('OCI_ATTR_KERBEROS_CID') ->               341; % Kerberos db service ticket') ->

%%------------------------ Attributes; for Rules objects ---------------------
attr_name_to_int('OCI_ATTR_CONDITION') ->                  342;            % rule condition
attr_name_to_int('OCI_ATTR_COMMENT') ->                    343;                   % comment
attr_name_to_int('OCI_ATTR_VALUE') ->                      344;             % Anydata value
attr_name_to_int('OCI_ATTR_EVAL_CONTEXT_OWNER') ->         345;        % eval context owner
attr_name_to_int('OCI_ATTR_EVAL_CONTEXT_NAME') ->          346;         % eval context name
attr_name_to_int('OCI_ATTR_EVALUATION_FUNCTION') ->        347;        % eval function name
attr_name_to_int('OCI_ATTR_VAR_TYPE') ->                   348;             % variable type
attr_name_to_int('OCI_ATTR_VAR_VALUE_FUNCTION') ->         349;   % variable value function

attr_name_to_int('OCI_ATTR_XSTREAM_ACK_INTERVAL') ->       350;      % XStream ack interval
attr_name_to_int('OCI_ATTR_VAR_METHOD_FUNCTION') ->         350; % variable method function
attr_name_to_int('OCI_ATTR_XSTREAM_IDLE_TIMEOUT') ->       351;      % XStream idle timeout
attr_name_to_int('OCI_ATTR_ACTION_CONTEXT') ->              351;           % action context
attr_name_to_int('OCI_ATTR_LIST_TABLE_ALIASES') ->         352;     % list of table aliases
attr_name_to_int('OCI_ATTR_LIST_VARIABLE_TYPES') ->        353;    % list of variable types
attr_name_to_int('OCI_ATTR_TABLE_NAME') ->                 356;                % table name

%% For values 357 - 359, see DirPathAPI attribute section in this file

attr_name_to_int('OCI_ATTR_MESSAGE_CSCN') ->               360;              % message cscn
attr_name_to_int('OCI_ATTR_MESSAGE_DSCN') ->               361;              % message dscn

%%--------------------- Audit Session ID ------------------------------------
attr_name_to_int('OCI_ATTR_AUDIT_SESSION_ID') ->           362;          % Audit session ID') ->

%%--------------------- Kerberos; TGT Keys -----------------------------------
attr_name_to_int('OCI_ATTR_KERBEROS_KEY') ->               363;  % n-tier Kerberos cred key
attr_name_to_int('OCI_ATTR_KERBEROS_CID_KEY') ->           364;    % SCID Kerberos cred key


attr_name_to_int('OCI_ATTR_TRANSACTION_NO') ->             365;         % AQ enq txn number

%%----------------------- Attributes for End To End Tracing -----------------
attr_name_to_int('OCI_ATTR_MODULE') ->                     366;        % module for tracing
attr_name_to_int('OCI_ATTR_ACTION') ->                     367;        % action for tracing
attr_name_to_int('OCI_ATTR_CLIENT_INFO') ->                368;               % client info
attr_name_to_int('OCI_ATTR_COLLECT_CALL_TIME') ->          369;         % collect call time
attr_name_to_int('OCI_ATTR_CALL_TIME') ->                  370;         % extract call time
attr_name_to_int('OCI_ATTR_ECONTEXT_ID') ->                371;      % execution-id context
attr_name_to_int('OCI_ATTR_ECONTEXT_SEQ') ->               372;  %execution-id sequence num

%%------------------------------ Session attributes -------------------------
attr_name_to_int('OCI_ATTR_SESSION_STATE') ->              373;             % session state
attr_name_to_int('OCI_SESSION_STATELESS') ->  1;                             % valid states
attr_name_to_int('OCI_SESSION_STATEFUL') ->   2;

attr_name_to_int('OCI_ATTR_SESSION_STATETYPE') ->          374;        % session state type
attr_name_to_int('OCI_SESSION_STATELESS_DEF') ->  0;                    % valid state types
attr_name_to_int('OCI_SESSION_STATELESS_CAL') ->  1;
attr_name_to_int('OCI_SESSION_STATELESS_TXN') ->  2;
attr_name_to_int('OCI_SESSION_STATELESS_APP') ->  3;
attr_name_to_int('OCI_ATTR_SESSION_STATE_CLEARED') ->      376;     % session state cleared
attr_name_to_int('OCI_ATTR_SESSION_MIGRATED') ->           377;       % did session migrate
attr_name_to_int('OCI_ATTR_SESSION_PRESERVE_STATE') ->     388;    % preserve session state

%% -------------------------- Admin Handle Attributes ----------------------
attr_name_to_int('OCI_ATTR_ADMIN_PFILE') ->                389;    % client-side param file

attr_name_to_int('OCI_ATTR_SUBSCR_PORTNO') ->     390;                  % port no to listen
attr_name_to_int('OCI_ATTR_HOSTNAME') ->         390;                % SYS_CONTEXT hostname
attr_name_to_int('OCI_ATTR_DBNAME') ->           391;                  % SYS_CONTEXT dbname
attr_name_to_int('OCI_ATTR_INSTNAME') ->         392;           % SYS_CONTEXT instance name
attr_name_to_int('OCI_ATTR_SERVICENAME') ->      393;            % SYS_CONTEXT service name
attr_name_to_int('OCI_ATTR_INSTSTARTTIME') ->    394;      % v$instance instance start time
attr_name_to_int('OCI_ATTR_HA_TIMESTAMP') ->     395;                          % event time
attr_name_to_int('OCI_ATTR_RESERVED_22') ->      396;                            % reserved
attr_name_to_int('OCI_ATTR_RESERVED_23') ->      397;                            % reserved
attr_name_to_int('OCI_ATTR_RESERVED_24') ->      398;                            % reserved
attr_name_to_int('OCI_ATTR_DBDOMAIN') ->         399;                           % db domain
attr_name_to_int('OCI_ATTR_EVENTTYPE') ->        400;                          % event type

attr_name_to_int('OCI_ATTR_HA_SOURCE') ->        401;


attr_name_to_int('OCI_ATTR_CHNF_TABLENAMES') ->   401;          % out: array of table names
attr_name_to_int('OCI_ATTR_CHNF_ROWIDS') ->      402;     % in: rowids needed 
attr_name_to_int('OCI_ATTR_HA_STATUS') ->         402;
%% # define OCI_HA_STATUS_DOWN          0 valid values for OCI_ATTR_HA_STATUS
%% # define OCI_HA_STATUS_UP            1
attr_name_to_int('OCI_ATTR_CHNF_OPERATIONS') ->   403;  % in: notification operation filter
attr_name_to_int('OCI_ATTR_HA_SRVFIRST') ->      403;

attr_name_to_int('OCI_ATTR_CHNF_CHANGELAG') ->    404;      % txn lag between notifications
attr_name_to_int('OCI_ATTR_HA_SRVNEXT') ->       404;

attr_name_to_int('OCI_ATTR_CHDES_DBNAME') ->     405;                     % source database
attr_name_to_int('OCI_ATTR_TAF_ENABLED') ->      405;

attr_name_to_int('OCI_ATTR_CHDES_NFYTYPE') ->     406;            % notification type flags
attr_name_to_int('OCI_ATTR_NFY_FLAGS') ->        406; 

attr_name_to_int('OCI_ATTR_CHDES_XID') ->                407;     % XID  of the transaction
attr_name_to_int('OCI_ATTR_MSG_DELIVERY_MODE') ->       407;            % msg delivery mode
attr_name_to_int('OCI_ATTR_CHDES_TABLE_CHANGES') ->     408;% array of table chg descriptors

attr_name_to_int('OCI_ATTR_CHDES_TABLE_NAME') ->        409;    % table name
attr_name_to_int('OCI_ATTR_CHDES_TABLE_OPFLAGS') ->     410;    % table operation flags
attr_name_to_int('OCI_ATTR_CHDES_TABLE_ROW_CHANGES') -> 411;   % array of changed rows  
attr_name_to_int('OCI_ATTR_CHDES_ROW_ROWID') ->         412;   % rowid of changed row   
attr_name_to_int('OCI_ATTR_CHDES_ROW_OPFLAGS') ->       413;   % row operation flags    

attr_name_to_int('OCI_ATTR_CHNF_REGHANDLE') ->          414;   % IN: subscription handle 
attr_name_to_int('OCI_ATTR_RESERVED_21') ->              415;                    % reserved
attr_name_to_int('OCI_ATTR_NETWORK_FILE_DESC') ->       415;   % network file descriptor

attr_name_to_int('OCI_ATTR_PROXY_CLIENT') ->            416;% client nam 4 single sess proxy
attr_name_to_int('OCI_ATTR_DB_CHARSET_ID') ->            416;         % database charset ID

attr_name_to_int('OCI_ATTR_TABLE_ENC') ->         417;% does table have any encrypt columns
attr_name_to_int('OCI_ATTR_DB_NCHARSET_ID') ->     417;              % database ncharset ID
attr_name_to_int('OCI_ATTR_TABLE_ENC_ALG') ->     418;         % Table encryption Algorithm
attr_name_to_int('OCI_ATTR_RESERVED_25') ->        418;                          % reserved
attr_name_to_int('OCI_ATTR_TABLE_ENC_ALG_ID') ->  419; % Internal Id of encryption Algorithm
attr_name_to_int('OCI_ATTR_STMTCACHE_CBKCTX') ->  420;             % opaque context on stmt
attr_name_to_int('OCI_ATTR_STMTCACHE_CBK') ->     421;          % callback fn for stmtcache
attr_name_to_int('OCI_ATTR_CQDES_OPERATION') ->     422;

attr_name_to_int('OCI_ATTR_RESERVED_26') ->          422;      
attr_name_to_int('OCI_ATTR_XMLTYPE_BINARY_XML') ->  422;       % XML type stored as binary?
attr_name_to_int('OCI_ATTR_CQDES_TABLE_CHANGES') ->  423;

attr_name_to_int('OCI_ATTR_FLOW_CONTROL_TIMEOUT') ->  423;       % AQ: flow control timeout
attr_name_to_int('OCI_ATTR_CQDES_QUERYID') ->        424;
attr_name_to_int('OCI_ATTR_DRIVER_NAME') ->           424;                    % Driver Name
attr_name_to_int('OCI_ATTR_ENV_NLS_LANGUAGE') ->     424; 

attr_name_to_int('OCI_ATTR_CHDES_QUERIES') ->        425;              % Top level change desc
                                                % array of queries
attr_name_to_int('OCI_ATTR_CONNECTION_CLASS') ->      425;

attr_name_to_int('OCI_ATTR_RESERVED_27') ->          425;                        % reserved
attr_name_to_int('OCI_ATTR_ENV_NLS_TERRITORY') ->     425; 

attr_name_to_int('OCI_ATTR_PURITY') ->               426;
attr_name_to_int('OCI_ATTR_RESERVED_28') ->          426;                       % reserved
attr_name_to_int('OCI_ATTR_RESERVED_29') ->          427;                        % reserved
attr_name_to_int('OCI_ATTR_RESERVED_30') ->          428;                        % reserved
attr_name_to_int('OCI_ATTR_RESERVED_31') ->          429;                        % reserved
attr_name_to_int('OCI_ATTR_RESERVED_32') ->          430;                        % reserved

%% ----------- Reserve internal attributes for workload replay  ------------
attr_name_to_int('OCI_ATTR_RESERVED_33') ->               433;

attr_name_to_int('OCI_ATTR_RESERVED_34') ->               434;

%% -------- Attributes for Network Session Time Out--------------------------
attr_name_to_int('OCI_ATTR_SEND_TIMEOUT') ->               435;           % NS send timeout
attr_name_to_int('OCI_ATTR_RECEIVE_TIMEOUT') ->            436;        % NS receive timeout

attr_name_to_int('OCI_ATTR_RESERVED_35') ->                437;

%%--------- Attributes related to LOB prefetch------------------------------
attr_name_to_int('OCI_ATTR_DEFAULT_LOBPREFETCH_SIZE') ->     438;   % default prefetch size
attr_name_to_int('OCI_ATTR_LOBPREFETCH_SIZE') ->             439;           % prefetch size
attr_name_to_int('OCI_ATTR_LOBPREFETCH_LENGTH') ->           440; % prefetch length & chunk

%%--------- Attributes related to LOB Deduplicate Regions ------------------
attr_name_to_int('OCI_ATTR_LOB_REGION_PRIMARY') ->       442;         % Primary LOB Locator
attr_name_to_int('OCI_ATTR_LOB_REGION_PRIMOFF') ->       443;     % Offset into Primary LOB 
attr_name_to_int('OCI_ATTR_RESERVED_36') ->              444;

attr_name_to_int('OCI_ATTR_LOB_REGION_OFFSET') ->        445;               % Region Offset
attr_name_to_int('OCI_ATTR_LOB_REGION_LENGTH') ->        446;   % Region Length Bytes/Chars
attr_name_to_int('OCI_ATTR_LOB_REGION_MIME') ->          447;            % Region mime type

%%--------------------Attribute to fetch ROWID ------------------------------
attr_name_to_int('OCI_ATTR_FETCH_ROWID') ->              448;

%% server attribute
attr_name_to_int('OCI_ATTR_RESERVED_37') ->              449;

%%-------------------Attributes for OCI column security-----------------------
attr_name_to_int('OCI_ATTR_NO_COLUMN_AUTH_WARNING') ->   450;

attr_name_to_int('OCI_ATTR_XDS_POLICY_STATUS') ->        451;

%% --------------- ip address attribute in environment handle --------------
attr_name_to_int('OCI_ATTR_SUBSCR_IPADDR') ->         452;       % ip address to listen on 

attr_name_to_int('OCI_ATTR_RESERVED_40') ->           453;  
attr_name_to_int('OCI_ATTR_RESERVED_41') ->           454;
attr_name_to_int('OCI_ATTR_RESERVED_42') ->           455;
attr_name_to_int('OCI_ATTR_RESERVED_43') ->           456;

%% statement attribute
attr_name_to_int('OCI_ATTR_UB8_ROW_COUNT') ->         457;         % ub8 value of row count

%% ------------- round trip callback attributes in the process  handle -----
attr_name_to_int('OCI_ATTR_RESERVED_458') ->          458;                       % reserved
attr_name_to_int('OCI_ATTR_RESERVED_459') ->          459;                       % reserved

attr_name_to_int('OCI_ATTR_SPOOL_AUTH') ->               460;  % Auth handle on pool handle
attr_name_to_int('OCI_ATTR_SHOW_INVISIBLE_COLUMNS') ->    460;  % invisible columns support
attr_name_to_int('OCI_ATTR_INVISIBLE_COL') ->             461;  % invisible columns support

%% support at most once transaction semantics
attr_name_to_int('OCI_ATTR_LTXID') ->                     462;     % logical transaction id

attr_name_to_int('OCI_ATTR_LAST_LOGON_TIME_UTC') ->       463; % Last Successful Logon Time
attr_name_to_int('OCI_ATTR_IMPLICIT_RESULT_COUNT') ->      463;
attr_name_to_int('OCI_ATTR_RESERVED_464') ->              464;
attr_name_to_int('OCI_ATTR_RESERVED_465') ->              465;
attr_name_to_int('OCI_ATTR_TRANSACTIONAL_TAF') ->         466;
attr_name_to_int('OCI_ATTR_RESERVED_467') ->              467;

%% SQL translation profile session attribute
attr_name_to_int('OCI_ATTR_SQL_TRANSLATION_PROFILE') ->   468;

%% Per Iteration array DML rowcount attribute
attr_name_to_int('OCI_ATTR_DML_ROW_COUNT_ARRAY') ->       469;

attr_name_to_int('OCI_ATTR_RESERVED_470') ->              470;

%% session handle attribute
attr_name_to_int('OCI_ATTR_MAX_OPEN_CURSORS') ->          471;

%% Can application failover and recover from this error? 
%% e.g. ORA-03113 is recoverable while ORA-942 (table or view does not exist)
%% is not.
%% 
attr_name_to_int('OCI_ATTR_ERROR_IS_RECOVERABLE') ->      472;

%% ONS specific private attribute 
attr_name_to_int('OCI_ATTR_RESERVED_473') ->              473; 

%% Attribute to check if ILM Write Access Tracking is enabled or not
attr_name_to_int('OCI_ATTR_ILM_TRACK_WRITE') ->           474;

%% Notification subscription failure callback and context
attr_name_to_int('OCI_ATTR_SUBSCR_FAILURE_CBK') ->        477;

attr_name_to_int('OCI_ATTR_SUBSCR_FAILURE_CTX') ->        478;

%% Reserved
attr_name_to_int('OCI_ATTR_RESERVED_479') ->              479;

attr_name_to_int('OCI_ATTR_RESERVED_480') ->              480;
attr_name_to_int('OCI_ATTR_RESERVED_481') ->              481;

attr_name_to_int('OCI_ATTR_RESERVED_482') ->              482;

%% A SQL translation profile with FOREIGN_SQL_SYNTAX attribute is set in the
%% database session.
%% 
attr_name_to_int('OCI_ATTR_TRANS_PROFILE_FOREIGN') ->     483;

%% is a transaction active on the session?
attr_name_to_int('OCI_ATTR_TRANSACTION_IN_PROGRESS') ->   484;

%% add attribute for DBOP: DataBase OPeration
attr_name_to_int('OCI_ATTR_DBOP') ->                      485;

%% FAN-HA private attribute
attr_name_to_int('OCI_ATTR_RESERVED_486') ->              486;

%% reserved
attr_name_to_int('OCI_ATTR_RESERVED_487') ->              487;
attr_name_to_int('OCI_ATTR_RESERVED_488') ->              488;

attr_name_to_int('OCI_ATTR_VARTYPE_MAXLEN_COMPAT') ->     489;

%% Max Lifetime for session
attr_name_to_int('OCI_ATTR_SPOOL_MAX_LIFETIME_SESSION') -> 490;
attr_name_to_int('OCI_ATTR_RESERVED_491') ->              491;
attr_name_to_int('OCI_ATTR_RESERVED_492') ->              492;
attr_name_to_int('OCI_ATTR_RESERVED_493') ->              493;                   % reserved

attr_name_to_int('OCI_ATTR_ITERS_PROCESSED') ->           494;

attr_name_to_int('OCI_ATTR_BREAK_ON_NET_TIMEOUT') ->      495;           % Break on timeout

attr_name_to_int('OCI_ATTR_SHARDING_KEY') ->              496;  
                                                % Sharding Key (OCIShardingKey *) attribute of OCIAuth/OCISvcCtx
attr_name_to_int('OCI_ATTR_SUPER_SHARDING_KEY') ->        497;
                                                % Super Sharding Key (OCIShardingKey *) attribute of OCIAuth/OCISvcCtx
attr_name_to_int('OCI_ATTR_SHARDING_KEY_B64') ->          498; 
                                                % Base64 Sharding Key: read only attribute of OCIShardingKey 
attr_name_to_int('OCI_ATTR_COLLATION_ID') ->              499;               % Collation ID

attr_name_to_int('OCI_ATTR_MAX_IDENTIFIER_LEN') ->        500;      % max identifier length

attr_name_to_int('OCI_ATTR_FIXUP_CALLBACK') ->            501;
                                                % session state fixup callback

attr_name_to_int('OCI_ATTR_VIRTUAL_COL') ->               502;


attr_name_to_int('OCI_ATTR_RESERVED_503') ->              503;                   % reserved

attr_name_to_int('OCI_ATTR_SQL_ID') ->                    504;        % SQL ID in text form

attr_name_to_int('OCI_ATTR_SHARD_HAS_WRITABLECHUNK') ->   505; 
attr_name_to_int('OCI_ATTR_SPOOL_WAIT_TIMEOUT') ->        506;
attr_name_to_int('OCI_ATTR_RESERVED_507') ->              507;  

attr_name_to_int('OCI_ATTR_FO_TYPE') ->                   508;             % see OCI_FO_xxx

%% HCS Hierarchy and Analytic View Column Attributes
attr_name_to_int('OCI_ATTR_OLAP_ROLE') ->                 509;  % hier / analytic view role
attr_name_to_int('OCI_ATTR_DIMENSION_NAME') ->            510;     % analytic view dim name
attr_name_to_int('OCI_ATTR_HIERARCHY_NAME') ->            511;    % analytic view hier name

attr_name_to_int('OCI_ATTR_RESERVED_512') ->              512;                   % reserved
attr_name_to_int('OCI_ATTR_RESERVED_513') ->              513;                   % reserved

%%------ Attributes for DirPathAPI default support 12.2 ---------------------
attr_name_to_int('OCI_ATTR_DIRPATH_DEFAULTS') ->           513;    % how to handle defaults
attr_name_to_int('OCI_ATTR_DIRPATH_DEF_EXP_CACHE_SIZE') -> 514;   % default expr cache size

attr_name_to_int('OCI_ATTR_RESERVED_515') ->              515; 

attr_name_to_int('OCI_ATTR_RESERVED_516') ->              516;

%%------ Attributes for client to server charset conversion ratio -----------
attr_name_to_int('OCI_ATTR_MAX_CHARSET_RATIO') ->              517;
attr_name_to_int('OCI_ATTR_MAX_NCHARSET_RATIO') ->             518;
attr_name_to_int('OCI_ATTR_RESERVED_519') ->              519;

attr_name_to_int('OCI_ATTR_LOB_REMOTE') ->                520;               % remote lob ?
attr_name_to_int('OCI_ATTR_RESERVED_521') ->              521;
attr_name_to_int('OCI_ATTR_RESERVED_522') ->              522;
attr_name_to_int('OCI_ATTR_RESERVED_523') ->              523.

%%---------------------------------------------------------------------------
