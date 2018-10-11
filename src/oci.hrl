-ifndef(OCI_HRL).
-define(OCI_HRL, true).

%% The following defines are imported from oci.h
% as per current use in erlang interface
% add more when used, sort them in by value

-define(OCI_HTYPE_ENV, 1).

-define(OCI_ATTR_ENV_NLS_LANGUAGE,  424).
-define(OCI_ATTR_ENV_NLS_TERRITORY, 425).

-define(OCI_NLS_DAYNAME1, 1).

-define(OCI_DEFAULT,            16#00000000).
-define(OCI_COMMIT_ON_SUCCESS,  16#00000020).

% some unusaed types are defined in anticipation
% TODO cleanup unused
-define(SQLT_CHR,           1).
-define(SQLT_NUM,           2).
-define(SQLT_INT,           3).
-define(SQLT_FLT,           4).
-define(SQLT_STR,           5).
-define(SQLT_VNU,           6).
-define(SQLT_PDN,           7).
-define(SQLT_LNG,           8).
-define(SQLT_VCS,           9).
-define(SQLT_NON,           10).
-define(SQLT_RID,           11).
-define(SQLT_DAT,           12).
-define(SQLT_VBI,           15).
-define(SQLT_BFLOAT,        21).
-define(SQLT_BDOUBLE,       22).
-define(SQLT_BIN,           23).
-define(SQLT_LBI,           24).
-define(SQLT_UIN,           68).
-define(SQLT_SLS,           91).
-define(SQLT_LVC,           94).
-define(SQLT_LVB,           95).
-define(SQLT_AFC,           96).
-define(SQLT_AVC,           97).
-define(SQLT_IBFLOAT,       100).
-define(SQLT_IBDOUBLE,      101).
-define(SQLT_CUR,           102).
-define(SQLT_RDD,           104).
-define(SQLT_LAB,           105).
-define(SQLT_OSL,           106).
-define(SQLT_NTY,           108).
-define(SQLT_REF,           110).
-define(SQLT_CLOB,          112).
-define(SQLT_BLOB,          113).
-define(SQLT_BFILEE,        114).
-define(SQLT_CFILEE,        115).
-define(SQLT_RSET,          116).
-define(SQLT_NCO,           122).
-define(SQLT_VST,           155).
-define(SQLT_ODT,           156).
-define(SQLT_DATE,          184).
-define(SQLT_TIME,          185).
-define(SQLT_TIME_TZ,       186).
-define(SQLT_TIMESTAMP,     187).
-define(SQLT_TIMESTAMP_TZ,  188).
-define(SQLT_INTERVAL_YM,   189).
-define(SQLT_INTERVAL_DS,   190).
-define(SQLT_TIMESTAMP_LTZ, 232).
-define(SQLT_PNTY,          241).
-define(SQLT_REC,           250).
-define(SQLT_TAB,           251).
-define(SQLT_BOL,           252).
-define(SQLT_FILE,          ?SQLT_BFILEE).
-define(SQLT_CFILE,         ?SQLT_CFILEE).
-define(SQLT_BFILE,         ?SQLT_BFILEE).

-define(OCI_STMT_UNKNOW,    0).
-define(OCI_STMT_SELECT,    1).
-define(OCI_STMT_UPDATE,    2).
-define(OCI_STMT_DELETE,    3).
-define(OCI_STMT_INSERT,    4).
-define(OCI_STMT_CREATE,    5).
-define(OCI_STMT_DROP,      6).
-define(OCI_STMT_ALTER,     7).
-define(OCI_STMT_BEGIN,     8).
-define(OCI_STMT_DECLARE,   9).
-define(OCI_STMT_CALL,      10).
-define(OCI_STMT_MERGE,     16).

-endif. % OCI_HRL