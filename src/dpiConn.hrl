-ifndef(_DPI_CONN_HRL_).
-define(_DPI_CONN_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiConn.html

-nifs({dpiConn, [
    ?CF(onn_addRef, [reference]),
    ?CF(onn_beginDistribTrans, [reference, integer, binary, binary]),
    ?CF(onn_breakExecution, [reference]),
    ?CF(onn_changePassword, [reference, binary, binary, binary]),
    ?CF(onn_close, [reference, list, binary]),
    ?CF(onn_commit, [reference]),
    ?CF(onn_create, [reference, binary, binary, binary, {map, null}, {map, null}], create),
    ?CF(onn_deqObject, [reference, binary, map, map, reference]),
    ?CF(onn_enqObject, [reference, binary, map, map, reference]),
    ?CF(onn_getCallTimeout, [reference]),
    ?CF(onn_getCurrentSchema, [reference]),
    ?CF(onn_getEdition, [reference]),
    ?CF(onn_getEncodingInfo, [reference]),
    ?CF(onn_getExternalName, [reference]),
    ?CF(onn_getHandle, [reference]),
    ?CF(onn_getInternalName, [reference]),
    ?CF(onn_getLTXID, [reference]),
    ?CF(onn_getObjectType, [reference, binary], create),
    ?CF(onn_getServerVersion, [reference]),
    ?CF(onn_getSodaDb, [reference]),
    ?CF(onn_getStmtCacheSize, [reference]),
    ?CF(onn_newDeqOptions, [reference]),
    ?CF(onn_newEnqOptions, [reference]),
    ?CF(onn_newMsgProps, [reference]),
    ?CF(onn_newTempLob, [reference, atom]),
    ?CF(onn_newVar, [reference, atom, atom, integer, integer, atom, atom, atom], create), %% bools are to be checked if atom true|false in NIF-C code
    ?CF(onn_ping, [reference]),
    ?CF(onn_prepareDistribTrans, [reference]),
    ?CF(onn_prepareStmt, [reference, atom, binary, binary], create), %% bool to be checked if atom true|false in NIF-C code
    ?CF(onn_release, [reference], delete),
    ?CF(onn_rollback, [reference]),
    ?CF(onn_setAction, [reference, binary]),
    ?CF(onn_setCallTimeout, [reference, integer]),
    ?CF(onn_setClientIdentifier, [reference, binary]),
    ?CF(onn_setClientInfo, [reference, binary]),
    ?CF(onn_setCurrentSchema, [reference, binary]),
    ?CF(onn_setDbOp, [reference, binary]),
    ?CF(onn_setExternalName, [reference, binary]),
    ?CF(onn_setInternalName, [reference, binary]),
    ?CF(onn_setModule, [reference, binary]),
    ?CF(onn_setStmtCacheSize, [reference, integer]),
    ?CF(onn_shutdownDatabase, [reference, atom]),
    ?CF(onn_startupDatabase, [reference, atom]),
    ?CF(onn_subscribe, [reference, map]),
    ?CF(onn_unsubscribe, [reference])
]}).

-endif. % _DPI_CONN_HRL_
