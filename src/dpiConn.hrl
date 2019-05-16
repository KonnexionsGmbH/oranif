-ifndef(_DPI_CONN_HRL_).
-define(_DPI_CONN_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiConn.html

-nifs({dpiConn, [
    {conn_addRef, [reference]},
    {conn_beginDistribTrans, [reference, integer, binary, binary]},
    {conn_breakExecution, [reference]},
    {conn_changePassword, [reference, binary, binary, binary]},
    {conn_close, [reference, list, binary]},
    {conn_commit, [reference]},
    {conn_create, [reference, binary, binary, binary, {map, null}, {map, null}]},
    {conn_deqObject, [reference, binary, map, map, reference]},
    {conn_enqObject, [reference, binary, map, map, reference]},
    {conn_getCallTimeout, [reference]},
    {conn_getCurrentSchema, [reference]},
    {conn_getEdition, [reference]},
    {conn_getEncodingInfo, [reference]},
    {conn_getExternalName, [reference]},
    {conn_getHandle, [reference]},
    {conn_getInternalName, [reference]},
    {conn_getLTXID, [reference]},
    {conn_getObjectType, [reference, binary]},
    {conn_getServerVersion, [reference]},
    {conn_getSodaDb, [reference]},
    {conn_getStmtCacheSize, [reference]},
    {conn_newDeqOptions, [reference]},
    {conn_newEnqOptions, [reference]},
    {conn_newMsgProps, [reference]},
    {conn_newTempLob, [reference, atom]},
    {conn_newVar, [reference, atom, atom, integer, integer, atom, atom, atom]}, %% bools are to be checked if atom true|false in NIF-C code
    {conn_ping, [reference]},
    {conn_prepareDistribTrans, [reference]},
    {conn_prepareStmt, [reference, atom, binary, binary]}, %% bool to be checked if atom true|false in NIF-C code
    {conn_release, [reference]},
    {conn_rollback, [reference]},
    {conn_setAction, [reference, binary]},
    {conn_setCallTimeout, [reference, integer]},
    {conn_setClientIdentifier, [reference, binary]},
    {conn_setClientInfo, [reference, binary]},
    {conn_setCurrentSchema, [reference, binary]},
    {conn_setDbOp, [reference, binary]},
    {conn_setExternalName, [reference, binary]},
    {conn_setInternalName, [reference, binary]},
    {conn_setModule, [reference, binary]},
    {conn_setStmtCacheSize, [reference, integer]},
    {conn_shutdownDatabase, [reference, atom]},
    {conn_startupDatabase, [reference, atom]},
    {conn_subscribe, [reference, map]},
    {conn_unsubscribe, [reference]}
]}).

-endif. % _DPI_CONN_HRL_
