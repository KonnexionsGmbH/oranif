#ifndef _DPICONN_NIF_H_
#define _DPICONN_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiConn *conn;
} dpiConn_res;

extern ErlNifResourceType *dpiConn_type;
extern void dpiConn_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(conn_addRef);
extern DPI_NIF_FUN(conn_beginDistribTrans);
extern DPI_NIF_FUN(conn_breakExecution);
extern DPI_NIF_FUN(conn_changePassword);
extern DPI_NIF_FUN(conn_close);
extern DPI_NIF_FUN(conn_commit);
extern DPI_NIF_FUN(conn_create);
extern DPI_NIF_FUN(conn_deqObject);
extern DPI_NIF_FUN(conn_enqObject);
extern DPI_NIF_FUN(conn_getCallTimeout);
extern DPI_NIF_FUN(conn_getCurrentSchema);
extern DPI_NIF_FUN(conn_getEdition);
extern DPI_NIF_FUN(conn_getEncodingInfo);
extern DPI_NIF_FUN(conn_getExternalName);
extern DPI_NIF_FUN(conn_getHandle);
extern DPI_NIF_FUN(conn_getInternalName);
extern DPI_NIF_FUN(conn_getLTXID);
extern DPI_NIF_FUN(conn_getObjectType);
extern DPI_NIF_FUN(conn_getServerVersion);
extern DPI_NIF_FUN(conn_getSodaDb);
extern DPI_NIF_FUN(conn_getStmtCacheSize);
extern DPI_NIF_FUN(conn_newDeqOptions);
extern DPI_NIF_FUN(conn_newEnqOptions);
extern DPI_NIF_FUN(conn_newMsgProps);
extern DPI_NIF_FUN(conn_newTempLob);
extern DPI_NIF_FUN(conn_newVar);
extern DPI_NIF_FUN(conn_ping);
extern DPI_NIF_FUN(conn_prepareDistribTrans);
extern DPI_NIF_FUN(conn_prepareStmt);
extern DPI_NIF_FUN(conn_release);
extern DPI_NIF_FUN(conn_rollback);
extern DPI_NIF_FUN(conn_setAction);
extern DPI_NIF_FUN(conn_setCallTimeout);
extern DPI_NIF_FUN(conn_setClientIdentifier);
extern DPI_NIF_FUN(conn_setClientInfo);
extern DPI_NIF_FUN(conn_setCurrentSchema);
extern DPI_NIF_FUN(conn_setDbOp);
extern DPI_NIF_FUN(conn_setExternalName);
extern DPI_NIF_FUN(conn_setInternalName);
extern DPI_NIF_FUN(conn_setModule);
extern DPI_NIF_FUN(conn_setStmtCacheSize);
extern DPI_NIF_FUN(conn_shutdownDatabase);
extern DPI_NIF_FUN(conn_startupDatabase);
extern DPI_NIF_FUN(conn_subscribe);
extern DPI_NIF_FUN(conn_unsubscribe);

#define DPICONN_NIFS                             \
    DEF_NIF(conn_addRef, 1),                  \
        DEF_NIF(conn_beginDistribTrans, 4),   \
        DEF_NIF(conn_breakExecution, 1),      \
        DEF_NIF(conn_changePassword, 4),      \
        DEF_NIF(conn_close, 3),               \
        DEF_NIF(conn_commit, 1),              \
        DEF_NIF(conn_create, 6),              \
        DEF_NIF(conn_deqObject, 5),           \
        DEF_NIF(conn_enqObject, 5),           \
        DEF_NIF(conn_getCallTimeout, 1),      \
        DEF_NIF(conn_getCurrentSchema, 1),    \
        DEF_NIF(conn_getEdition, 1),          \
        DEF_NIF(conn_getEncodingInfo, 1),     \
        DEF_NIF(conn_getExternalName, 1),     \
        DEF_NIF(conn_getHandle, 1),           \
        DEF_NIF(conn_getInternalName, 1),     \
        DEF_NIF(conn_getLTXID, 1),            \
        DEF_NIF(conn_getObjectType, 2),       \
        DEF_NIF(conn_getServerVersion, 1),    \
        DEF_NIF(conn_getSodaDb, 1),           \
        DEF_NIF(conn_getStmtCacheSize, 1),    \
        DEF_NIF(conn_newDeqOptions, 1),       \
        DEF_NIF(conn_newEnqOptions, 1),       \
        DEF_NIF(conn_newMsgProps, 1),         \
        DEF_NIF(conn_newTempLob, 2),          \
        DEF_NIF(conn_newVar, 8),              \
        DEF_NIF(conn_ping, 1),                \
        DEF_NIF(conn_prepareDistribTrans, 1), \
        DEF_NIF(conn_prepareStmt, 4),         \
        DEF_NIF(conn_release, 1),             \
        DEF_NIF(conn_rollback, 1),            \
        DEF_NIF(conn_setAction, 2),           \
        DEF_NIF(conn_setCallTimeout, 2),      \
        DEF_NIF(conn_setClientIdentifier, 2), \
        DEF_NIF(conn_setClientInfo, 2),       \
        DEF_NIF(conn_setCurrentSchema, 2),    \
        DEF_NIF(conn_setDbOp, 2),             \
        DEF_NIF(conn_setExternalName, 2),     \
        DEF_NIF(conn_setInternalName, 2),     \
        DEF_NIF(conn_setModule, 2),           \
        DEF_NIF(conn_setStmtCacheSize, 2),    \
        DEF_NIF(conn_shutdownDatabase, 2),    \
        DEF_NIF(conn_startupDatabase, 2),     \
        DEF_NIF(conn_subscribe, 2),           \
        DEF_NIF(conn_unsubscribe, 1)

#endif // _conn_NIF_H_

