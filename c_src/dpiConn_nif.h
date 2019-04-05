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

extern DPI_NIF_FUN(dpiConn_addRef);
extern DPI_NIF_FUN(dpiConn_beginDistribTrans);
extern DPI_NIF_FUN(dpiConn_breakExecution);
extern DPI_NIF_FUN(dpiConn_changePassword);
extern DPI_NIF_FUN(dpiConn_close);
extern DPI_NIF_FUN(dpiConn_commit);
extern DPI_NIF_FUN(dpiConn_create);
extern DPI_NIF_FUN(dpiConn_deqObject);
extern DPI_NIF_FUN(dpiConn_enqObject);
extern DPI_NIF_FUN(dpiConn_getCallTimeout);
extern DPI_NIF_FUN(dpiConn_getCurrentSchema);
extern DPI_NIF_FUN(dpiConn_getEdition);
extern DPI_NIF_FUN(dpiConn_getEncodingInfo);
extern DPI_NIF_FUN(dpiConn_getExternalName);
extern DPI_NIF_FUN(dpiConn_getHandle);
extern DPI_NIF_FUN(dpiConn_getInternalName);
extern DPI_NIF_FUN(dpiConn_getLTXID);
extern DPI_NIF_FUN(dpiConn_getObjectType);
extern DPI_NIF_FUN(dpiConn_getServerVersion);
extern DPI_NIF_FUN(dpiConn_getSodaDb);
extern DPI_NIF_FUN(dpiConn_getStmtCacheSize);
extern DPI_NIF_FUN(dpiConn_newDeqOptions);
extern DPI_NIF_FUN(dpiConn_newEnqOptions);
extern DPI_NIF_FUN(dpiConn_newMsgProps);
extern DPI_NIF_FUN(dpiConn_newTempLob);
extern DPI_NIF_FUN(dpiConn_newVar);
extern DPI_NIF_FUN(dpiConn_ping);
extern DPI_NIF_FUN(dpiConn_prepareDistribTrans);
extern DPI_NIF_FUN(dpiConn_prepareStmt);
extern DPI_NIF_FUN(dpiConn_release);
extern DPI_NIF_FUN(dpiConn_rollback);
extern DPI_NIF_FUN(dpiConn_setAction);
extern DPI_NIF_FUN(dpiConn_setCallTimeout);
extern DPI_NIF_FUN(dpiConn_setClientIdentifier);
extern DPI_NIF_FUN(dpiConn_setClientInfo);
extern DPI_NIF_FUN(dpiConn_setCurrentSchema);
extern DPI_NIF_FUN(dpiConn_setDbOp);
extern DPI_NIF_FUN(dpiConn_setExternalName);
extern DPI_NIF_FUN(dpiConn_setInternalName);
extern DPI_NIF_FUN(dpiConn_setModule);
extern DPI_NIF_FUN(dpiConn_setStmtCacheSize);
extern DPI_NIF_FUN(dpiConn_shutdownDatabase);
extern DPI_NIF_FUN(dpiConn_startupDatabase);
extern DPI_NIF_FUN(dpiConn_subscribe);
extern DPI_NIF_FUN(dpiConn_unsubscribe);

#define DPICONN_NIFS                             \
    DEF_NIF(dpiConn_addRef, 1),                  \
        DEF_NIF(dpiConn_beginDistribTrans, 4),   \
        DEF_NIF(dpiConn_breakExecution, 1),      \
        DEF_NIF(dpiConn_changePassword, 4),      \
        DEF_NIF(dpiConn_close, 3),               \
        DEF_NIF(dpiConn_commit, 1),              \
        DEF_NIF(dpiConn_create, 6),              \
        DEF_NIF(dpiConn_deqObject, 5),           \
        DEF_NIF(dpiConn_enqObject, 5),           \
        DEF_NIF(dpiConn_getCallTimeout, 1),      \
        DEF_NIF(dpiConn_getCurrentSchema, 1),    \
        DEF_NIF(dpiConn_getEdition, 1),          \
        DEF_NIF(dpiConn_getEncodingInfo, 1),     \
        DEF_NIF(dpiConn_getExternalName, 1),     \
        DEF_NIF(dpiConn_getHandle, 1),           \
        DEF_NIF(dpiConn_getInternalName, 1),     \
        DEF_NIF(dpiConn_getLTXID, 1),            \
        DEF_NIF(dpiConn_getObjectType, 2),       \
        DEF_NIF(dpiConn_getServerVersion, 1),    \
        DEF_NIF(dpiConn_getSodaDb, 1),           \
        DEF_NIF(dpiConn_getStmtCacheSize, 1),    \
        DEF_NIF(dpiConn_newDeqOptions, 1),       \
        DEF_NIF(dpiConn_newEnqOptions, 1),       \
        DEF_NIF(dpiConn_newMsgProps, 1),         \
        DEF_NIF(dpiConn_newTempLob, 2),          \
        DEF_NIF(dpiConn_newVar, 8),              \
        DEF_NIF(dpiConn_ping, 1),                \
        DEF_NIF(dpiConn_prepareDistribTrans, 1), \
        DEF_NIF(dpiConn_prepareStmt, 4),         \
        DEF_NIF(dpiConn_release, 1),             \
        DEF_NIF(dpiConn_rollback, 1),            \
        DEF_NIF(dpiConn_setAction, 2),           \
        DEF_NIF(dpiConn_setCallTimeout, 2),      \
        DEF_NIF(dpiConn_setClientIdentifier, 2), \
        DEF_NIF(dpiConn_setClientInfo, 2),       \
        DEF_NIF(dpiConn_setCurrentSchema, 2),    \
        DEF_NIF(dpiConn_setDbOp, 2),             \
        DEF_NIF(dpiConn_setExternalName, 2),     \
        DEF_NIF(dpiConn_setInternalName, 2),     \
        DEF_NIF(dpiConn_setModule, 2),           \
        DEF_NIF(dpiConn_setStmtCacheSize, 2),    \
        DEF_NIF(dpiConn_shutdownDatabase, 2),    \
        DEF_NIF(dpiConn_startupDatabase, 2),     \
        DEF_NIF(dpiConn_subscribe, 2),           \
        DEF_NIF(dpiConn_unsubscribe, 1)

#endif // _DPICONN_NIF_H_
