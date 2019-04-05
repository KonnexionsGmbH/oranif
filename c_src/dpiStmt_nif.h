#ifndef _DPISTMT_NIF_H_
#define _DPISTMT_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiData *ptr;
} dpiDataPointer_res;

typedef struct
{
    dpiStmt *stmt;
} dpiStmt_res;

extern ErlNifResourceType *dpiStmt_type;

extern void dpiStmt_res_dtor(ErlNifEnv *env, void *resource);
extern void dpiDataPointer_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(dpiStmt_addRef);
extern DPI_NIF_FUN(dpiStmt_bindByName);
extern DPI_NIF_FUN(dpiStmt_bindByPos);
extern DPI_NIF_FUN(dpiStmt_bindValueByName);
extern DPI_NIF_FUN(dpiStmt_bindValueByPos);
extern DPI_NIF_FUN(dpiStmt_close);
extern DPI_NIF_FUN(dpiStmt_define);
extern DPI_NIF_FUN(dpiStmt_defineValue);
extern DPI_NIF_FUN(dpiStmt_execute);
extern DPI_NIF_FUN(dpiStmt_executeMany);
extern DPI_NIF_FUN(dpiStmt_fetch);
extern DPI_NIF_FUN(dpiStmt_fetchRows);
extern DPI_NIF_FUN(dpiStmt_getBatchErrorCount);
extern DPI_NIF_FUN(dpiStmt_getBatchErrors);
extern DPI_NIF_FUN(dpiStmt_getBindCount);
extern DPI_NIF_FUN(dpiStmt_getBindNames);
extern DPI_NIF_FUN(dpiStmt_getFetchArraySize);
extern DPI_NIF_FUN(dpiStmt_getImplicitResult);
extern DPI_NIF_FUN(dpiStmt_getInfo);
extern DPI_NIF_FUN(dpiStmt_getNumQueryColumns);
extern DPI_NIF_FUN(dpiStmt_getQueryInfo);
extern DPI_NIF_FUN(dpiStmt_getQueryValue);
extern DPI_NIF_FUN(dpiStmt_getRowCount);
extern DPI_NIF_FUN(dpiStmt_getSubscrQueryId);
extern DPI_NIF_FUN(dpiStmt_release);
extern DPI_NIF_FUN(dpiStmt_scroll);
extern DPI_NIF_FUN(dpiStmt_setFetchArraySize);

#define DPISTMT_NIFS                            \
    DEF_NIF(dpiStmt_addRef, 1),                 \
        DEF_NIF(dpiStmt_bindByName, 3),         \
        DEF_NIF(dpiStmt_bindByPos, 3),          \
        DEF_NIF(dpiStmt_bindValueByName, 4),    \
        DEF_NIF(dpiStmt_bindValueByPos, 4),     \
        DEF_NIF(dpiStmt_close, 2),              \
        DEF_NIF(dpiStmt_define, 3),             \
        DEF_NIF(dpiStmt_defineValue, 7),        \
        DEF_NIF(dpiStmt_execute, 2),            \
        DEF_NIF(dpiStmt_executeMany, 3),        \
        DEF_NIF(dpiStmt_fetch, 1),              \
        DEF_NIF(dpiStmt_fetchRows, 2),          \
        DEF_NIF(dpiStmt_getBatchErrorCount, 1), \
        DEF_NIF(dpiStmt_getBatchErrors, 3),     \
        DEF_NIF(dpiStmt_getBindCount, 1),       \
        DEF_NIF(dpiStmt_getBindNames, 1),       \
        DEF_NIF(dpiStmt_getFetchArraySize, 1),  \
        DEF_NIF(dpiStmt_getImplicitResult, 1),  \
        DEF_NIF(dpiStmt_getInfo, 1),            \
        DEF_NIF(dpiStmt_getNumQueryColumns, 1), \
        DEF_NIF(dpiStmt_getQueryInfo, 2),       \
        DEF_NIF(dpiStmt_getQueryValue, 2),      \
        DEF_NIF(dpiStmt_getRowCount, 1),        \
        DEF_NIF(dpiStmt_getSubscrQueryId, 1),   \
        DEF_NIF(dpiStmt_release, 1),            \
        DEF_NIF(dpiStmt_scroll, 4),             \
        DEF_NIF(dpiStmt_setFetchArraySize, 2)

#define DPI_EXEC_MODE_FROM_ATOM(_atom, _assign)                \
    A2M(DPI_MODE_EXEC_DEFAULT, _atom, _assign);                \
    else A2M(DPI_MODE_EXEC_DESCRIBE_ONLY, _atom, _assign);     \
    else A2M(DPI_MODE_EXEC_COMMIT_ON_SUCCESS, _atom, _assign); \
    else A2M(DPI_MODE_EXEC_BATCH_ERRORS, _atom, _assign);      \
    else A2M(DPI_MODE_EXEC_PARSE_ONLY, _atom, _assign);        \
    else A2M(DPI_MODE_EXEC_ARRAY_DML_ROWCOUNTS, _atom, _assign)

#define DPI_CLOSE_MODE_FROM_ATOM(_atom, _assign)        \
    A2M(DPI_MODE_CONN_CLOSE_DEFAULT, _atom, _assign);   \
    else A2M(DPI_MODE_CONN_CLOSE_DROP, _atom, _assign); \
    else A2M(DPI_MODE_CONN_CLOSE_RETAG, _atom, _assign)

#endif // _DPISTMT_NIF_H_
