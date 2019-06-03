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

extern DPI_NIF_FUN(stmt_addRef);
extern DPI_NIF_FUN(stmt_bindByName);
extern DPI_NIF_FUN(stmt_bindByPos);
extern DPI_NIF_FUN(stmt_bindValueByName);
extern DPI_NIF_FUN(stmt_bindValueByPos);
extern DPI_NIF_FUN(stmt_close);
extern DPI_NIF_FUN(stmt_define);
extern DPI_NIF_FUN(stmt_defineValue);
extern DPI_NIF_FUN(stmt_execute);
extern DPI_NIF_FUN(stmt_executeMany);
extern DPI_NIF_FUN(stmt_fetch);
extern DPI_NIF_FUN(stmt_fetchRows);
extern DPI_NIF_FUN(stmt_getBatchErrorCount);
extern DPI_NIF_FUN(stmt_getBatchErrors);
extern DPI_NIF_FUN(stmt_getBindCount);
extern DPI_NIF_FUN(stmt_getBindNames);
extern DPI_NIF_FUN(stmt_getFetchArraySize);
extern DPI_NIF_FUN(stmt_getImplicitResult);
extern DPI_NIF_FUN(stmt_getInfo);
extern DPI_NIF_FUN(stmt_getNumQueryColumns);
extern DPI_NIF_FUN(stmt_getQueryInfo);
extern DPI_NIF_FUN(stmt_getQueryValue);
extern DPI_NIF_FUN(stmt_getRowCount);
extern DPI_NIF_FUN(stmt_getSubscrQueryId);
extern DPI_NIF_FUN(stmt_release);
extern DPI_NIF_FUN(stmt_scroll);
extern DPI_NIF_FUN(stmt_setFetchArraySize);

#define DPISTMT_NIFS                            \
    DEF_NIF(stmt_addRef, 1),                 \
        IOB_NIF(stmt_bindByName, 3),         \
        IOB_NIF(stmt_bindByPos, 3),          \
        IOB_NIF(stmt_bindValueByName, 4),    \
        IOB_NIF(stmt_bindValueByPos, 4),     \
        DEF_NIF(stmt_close, 2),              \
        IOB_NIF(stmt_define, 3),             \
        IOB_NIF(stmt_defineValue, 7),        \
        IOB_NIF(stmt_execute, 2),            \
        DEF_NIF(stmt_executeMany, 3),        \
        IOB_NIF(stmt_fetch, 1),              \
        DEF_NIF(stmt_fetchRows, 2),          \
        DEF_NIF(stmt_getBatchErrorCount, 1), \
        DEF_NIF(stmt_getBatchErrors, 3),     \
        DEF_NIF(stmt_getBindCount, 1),       \
        DEF_NIF(stmt_getBindNames, 1),       \
        DEF_NIF(stmt_getFetchArraySize, 1),  \
        DEF_NIF(stmt_getImplicitResult, 1),  \
        DEF_NIF(stmt_getInfo, 1),            \
        DEF_NIF(stmt_getNumQueryColumns, 1), \
        IOB_NIF(stmt_getQueryInfo, 2),       \
        IOB_NIF(stmt_getQueryValue, 2),      \
        DEF_NIF(stmt_getRowCount, 1),        \
        DEF_NIF(stmt_getSubscrQueryId, 1),   \
        DEF_NIF(stmt_release, 1),            \
        DEF_NIF(stmt_scroll, 4),             \
        DEF_NIF(stmt_setFetchArraySize, 2)

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
