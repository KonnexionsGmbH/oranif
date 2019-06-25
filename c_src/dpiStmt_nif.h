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
    dpiContext *context;
} dpiStmt_res;

extern ErlNifResourceType *dpiStmt_type;

extern void dpiStmt_res_dtor(ErlNifEnv *env, void *resource);
extern void dpiDataPointer_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(stmt_bindByName);
extern DPI_NIF_FUN(stmt_bindByPos);
extern DPI_NIF_FUN(stmt_bindValueByName);
extern DPI_NIF_FUN(stmt_bindValueByPos);
extern DPI_NIF_FUN(stmt_define);
extern DPI_NIF_FUN(stmt_defineValue);
extern DPI_NIF_FUN(stmt_execute);
extern DPI_NIF_FUN(stmt_fetch);
extern DPI_NIF_FUN(stmt_getQueryInfo);
extern DPI_NIF_FUN(stmt_getQueryValue);
extern DPI_NIF_FUN(stmt_getNumQueryColumns);
extern DPI_NIF_FUN(stmt_release);

#define DPISTMT_NIFS                         \
    IOB_NIF(stmt_bindByName, 3),             \
        IOB_NIF(stmt_bindByPos, 3),          \
        IOB_NIF(stmt_bindValueByName, 4),    \
        IOB_NIF(stmt_bindValueByPos, 4),     \
        IOB_NIF(stmt_define, 3),             \
        IOB_NIF(stmt_defineValue, 7),        \
        IOB_NIF(stmt_execute, 2),            \
        IOB_NIF(stmt_fetch, 1),              \
        IOB_NIF(stmt_getQueryInfo, 2),       \
        IOB_NIF(stmt_getQueryValue, 2),      \
        IOB_NIF(stmt_getNumQueryColumns, 1), \
        DEF_NIF(stmt_release, 1)

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
