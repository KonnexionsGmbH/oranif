#ifndef _DPICONN_NIF_H_
#define _DPICONN_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiConn *conn;
    dpiContext *context;
} dpiConn_res;

extern ErlNifResourceType *dpiConn_type;
extern void dpiConn_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(conn_close);
extern DPI_NIF_FUN(conn_commit);
extern DPI_NIF_FUN(conn_create);
extern DPI_NIF_FUN(conn_getServerVersion);
extern DPI_NIF_FUN(conn_newVar);
extern DPI_NIF_FUN(conn_ping);
extern DPI_NIF_FUN(conn_prepareStmt);
extern DPI_NIF_FUN(conn_release);
extern DPI_NIF_FUN(conn_rollback);

#define DPICONN_NIFS                       \
    DEF_NIF(conn_close, 3),                \
        DEF_NIF(conn_commit, 1),           \
        IOB_NIF(conn_create, 6),           \
        DEF_NIF(conn_getServerVersion, 1), \
        DEF_NIF(conn_newVar, 8),           \
        DEF_NIF(conn_ping, 1),             \
        IOB_NIF(conn_prepareStmt, 4),      \
        DEF_NIF(conn_release, 1),          \
        DEF_NIF(conn_rollback, 1)

#endif // _conn_NIF_H_
