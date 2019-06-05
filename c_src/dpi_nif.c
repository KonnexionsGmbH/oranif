#ifdef EMBED
#include "odpi/embed/dpi.c"
#endif

#include "dpi_nif.h"
#include "dpiContext_nif.h"
#include "dpiConn_nif.h"
#include "dpiStmt_nif.h"
#include "dpiQueryInfo_nif.h"
#include "dpiData_nif.h"
#include "dpiVar_nif.h"
#include "dpiObjectType_nif.h"

ERL_NIF_TERM ATOM_OK;
ERL_NIF_TERM ATOM_NULL;
ERL_NIF_TERM ATOM_TRUE;
ERL_NIF_TERM ATOM_FALSE;
ERL_NIF_TERM ATOM_ERROR;
ERL_NIF_TERM ATOM_ENOMEM;

static ErlNifFunc nif_funcs[] = {
    DPICONTEXT_NIFS,
    DPICONN_NIFS,
    DPISTMT_NIFS,
    DPIQUERYINFO_NIFS,
    DPIDATA_NIFS,
    DPIVAR_NIFS,
    DPIOBJECTTYPE_NIFS,
};

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    TRACE;

    DEF_RES(dpiContext);
    DEF_RES(dpiConn);
    DEF_RES(dpiStmt);
    DEF_RES(dpiQueryInfo);
    DEF_RES(dpiData);
    DEF_RES(dpiDataPtr);
    DEF_RES(dpiVar);
    DEF_RES(dpiObjectType);

    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NULL = enif_make_atom(env, "null");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_ENOMEM = enif_make_atom(env, "enomem");

    return 0;
}

static int upgrade(ErlNifEnv *env, void **priv_data,
                   void **old_priv_data, ERL_NIF_TERM load_info)
{
    TRACE;

    return 0;
}

static void unload(ErlNifEnv *env, void *priv_data)
{
    TRACE;
}

ERL_NIF_INIT(dpi, nif_funcs, load, NULL, upgrade, unload)
