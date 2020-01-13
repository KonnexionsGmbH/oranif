#include "dpiLob_nif.h"

ErlNifResourceType *dpiLob_type;

void dpiLob_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}


DPI_NIF_FUN(lob_release)
{
    CHECK_ARGCOUNT(1);

    dpiLob_res *lRes = NULL;

    if ((!enif_get_resource(env, argv[0], dpiLob_type, (void **)&lRes)))
        BADARG_EXCEPTION(0, "resource lob");

    RAISE_EXCEPTION_ON_DPI_ERROR(lRes->context, dpiLob_release(lRes->lob));

    RELEASE_RESOURCE(lRes, dpiLob);

    RETURNED_TRACE;
    return ATOM_OK;
}
