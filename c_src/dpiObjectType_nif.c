#include "dpiObjectType_nif.h"
#include <stdio.h>

ErlNifResourceType *dpiObjectType_type;

void dpiObjectType_res_dtor(ErlNifEnv *env, void *resource)
{
    TRACE;

    L("dpiObjectType destroyed\r\n");
}

DPI_NIF_FUN(objectType_release)
{
    CHECK_ARGCOUNT(1);

    dpiObjectType_res *oRes;

    if ((!enif_get_resource(env, argv[0], dpiObjectType_type, &oRes)))
        return BADARG_EXCEPTION(0, "resource objectType");

    return ATOM_OK;
}

UNIMPLEMENTED(objectType_addRef);
UNIMPLEMENTED(objectType_createObject);
UNIMPLEMENTED(objectType_getAttributes);
UNIMPLEMENTED(objectType_getInfo);
