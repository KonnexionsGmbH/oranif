#include "dpiLob_nif.h"

ErlNifResourceType *dpiLob_type;

void dpiLob_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(lob_setFromBytes)
{
    CHECK_ARGCOUNT(2);

    dpiLob_res *lRes = NULL;
    ErlNifBinary value;

    if ((!enif_get_resource(env, argv[0], dpiLob_type, (void **)&lRes)))
        BADARG_EXCEPTION(0, "resource lob");
    if (!enif_inspect_binary(env, argv[1], &value))
        BADARG_EXCEPTION(1, "binary value");
    RAISE_EXCEPTION_ON_DPI_ERROR(
        lRes->context,
        dpiLob_setFromBytes(lRes->lob, (const char *)value.data, value.size)
        );

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(lob_readBytes)
{
    CHECK_ARGCOUNT(3);

    dpiLob_res *lRes = NULL;
    uint64_t offset;
    uint64_t size;
    uint64_t length;
    if ((!enif_get_resource(env, argv[0], dpiLob_type, (void **)&lRes)))
        BADARG_EXCEPTION(0, "resource lob");
    if (!enif_get_uint64(env, argv[1], &offset))
        BADARG_EXCEPTION(1, "uint64 offset");
        if (!enif_get_uint64(env, argv[2], &length))
        BADARG_EXCEPTION(2, "uint64 length");
    
    size = length;
    ErlNifBinary bin;
    enif_alloc_binary(length, &bin);

    RAISE_EXCEPTION_ON_DPI_ERROR(
        lRes->context,
        dpiLob_readBytes(lRes->lob, offset, size, bin.data, &length));

    if (size == length){
        RETURNED_TRACE;
        return enif_make_binary(env, &bin);
    }
    ErlNifBinary bin2;
    enif_alloc_binary(length, &bin2);
    memcpy(bin2.data, bin.data, length);
    return enif_make_binary(env, &bin2);
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
