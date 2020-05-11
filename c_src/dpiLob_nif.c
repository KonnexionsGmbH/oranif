#include "dpiLob_nif.h"

#ifndef __WIN32__
#include <string.h>
#endif

ErlNifResourceType *dpiLob_type;

void dpiLob_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(lob_setFromBytes)
{
    CHECK_ARGCOUNT(2);

    dpiLob_res *lobRes = NULL;
    ErlNifBinary value;

    if ((!enif_get_resource(env, argv[0], dpiLob_type, (void **)&lobRes)))
        BADARG_EXCEPTION(0, "resource lob");
    if (!enif_inspect_binary(env, argv[1], &value))
        BADARG_EXCEPTION(1, "binary value");
    RAISE_EXCEPTION_ON_DPI_ERROR(
        lobRes->context,
        dpiLob_setFromBytes(lobRes->lob, (const char *)value.data, value.size));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(lob_readBytes)
{
    CHECK_ARGCOUNT(3);

    dpiLob_res *lobRes = NULL;
    uint64_t offset;
    uint64_t amount;
    uint64_t valueLength;
    if ((!enif_get_resource(env, argv[0], dpiLob_type, (void **)&lobRes)))
        BADARG_EXCEPTION(0, "resource lob");
    if (!enif_get_uint64(env, argv[1], &offset))
        BADARG_EXCEPTION(1, "uint64 offset");
    if (!enif_get_uint64(env, argv[2], &amount))
        BADARG_EXCEPTION(2, "uint64 amount");

    ErlNifBinary value;
    enif_alloc_binary(amount, &value);

    RAISE_EXCEPTION_ON_DPI_ERROR(
        lobRes->context,
        dpiLob_readBytes(
            lobRes->lob, offset, amount, (char *)value.data, &valueLength));

    if (amount > valueLength)
    {
        ErlNifBinary smaller_value;
        enif_alloc_binary(valueLength, &smaller_value);
        memcpy(smaller_value.data, value.data, valueLength);
        enif_release_binary(&value);
        RETURNED_TRACE;
        return enif_make_binary(env, &smaller_value);
    }
    else
    {
        RETURNED_TRACE;
        return enif_make_binary(env, &value);
    }
}

DPI_NIF_FUN(lob_release)
{
    CHECK_ARGCOUNT(1);

    dpiLob_res *lobRes = NULL;

    if ((!enif_get_resource(env, argv[0], dpiLob_type, (void **)&lobRes)))
        BADARG_EXCEPTION(0, "resource lob");

    RAISE_EXCEPTION_ON_DPI_ERROR(lobRes->context, dpiLob_release(lobRes->lob));

    RELEASE_RESOURCE(lobRes, dpiLob);

    RETURNED_TRACE;
    return ATOM_OK;
}
