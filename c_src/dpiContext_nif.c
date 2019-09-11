#include "dpiContext_nif.h"

ErlNifResourceType *dpiContext_type;

void dpiContext_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(context_create)
{
    CHECK_ARGCOUNT(2);

    unsigned int major, minor;
    dpiErrorInfo error;

    if (!enif_get_uint(env, argv[0], &major))
        BADARG_EXCEPTION(0, "uint major");
    if (!enif_get_uint(env, argv[1], &minor))
        BADARG_EXCEPTION(1, "uint minor");

    dpiContext_res *contextRes;
    ALLOC_RESOURCE(contextRes, dpiContext);

    // RAISE_EXCEPTION_ON_DPI_ERROR macro can't be used since we need to return
    // the error details too
    // RAISE_EXCEPTION can't be used since we want to throw the error map
    if (DPI_FAILURE ==
        dpiContext_create(major, minor, &contextRes->context, &error))
    {
        RELEASE_RESOURCE(contextRes, dpiContext);

        RETURNED_TRACE;
        return enif_raise_exception(
            env,
            enif_make_tuple4(
                env, ATOM_ERROR,
                enif_make_string(env, __FILE__, ERL_NIF_LATIN1),
                enif_make_int(env, __LINE__), dpiErrorInfoMap(env, error)));
    }

    ERL_NIF_TERM contextResTerm = enif_make_resource(env, contextRes);

    RETURNED_TRACE;
    return contextResTerm;
}

DPI_NIF_FUN(context_create_n)
{
    CHECK_ARGCOUNT(3);

    unsigned int major, minor;
    dpiErrorInfo error;
    ErlNifBinary resName;

    if (!enif_get_uint(env, argv[0], &major))
        BADARG_EXCEPTION(0, "uint major");
    if (!enif_get_uint(env, argv[1], &minor))
        BADARG_EXCEPTION(1, "uint minor");
    if (!enif_inspect_binary(env, argv[2], &resName))
        BADARG_EXCEPTION(2, "res name");

    dpiContext_res *contextRes;
    ALLOC_RESOURCE_N(contextRes, dpiContext, resName.data, resName.size);

    // RAISE_EXCEPTION_ON_DPI_ERROR macro can't be used since we need to return
    // the error details too
    // RAISE_EXCEPTION can't be used since we want to throw the error map
    if (DPI_FAILURE ==
        dpiContext_create(major, minor, &contextRes->context, &error))
    {
        RELEASE_RESOURCE_N(contextRes, dpiContext, resName.data, resName.size);

        RETURNED_TRACE;
        return enif_raise_exception(
            env,
            enif_make_tuple4(
                env, ATOM_ERROR,
                enif_make_string(env, __FILE__, ERL_NIF_LATIN1),
                enif_make_int(env, __LINE__), dpiErrorInfoMap(env, error)));
    }

    ERL_NIF_TERM contextResTerm = enif_make_resource(env, contextRes);

    RETURNED_TRACE;
    return contextResTerm;
}

DPI_NIF_FUN(context_destroy)
{
    CHECK_ARGCOUNT(1);

    dpiContext_res *contextRes;

    if (!enif_get_resource(env, argv[0], dpiContext_type, (void **)&contextRes))
        BADARG_EXCEPTION(0, "resource context");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context, dpiContext_destroy(contextRes->context));

    RELEASE_RESOURCE(contextRes, dpiContext);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(context_destroy_n)
{
    CHECK_ARGCOUNT(2);

    dpiContext_res *contextRes;
    ErlNifBinary resName;

    if (!enif_get_resource(env, argv[0], dpiContext_type, (void **)&contextRes))
        BADARG_EXCEPTION(0, "resource context");
    if (!enif_inspect_binary(env, argv[1], &resName))
        BADARG_EXCEPTION(1, "res name");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context, dpiContext_destroy(contextRes->context));

    RELEASE_RESOURCE_N(contextRes, dpiContext, resName.data, resName.size);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(context_getClientVersion)
{
    CHECK_ARGCOUNT(1);

    dpiContext_res *contextRes;

    if (!enif_get_resource(env, argv[0], dpiContext_type, (void **)&contextRes))
        BADARG_EXCEPTION(0, "resource context");

    dpiVersionInfo version;

    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context,
        dpiContext_getClientVersion(contextRes->context, &version));

    ERL_NIF_TERM map = enif_make_new_map(env);

    enif_make_map_put(
        env, map, enif_make_atom(env, "versionNum"),
        enif_make_int(env, version.versionNum), &map);

    enif_make_map_put(
        env, map, enif_make_atom(env, "releaseNum"),
        enif_make_int(env, version.releaseNum), &map);

    enif_make_map_put(
        env, map, enif_make_atom(env, "updateNum"),
        enif_make_int(env, version.updateNum), &map);

    enif_make_map_put(
        env, map, enif_make_atom(env, "portReleaseNum"),
        enif_make_int(env, version.portReleaseNum), &map);

    enif_make_map_put(
        env, map, enif_make_atom(env, "portUpdateNum"),
        enif_make_int(env, version.portUpdateNum), &map);

    enif_make_map_put(
        env, map, enif_make_atom(env, "fullVersionNum"),
        enif_make_int(env, version.fullVersionNum), &map);

    /* #{versionNum => integer, releaseNum => integer, updateNum => integer,
         portReleaseNum => integer, portUpdateNum => integer,
         fullVersionNum => integer} */
    RETURNED_TRACE;
    return map;
}
