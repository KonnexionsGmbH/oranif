#include "dpiContext_nif.h"

ErlNifResourceType *dpiContext_type;

void dpiContext_res_dtor(ErlNifEnv *env, void *resource)
{
    TRACE;
    L("dpiContext destroyed\r\n");
}

DPI_NIF_FUN(context_create)
{
    CHECK_ARGCOUNT(2);

    int major, minor;
    dpiErrorInfo error;

    if (!enif_get_uint(env, argv[0], &major))
        return BADARG_EXCEPTION(0, "uint major");
    if (!enif_get_uint(env, argv[1], &minor))
        return BADARG_EXCEPTION(1, "uint minor");

    dpiContext_res *contextRes =
        enif_alloc_resource(dpiContext_type, sizeof(dpiContext_res));

    // RAISE_EXCEPTION_ON_DPI_ERROR macro can't be used since we need to return
    // the error details too
    // RAISE_EXCEPTION can't be used since we want to throw the error map
    if (DPI_FAILURE ==
        dpiContext_create(major, minor, &contextRes->context, &error))
    {
        enif_release_resource(contextRes);
        return enif_raise_exception(
            env,
            enif_make_tuple2(
                env, ATOM_ERROR,
                dpiErrorInfoMap(env, error)));
    }

    ERL_NIF_TERM contextResTerm = enif_make_resource(env, contextRes);

    return contextResTerm;
}

DPI_NIF_FUN(context_getError)
{
    CHECK_ARGCOUNT(1);

    dpiErrorInfo error;
    dpiContext_res *contextRes;

    if (!enif_get_resource(env, argv[0], dpiContext_type, &contextRes))
        return BADARG_EXCEPTION(0, "resource context");

    dpiContext_getError(contextRes->context, &error);

    return dpiErrorInfoMap(env, error);
}

DPI_NIF_FUN(context_destroy)
{
    CHECK_ARGCOUNT(1);

    dpiContext_res *contextRes;

    if (!enif_get_resource(env, argv[0], dpiContext_type, &contextRes))
        return BADARG_EXCEPTION(0, "resource context");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context,
        dpiContext_destroy(contextRes->context),
        NULL);

    enif_release_resource(contextRes);
    return ATOM_OK;
}

DPI_NIF_FUN(context_getClientVersion)
{
    CHECK_ARGCOUNT(1);

    dpiContext_res *contextRes;

    if (!enif_get_resource(env, argv[0], dpiContext_type, &contextRes))
        return BADARG_EXCEPTION(0, "resource context");

    dpiVersionInfo version;

    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context,
        dpiContext_getClientVersion(contextRes->context, &version),
        NULL);

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
    return map;
}
