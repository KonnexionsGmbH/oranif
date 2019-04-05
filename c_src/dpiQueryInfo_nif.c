#include "dpiQueryInfo_nif.h"
#include "dpiData_nif.h"
#include <stdio.h>

ErlNifResourceType *dpiQueryInfo_type;

void dpiQueryInfo_res_dtor(ErlNifEnv *env, void *resource)
{
    TRACE;

    L("dpiQueryInfo destroyed\r\n");
}

/** just gets everything, including the TypeInfo contained within,
 * and returns it all as a map */
DPI_NIF_FUN(dpiQueryInfo_get)
{
    CHECK_ARGCOUNT(1);

    dpiQueryInfo_res *queryInfoRes;

    if (!enif_get_resource(env, argv[0], dpiQueryInfo_type, &queryInfoRes))
        return BADARG_EXCEPTION(0, "resource queryinfo");

    dpiQueryInfo qi = queryInfoRes->queryInfo;
    dpiDataTypeInfo dti = qi.typeInfo;
    ERL_NIF_TERM typeInfo = enif_make_new_map(env);

    ERL_NIF_TERM oracleTypeNumAtom;
    DPI_ORACLE_TYPE_NUM_TO_ATOM(dti.oracleTypeNum, oracleTypeNumAtom);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "oracleTypeNum"),
                      oracleTypeNumAtom, &typeInfo);

    ERL_NIF_TERM defaultNativeTypeNumAtom;
    DPI_NATIVE_TYPE_NUM_TO_ATOM(
        dti.defaultNativeTypeNum, defaultNativeTypeNumAtom);

    enif_make_map_put(env, typeInfo,
                      enif_make_atom(env, "defaultNativeTypeNum"),
                      defaultNativeTypeNumAtom, &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "ociTypeCode"),
                      enif_make_uint(env, dti.ociTypeCode), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "dbSizeInBytes"),
                      enif_make_uint(env, dti.dbSizeInBytes), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "clientSizeInBytes"),
                      enif_make_uint(env, dti.clientSizeInBytes), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "sizeInChars"),
                      enif_make_uint(env, dti.sizeInChars), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "precision"),
                      enif_make_int(env, dti.precision), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "scale"),
                      enif_make_int(env, dti.scale), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "fsPrecision"),
                      enif_make_int(env, dti.fsPrecision), &typeInfo);
    enif_make_map_put(env, typeInfo, enif_make_atom(env, "objectType"),
                      enif_make_atom(env, "featureNotImplemented"), &typeInfo);

    ERL_NIF_TERM resultMap = enif_make_new_map(env);
    enif_make_map_put(env, resultMap, enif_make_atom(env, "typeInfo"),
                      typeInfo, &resultMap);
    enif_make_map_put(env, resultMap, enif_make_atom(env, "name"),
                      enif_make_string_len(env, qi.name, qi.nameLength,
                                           ERL_NIF_LATIN1),
                      &resultMap);
    enif_make_map_put(env, resultMap, enif_make_atom(env, "nullOk"),
                      enif_make_atom(
                          env, qi.nullOk ? "true" : "false"),
                      &resultMap);

    /* #{name => "A", nullOk => atom,
         typeInfo => #{clientSizeInBytes => integer, dbSizeInBytes => integer,
                       defaultNativeTypeNum => atom, fsPrecision => integer,
                       objectType => atom, ociTypeCode => integer,
                       oracleTypeNum => atom , precision => integer,
                       scale => integer, sizeInChars => integer}
        } */
    return resultMap;
}

DPI_NIF_FUN(dpiQueryInfo_delete)
{
    CHECK_ARGCOUNT(1);

    dpiQueryInfo_res *qRes;

    if ((!enif_get_resource(env, argv[0], dpiQueryInfo_type, &qRes)))
        return BADARG_EXCEPTION(0, "resource queryInfo");

    return ATOM_OK;
}
