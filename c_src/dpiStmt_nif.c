#include "dpiStmt_nif.h"
#include "dpiConn_nif.h"
#include "dpiQueryInfo_nif.h"
#include "dpiData_nif.h"
#include "dpiVar_nif.h"
#include <stdio.h>

ErlNifResourceType *dpiStmt_type;

void dpiStmt_res_dtor(ErlNifEnv *env, void *resource)
{
    TRACE;

    L("dpiStmt destroyed\r\n");
}

DPI_NIF_FUN(stmt_execute)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    uint32_t numCols = 0;
    unsigned int len;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");

    ERL_NIF_TERM head, tail;
    if (!enif_is_list(env, argv[1]) &&
        !enif_get_list_cell(env, argv[1], &head, &tail))
        return BADARG_EXCEPTION(1, "atom list modes");

    enif_get_list_length(env, argv[1], &len);
    dpiExecMode m;
    dpiExecMode mode = 0;
    if (len > 0)
        do
        {
            if (!enif_is_atom(env, head))
                return RAISE_STR_EXCEPTION("Mode is list from arg is not atom");
            DPI_EXEC_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_execute(stmtRes->stmt, mode, &numCols),
        NULL);

    return enif_make_uint(env, numCols);
}

DPI_NIF_FUN(stmt_fetch)
{
    CHECK_ARGCOUNT(1);

    dpiStmt_res *stmtRes;
    int found = 0;
    uint32_t bufferRowIndex;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_fetch(stmtRes->stmt, &found, &bufferRowIndex),
        NULL);

    ERL_NIF_TERM map = enif_make_new_map(env);
    enif_make_map_put(
        env, map, enif_make_atom(env, "found"),
        enif_make_atom(
            env, found ? "true" : "false"),
        &map);
    enif_make_map_put(
        env, map, enif_make_atom(env, "bufferRowIndex"),
        enif_make_uint(env, bufferRowIndex), &map);

    // #{bufferRowIndex => integer, found => atom}
    return map;
}

DPI_NIF_FUN(stmt_getQueryValue)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0;
    dpiNativeTypeNum nativeTypeNum = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");

    dpiDataPtr_res *data = enif_alloc_resource(
        dpiDataPtr_type, sizeof(dpiDataPtr_res));
    data->stmtRes = NULL;

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_getQueryValue(
            stmtRes->stmt, pos, &nativeTypeNum, &(data->dpiDataPtr)),
        data);

    data->type = nativeTypeNum;
    ERL_NIF_TERM dpiDataRes = enif_make_resource(env, data);

    ERL_NIF_TERM nativeTypeNumAtom;
    DPI_NATIVE_TYPE_NUM_TO_ATOM(nativeTypeNum, nativeTypeNumAtom);

    ERL_NIF_TERM map = enif_make_new_map(env);

    enif_make_map_put(
        env, map, enif_make_atom(env, "nativeTypeNum"), nativeTypeNumAtom,
        &map);
    enif_make_map_put(
        env, map, enif_make_atom(env, "data"), dpiDataRes, &map);

    // return #{ nativeTypeNum => atom, data => term  }
    return map;
}

DPI_NIF_FUN(stmt_getQueryInfo)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");

    dpiQueryInfo_res *infoPointer = enif_alloc_resource(
        dpiQueryInfo_type, sizeof(dpiQueryInfo_res));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_getQueryInfo(stmtRes->stmt, pos, &(infoPointer->queryInfo)),
        infoPointer);

    ERL_NIF_TERM infoRes = enif_make_resource(env, infoPointer);

    return infoRes;
}

DPI_NIF_FUN(stmt_getNumQueryColumns)
{
    CHECK_ARGCOUNT(1);

    dpiStmt_res *stmtRes;
    uint32_t numQueryColumns;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_getNumQueryColumns(stmtRes->stmt, &numQueryColumns),
        NULL);

    return enif_make_uint(env, numQueryColumns);
}

DPI_NIF_FUN(stmt_bindValueByPos)
{
    CHECK_ARGCOUNT(4);

    dpiStmt_res *stmtRes;
    dpiData_res *dataRes;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");
    if (!enif_get_resource(env, argv[3], dpiData_type, &dataRes))
        return BADARG_EXCEPTION(3, "resource data");

    dpiNativeTypeNum bindType;
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[2], bindType);

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindValueByPos(stmtRes->stmt, pos, bindType, &dataRes->dpiData),
        NULL);

    return ATOM_OK;
}

DPI_NIF_FUN(stmt_bindValueByName)
{
    CHECK_ARGCOUNT(4);

    dpiStmt_res *stmtRes;
    dpiData_res *dataRes;
    ErlNifBinary binary;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_inspect_binary(env, argv[1], &binary))
        return BADARG_EXCEPTION(1, "string/list name");
    if (!enif_get_resource(env, argv[3], dpiData_type, &dataRes))
        return BADARG_EXCEPTION(3, "resource data");

    dpiNativeTypeNum bindType;
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[2], bindType);

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindValueByName(
            stmtRes->stmt, binary.data, binary.size, bindType,
            &dataRes->dpiData),
        NULL);

    return ATOM_OK;
}

DPI_NIF_FUN(stmt_bindByPos)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes = NULL;
    dpiVar_res *varRes = NULL;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");
    if (!enif_get_resource(env, argv[2], dpiVar_type, &varRes))
        return BADARG_EXCEPTION(3, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindByPos(stmtRes->stmt, pos, varRes->var), NULL);

    return ATOM_OK;
}

DPI_NIF_FUN(stmt_bindByName)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes = NULL;
    dpiVar_res *varRes = NULL;
    ErlNifBinary binary;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_inspect_binary(env, argv[1], &binary))
        return BADARG_EXCEPTION(1, "string/list name");
    if (!enif_get_resource(env, argv[2], dpiVar_type, &varRes))
        return BADARG_EXCEPTION(3, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindByName(
            stmtRes->stmt, binary.data, binary.size, varRes->var),
        NULL);

    return ATOM_OK;
}

DPI_NIF_FUN(stmt_release)
{
    CHECK_ARGCOUNT(1);

    dpiStmt_res *stmtRes;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");

    dpiStmt_release(stmtRes->stmt);
    enif_release_resource(stmtRes);
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_define)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes = NULL;
    dpiVar_res *varRes = NULL;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");
    if (!enif_get_resource(env, argv[2], dpiVar_type, &varRes))
        return BADARG_EXCEPTION(2, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_define(stmtRes->stmt, pos, varRes->var), NULL);

    return ATOM_OK;
}

DPI_NIF_FUN(stmt_defineValue)
{
    CHECK_ARGCOUNT(7);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0, size = 0;
    dpiOracleTypeNum oraType;
    dpiNativeTypeNum nativeType;
    int sizeIsBytes = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");
    DPI_ORACLE_TYPE_NUM_FROM_ATOM(argv[2], oraType);
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[3], nativeType);
    if (!enif_get_uint(env, argv[4], &size))
        return BADARG_EXCEPTION(4, "uint size");

    if (enif_compare(argv[5], ATOM_TRUE) == 0)
        sizeIsBytes = 1;
    else if (enif_compare(argv[5], ATOM_FALSE) == 0)
        sizeIsBytes = 0;
    else
        return BADARG_EXCEPTION(5, "bool/atom sizeIsBytes");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_defineValue(
            stmtRes->stmt, pos, oraType, nativeType, size, sizeIsBytes,
            NULL // TODO: support dpiObjectType
            ),
        NULL);

    return ATOM_OK;
}
