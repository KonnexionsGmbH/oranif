#include "dpiStmt_nif.h"
#include "dpiConn_nif.h"
#include "dpiQueryInfo_nif.h"
#include "dpiData_nif.h"
#include "dpiVar_nif.h"
#include "dpiContext_nif.h"
#include <stdio.h>

ErlNifResourceType *dpiStmt_type;

void dpiStmt_res_dtor(ErlNifEnv *env, void *resource)
{
    TRACE;

    L("dpiStmt destroyed\r\n");
}

DPI_NIF_FUN(stmt_execute_default)
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
                return RAISE_EXCEPTION("Mode is list from arg is not atom");
            DPI_EXEC_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiStmt_execute(stmtRes->stmt, mode, &numCols));

    return enif_make_uint(env, numCols);
}


DPI_NIF_FUN(stmt_execute_no_exceptions)
{
    CHECK_ARGCOUNT(3);

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
                return RAISE_EXCEPTION("Mode is list from arg is not atom");
            DPI_EXEC_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));
    if (-1 == dpiStmt_execute(stmtRes->stmt, mode, &numCols)){
        dpiErrorInfo error;
        dpiContext_res *contextRes;
        if (!enif_get_resource(env, argv[2], dpiContext_type, &contextRes))
            return ATOM_ERROR;
        dpiContext_getError(contextRes->context, &error);
        return dpiErrorInfoMap(env, error);
    }
    return enif_make_uint(env, numCols);
}

DPI_NIF_FUN(stmt_execute_io)
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
                return RAISE_EXCEPTION("Mode is list from arg is not atom");
            DPI_EXEC_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiStmt_execute(stmtRes->stmt, mode, &numCols));

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
        dpiStmt_fetch(stmtRes->stmt, &found, &bufferRowIndex));

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

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiStmt_getQueryValue(
            stmtRes->stmt, pos, &nativeTypeNum, &(data->dpiDataPtr)));

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
        dpiStmt_getQueryInfo(stmtRes->stmt, pos, &(infoPointer->queryInfo)));

    ERL_NIF_TERM infoRes = enif_make_resource(env, infoPointer);

    return infoRes;
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
        dpiStmt_bindValueByPos(
            stmtRes->stmt, pos, bindType, &dataRes->dpiData));

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
        dpiStmt_bindValueByName(
            stmtRes->stmt, binary.data, binary.size, bindType, &dataRes->dpiData));

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
        dpiStmt_bindByPos(stmtRes->stmt, pos, varRes->var));

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
        dpiStmt_bindByName(stmtRes->stmt, binary.data, binary.size, varRes->var));

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
        dpiStmt_define(stmtRes->stmt, pos, varRes->var));

    return ATOM_OK;
}

DPI_NIF_FUN(stmt_defineValue)
{
    CHECK_ARGCOUNT(7);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0, size = 0;
    dpiOracleTypeNum oraType;
    dpiNativeTypeNum nativeType;
    char sizeIsBytesBuf[32];
    int sizeIsBytes = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, &stmtRes))
        return BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        return BADARG_EXCEPTION(1, "uint pos");
    DPI_ORACLE_TYPE_NUM_FROM_ATOM(argv[2], oraType);
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[3], nativeType);
    if (!enif_get_uint(env, argv[4], &size))
        return BADARG_EXCEPTION(4, "uint size");
    if (!enif_get_atom(env, argv[5], sizeIsBytesBuf, 32, ERL_NIF_LATIN1))
        return BADARG_EXCEPTION(5, "atom sizeIsBytes");
    sizeIsBytes = strcmp(sizeIsBytesBuf, "false");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiStmt_defineValue(
            stmtRes->stmt, pos, oraType, nativeType, size, sizeIsBytes,
            NULL // TODO: support dpiObjectType
            ));

    return ATOM_OK;
}


ERL_NIF_TERM dpiErrorInfoMap(ErlNifEnv *env, dpiErrorInfo e)
{
    TRACE;

    ERL_NIF_TERM map = enif_make_new_map(env);

    enif_make_map_put(
        env, map,
        enif_make_atom(env, "code"), enif_make_int(env, e.code), &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "offset"), enif_make_uint(env, e.offset), &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "message"),
        enif_make_string_len(env, e.message, e.messageLength, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "encoding"),
        enif_make_string(env, e.encoding, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "fnName"),
        enif_make_string(env, e.fnName, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "action"),
        enif_make_string(env, e.action, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "sqlState"),
        enif_make_string(env, e.sqlState, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "isRecoverable"),
        (e.isRecoverable == 0 ? ATOM_FALSE : ATOM_TRUE), &map);

    /* #{ code => integer(), offset => integer(), message => string(),
          encoding => string(), fnName => string(), action => string(),
          sqlState => string, isRecoverable => true | false } */
    return map;
}
