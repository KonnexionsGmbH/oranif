#include "dpiVar_nif.h"
#include "dpiStmt_nif.h"
#include "dpiConn_nif.h"
#include "dpiData_nif.h"
#include "dpiQueryInfo_nif.h"

ErlNifResourceType *dpiStmt_type;

void dpiStmt_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(stmt_execute)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    uint32_t numCols = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    ERL_NIF_TERM head, tail;

    unsigned len;
    if (!enif_get_list_length(env, argv[1], &len))
        BADARG_EXCEPTION(1, "list of atoms");
    if (len > 0)
        enif_get_list_cell(env, argv[1], &head, &tail);

    dpiExecMode m = 0, mode = 0;
    if (len > 0)
        do
        {
            if (!enif_is_atom(env, head))
                RAISE_STR_EXCEPTION("mode must be a list of atoms");
            DPI_EXEC_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_execute(stmtRes->stmt, mode, &numCols));

    RETURNED_TRACE;
    return enif_make_uint(env, numCols);
}

DPI_NIF_FUN(stmt_executeMany)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes;
    uint32_t numIters;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    ERL_NIF_TERM head, tail;

    unsigned len;
    if (!enif_get_list_length(env, argv[1], &len))
        BADARG_EXCEPTION(1, "list of atoms");
    if (len > 0)
        enif_get_list_cell(env, argv[1], &head, &tail);

    if (!enif_get_uint(env, argv[2], &numIters))
        BADARG_EXCEPTION(2, "uint32 numIters");

    dpiExecMode m = 0, mode = 0;
    if (len > 0)
        do
        {
            if (!enif_is_atom(env, head))
                RAISE_STR_EXCEPTION("mode must be a list of atoms");
            DPI_EXEC_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_executeMany(stmtRes->stmt, mode, numIters));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_fetch)
{
    CHECK_ARGCOUNT(1);

    dpiStmt_res *stmtRes;
    int found = 0;
    uint32_t bufferRowIndex;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
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
    RETURNED_TRACE;
    return map;
}

DPI_NIF_FUN(stmt_getQueryValue)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0;
    dpiNativeTypeNum nativeTypeNum = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");

    dpiDataPtr_res *data;
    ALLOC_RESOURCE(data, dpiDataPtr);

    data->next = NULL;
    data->stmtRes = NULL;
    data->isQueryValue = 1;
    data->context = stmtRes->context;

    RAISE_EXCEPTION_ON_DPI_ERROR_RESOURCE(
        stmtRes->context,
        dpiStmt_getQueryValue(
            stmtRes->stmt, pos, &nativeTypeNum, &(data->dpiDataPtr)),
        data, dpiDataPtr);

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

    // #{ nativeTypeNum => atom, data => term  }
    RETURNED_TRACE;
    return map;
}

DPI_NIF_FUN(stmt_getQueryInfo)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");

    dpiQueryInfo queryInfo;
    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_getQueryInfo(stmtRes->stmt, pos, &queryInfo));

    dpiDataTypeInfo dti = queryInfo.typeInfo;
    ERL_NIF_TERM typeInfo = enif_make_new_map(env);

    // constructuing a map of
    // https://oracle.github.io/odpi/doc/structs/dpiDataTypeInfo.html
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
                      enif_make_string_len(env, queryInfo.name,
                                           queryInfo.nameLength,
                                           ERL_NIF_LATIN1),
                      &resultMap);
    enif_make_map_put(env, resultMap, enif_make_atom(env, "nullOk"),
                      enif_make_atom(
                          env, queryInfo.nullOk ? "true" : "false"),
                      &resultMap);

    /* #{name => "A", nullOk => atom,
         typeInfo => #{clientSizeInBytes => integer, dbSizeInBytes => integer,
                       defaultNativeTypeNum => atom, fsPrecision => integer,
                       objectType => atom, ociTypeCode => integer,
                       oracleTypeNum => atom , precision => integer,
                       scale => integer, sizeInChars => integer}
        } */
    RETURNED_TRACE;
    return resultMap;
}

DPI_NIF_FUN(stmt_getNumQueryColumns)
{
    CHECK_ARGCOUNT(1);

    dpiStmt_res *stmtRes;
    uint32_t numQueryColumns;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_getNumQueryColumns(stmtRes->stmt, &numQueryColumns));

    RETURNED_TRACE;
    return enif_make_uint(env, numQueryColumns);
}

DPI_NIF_FUN(stmt_bindValueByPos)
{
    CHECK_ARGCOUNT(4);

    dpiStmt_res *stmtRes;
    dpiData_res *dataRes;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");
    if (!enif_get_resource(env, argv[3], dpiData_type, (void **)&dataRes))
        BADARG_EXCEPTION(3, "resource data");

    dpiNativeTypeNum bindType = DPI_NATIVE_TYPE_INT64;
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[2], bindType);

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindValueByPos(stmtRes->stmt, pos, bindType,
                               &dataRes->dpiData));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_bindValueByName)
{
    CHECK_ARGCOUNT(4);

    dpiStmt_res *stmtRes;
    dpiData_res *dataRes;
    ErlNifBinary binary;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_inspect_binary(env, argv[1], &binary))
        BADARG_EXCEPTION(1, "string/list name");
    if (!enif_get_resource(env, argv[3], dpiData_type, (void **)&dataRes))
        BADARG_EXCEPTION(3, "resource data");

    dpiNativeTypeNum bindType = DPI_NATIVE_TYPE_INT64;
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[2], bindType);

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindValueByName(
            stmtRes->stmt, (const char *)binary.data, binary.size, bindType,
            &dataRes->dpiData));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_bindByPos)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes = NULL;
    dpiVar_res *varRes = NULL;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");
    if (!enif_get_resource(env, argv[2], dpiVar_type, (void **)&varRes))
        BADARG_EXCEPTION(3, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindByPos(stmtRes->stmt, pos, varRes->var));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_bindByName)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes = NULL;
    dpiVar_res *varRes = NULL;
    ErlNifBinary binary;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_inspect_binary(env, argv[1], &binary))
        BADARG_EXCEPTION(1, "string/list name");
    if (!enif_get_resource(env, argv[2], dpiVar_type, (void **)&varRes))
        BADARG_EXCEPTION(3, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_bindByName(
            stmtRes->stmt, (const char *)binary.data, binary.size,
            varRes->var));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_close)
{
    CHECK_ARGCOUNT(2);

    dpiStmt_res *stmtRes;
    ErlNifBinary tag;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_inspect_binary(env, argv[1], &tag))
        BADARG_EXCEPTION(1, "string tag");

    RAISE_EXCEPTION_ON_DPI_ERROR_RESOURCE(
        stmtRes->context,
        dpiStmt_close(stmtRes->stmt, (const char *)tag.data, tag.size),
        stmtRes, dpiStmt);

    RELEASE_RESOURCE(stmtRes, dpiStmt);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_close_n)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes;
    ErlNifBinary tag, resName;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_inspect_binary(env, argv[1], &tag))
        BADARG_EXCEPTION(1, "string tag");
        if (!enif_inspect_binary(env, argv[2], &resName))
        BADARG_EXCEPTION(2, "res name");

    RAISE_EXCEPTION_ON_DPI_ERROR_RESOURCE(
        stmtRes->context,
        dpiStmt_close(stmtRes->stmt, (const char *)tag.data, tag.size),
        stmtRes, dpiStmt);

    RELEASE_RESOURCE_N(stmtRes, dpiStmt, resName.data, resName.size);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_getInfo)
{
    CHECK_ARGCOUNT(1);

    dpiStmt_res *stmtRes = NULL;
    dpiStmtInfo info;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    RAISE_EXCEPTION_ON_DPI_ERROR_RESOURCE(
        stmtRes->context, dpiStmt_getInfo(stmtRes->stmt, &info),
        stmtRes, dpiStmt);

    ERL_NIF_TERM map = enif_make_new_map(env);

    enif_make_map_put(
        env, map, enif_make_atom(env, "isDDL"),
        info.isDDL ? ATOM_TRUE : ATOM_FALSE,
        &map);
    enif_make_map_put(
        env, map, enif_make_atom(env, "isDML"),
        info.isDML ? ATOM_TRUE : ATOM_FALSE,
        &map);
    enif_make_map_put(
        env, map, enif_make_atom(env, "isPLSQL"),
        info.isPLSQL ? ATOM_TRUE : ATOM_FALSE,
        &map);
    enif_make_map_put(
        env, map, enif_make_atom(env, "isQuery"),
        info.isQuery ? ATOM_TRUE : ATOM_FALSE,
        &map);
    enif_make_map_put(
        env, map, enif_make_atom(env, "isReturning"),
        info.isReturning ? ATOM_TRUE : ATOM_FALSE,
        &map);

    ERL_NIF_TERM type;

    switch (info.statementType)
    {
    case DPI_STMT_TYPE_UNKNOWN:
        type = enif_make_atom(env, "DPI_STMT_TYPE_UNKNOWN");
        break;
    case DPI_STMT_TYPE_SELECT:
        type = enif_make_atom(env, "DPI_STMT_TYPE_SELECT");
        break;
    case DPI_STMT_TYPE_UPDATE:
        type = enif_make_atom(env, "DPI_STMT_TYPE_UPDATE");
        break;
    case DPI_STMT_TYPE_DELETE:
        type = enif_make_atom(env, "DPI_STMT_TYPE_DELETE");
        break;
    case DPI_STMT_TYPE_INSERT:
        type = enif_make_atom(env, "DPI_STMT_TYPE_INSERT");
        break;
    case DPI_STMT_TYPE_CREATE:
        type = enif_make_atom(env, "DPI_STMT_TYPE_CREATE");
        break;
    case DPI_STMT_TYPE_DROP:
        type = enif_make_atom(env, "DPI_STMT_TYPE_DROP");
        break;
    case DPI_STMT_TYPE_ALTER:
        type = enif_make_atom(env, "DPI_STMT_TYPE_ALTER");
        break;
    case DPI_STMT_TYPE_BEGIN:
        type = enif_make_atom(env, "DPI_STMT_TYPE_BEGIN");
        break;
    case DPI_STMT_TYPE_DECLARE:
        type = enif_make_atom(env, "DPI_STMT_TYPE_DECLARE");
        break;
    case DPI_STMT_TYPE_CALL:
        type = enif_make_atom(env, "DPI_STMT_TYPE_CALL");
        break;
    case DPI_STMT_TYPE_MERGE:
        type = enif_make_atom(env, "DPI_STMT_TYPE_MERGE");
        break;
    case DPI_STMT_TYPE_EXPLAIN_PLAN:
        type = enif_make_atom(env, "DPI_STMT_TYPE_EXPLAIN_PLAN");
        break;
    case DPI_STMT_TYPE_COMMIT:
        type = enif_make_atom(env, "DPI_STMT_TYPE_COMMIT");
        break;
    case DPI_STMT_TYPE_ROLLBACK:
        type = enif_make_atom(env, "DPI_STMT_TYPE_ROLLBACK");
        break;
    }
    enif_make_map_put(
        env, map, enif_make_atom(env, "statementType"), type, &map);

    // #{ isDDL => atom, isDML => atom, isPLSQL => atom, isQuery => atom,
    //    isReturning => atom, statementType => atom }
    RETURNED_TRACE;
    return map;
}

DPI_NIF_FUN(stmt_define)
{
    CHECK_ARGCOUNT(3);

    dpiStmt_res *stmtRes = NULL;
    dpiVar_res *varRes = NULL;
    uint32_t pos = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");
    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");
    if (!enif_get_resource(env, argv[2], dpiVar_type, (void **)&varRes))
        BADARG_EXCEPTION(2, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_define(stmtRes->stmt, pos, varRes->var));

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(stmt_defineValue)
{
    CHECK_ARGCOUNT(7);

    dpiStmt_res *stmtRes;
    uint32_t pos = 0, size = 0;
    dpiOracleTypeNum oraType = DPI_ORACLE_TYPE_VARCHAR;
    dpiNativeTypeNum nativeType = DPI_NATIVE_TYPE_INT64;
    int sizeIsBytes = 0;

    if (!enif_get_resource(env, argv[0], dpiStmt_type, (void **)&stmtRes))
        BADARG_EXCEPTION(0, "resource statement");

    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");
    DPI_ORACLE_TYPE_NUM_FROM_ATOM(argv[2], oraType);
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[3], nativeType);
    if (!enif_get_uint(env, argv[4], &size))
        BADARG_EXCEPTION(4, "uint size");

    if (enif_compare(argv[5], ATOM_TRUE) == 0)
        sizeIsBytes = 1;
    else if (enif_compare(argv[5], ATOM_FALSE) == 0)
        sizeIsBytes = 0;
    else
        BADARG_EXCEPTION(5, "bool/atom sizeIsBytes");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        stmtRes->context,
        dpiStmt_defineValue(
            stmtRes->stmt, pos, oraType, nativeType, size, sizeIsBytes,
            NULL // TODO: support dpiObjectType
            ));

    RETURNED_TRACE;
    return ATOM_OK;
}
