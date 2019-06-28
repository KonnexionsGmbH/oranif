#include "dpiConn_nif.h"
#include "dpiContext_nif.h"
#include "dpiStmt_nif.h"
#include "dpiVar_nif.h"
#include "dpiData_nif.h"
#include "dpiQueryInfo_nif.h"
#include "stdio.h"

ERL_NIF_TERM ATOM_encoding = 0;
ERL_NIF_TERM ATOM_nencoding = 0;

ErlNifResourceType *dpiConn_type;

void dpiConn_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(conn_create)
{
    CHECK_ARGCOUNT(6);

    dpiContext_res *contextRes;
    ErlNifBinary userName, password, connectString;
    size_t commonParamsMapSize = 0;
    if (!enif_get_resource(env, argv[0], dpiContext_type, (void **)&contextRes))
        BADARG_EXCEPTION(0, "resource context");
    if (!enif_inspect_binary(env, argv[1], &userName))
        BADARG_EXCEPTION(1, "string/binary userName");
    if (!enif_inspect_binary(env, argv[2], &password))
        BADARG_EXCEPTION(2, "string/binary password");
    if (!enif_inspect_binary(env, argv[3], &connectString))
        BADARG_EXCEPTION(3, "string/binary connectString");
    if (!enif_get_map_size(env, argv[4], &commonParamsMapSize))
        BADARG_EXCEPTION(4, "map commonParams");

    dpiCommonCreateParams commonParams;
    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context,
        dpiContext_initCommonCreateParams(contextRes->context, &commonParams),
        NULL);

    if (commonParamsMapSize > 0)
    {
        // lazy create
        if (!(ATOM_encoding | ATOM_nencoding))
        {
            ATOM_encoding = enif_make_atom(env, "encoding");
            ATOM_nencoding = enif_make_atom(env, "nencoding");
        }

        ERL_NIF_TERM mapval;
        char encodeStr[128];
        if (enif_get_map_value(env, argv[4], ATOM_encoding, &mapval))
        {
            if (!enif_get_string(
                    env, mapval, encodeStr, sizeof(encodeStr), ERL_NIF_LATIN1))
                BADARG_EXCEPTION(4, "string\0 commonParams.encoding");
            commonParams.encoding = encodeStr;
        }

        char nencodeStr[128];
        if (enif_get_map_value(env, argv[4], ATOM_nencoding, &mapval))
        {
            if (!enif_get_string(
                    env, mapval, nencodeStr, sizeof(nencodeStr),
                    ERL_NIF_LATIN1))
                BADARG_EXCEPTION(4, "string\0 commonParams.nencoding");
            commonParams.nencoding = nencodeStr;
        }
    }

    dpiConn_res *connRes =
        enif_alloc_resource(dpiConn_type, sizeof(dpiConn_res));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        contextRes->context,
        dpiConn_create(
            contextRes->context, (const char *)userName.data, userName.size,
            (const char *)password.data, password.size,
            (const char *)connectString.data, connectString.size,
            &commonParams,
            NULL, // TODO implement connCreateParams
            &connRes->conn),
        connRes);

    // Save context into connection for access from dpiError
    connRes->context = contextRes->context;

    ERL_NIF_TERM connResTerm = enif_make_resource(env, connRes);

    RETURNED_TRACE;
    return connResTerm;
}

DPI_NIF_FUN(conn_prepareStmt)
{
    CHECK_ARGCOUNT(4);

    int scrollable = 0;
    dpiConn_res *connRes;
    ErlNifBinary sql, tag;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    if (enif_compare(argv[1], ATOM_TRUE) == 0)
        scrollable = 1;
    else if (enif_compare(argv[1], ATOM_FALSE) == 0)
        scrollable = 0;
    else
        BADARG_EXCEPTION(1, "bool/atom scrollable");

    if (!enif_inspect_binary(env, argv[2], &sql))
        BADARG_EXCEPTION(2, "binary/string sql");
    if (!enif_inspect_binary(env, argv[3], &tag))
        BADARG_EXCEPTION(3, "binary/string tag");

    dpiStmt_res *stmtRes =
        enif_alloc_resource(dpiStmt_type, sizeof(dpiStmt_res));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_prepareStmt(
            connRes->conn, scrollable, (const char *)sql.data, sql.size,
            tag.size > 0 ? (const char *)tag.data : NULL, tag.size,
            &stmtRes->stmt),
        stmtRes);

    stmtRes->context = connRes->context;

    ERL_NIF_TERM stmtResTerm = enif_make_resource(env, stmtRes);

    RETURNED_TRACE;
    return stmtResTerm;
}

DPI_NIF_FUN(conn_newVar)
{
    CHECK_ARGCOUNT(8);

    dpiConn_res *connRes = NULL;
    dpiOracleTypeNum oracleTypeNum = 0;
    dpiNativeTypeNum nativeTypeNum = 0;
    uint32_t maxArraySize = 0;
    uint32_t size = 0;
    int sizeIsBytes = 0, isArray = 0;
    dpiData *data;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    DPI_ORACLE_TYPE_NUM_FROM_ATOM(argv[1], oracleTypeNum);
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[2], nativeTypeNum);
    if (!enif_get_uint(env, argv[3], &maxArraySize))
        BADARG_EXCEPTION(3, "uint size");
    if (!enif_get_uint(env, argv[4], &size))
        BADARG_EXCEPTION(4, "uint size");

    if (enif_compare(argv[5], ATOM_TRUE) == 0)
        sizeIsBytes = 1;
    else if (enif_compare(argv[5], ATOM_FALSE) == 0)
        sizeIsBytes = 0;
    else
        BADARG_EXCEPTION(5, "atom sizeIsBytes");

    if (enif_compare(argv[6], ATOM_TRUE) == 0)
        isArray = 1;
    else if (enif_compare(argv[6], ATOM_FALSE) == 0)
        isArray = 0;
    else
        BADARG_EXCEPTION(6, "atom isArray");

    if (enif_compare(argv[7], ATOM_NULL))
        BADARG_EXCEPTION(7, "atom objType");

    dpiVar_res *varRes =
        enif_alloc_resource(dpiVar_type, sizeof(dpiVar_res));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_newVar(
            connRes->conn, oracleTypeNum, nativeTypeNum, maxArraySize, size,
            sizeIsBytes, isArray,
            NULL, &varRes->var, &data),
        varRes);

    varRes->context = connRes->context;

    ERL_NIF_TERM varResTerm = enif_make_resource(env, varRes);

    ERL_NIF_TERM dataList = enif_make_list(env, 0);

    dpiDataPtr_res *dataRes;
    varRes->head = NULL;
    for (int i = maxArraySize - 1; i >= 0; i--)
    {
        dataRes = enif_alloc_resource(dpiDataPtr_type, sizeof(dpiDataPtr_res));
        dataRes->stmtRes = NULL;
        dataRes->next = NULL;
        dataRes->isQueryValue = 0;
        if (varRes->head == NULL)
        {
            varRes->head = dataRes;
        }
        else
        {
            dataRes->next = varRes->head;
            varRes->head = dataRes;
        }
        dataRes->dpiDataPtr = data + i;
        dataRes->type = nativeTypeNum;
        ERL_NIF_TERM dataResTerm = enif_make_resource(env, dataRes);
        dataList = enif_make_list_cell(env, dataResTerm, dataList);
    }
    ERL_NIF_TERM ret = enif_make_new_map(env);
    ret = enif_make_new_map(env);
    enif_make_map_put(env, ret, enif_make_atom(env, "var"), varResTerm, &ret);
    enif_make_map_put(env, ret, enif_make_atom(env, "data"), dataList, &ret);

    RETURNED_TRACE;
    return ret;
}

DPI_NIF_FUN(conn_commit)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_commit(connRes->conn),
        NULL);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(conn_rollback)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_rollback(connRes->conn),
        NULL);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(conn_ping)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_ping(connRes->conn),
        NULL);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(conn_close)
{
    CHECK_ARGCOUNT(3);

    dpiConn_res *connRes;
    ErlNifBinary tag;
    ERL_NIF_TERM head, tail;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    if (!enif_is_list(env, argv[1]) &&
        !enif_get_list_cell(env, argv[1], &head, &tail))
        BADARG_EXCEPTION(1, "atom list modes");
    if (!enif_inspect_binary(env, argv[2], &tag))
        BADARG_EXCEPTION(2, "binary/string tag");

    dpiConnCloseMode m = 0, mode = 0;
    unsigned int len;
    enif_get_list_length(env, argv[1], &len);
    if (len > 0)
        do
        {
            if (!enif_is_atom(env, head))
                BADARG_EXCEPTION(1, "mode list value");
            DPI_CLOSE_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_close(
            connRes->conn, mode,
            tag.size > 0 ? (const char *)tag.data : NULL,
            tag.size),
        NULL);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(conn_getServerVersion)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes = NULL;

    if (!enif_get_resource(env, argv[0], dpiConn_type, (void **)&connRes))
        BADARG_EXCEPTION(0, "resource connection");

    dpiVersionInfo version;
    char *releaseString;
    uint32_t releaseStringLength;
    dpiConn_getServerVersion(connRes->conn, (const char **)&releaseString,
                             &releaseStringLength, &version);
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

    enif_make_map_put(
        env, map, enif_make_atom(env, "releaseString"),
        enif_make_string_len(env, releaseString, releaseStringLength,
                             ERL_NIF_LATIN1),
        &map);

    /* #{versionNum => integer, releaseNum => integer, updateNum => integer,
         portReleaseNum => integer, portUpdateNum => integer,
         fullVersionNum => integer} */
    RETURNED_TRACE;
    return map;
}

DPI_NIF_FUN(conn_setClientIdentifier)
{
    CHECK_ARGCOUNT(2);

    dpiConn_res *connRes = NULL;
    ErlNifBinary value;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        BADARG_EXCEPTION(0, "resource connection");
    if (!enif_inspect_binary(env, argv[1], &value))
        BADARG_EXCEPTION(1, "string/binary value");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        connRes->context,
        dpiConn_setClientIdentifier(
            connRes->conn, value.data, value.size),
        NULL);

    return ATOM_OK;
}