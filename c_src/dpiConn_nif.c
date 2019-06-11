#include "dpiConn_nif.h"
#include "dpiContext_nif.h"
#include "dpiStmt_nif.h"
#include "dpiVar_nif.h"
#include "dpiData_nif.h"
#include "dpiQueryInfo_nif.h"
#include "stdio.h"

ErlNifResourceType *dpiConn_type;

void dpiConn_res_dtor(ErlNifEnv *env, void *resource)
{
    TRACE;
    L("dpiConn destroyed\r\n");
}

DPI_NIF_FUN(conn_create)
{
    CHECK_ARGCOUNT(6);

    dpiContext_res *contextRes;
    ErlNifBinary userName, password, connectString;
    if (!enif_get_resource(env, argv[0], dpiContext_type, &contextRes))
        return BADARG_EXCEPTION(0, "resource context");
    if (!enif_inspect_binary(env, argv[1], &userName))
        return BADARG_EXCEPTION(1, "string/binary userName");
    if (!enif_inspect_binary(env, argv[2], &password))
        return BADARG_EXCEPTION(2, "string/binary password");
    if (!enif_inspect_binary(env, argv[3], &connectString))
        return BADARG_EXCEPTION(3, "string/binary connectString");
    dpiConn_res *connRes =
        enif_alloc_resource(dpiConn_type, sizeof(dpiConn_res));
    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiConn_create(
            contextRes->context, userName.data, userName.size,
            password.data, password.size, connectString.data,
            connectString.size,
            NULL, // TODO implement commonParams
            NULL, // TODO implement createParams
            &connRes->conn));

    ERL_NIF_TERM connResTerm = enif_make_resource(env, connRes);

    return connResTerm;
}

DPI_NIF_FUN(conn_prepareStmt)
{
    CHECK_ARGCOUNT(4);

    char atomBuf[32];
    dpiConn_res *connRes;
    ErlNifBinary sql, tag;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");
    if (!enif_get_atom(env, argv[1], atomBuf, 31, ERL_NIF_LATIN1))
        return BADARG_EXCEPTION(1, "bool/atom scrollable");
    if (!enif_inspect_binary(env, argv[2], &sql))
        return BADARG_EXCEPTION(2, "binary/string sql");
    if (!enif_inspect_binary(env, argv[3], &tag))
        return BADARG_EXCEPTION(3, "binary/string tag");

    dpiStmt_res *stmtRes =
        enif_alloc_resource(dpiStmt_type, sizeof(dpiStmt_res));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiConn_prepareStmt(
            connRes->conn, !strcmp(atomBuf, "true"), sql.data, sql.size,
            tag.size > 0 ? tag.data : NULL, tag.size, &stmtRes->stmt));

    ERL_NIF_TERM stmtResTerm = enif_make_resource(env, stmtRes);

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
    char sizeIsBytesBuf[32];
    int sizeIsBytes = 0, isArray = 0;
    dpiData *data;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");
    DPI_ORACLE_TYPE_NUM_FROM_ATOM(argv[1], oracleTypeNum);
    DPI_NATIVE_TYPE_NUM_FROM_ATOM(argv[2], nativeTypeNum);
    if (!enif_get_uint(env, argv[3], &maxArraySize))
        return BADARG_EXCEPTION(3, "uint size");
    if (!enif_get_uint(env, argv[4], &size))
        return BADARG_EXCEPTION(4, "uint size");

    if (enif_compare(argv[5], ATOM_TRUE) == 0)
        sizeIsBytes = 1;
    else if (enif_compare(argv[5], ATOM_FALSE) == 0)
        sizeIsBytes = 0;
    else
        return BADARG_EXCEPTION(5, "atom sizeIsBytes");

    if (enif_compare(argv[6], ATOM_TRUE) == 0)
        isArray = 1;
    else if (enif_compare(argv[6], ATOM_FALSE) == 0)
        isArray = 0;
    else
        return BADARG_EXCEPTION(6, "atom isArray");

    if (enif_compare(argv[7], ATOM_NULL))
        return BADARG_EXCEPTION(7, "atom objType");

    dpiVar_res *varRes =
        enif_alloc_resource(dpiVar_type, sizeof(dpiVar_res));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiConn_newVar(
            connRes->conn, oracleTypeNum, nativeTypeNum, maxArraySize, size,
            sizeIsBytes, isArray,
            NULL, &varRes->var, &data));

    ERL_NIF_TERM varResTerm = enif_make_resource(env, varRes);

    ERL_NIF_TERM dataList = enif_make_list(env, 0);

    dpiDataPtr_res *dataRes;
    varRes->head = NULL;
    for (int i = maxArraySize - 1; i >= 0; i--)
    {
        dataRes = enif_alloc_resource(dpiDataPtr_type, sizeof(dpiDataPtr_res));
        dataRes->next = NULL;
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

    return ret;
}

DPI_NIF_FUN(conn_commit)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(dpiConn_commit(connRes->conn));
    return ATOM_OK;
}

DPI_NIF_FUN(conn_rollback)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(dpiConn_rollback(connRes->conn));

    return ATOM_OK;
}

DPI_NIF_FUN(conn_ping)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(dpiConn_ping(connRes->conn));

    return ATOM_OK;
}

DPI_NIF_FUN(conn_release)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");

    RAISE_EXCEPTION_ON_DPI_ERROR(dpiConn_release(connRes->conn));
    enif_release_resource(connRes);
    return ATOM_OK;
}

DPI_NIF_FUN(conn_close)
{
    CHECK_ARGCOUNT(3);

    dpiConn_res *connRes;
    ErlNifBinary tag;
    ERL_NIF_TERM head, tail;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");
    if (!enif_is_list(env, argv[1]) &&
        !enif_get_list_cell(env, argv[1], &head, &tail))
        return BADARG_EXCEPTION(1, "atom list modes");
    if (!enif_inspect_binary(env, argv[2], &tag))
        return BADARG_EXCEPTION(2, "binary/string tag");

    dpiConnCloseMode m;
    dpiConnCloseMode mode = 0;
    unsigned int len;
    enif_get_list_length(env, argv[1], &len);
    if (len > 0)
        do
        {
            if (!enif_is_atom(env, head))
                return BADARG_EXCEPTION(1, "mode list value");
            DPI_CLOSE_MODE_FROM_ATOM(head, m);
            mode |= m;
        } while (enif_get_list_cell(env, tail, &head, &tail));

    RAISE_EXCEPTION_ON_DPI_ERROR(
        dpiConn_close(
            connRes->conn, mode,
            tag.size > 0 ? tag.data : NULL,
            tag.size));

    return ATOM_OK;
}

DPI_NIF_FUN(conn_getServerVersion)
{
    CHECK_ARGCOUNT(1);

    dpiConn_res *connRes = NULL;

    if (!enif_get_resource(env, argv[0], dpiConn_type, &connRes))
        return BADARG_EXCEPTION(0, "resource connection");

    dpiVersionInfo version;
    char *releaseString;
    int releaseStringLength;
    dpiConn_getServerVersion(connRes->conn, &releaseString,
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
    return map;
}
