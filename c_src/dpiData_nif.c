#include "dpiData_nif.h"
#include "dpiStmt_nif.h"

#ifndef __WIN32__
#include <string.h>
#endif

ErlNifResourceType *dpiData_type;
ErlNifResourceType *dpiDataPtr_type;

void dpiData_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;

    dpiData_res *data = (dpiData_res *)resource;
    if (data->env)
    {
        TRACE;
        enif_free_env(data->env);
        data->env = NULL;
    }

    RETURNED_TRACE;
}

void dpiDataPtr_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(data_ctor)
{
    CHECK_ARGCOUNT(0);

    dpiData_res *data;
    ALLOC_RESOURCE(data, dpiData);

    data->dpiData.isNull = 1; // starts out being null

    // erlang process independent environment to persist data between NIF calls
    data->env = enif_alloc_env();

    ERL_NIF_TERM dpiDataRes = enif_make_resource(env, data);

    RETURNED_TRACE;
    return dpiDataRes;
}

DPI_NIF_FUN(data_setTimestamp)
{
    CHECK_ARGCOUNT(10);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;
    int year, month, day, hour, minute, second, fsecond, tzHourOffset,
        tzMinuteOffset;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (!enif_get_int(env, argv[1], &year))
        BADARG_EXCEPTION(1, "int year");
    if (!enif_get_int(env, argv[2], &month))
        BADARG_EXCEPTION(2, "int month");
    if (!enif_get_int(env, argv[3], &day))
        BADARG_EXCEPTION(3, "int day");
    if (!enif_get_int(env, argv[4], &hour))
        BADARG_EXCEPTION(4, "int hour");
    if (!enif_get_int(env, argv[5], &minute))
        BADARG_EXCEPTION(5, "int minute");
    if (!enif_get_int(env, argv[6], &second))
        BADARG_EXCEPTION(6, "int second");
    if (!enif_get_int(env, argv[7], &fsecond))
        BADARG_EXCEPTION(7, "int fsecond");
    if (!enif_get_int(env, argv[8], &tzHourOffset))
        BADARG_EXCEPTION(8, "int tzHourOffset");
    if (!enif_get_int(env, argv[9], &tzMinuteOffset))
        BADARG_EXCEPTION(9, "int tzMinuteOffset");

    dpiData_setTimestamp(
        data, year, month, day, hour, minute,
        second, fsecond, tzHourOffset, tzMinuteOffset);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(data_setIntervalDS)
{
    CHECK_ARGCOUNT(6);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;

    int days, hours, minutes, seconds, fseconds;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (!enif_get_int(env, argv[1], &days))
        BADARG_EXCEPTION(1, "int days");
    if (!enif_get_int(env, argv[2], &hours))
        BADARG_EXCEPTION(2, "int hours");
    if (!enif_get_int(env, argv[3], &minutes))
        BADARG_EXCEPTION(3, "int minutes");
    if (!enif_get_int(env, argv[4], &seconds))
        BADARG_EXCEPTION(4, "int seconds");
    if (!enif_get_int(env, argv[5], &fseconds))
        BADARG_EXCEPTION(5, "int fseconds");

    dpiData_setIntervalDS(
        data, days, hours, minutes, seconds, fseconds);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(data_setIntervalYM)
{
    CHECK_ARGCOUNT(3);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;

    int years, months;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (!enif_get_int(env, argv[1], &years))
        BADARG_EXCEPTION(1, "int years");
    if (!enif_get_int(env, argv[2], &months))
        BADARG_EXCEPTION(2, "int months");

    dpiData_setIntervalYM(data, years, months);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(data_setInt64)
{
    CHECK_ARGCOUNT(2);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;

    int64_t amount;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (!enif_get_int64(env, argv[1], &amount))
        BADARG_EXCEPTION(1, "int amount");

    dpiData_setInt64(data, amount);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(data_setBytes)
{
    CHECK_ARGCOUNT(2);

    dpiData_res *dataRes = NULL;
    dpiData *data = NULL;

    if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    // binary is copied to process independent env for NIF calls persistance
    ERL_NIF_TERM binData = enif_make_copy(dataRes->env, argv[1]);
    ErlNifBinary ptr;
    if (!enif_inspect_binary(dataRes->env, binData, &ptr))
        BADARG_EXCEPTION(1, "binary data");

    dpiData_setBytes(data, (char *)ptr.data, ptr.size);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(data_setIsNull)
{
    CHECK_ARGCOUNT(2);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (enif_compare(argv[1], ATOM_TRUE) == 0)
        data->isNull = 1;
    else if (enif_compare(argv[1], ATOM_FALSE) == 0)
        data->isNull = 0;
    else
        BADARG_EXCEPTION(1, "bool/atom isNull");

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(data_get)
{
    CHECK_ARGCOUNT(1);

    dpiDataPtr_res *dataRes;

    if (!enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataRes))
        BADARG_EXCEPTION(0, "resource data");

    ERL_NIF_TERM dataRet;
    dpiData *data = dataRes->dpiDataPtr;

    // if NULL, no further processing of data is necessary
    if (data->isNull)
    {
        RETURNED_TRACE;
        return ATOM_NULL;
    }

    switch (dataRes->type)
    {
    case DPI_NATIVE_TYPE_INT64:
        dataRet = enif_make_int64(env, data->value.asInt64);
        break;
    case DPI_NATIVE_TYPE_UINT64:
        dataRet = enif_make_uint64(env, data->value.asUint64);
        break;
    case DPI_NATIVE_TYPE_FLOAT:
        dataRet = enif_make_double(env, data->value.asFloat);
        break;
    case DPI_NATIVE_TYPE_DOUBLE:
        dataRet = enif_make_double(env, data->value.asDouble);
        break;
    case DPI_NATIVE_TYPE_BYTES:
    {
        ErlNifBinary bin;
        enif_alloc_binary(data->value.asBytes.length, &bin);
        memcpy(bin.data, data->value.asBytes.ptr, data->value.asBytes.length);
        dataRet = enif_make_binary(env, &bin);
    }
    break;
    case DPI_NATIVE_TYPE_TIMESTAMP:
        dataRet = enif_make_new_map(env);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "fsecond"),
            enif_make_uint(env, data->value.asTimestamp.fsecond),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "second"),
            enif_make_uint(env, data->value.asTimestamp.second),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "minute"),
            enif_make_uint(env, data->value.asTimestamp.minute),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "hour"),
            enif_make_uint(env, data->value.asTimestamp.hour),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "day"),
            enif_make_uint(env, data->value.asTimestamp.day),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "month"),
            enif_make_uint(env, data->value.asTimestamp.month),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "year"),
            enif_make_int(env, data->value.asTimestamp.year),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "tzMinuteOffset"),
            enif_make_int(env, data->value.asTimestamp.tzMinuteOffset),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "tzHourOffset"),
            enif_make_int(env, data->value.asTimestamp.tzHourOffset), &dataRet);
        break;
    case DPI_NATIVE_TYPE_INTERVAL_DS:
        dataRet = enif_make_new_map(env);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "fseconds"),
            enif_make_uint(env, data->value.asIntervalDS.fseconds), &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "seconds"),
            enif_make_uint(env, data->value.asIntervalDS.seconds), &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "minutes"),
            enif_make_uint(env, data->value.asIntervalDS.minutes), &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "hours"),
            enif_make_uint(env, data->value.asIntervalDS.hours),
            &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "days"),
            enif_make_uint(env, data->value.asIntervalDS.days),
            &dataRet);
        break;
    case DPI_NATIVE_TYPE_INTERVAL_YM:
        dataRet = enif_make_new_map(env);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "months"),
            enif_make_uint(env, data->value.asIntervalYM.months), &dataRet);
        enif_make_map_put(
            env, dataRet, enif_make_atom(env, "years"),
            enif_make_uint(env, data->value.asIntervalYM.years),
            &dataRet);
        break;
    case DPI_NATIVE_TYPE_STMT:
    {
        dpiStmt_res *stmtRes = (dpiStmt_res *)dataRes->stmtRes;
        if (!stmtRes)
        {
            // first time
            ALLOC_RESOURCE(stmtRes, dpiStmt);
            dataRes->stmtRes = stmtRes;
        }
        stmtRes->stmt = data->value.asStmt;
        dataRet = enif_make_resource(env, stmtRes);
    }
    break;
    case DPI_NATIVE_TYPE_ROWID:
    {
        const char *string;
        uint32_t stringlen;
        RAISE_EXCEPTION_ON_DPI_ERROR(
            dataRes->context,
            dpiRowid_getStringValue(data->value.asRowid, &string, &stringlen));
        ErlNifBinary bin;
        enif_alloc_binary(stringlen, &bin);
        memcpy(bin.data, string, stringlen);
        dataRet = enif_make_binary(env, &bin);
    }
    break;
    default:
        RAISE_STR_EXCEPTION("Unsupported nativeTypeNum");
    }

    RETURNED_TRACE;
    return dataRet;
}

DPI_NIF_FUN(data_getInt64) // TODO: unit test
{
    CHECK_ARGCOUNT(1);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (data->isNull)
    {
        RETURNED_TRACE;
        return ATOM_NULL;
    }
    int64_t result = dpiData_getInt64(data);

    RETURNED_TRACE;
    return enif_make_int64(env, result);
}

DPI_NIF_FUN(data_getBytes) // TODO: unit test
{
    CHECK_ARGCOUNT(1);

    dpiData_res *dataRes;
    dpiDataPtr_res *dataPtr;
    dpiData *data;

    if (enif_get_resource(env, argv[0], dpiDataPtr_type, (void **)&dataPtr))
        data = dataPtr->dpiDataPtr;
    else if (enif_get_resource(env, argv[0], dpiData_type, (void **)&dataRes))
        data = &dataRes->dpiData;
    else
        BADARG_EXCEPTION(0, "resource data/ptr");

    if (data->isNull)
    {
        RETURNED_TRACE;
        return ATOM_NULL;
    }
    dpiBytes *bytes = dpiData_getBytes(data);
    ErlNifBinary bin;

    enif_alloc_binary(bytes->length, &bin);
    memcpy(bin.data, bytes->ptr, bytes->length);

    RETURNED_TRACE;
    return enif_make_binary(env, &bin);
}

DPI_NIF_FUN(data_release)
{
    CHECK_ARGCOUNT(1);

    union {
        dpiData_res *dataRes;
        dpiDataPtr_res *dataPtrRes;
    } res;

    if (enif_get_resource(env, argv[0], dpiData_type, (void **)&res.dataRes))
    {
        // nothing to set to NULL
        RELEASE_RESOURCE(res.dataRes, dpiData);
    }
    else if (enif_get_resource(
                 env, argv[0], dpiDataPtr_type, (void **)&res.dataPtrRes))
    {
        if (res.dataPtrRes->stmtRes)
        {
            RELEASE_RESOURCE(res.dataPtrRes->stmtRes, dpiStmt);
            res.dataPtrRes->stmtRes = NULL;
        }
        res.dataPtrRes->dpiDataPtr = NULL;
        if (res.dataPtrRes->isQueryValue == 1)
            RELEASE_RESOURCE(res.dataPtrRes, dpiDataPtr);
    }
    else
        BADARG_EXCEPTION(0, "resource data");

    RETURNED_TRACE;
    return ATOM_OK;
}
