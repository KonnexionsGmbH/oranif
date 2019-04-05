#ifndef _DPIDATA_NIF_H_
#define _DPIDATA_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiData dpiData;
    ErlNifEnv *env;
} dpiData_res;

typedef struct
{
    dpiData *dpiDataPtr;
    dpiNativeTypeNum type;
} dpiDataPtr_res;

extern ErlNifResourceType *dpiData_type;
extern ErlNifResourceType *dpiDataPtr_type;

extern void dpiData_res_dtor(ErlNifEnv *env, void *resource);
extern void dpiDataPtr_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(dpiData_getBool);
extern DPI_NIF_FUN(dpiData_getBytes);
extern DPI_NIF_FUN(dpiData_getDouble);
extern DPI_NIF_FUN(dpiData_getFloat);
extern DPI_NIF_FUN(dpiData_getInt64);
extern DPI_NIF_FUN(dpiData_getIntervalDS);
extern DPI_NIF_FUN(dpiData_getIntervalYM);
extern DPI_NIF_FUN(dpiData_getLOB);
extern DPI_NIF_FUN(dpiData_getObject);
extern DPI_NIF_FUN(dpiData_getStmt);
extern DPI_NIF_FUN(dpiData_getTimestamp);
extern DPI_NIF_FUN(dpiData_getUint64);
extern DPI_NIF_FUN(dpiData_setBool);
extern DPI_NIF_FUN(dpiData_setBytes);
extern DPI_NIF_FUN(dpiData_setDouble);
extern DPI_NIF_FUN(dpiData_setFloat);
extern DPI_NIF_FUN(dpiData_setInt64);
extern DPI_NIF_FUN(dpiData_setIntervalDS);
extern DPI_NIF_FUN(dpiData_setIntervalYM);
extern DPI_NIF_FUN(dpiData_setLOB);
extern DPI_NIF_FUN(dpiData_setObject);
extern DPI_NIF_FUN(dpiData_setStmt);
extern DPI_NIF_FUN(dpiData_setTimestamp);
extern DPI_NIF_FUN(dpiData_setUint64);
extern DPI_NIF_FUN(dpiData_ctor);
extern DPI_NIF_FUN(dpiData_get);
extern DPI_NIF_FUN(dpiData_setIsNull);
extern DPI_NIF_FUN(dpiData_release);

#define DPIDATA_NIFS                       \
    DEF_NIF(dpiData_getBool, 1),           \
        DEF_NIF(dpiData_getBytes, 1),      \
        DEF_NIF(dpiData_getDouble, 1),     \
        DEF_NIF(dpiData_getFloat, 1),      \
        DEF_NIF(dpiData_getInt64, 1),      \
        DEF_NIF(dpiData_getIntervalDS, 1), \
        DEF_NIF(dpiData_getIntervalYM, 1), \
        DEF_NIF(dpiData_getLOB, 1),        \
        DEF_NIF(dpiData_getObject, 1),     \
        DEF_NIF(dpiData_getStmt, 1),       \
        DEF_NIF(dpiData_getTimestamp, 1),  \
        DEF_NIF(dpiData_getUint64, 1),     \
        DEF_NIF(dpiData_setBool, 2),       \
        DEF_NIF(dpiData_setBytes, 2),      \
        DEF_NIF(dpiData_setDouble, 2),     \
        DEF_NIF(dpiData_setFloat, 2),      \
        DEF_NIF(dpiData_setInt64, 2),      \
        DEF_NIF(dpiData_setIntervalDS, 6), \
        DEF_NIF(dpiData_setIntervalYM, 3), \
        DEF_NIF(dpiData_setLOB, 2),        \
        DEF_NIF(dpiData_setObject, 2),     \
        DEF_NIF(dpiData_setStmt, 2),       \
        DEF_NIF(dpiData_setTimestamp, 10), \
        DEF_NIF(dpiData_ctor, 0),          \
        DEF_NIF(dpiData_get, 1),           \
        DEF_NIF(dpiData_setIsNull, 2),     \
        DEF_NIF(dpiData_release, 1)

#define DPI_NATIVE_TYPE_NUM_FROM_ATOM(_atom, _assign)      \
    A2M(DPI_NATIVE_TYPE_INT64, _atom, _assign);            \
    else A2M(DPI_NATIVE_TYPE_UINT64, _atom, _assign);      \
    else A2M(DPI_NATIVE_TYPE_FLOAT, _atom, _assign);       \
    else A2M(DPI_NATIVE_TYPE_DOUBLE, _atom, _assign);      \
    else A2M(DPI_NATIVE_TYPE_BYTES, _atom, _assign);       \
    else A2M(DPI_NATIVE_TYPE_TIMESTAMP, _atom, _assign);   \
    else A2M(DPI_NATIVE_TYPE_INTERVAL_DS, _atom, _assign); \
    else A2M(DPI_NATIVE_TYPE_INTERVAL_YM, _atom, _assign); \
    else A2M(DPI_NATIVE_TYPE_LOB, _atom, _assign);         \
    else A2M(DPI_NATIVE_TYPE_OBJECT, _atom, _assign);      \
    else A2M(DPI_NATIVE_TYPE_STMT, _atom, _assign);        \
    else A2M(DPI_NATIVE_TYPE_BOOLEAN, _atom, _assign);     \
    else A2M(DPI_NATIVE_TYPE_ROWID, _atom, _assign)

#define DPI_NATIVE_TYPE_NUM_TO_ATOM(_type, _assign)                  \
    switch (_type)                                                   \
    {                                                                \
        M2A(DPI_NATIVE_TYPE_INT64, _assign);                         \
        M2A(DPI_NATIVE_TYPE_UINT64, _assign);                        \
        M2A(DPI_NATIVE_TYPE_FLOAT, _assign);                         \
        M2A(DPI_NATIVE_TYPE_DOUBLE, _assign);                        \
        M2A(DPI_NATIVE_TYPE_BYTES, _assign);                         \
        M2A(DPI_NATIVE_TYPE_TIMESTAMP, _assign);                     \
        M2A(DPI_NATIVE_TYPE_INTERVAL_DS, _assign);                   \
        M2A(DPI_NATIVE_TYPE_INTERVAL_YM, _assign);                   \
        M2A(DPI_NATIVE_TYPE_LOB, _assign);                           \
        M2A(DPI_NATIVE_TYPE_OBJECT, _assign);                        \
        M2A(DPI_NATIVE_TYPE_STMT, _assign);                          \
        M2A(DPI_NATIVE_TYPE_BOOLEAN, _assign);                       \
        M2A(DPI_NATIVE_TYPE_ROWID, _assign);                         \
    default:                                                         \
        return RAISE_EXCEPTION("dpiNativeType value not supported"); \
    }

#endif // _DPIDATA_NIF_H_
