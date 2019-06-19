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
    void *next;
    void *stmtRes;
    unsigned char isQueryValue;
} dpiDataPtr_res;

extern ErlNifResourceType *dpiData_type;
extern ErlNifResourceType *dpiDataPtr_type;

extern void dpiData_res_dtor(ErlNifEnv *env, void *resource);
extern void dpiDataPtr_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(data_getBytes);
extern DPI_NIF_FUN(data_getInt64);
extern DPI_NIF_FUN(data_setBytes);
extern DPI_NIF_FUN(data_setInt64);
extern DPI_NIF_FUN(data_setDouble);
extern DPI_NIF_FUN(data_setIntervalDS);
extern DPI_NIF_FUN(data_setIntervalYM);
extern DPI_NIF_FUN(data_setTimestamp);
extern DPI_NIF_FUN(data_ctor);
extern DPI_NIF_FUN(data_get);
extern DPI_NIF_FUN(data_setIsNull);
extern DPI_NIF_FUN(data_release);

#define DPIDATA_NIFS                       \
        DEF_NIF(data_getBytes, 1),      \
        DEF_NIF(data_getInt64, 1),      \
        DEF_NIF(data_setBytes, 2),      \
        DEF_NIF(data_setInt64, 2),      \
        DEF_NIF(data_setDouble, 2),      \
        DEF_NIF(data_setIntervalDS, 6), \
        DEF_NIF(data_setIntervalYM, 3), \
        DEF_NIF(data_setTimestamp, 10), \
        DEF_NIF(data_ctor, 0),          \
        DEF_NIF(data_get, 1),           \
        DEF_NIF(data_setIsNull, 2),     \
        DEF_NIF(data_release, 1)

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
        return RAISE_STR_EXCEPTION("dpiNativeType value not supported"); \
    }

#endif // _data_NIF_H_
