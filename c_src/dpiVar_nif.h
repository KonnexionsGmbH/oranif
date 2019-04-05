#ifndef _DPIVAR_NIF_H_
#define _DPIVAR_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiVar *var;
} dpiVar_res;

extern ErlNifResourceType *dpiVar_type;

extern void dpiVar_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(dpiVar_addRef);
extern DPI_NIF_FUN(dpiVar_copyData);
extern DPI_NIF_FUN(dpiVar_getNumElementsInArray);
extern DPI_NIF_FUN(dpiVar_getReturnedData);
extern DPI_NIF_FUN(dpiVar_getSizeInBytes);
extern DPI_NIF_FUN(dpiVar_release);
extern DPI_NIF_FUN(dpiVar_setFromBytes);
extern DPI_NIF_FUN(dpiVar_setFromLob);
extern DPI_NIF_FUN(dpiVar_setFromObject);
extern DPI_NIF_FUN(dpiVar_setFromRowid);
extern DPI_NIF_FUN(dpiVar_setFromStmt);
extern DPI_NIF_FUN(dpiVar_setNumElementsInArray);

#define DPIVAR_NIFS                               \
    DEF_NIF(dpiVar_addRef, 1),                    \
        DEF_NIF(dpiVar_copyData, 4),              \
        DEF_NIF(dpiVar_getNumElementsInArray, 1), \
        DEF_NIF(dpiVar_getReturnedData, 2),       \
        DEF_NIF(dpiVar_getSizeInBytes, 1),        \
        DEF_NIF(dpiVar_release, 1),               \
        DEF_NIF(dpiVar_setFromBytes, 3),          \
        DEF_NIF(dpiVar_setFromLob, 3),            \
        DEF_NIF(dpiVar_setFromObject, 3),         \
        DEF_NIF(dpiVar_setFromRowid, 3),          \
        DEF_NIF(dpiVar_setFromStmt, 3),           \
        DEF_NIF(dpiVar_setNumElementsInArray, 2)

#endif // _DPIVAR_NIF_H_
