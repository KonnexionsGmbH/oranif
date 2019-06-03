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

extern DPI_NIF_FUN(var_addRef);
extern DPI_NIF_FUN(var_copyData);
extern DPI_NIF_FUN(var_getNumElementsInArray);
extern DPI_NIF_FUN(var_getReturnedData);
extern DPI_NIF_FUN(var_getSizeInBytes);
extern DPI_NIF_FUN(var_release);
extern DPI_NIF_FUN(var_setFromBytes);
extern DPI_NIF_FUN(var_setFromLob);
extern DPI_NIF_FUN(var_setFromObject);
extern DPI_NIF_FUN(var_setFromRowid);
extern DPI_NIF_FUN(var_setFromStmt);
extern DPI_NIF_FUN(var_setNumElementsInArray);

#define DPIVAR_NIFS                               \
    DEF_NIF(var_addRef, 1),                    \
        DEF_NIF(var_copyData, 4),              \
        DEF_NIF(var_getNumElementsInArray, 1), \
        DEF_NIF(var_getReturnedData, 2),       \
        DEF_NIF(var_getSizeInBytes, 1),        \
        DEF_NIF(var_release, 1),               \
        IOB_NIF(var_setFromBytes, 3),          \
        DEF_NIF(var_setFromLob, 3),            \
        DEF_NIF(var_setFromObject, 3),         \
        DEF_NIF(var_setFromRowid, 3),          \
        DEF_NIF(var_setFromStmt, 3),           \
        DEF_NIF(var_setNumElementsInArray, 2)
        

#endif // _DPIVAR_NIF_H_
