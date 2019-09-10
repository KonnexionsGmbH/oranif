#ifndef _DPIVAR_NIF_H_
#define _DPIVAR_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiVar *var;
    dpiContext *context;
    void *head;
} dpiVar_res;

extern ErlNifResourceType *dpiVar_type;

extern void dpiVar_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(var_release);
extern DPI_NIF_FUN(var_setFromBytes);
extern DPI_NIF_FUN(var_setNumElementsInArray);
extern DPI_NIF_FUN(var_getReturnedData);

#define DPIVAR_NIFS                            \
    DEF_NIF(var_release, 1),                   \
        IOB_NIF(var_setFromBytes, 3),          \
        DEF_NIF(var_setNumElementsInArray, 2), \
        DEF_NIF(var_getReturnedData, 2)

#endif // _DPIVAR_NIF_H_
