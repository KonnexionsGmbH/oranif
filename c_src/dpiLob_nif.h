#ifndef _DPILOB_NIF_H_
#define _DPILOB_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiLob *lob;
    dpiContext *context;
} dpiLob_res;

extern ErlNifResourceType *dpiLob_type;

extern void dpiLob_res_dtor(ErlNifEnv *env, void *resource);

//extern DPI_NIF_FUN(var_release);

#define DPILOB_NIFS                            \
    //DEF_NIF(var_release, 1),                   

#endif // _DPILOB_NIF_H_
