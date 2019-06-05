#ifndef _DPIOBJECTTYPE_NIF_H_
#define _DPIOBJECTTYPE_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiObjectType *objectType;
} dpiObjectType_res;

extern ErlNifResourceType *dpiObjectType_type;

extern void dpiObjectType_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(objectType_release);

#define DPIOBJECTTYPE_NIFS DEF_NIF(objectType_release, 1)

#endif // _DPIOBJECTTYPE_NIF_H_
