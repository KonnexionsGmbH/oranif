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

extern DPI_NIF_FUN(dpiObjectType_addRef);
extern DPI_NIF_FUN(dpiObjectType_createObject);
extern DPI_NIF_FUN(dpiObjectType_getAttributes);
extern DPI_NIF_FUN(dpiObjectType_getInfo);
extern DPI_NIF_FUN(dpiObjectType_release);

#define DPIOBJECTTYPE_NIFS DEF_NIF(dpiObjectType_addRef, 1),        \
                           DEF_NIF(dpiObjectType_createObject, 1),  \
                           DEF_NIF(dpiObjectType_getAttributes, 2), \
                           DEF_NIF(dpiObjectType_getInfo, 1),       \
                           DEF_NIF(dpiObjectType_release, 1)

#endif // _DPIOBJECTTYPE_NIF_H_
