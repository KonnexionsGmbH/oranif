#ifndef _DPICONTEXT_NIF_H_
#define _DPICONTEXT_NIF_H_

#include "dpi_nif.h"
#include "dpi.h"

typedef struct
{
    dpiContext *context;
} dpiContext_res;

extern ErlNifResourceType *dpiContext_type;
extern void dpiContext_res_dtor(ErlNifEnv *env, void *resource);

extern DPI_NIF_FUN(context_create);
extern DPI_NIF_FUN(context_destroy);
extern DPI_NIF_FUN(context_getClientVersion);

#define DPICONTEXT_NIFS DEF_NIF(context_create, 2),           \
                        DEF_NIF(context_destroy, 1),          \
                        DEF_NIF(context_getClientVersion, 1)
                        
#endif // _DPICONTEXT_NIF_H_
