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

extern DPI_NIF_FUN(dpiContext_create);
extern DPI_NIF_FUN(dpiContext_destroy);
extern DPI_NIF_FUN(dpiContext_getClientVersion);
extern DPI_NIF_FUN(dpiContext_getError);
extern DPI_NIF_FUN(dpiContext_initCommonCreateParams);
extern DPI_NIF_FUN(dpiContext_initConnCreateParams);
extern DPI_NIF_FUN(dpiContext_initPoolCreateParams);
extern DPI_NIF_FUN(dpiContext_initSodaOperOptions);
extern DPI_NIF_FUN(dpiContext_initSubscrCreateParams);

#define DPICONTEXT_NIFS DEF_NIF(dpiContext_create, 2),                 \
                        DEF_NIF(dpiContext_destroy, 1),                \
                        DEF_NIF(dpiContext_getClientVersion, 1),       \
                        DEF_NIF(dpiContext_getError, 1),               \
                        DEF_NIF(dpiContext_initCommonCreateParams, 1), \
                        DEF_NIF(dpiContext_initConnCreateParams, 1),   \
                        DEF_NIF(dpiContext_initPoolCreateParams, 1),   \
                        DEF_NIF(dpiContext_initSodaOperOptions, 1),    \
                        DEF_NIF(dpiContext_initSubscrCreateParams, 1)

#endif // _DPICONTEXT_NIF_H_
