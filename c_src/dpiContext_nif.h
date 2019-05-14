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
extern DPI_NIF_FUN(context_getError);
extern DPI_NIF_FUN(context_initCommonCreateParams);
extern DPI_NIF_FUN(context_initConnCreateParams);
extern DPI_NIF_FUN(context_initPoolCreateParams);
extern DPI_NIF_FUN(context_initSodaOperOptions);
extern DPI_NIF_FUN(context_initSubscrCreateParams);

#define DPICONTEXT_NIFS DEF_NIF(context_create, 2),                 \
                        DEF_NIF(context_destroy, 1),                \
                        DEF_NIF(context_getClientVersion, 1),       \
                        DEF_NIF(context_getError, 1),               \
                        DEF_NIF(context_initCommonCreateParams, 1), \
                        DEF_NIF(context_initConnCreateParams, 1),   \
                        DEF_NIF(context_initPoolCreateParams, 1),   \
                        DEF_NIF(context_initSodaOperOptions, 1),    \
                        DEF_NIF(context_initSubscrCreateParams, 1)

#endif // _DPICONTEXT_NIF_H_
