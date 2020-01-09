#include "dpiLob_nif.h"

ErlNifResourceType *dpiLob_type;

void dpiLob_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}


