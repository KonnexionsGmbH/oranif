#include "erl_nif.h"
#include <oci.h>
#include <stdio.h>
#include <string.h>

static ErlNifResourceType *envhp_resource_type;
static ErlNifResourceType *spoolhp_resource_type;
static ErlNifResourceType *authhp_resource_type;
static ErlNifResourceType *svchp_resource_type;
static ErlNifResourceType *stmthp_resource_type;
static ErlNifResourceType *bindhp_resource_type;

static ERL_NIF_TERM ATOM_OK;
static ERL_NIF_TERM ATOM_TRUE;
static ERL_NIF_TERM ATOM_FALSE;
static ERL_NIF_TERM ATOM_UNDEFINED;

typedef struct {
    OCIEnv *envhp;
    OCIError *errhp;
} envhp_res;

typedef struct {
    OCISPool *spoolhp;
} spoolhp_res;

typedef struct {
    OCIAuthInfo *authhp;
} authhp_res;

typedef struct {
    OCISvcCtx *svchp;
} svchp_res;

// struct to keep the definitions of Define columns around
typedef struct {
    ub4 col_type;
    ub1 char_semantics;
    ub2 col_size;
    ErlNifBinary col_name;
} col_info;

typedef struct {
    OCIStmt *stmthp;
    int col_info_retrieved;
    int num_cols;
    col_info *col_info;
} stmthp_res;

typedef struct {
    OCIBind *bindhp;
} bindhp_res;



static void checkerr(OCIError *errhp, sword status)
{
  text errbuf[512];
  sb4 errcode;

  if (status == OCI_SUCCESS) return;

  switch (status)
  {
  case OCI_SUCCESS_WITH_INFO:
    printf("Error - OCI_SUCCESS_WITH_INFO\r\n");
    OCIErrorGet ((void  *) errhp, (ub4) 1, (text *) NULL, &errcode,
            errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
    printf("Error - %s\n", errbuf);
    break;
  case OCI_NEED_DATA:
    printf("Error - OCI_NEED_DATA\r\n");
    break;
  case OCI_NO_DATA:
    printf("Error - OCI_NO_DATA\r\n");
    break;
  case OCI_ERROR:
    OCIErrorGet ((void  *) errhp, (ub4) 1, (text *) NULL, &errcode,
            errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
    printf("Error - %s\n", errbuf);
    break;
  case OCI_INVALID_HANDLE:
    printf("Error - OCI_INVALID_HANDLE\r\n");
    break;
  case OCI_STILL_EXECUTING:
    printf("Error - OCI_STILL_EXECUTING\r\n");
    break;
  case OCI_CONTINUE:
    printf("Error - OCI_CONTINUE\r\n");
    break;
  default:
    printf("Error - %d\r\n", status);
    break;
  }
}

static ERL_NIF_TERM ociEnvCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIEnv *envhp = (OCIEnv *)NULL;
    OCIError *errhp = (OCIError *)NULL;
    text errbuf[OCI_ERROR_MAXMSG_SIZE2];
    sb4 errcode;
    int status = OCIEnvCreate(&envhp,
                                OCI_THREADED,
                                NULL, NULL, NULL, NULL,
                                (size_t) 0,
                                (void **)NULL);
    switch (status) {
        case OCI_ERROR:
            if (envhp) {
                (void) OCIErrorGet(envhp, 1, (text *)NULL, &errcode,
                                errbuf, (ub4)sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
                (void) printf("Error - %s\r\n", errbuf);
            } else {
                (void) printf("NULL Handle\r\n");
            }
            return enif_make_badarg(env);
        case OCI_INVALID_HANDLE:
            (void) printf("Invalid Handle\r\n");
            return enif_make_badarg(env);

        default: {
            /* allocate an error handle */
            (void) OCIHandleAlloc ((void  *)envhp, (void  **)&errhp,
                OCI_HTYPE_ERROR, 0, (void  **) 0);
            // Create the enif resource to hold the handles
            envhp_res *res = (envhp_res *)enif_alloc_resource(envhp_resource_type, sizeof(envhp_res));
            if(!res) return enif_make_badarg(env);

            res->envhp = envhp;
            res->errhp = errhp;
            ERL_NIF_TERM nif_res = enif_make_resource(env, res);
            enif_release_resource(res);
            return nif_res;
        }
    }
}

static ERL_NIF_TERM ociSpoolHandleCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCISPool *spoolhp = (OCISPool *)NULL;

    // Retrieve the envhp passed in from Erlang in argv[0]
    envhp_res *res;
    if(!(argc == 1 && enif_get_resource(env, argv[0], envhp_resource_type, (void**)&res))) {
        return enif_make_badarg(env);
    }

    int status = OCIHandleAlloc((void *) res->envhp, (void **) &spoolhp, OCI_HTYPE_SPOOL, 
                        (size_t) 0, (void **) 0);
    switch (status) {
        case OCI_SUCCESS: {
            spoolhp_res *res2 = (spoolhp_res *)enif_alloc_resource(spoolhp_resource_type, sizeof(spoolhp_res));
            if(!res2) return enif_make_badarg(env);
            res2->spoolhp = spoolhp;

            ERL_NIF_TERM nif_res = enif_make_resource(env, res2);
            enif_release_resource(res2);
            printf("Session Pool Handle Created %p\r\n", res2->spoolhp);
            return nif_res;
        }
        default: {
            printf("Session Pool Handle Failed\r\n");
            return enif_make_badarg(env);
        }
    }
}

static ERL_NIF_TERM ociSessionPoolCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    char *poolName = NULL;
    ub4 poolNameLen;
    ErlNifBinary bin = {0};
    ERL_NIF_TERM term;

    // Retrieve the envhp and spoolhp passed in from Erlang in argv[0] and argv[1] along with the
    // other parameters
    envhp_res *envhp_res;
    spoolhp_res *spoolhp_res;
    ErlNifBinary database, username, password;
    ub4 sessMin, sessMax, sessIncr;
    if(!(argc == 8 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], spoolhp_resource_type, (void**)&spoolhp_res) &&
        enif_inspect_binary(env, argv[2], &database) &&
        enif_get_uint(env, argv[3], &sessMin) &&
        enif_get_uint(env, argv[4], &sessMax) &&
        enif_get_uint(env, argv[5], &sessIncr) &&
        enif_inspect_binary(env, argv[6], &username) &&
        enif_inspect_binary(env, argv[7], &password) )) {
            return enif_make_badarg(env);
        }

    // Create the session pool. returns the name as a string we can pass back to Erlang to refer to the pool in future calls
    int status = OCISessionPoolCreate(envhp_res->envhp, envhp_res->errhp, spoolhp_res->spoolhp, (OraText **)&poolName, 
                     (ub4 *)&poolNameLen,
                     database.data, (ub4) database.size,
                     sessMin, sessMax, sessIncr,
                     (OraText *) username.data, (ub4) username.size,
                     (OraText *) password.data, (ub4) password.size,
                     OCI_SPC_HOMOGENEOUS);
    switch (status) {
        case OCI_SUCCESS: {
            bin.size = poolNameLen;
            bin.data = (unsigned char *)poolName;
            term = enif_make_binary(env, &bin);
            if (! term) {
                printf("Binary failed\r\n");
                return enif_make_badarg(env);
            }
            printf("Session Pool Created\r\n");
            return enif_make_tuple(env, 2,
                         ATOM_OK,
                         term);
        }
        default: {
            printf("Session Pool Failed\r\n");
            checkerr(envhp_res->errhp, status);
            return enif_make_badarg(env);
        }
    }
}

static ERL_NIF_TERM ociAuthHandleCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIAuthInfo *authhp = (OCIAuthInfo *)0; // Ready to populate and store this
    int status;
    envhp_res *envhp_res;
    ErlNifBinary username, password;

    if(!(argc == 3 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_inspect_binary(env, argv[1], &username) &&
        enif_inspect_binary(env, argv[2], &password) )) {
            return enif_make_badarg(env);
        }
    status =  OCIHandleAlloc(envhp_res->envhp,
                            (dvoid **)&authhp, (ub4) OCI_HTYPE_AUTHINFO,
                            (size_t) 0, (dvoid **) 0);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    status = OCIAttrSet((dvoid *) authhp,(ub4) OCI_HTYPE_AUTHINFO, 
                       (OraText *) username.data, (ub4) username.size,
                        (ub4) OCI_ATTR_USERNAME, envhp_res->errhp);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    status = OCIAttrSet((dvoid *) authhp,(ub4) OCI_HTYPE_AUTHINFO, 
           (OraText *) password.data, (ub4) password.size,
           (ub4) OCI_ATTR_PASSWORD, envhp_res->errhp);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    // Create the enif resource to hold the handle
    authhp_res *res = (authhp_res *)enif_alloc_resource(authhp_resource_type, sizeof(authhp_res));
    if(!res) return enif_make_badarg(env);

    res->authhp = authhp;
    ERL_NIF_TERM nif_res = enif_make_resource(env, res);
    enif_release_resource(res);
    return enif_make_tuple(env, 2,
                         ATOM_OK,
                         nif_res);

}

static ERL_NIF_TERM ociSessionGet(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCISvcCtx *svchp = (OCISvcCtx *)NULL;
    envhp_res *envhp_res;
    authhp_res *authhp_res;
    ErlNifBinary poolName;

    if(!(argc == 3 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], authhp_resource_type, (void**)&authhp_res) &&
        enif_inspect_binary(env, argv[2], &poolName) )) {
            return enif_make_badarg(env);
        }
    int status = OCISessionGet(envhp_res->envhp, envhp_res->errhp, &svchp, authhp_res->authhp,
               (OraText *) poolName.data, (ub4) poolName.size, NULL, 
               0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    svchp_res *res = (svchp_res *)enif_alloc_resource(svchp_resource_type, sizeof(svchp_res));
    if(!res) return enif_make_badarg(env);

    res->svchp = svchp;
    ERL_NIF_TERM nif_res = enif_make_resource(env, res);
    enif_release_resource(res);
    return enif_make_tuple(env, 2,
                         ATOM_OK,
                         nif_res);

}

static ERL_NIF_TERM ociStmtHandleCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIStmt *stmthp = (OCIStmt *)NULL;
    envhp_res *envhp_res;
    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res))) {
            return enif_make_badarg(env);
        }
    // Create the enif resource to hold the handle
    stmthp_res *res = (stmthp_res *)enif_alloc_resource(stmthp_resource_type, sizeof(stmthp_res));
    if(!res) return enif_make_badarg(env);

    int status =  OCIHandleAlloc(envhp_res->envhp,
                            (dvoid **)&stmthp, (ub4) OCI_HTYPE_STMT,
                            (size_t) 0, (dvoid **) 0);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    res->stmthp = stmthp;
    res->col_info_retrieved = 0;
    res->col_info = NULL;
    res->num_cols = 0;
    ERL_NIF_TERM nif_res = enif_make_resource(env, res);
    enif_release_resource(res);
    return enif_make_tuple(env, 2,
                         ATOM_OK,
                         nif_res);
}

static ERL_NIF_TERM ociStmtPrepare(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    stmthp_res *stmthp_res;
    ErlNifBinary statement;

    if(!(argc == 3 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_inspect_binary(env, argv[2], &statement))) {
            return enif_make_badarg(env);
        }
    
    int status = OCIStmtPrepare (stmthp_res->stmthp, envhp_res->errhp,
                                 (OraText *) statement.data, (ub4) statement.size,
                                  OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    } else {
        return ATOM_OK;
    }
}

static ERL_NIF_TERM ociBindByName(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIBind *bindhp = (OCIBind *)NULL;
    stmthp_res *stmthp_res;
    envhp_res *envhp_res;
    int dty, ind;
    ErlNifBinary name, value;

    if(!(argc == 6 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_inspect_binary(env, argv[2], &name) &&
        enif_get_int(env, argv[3], &ind) &&
        enif_get_int(env, argv[4], &dty) &&
        enif_inspect_binary(env, argv[5], &value))) {
            return enif_make_badarg(env);
        }
            
    int status = OCIBindByName(stmthp_res->stmthp, &bindhp, envhp_res->errhp,
                                (OraText *) name.data, (sword) name.size, // Bind name
                                (void *) value.data, (sword) value.size, // Bind value
                                (sb2) dty,           // Oracle DataType
                                (void *) &ind, // Indicator variable
                                (ub2*) NULL,    // address of actual length
                                (ub2*) NULL,    // address of return code
                                0,             // max array length for PLSQL indexed tables
                                (ub4*) NULL,    // current array length for PLSQL indexed tables
                                OCI_DEFAULT); // mode

    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    // Create the enif resource to hold the handle
    bindhp_res *res = (bindhp_res *)enif_alloc_resource(bindhp_resource_type, sizeof(bindhp_res));
    if(!res) return enif_make_badarg(env);

    res->bindhp = bindhp;
    ERL_NIF_TERM nif_res = enif_make_resource(env, res);
    enif_release_resource(res);
    return enif_make_tuple(env, 2,
                         ATOM_OK,
                         nif_res);

}

static ERL_NIF_TERM ociStmtExecute(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    svchp_res *svchp_res;
    stmthp_res *stmthp_res;
    ub4 iters, row_off, mode;

    // Vars needed if we want to figure out fetch column types
    OCIParam *paramd = (OCIParam *) 0;
    ub1 char_semantics;
    ub2 col_type;
    ub4 counter, col_name_len, col_count, col_width;
    text *col_name;

    if(!(argc == 6 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], svchp_resource_type, (void**)&svchp_res) &&
        enif_get_resource(env, argv[2], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_get_uint(env, argv[3], &iters) &&
        enif_get_uint(env, argv[4], &row_off) &&
        enif_get_uint(env, argv[5], &mode) )) {
            return enif_make_badarg(env);
        }
    
    int status = OCIStmtExecute (svchp_res->svchp, stmthp_res->stmthp, envhp_res->errhp, iters, row_off,
                                  (OCISnapshot *)0, (OCISnapshot *)0, 
                                  mode);
    if (status) {
        // Fixme, might need to do a rollback
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    if (stmthp_res->col_info_retrieved) {
        // Column info already populated by a previous call to OCIStmtExecute
        return ATOM_OK;
    }

    /* After a successful execute it seems convenient to check
       immediately whether this statement could supply any 
       output rows and if so setting up any OCIDefines needed. */

    // Retrieve the column count
    status = OCIAttrGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                        (dvoid *)&col_count, (ub4 *)0,
                        OCI_ATTR_PARAM_COUNT, envhp_res->errhp);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    }

    if (col_count == 0) {
        // No output, we are done
        return ATOM_OK;
    }

    // Keep column info around for future execs of the same statement
    stmthp_res->num_cols = col_count;
    col_info *col_array = malloc(col_count * sizeof(*col_array));
    stmthp_res->col_info = col_array;

    // Maybe some rows to fetch, setup to receive them and retrieve the first column
    counter = 1;    
    int param_status = OCIParamGet(stmthp_res->stmthp, OCI_HTYPE_STMT, envhp_res->errhp,
                            (dvoid **)&paramd, counter);

    // if we got a description for the first column, get them all
    while (param_status == OCI_SUCCESS) {
        col_info *column_info = &col_array[counter - 1];

        /* Retrieve the datatype attribute */
        status = OCIAttrGet((dvoid*) paramd, (ub4) OCI_DTYPE_PARAM,
                (dvoid*) &col_type,(ub4 *) 0, (ub4) OCI_ATTR_DATA_TYPE,
                (OCIError *) envhp_res->errhp  );
        if (status) {
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            checkerr(envhp_res->errhp, status);
            return enif_make_badarg(env);
        }
        column_info->col_type = col_type;

        /* Retrieve the column name attribute and put in a locally stored erlang binary
           Could just have used a text* for this, but convenient for sending back to beam */
        col_name_len = 0;
        status = OCIAttrGet((dvoid*) paramd, (ub4) OCI_DTYPE_PARAM,
                            (dvoid**) &col_name, (ub4 *) &col_name_len, (ub4) OCI_ATTR_NAME,
                            (OCIError *) envhp_res->errhp );
        
        if (status) {
            /* Explicitly free the paramd descriptor. It's not hooked to anything
               that will be garbage collected by Erlang. */
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            checkerr(envhp_res->errhp, status);
            return enif_make_badarg(env);
        }
        if (!enif_alloc_binary(col_name_len, &column_info->col_name)) {
            // binary creation failed. out of memory?
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return enif_make_badarg(env);
        }
        memcpy(column_info->col_name.data, col_name, col_name_len);

        /* Retrieve the length semantics for the column */
        char_semantics = 0;
        status = OCIAttrGet((void*) paramd, (ub4) OCI_DTYPE_PARAM,
                            (void*) &char_semantics,(ub4 *) 0, (ub4) OCI_ATTR_CHAR_USED,
                            (OCIError *) envhp_res->errhp);
        if (status) {
            /* Explicitly free the paramd descriptor. It's not hooked to anything
               that will be garbage collected by Erlang. */
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            checkerr(envhp_res->errhp, status);
            return enif_make_badarg(env);
        }
        column_info->char_semantics = char_semantics;

        /* Retrieve the column width in characters */
        col_width = 0;
        if (char_semantics) {
            status = OCIAttrGet((void*) paramd, (ub4) OCI_DTYPE_PARAM,
                                (void*) &col_width, (ub4 *) 0, (ub4) OCI_ATTR_CHAR_SIZE,
                                (OCIError *) envhp_res->errhp);
            if (status) {
                /* Explicitly free the paramd descriptor. It's not hooked to anything
                   that will be garbage collected by Erlang. */
                OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
                checkerr(envhp_res->errhp, status);
                return enif_make_badarg(env);
                }
            column_info->col_size = col_width;
        } else {
            /* Retrieve the column width in bytes */
            status = OCIAttrGet((void*) paramd, (ub4) OCI_DTYPE_PARAM,
                                (void*) &col_width,(ub4 *) 0, (ub4) OCI_ATTR_DATA_SIZE,
                                (OCIError *) envhp_res->errhp);
            if (status) {
                /* Explicitly free the paramd descriptor. It's not hooked to anything
                   that will be garbage collected by Erlang. */
                OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
                checkerr(envhp_res->errhp, status);
                return enif_make_badarg(env);
                }
            column_info->col_size = col_width;
        }
        counter++;
        param_status = OCIParamGet(stmthp_res->stmthp, OCI_HTYPE_STMT, envhp_res->errhp,
                                  (dvoid **)&paramd, counter);
    }
    stmthp_res->col_info_retrieved = 1;
    return ATOM_OK;
}


static ERL_NIF_TERM ociStmtFetch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    stmthp_res *stmthp_res;
    ub4 nrows;
    
    if(!(argc == 3 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_get_uint(env, argv[2], &nrows) )) {
            return enif_make_badarg(env);
        }
    
    int status = OCIStmtFetch2 (stmthp_res->stmthp, envhp_res->errhp,
                                nrows,               // Number of rows to return
                                (ub2) OCI_DEFAULT,   // orientation
                                (sb4) 0,             // fetchOffset
                                OCI_DEFAULT);
    if (status) {
        checkerr(envhp_res->errhp, status);
        return enif_make_badarg(env);
    } else {
        return ATOM_OK;
    }
}


static ErlNifFunc nif_funcs[] =
{
    // All functions that execure server round trips MUST be marked as IO_BOUND
    // Knowledge from: https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/oci-function-server-round-trips.html
    {"ociEnvCreate", 0, ociEnvCreate},
    {"ociSpoolHandleCreate", 1, ociSpoolHandleCreate},
    {"ociSessionPoolCreate", 8, ociSessionPoolCreate, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociAuthHandleCreate", 3, ociAuthHandleCreate},
    {"ociSessionGet", 3, ociSessionGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociStmtHandleCreate", 1, ociStmtHandleCreate},
    {"ociStmtPrepare", 3, ociStmtPrepare},
    {"ociStmtExecute", 6, ociStmtExecute, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociBindByName", 6, ociBindByName},
    {"ociStmtFetch", 3, ociStmtFetch, ERL_NIF_DIRTY_JOB_IO_BOUND}
};

static void envhp_res_dtor(ErlNifEnv *env, void *resource) {

  envhp_res *res = (envhp_res*)resource;
printf("envhp_res_dtor called\r\n");
  if(res->envhp) {
    // Clear up the OCIEnv
  }
    
}

static void spoolhp_res_dtor(ErlNifEnv *env, void *resource) {

  envhp_res *res = (envhp_res*)resource;
  printf("spoolhp_res_ called\r\n");

  if(res->envhp) {
    // Clear up the OCIEnv
  }
    
}

static void authhp_res_dtor(ErlNifEnv *env, void *resource) {

  envhp_res *res = (envhp_res*)resource;
printf("authhp_res_ called\r\n");

  if(res->envhp) {
    // Clear up the OCIEnv
  }
    
}

static void svchp_res_dtor(ErlNifEnv *env, void *resource) {

  envhp_res *res = (envhp_res*)resource;

  if(res->envhp) {
    // Clear up the OCIEnv
  }
  printf("svchp_res_ called\r\n");
    
}

static void stmthp_res_dtor(ErlNifEnv *env, void *resource) {

  stmthp_res *res = (stmthp_res*)resource;

  if(res->stmthp) {
    // Clear up the Statement Handle
    // and column descriptions
  }
  printf("stmthp_res_ called\r\n");
    
}

static void bindhp_res_dtor(ErlNifEnv *env, void *resource) {

  envhp_res *res = (envhp_res*)resource;

  if(res->envhp) {
    // Clear up the OCIEnv
  }
  printf("bindhp_res_ called\r\n");
    
}

static int load(ErlNifEnv* env, void** priv_data, ERL_NIF_TERM load_info) {
    envhp_resource_type = enif_open_resource_type(env, NULL, "envhp",
        envhp_res_dtor, ERL_NIF_RT_CREATE, NULL);

    spoolhp_resource_type = enif_open_resource_type(env, NULL, "spoolhp",
        spoolhp_res_dtor, ERL_NIF_RT_CREATE, NULL);

    authhp_resource_type = enif_open_resource_type(env, NULL, "authhp",
        authhp_res_dtor, ERL_NIF_RT_CREATE, NULL);

    svchp_resource_type = enif_open_resource_type(env, NULL, "svchp",
        svchp_res_dtor, ERL_NIF_RT_CREATE, NULL);

    stmthp_resource_type = enif_open_resource_type(env, NULL, "stmthp",
        stmthp_res_dtor, ERL_NIF_RT_CREATE, NULL);

    bindhp_resource_type = enif_open_resource_type(env, NULL, "bindhp",
        bindhp_res_dtor, ERL_NIF_RT_CREATE, NULL);

    ATOM_OK = enif_make_atom(env, "ok");    
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_UNDEFINED = enif_make_atom(env, "undefined");
    return 0;
}

ERL_NIF_INIT(erloci_nif_drv,nif_funcs,&load,NULL,NULL,NULL)