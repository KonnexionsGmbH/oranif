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
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_NULL;
static ERL_NIF_TERM ATOM_COLS;
static ERL_NIF_TERM ATOM_ROWIDS;
static ERL_NIF_TERM ATOM_ENOMEM;

typedef struct {
    OCIEnv *envhp;
    OCIError *errhp;
} envhp_res;

typedef struct {
    OCISPool *spoolhp;
    OCIError *errhp;
    OraText *poolName;
    ub4 poolNameLen;
} spoolhp_res;

typedef struct {
    OCIAuthInfo *authhp;
} authhp_res;

typedef struct {
    OCISvcCtx *svchp;
    OCIError *errhp;
} svchp_res;

// struct to keep the definitions of Define columns around
typedef struct {
    ub4 col_type;
    ub1 char_semantics;
    ub2 col_size;
    ub1 col_scale;
    ub2 col_precision;
    ErlNifBinary col_name;
    OCIDefine *definehp;
    ub2 ind;                // OUT - fetched NULL? indicator placed here
    void *valuep;           // OUT - value here
} col_info;

typedef struct {
    OCIStmt *stmthp;        // statement handle
    OCIError *errhp;        // per statement error handle
    ub2 stmt_type;          // Retrieved statement type (e.g. OCI_SELECT)
    int col_info_retrieved; // Set on first call to execute
    int num_cols;           // number of define columns for select
    col_info *col_info;     // array of *col_info for define data
} stmthp_res;

// One of these is created for each bind variable. They end up stored in a map
typedef struct {
    OCIBind *bindhp;
    ErlNifBinary name;
    ErlNifBinary value;
    size_t value_sz;
} bindhp_res;

static ERL_NIF_TERM reterr(ErlNifEnv* env, OCIError *errhp, sword status) {
    text errbuf[OCI_ERROR_MAXMSG_SIZE2];
    size_t msg_sz;
    unsigned char *buf;
    sb4 errcode;
    ERL_NIF_TERM err_bin;

    switch (status) {
    case OCI_SUCCESS_WITH_INFO:
        OCIErrorGet ((void  *) errhp, (ub4) 1, (text *) NULL, &errcode,
            errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
        msg_sz = strnlen((const char *)errbuf, OCI_ERROR_MAXMSG_SIZE2);
        buf = enif_make_new_binary(env, msg_sz, &err_bin);
        memcpy(buf, errbuf, msg_sz);
        return enif_make_tuple2(env, ATOM_ERROR,
                enif_make_tuple2(env, enif_make_int(env, errcode), err_bin));
    case OCI_NEED_DATA: {
        static char *msg = "Error - OCI_NEED_DATA";
        buf = enif_make_new_binary(env, sizeof(*msg), &err_bin);
        memcpy(buf, msg, sizeof(*msg));
        return enif_make_tuple2(env, ATOM_OK, err_bin);
    }
  case OCI_NO_DATA:
    printf("Error - OCI_NO_DATA\r\n");
    break;
  case OCI_ERROR:
    OCIErrorGet ((void  *) errhp, (ub4) 1, (text *) NULL, &errcode,
            errbuf, (ub4) sizeof(errbuf), (ub4) OCI_HTYPE_ERROR);
    msg_sz = strnlen((const char *)errbuf, OCI_ERROR_MAXMSG_SIZE2);
    buf = enif_make_new_binary(env, msg_sz, &err_bin);
    memcpy(buf, errbuf, msg_sz);
    return enif_make_tuple2(env, ATOM_ERROR,
                enif_make_tuple2(env, enif_make_int(env, errcode), err_bin));
  case OCI_INVALID_HANDLE: {
    static char *msg = "Error - OCI_INVALID_HANDLE";
    buf = enif_make_new_binary(env, sizeof(*msg), &err_bin);
    memcpy(buf, msg, sizeof(*msg));
    return enif_make_tuple2(env, ATOM_ERROR, err_bin);
  }
  case OCI_STILL_EXECUTING:
    printf("Error - OCI_STILL_EXECUTING\r\n");
    return ATOM_ERROR;
  case OCI_CONTINUE:
    printf("Error - OCI_CONTINUE\r\n");
    return ATOM_ERROR;
  default:
    printf("Error - %d\r\n", status);
    return ATOM_ERROR;
  }
  return ATOM_ERROR;
}

static ERL_NIF_TERM execute_result_map(ErlNifEnv* env, stmthp_res *stmthp) {
    ERL_NIF_TERM map;
    map = enif_make_new_map(env);
    if (stmthp->stmt_type == OCI_STMT_SELECT) {
        // return collected column info
        ERL_NIF_TERM list = enif_make_list(env, 0);
        ERL_NIF_TERM new_map;
        for (int i = 0; i < stmthp->num_cols; i++) {
            col_info *col = &stmthp->col_info[i];
            ERL_NIF_TERM name;
            text *buf = enif_make_new_binary(env, col->col_name.size, &name);
            memcpy(buf, col->col_name.data, col->col_name.size);
            ERL_NIF_TERM term = enif_make_tuple5(
                env,
                name,
                enif_make_int(env, col->col_type),
                enif_make_int(env, col->col_size),
                enif_make_int(env, col->col_precision),
                enif_make_int(env, col->col_scale));
            list = enif_make_list_cell(env, term, list);
        }
        enif_make_map_put(env, map, ATOM_COLS, list, &new_map);
        return new_map;
    } else if (stmthp->stmt_type == OCI_STMT_INSERT
               || stmthp->stmt_type == OCI_STMT_UPDATE
               || stmthp->stmt_type == OCI_STMT_DELETE) {
        // Return any updated row_ids
        return map;
    } else {
        return map;
    }
}

/* If we re-prepare an existing stmt handle we must manually
   reset the retrieved col info from previous executions.
   Also needed by the GC callbacks when our stmt resource is
   destroyed
*/
static void free_col_info(col_info *col_info, int num_cols) {
    for (int i = 0; i < num_cols; i++) {
        enif_release_binary(&col_info[i].col_name);
    }
    enif_free(col_info);
}

static ERL_NIF_TERM ociEnvNlsCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIEnv *envhp = (OCIEnv *)NULL;
    OCIError *errhp = (OCIError *)NULL;
    text errbuf[OCI_ERROR_MAXMSG_SIZE2];
    sb4 errcode;
    int status = OCIEnvNlsCreate(&envhp,
                                OCI_THREADED,
                                NULL, NULL, NULL, NULL,
                                (size_t) 0,
                                (void **)NULL,
                                (ub2) 0, (ub2) 0);
    switch (status) {
        case OCI_ERROR:
            if (envhp) {
                (void) OCIErrorGet(envhp, 1, (text *)NULL, &errcode,
                                    errbuf, (ub4)sizeof(errbuf),
                                    (ub4) OCI_HTYPE_ERROR);
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

static ERL_NIF_TERM ociSessionPoolCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCISPool *spoolhp = (OCISPool *)NULL;
    OCIError *errhp = (OCIError *)NULL;
    spoolhp_res *sphp_res;
    ERL_NIF_TERM nif_res;
    OraText *poolName;
    ub4 poolNameLen;

    // Retrieve the envhp passed in from Erlang in argv[0] along with the other parameters
    envhp_res *envhp_res;
    ErlNifBinary database, username, password;
    ub4 sessMin, sessMax, sessIncr;
    if(!(argc == 7 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_inspect_binary(env, argv[1], &database) &&
        enif_get_uint(env, argv[2], &sessMin) &&
        enif_get_uint(env, argv[3], &sessMax) &&
        enif_get_uint(env, argv[4], &sessIncr) &&
        enif_inspect_binary(env, argv[5], &username) &&
        enif_inspect_binary(env, argv[6], &password) )) {
            return enif_make_badarg(env);
        }

    // Create the session pool handle
    int status = OCIHandleAlloc((void *) envhp_res->envhp, (void **) &spoolhp,
                                OCI_HTYPE_SPOOL,
                                (size_t) 0, (void **) 0);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }
    sphp_res = (spoolhp_res *)enif_alloc_resource(spoolhp_resource_type, sizeof(spoolhp_res));
    if(!sphp_res) return enif_raise_exception(env, ATOM_ENOMEM);
    sphp_res->spoolhp = spoolhp;

    // Create a private errhp for use in destructor mainly
    status = OCIHandleAlloc (envhp_res->envhp, (void  **)&errhp,
                            OCI_HTYPE_ERROR, 0, (void  **) 0);
    if (status) {
        return enif_raise_exception(env, ATOM_ENOMEM);
    }
    sphp_res->errhp = errhp;

    // Create a session pool
    status = OCISessionPoolCreate(envhp_res->envhp, envhp_res->errhp,
                     spoolhp, (OraText **)&poolName, 
                     (ub4 *)&poolNameLen,
                     database.data, (ub4) database.size,
                     sessMin, sessMax, sessIncr,
                     (OraText *) username.data, (ub4) username.size,
                     (OraText *) password.data, (ub4) password.size,
                     OCI_SPC_HOMOGENEOUS);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }
    sphp_res->poolName = poolName;
    sphp_res->poolNameLen = poolNameLen;
    nif_res = enif_make_resource(env, sphp_res);
    enif_release_resource(sphp_res);
    return enif_make_tuple2(env, ATOM_OK, nif_res);
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
        return reterr(env, envhp_res->errhp, status);
    }

    status = OCIAttrSet((dvoid *) authhp,(ub4) OCI_HTYPE_AUTHINFO, 
                       (OraText *) username.data, (ub4) username.size,
                        (ub4) OCI_ATTR_USERNAME, envhp_res->errhp);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }

    status = OCIAttrSet((dvoid *) authhp,(ub4) OCI_HTYPE_AUTHINFO, 
           (OraText *) password.data, (ub4) password.size,
           (ub4) OCI_ATTR_PASSWORD, envhp_res->errhp);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
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
    OCIError *errhp = (OCIError *)NULL;
    envhp_res *envhp_res;
    authhp_res *authhp_res;
    spoolhp_res *spoolhp_res;

    if(!(argc == 3 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_resource(env, argv[1], authhp_resource_type, (void**)&authhp_res) &&
        enif_get_resource(env, argv[2], spoolhp_resource_type, (void**)&spoolhp_res))) {
            return enif_make_badarg(env);
        }
    int status = OCISessionGet(envhp_res->envhp, spoolhp_res->errhp, &svchp,
                authhp_res->authhp,
                spoolhp_res->poolName, spoolhp_res->poolNameLen, NULL, 
                0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }

    svchp_res *res = (svchp_res *)enif_alloc_resource(svchp_resource_type,
                                                      sizeof(svchp_res));
    if(!res) return enif_raise_exception(env, ATOM_ENOMEM);

    res->svchp = svchp;

    // Create a private errhp. Used in GC
    status = OCIHandleAlloc (envhp_res->envhp, (void  **)&errhp,
                            OCI_HTYPE_ERROR, 0, (void  **) 0);
    if (status) {
        return enif_raise_exception(env, ATOM_ENOMEM);
    }
    res->errhp = errhp;

    ERL_NIF_TERM nif_res = enif_make_resource(env, res);
    enif_release_resource(res);
    return enif_make_tuple(env, 2,
                         ATOM_OK,
                         nif_res);
}

static ERL_NIF_TERM ociStmtHandleCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIStmt *stmthp = (OCIStmt *)NULL;
    OCIError *errhp = (OCIError *)NULL;
    envhp_res *envhp_res;
    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res))) {
            return enif_make_badarg(env);
        }
    // Create the enif resource to hold the handle
    stmthp_res *res = (stmthp_res *)enif_alloc_resource(stmthp_resource_type,
                                                        sizeof(stmthp_res));
    if(!res) return enif_raise_exception(env, ATOM_ENOMEM);

    int status =  OCIHandleAlloc(envhp_res->envhp,
                            (dvoid **)&stmthp, (ub4) OCI_HTYPE_STMT,
                            (size_t) 0, (dvoid **) 0);
    if (status) {
        return enif_raise_exception(env, ATOM_ENOMEM);
    }

    /* allocate a per statement error handle to avoid multiple 
       statements having to use the global one.
       The docs suggest one error handle per thread, but we
       don't have control over erlang thread pools */
    status = OCIHandleAlloc (envhp_res->envhp, (void  **)&errhp,
                            OCI_HTYPE_ERROR, 0, (void  **) 0);
    if (status) {
        enif_raise_exception(env, ATOM_ENOMEM);
    }

    res->stmthp = stmthp;
    res->errhp = errhp;

    // Initialise Define column data
    res->col_info_retrieved = 0;
    res->col_info = NULL;
    res->num_cols = 0;

    ERL_NIF_TERM nif_res = enif_make_resource(env, res);
    enif_release_resource(res);

    return enif_make_tuple2(env, ATOM_OK, nif_res);
}

static ERL_NIF_TERM ociStmtPrepare(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    stmthp_res *stmthp_res;
    ErlNifBinary statement;

    if(!(argc == 2 &&
        enif_get_resource(env, argv[0], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_inspect_binary(env, argv[1], &statement))) {
            return enif_make_badarg(env);
        }

    /*
    Calling OCIStmtPrepare after it's been used implicitly 
    frees any existing bind and define handles.
    We also allocated memory associated with the
    define handles to hold column names, so
    here seems like the right place to free anything we hold
    against this stmthp.
    */
   if (stmthp_res->col_info_retrieved && stmthp_res->num_cols) {
        if (stmthp_res->col_info) {
            free_col_info(stmthp_res->col_info, stmthp_res->num_cols);
        } else {
            printf("DRIVER ERROR: col_info NULL but cols retrieved");
        }
    }
    stmthp_res->col_info_retrieved = 0;
    stmthp_res->col_info = NULL;
    stmthp_res->num_cols = 0;

    int status = OCIStmtPrepare (stmthp_res->stmthp, stmthp_res->errhp,
                                 (OraText *) statement.data, (ub4) statement.size,
                                  OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    } else {
        return ATOM_OK;
    }
}

static ERL_NIF_TERM ociBindByName(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    bindhp_res *bindhp_resource;
    stmthp_res *stmthp_res;
    int dty, ind, status;
    ErlNifBinary name, value;

    if(!(argc == 6 &&
        enif_get_resource(env, argv[0], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_is_map(env, argv[1]) &&
        enif_inspect_binary(env, argv[2], &name) &&
        enif_get_int(env, argv[3], &ind) &&
        enif_get_int(env, argv[4], &dty) &&
        enif_inspect_binary(env, argv[5], &value))) {
            return enif_make_badarg(env);
        }

    ERL_NIF_TERM return_map = argv[1];
    ERL_NIF_TERM current_value;
    if(enif_get_map_value(env, argv[1], argv[2], &current_value)) {
        if (!enif_get_resource(env, current_value, bindhp_resource_type, (void**)&bindhp_resource)) {
            return enif_make_badarg(env);
        }

        // See if we can re-use the value memory
        if (bindhp_resource->value.size >= value.size) {
            memcpy(bindhp_resource->value.data, value.data, value.size);
            bindhp_resource->value_sz = value.size;
            // Nothing else to do - the existing OCIBind will use the new value
            return enif_make_tuple2(env, ATOM_OK, return_map);
        } else {
            ErlNifBinary new;
            if(!enif_alloc_binary(value.size, &new)) {
                enif_raise_exception(env, ATOM_ENOMEM);
            }
            memcpy(new.data, value.data, value.size);
            enif_release_binary(&bindhp_resource->value);
            bindhp_resource->value = new;
            bindhp_resource->value_sz = value.size;
        }
    } else {
        // No existing bind for this name
        // Create the managed pointer / resource
        bindhp_resource = (bindhp_res *) enif_alloc_resource(bindhp_resource_type, sizeof(bindhp_res));
        if(!bindhp_resource) return enif_make_badarg(env);
        bindhp_resource->bindhp = NULL; // Ask the OCIBindByName call to allocate this

        // Make copy of the name for oci use
        if (!enif_alloc_binary(name.size, &bindhp_resource->name)) {
            enif_raise_exception(env, ATOM_ENOMEM);
        }
        memcpy(bindhp_resource->name.data, name.data, name.size);

        // Make copy of the value
        if (!enif_alloc_binary(value.size, &bindhp_resource->value)) {
            enif_raise_exception(env, ATOM_ENOMEM);
        }
        memcpy(bindhp_resource->value.data, value.data, value.size);
        bindhp_resource->value_sz = value.size;
        
        // Put this new handle in the map
        ERL_NIF_TERM nif_res = enif_make_resource(env, bindhp_resource);
        enif_release_resource(bindhp_resource);
        enif_make_map_put(env, argv[1], argv[2], nif_res, &return_map);
    }

    status = OCIBindByName(stmthp_res->stmthp, &bindhp_resource->bindhp,
                stmthp_res->errhp,
                (OraText *) bindhp_resource->name.data, (sword) bindhp_resource->name.size, // Bind name
                (void *) bindhp_resource->value.data, (sword) bindhp_resource->value_sz, // Bind value
                (sb2) dty,           // Oracle DataType
                (void *) &ind, // Indicator variable
                (ub2*) NULL,    // address of actual length
                (ub2*) NULL,    // address of return code
                0,             // max array length for PLSQL indexed tables
                (ub4*) NULL,    // current array length for PLSQL indexed tables
                OCI_DEFAULT); // mode

    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    }
    return enif_make_tuple2(env, ATOM_OK, return_map);
}

static ERL_NIF_TERM ociStmtExecute(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    svchp_res *svchp_res;
    stmthp_res *stmthp_res;
    ub4 iters, row_off, mode;

    // Vars needed if we want to figure out fetch column types
    OCIParam *paramd = (OCIParam *) 0;
    ub1 char_semantics;
    ub2 col_type;
    ub4 counter, col_name_len, col_count, col_width;
    text *col_name;

    // Vars for the define step
    void *valuep;
    OCIDefine *definehp = (OCIDefine *) 0;

    if(!(argc == 6 &&
        enif_get_resource(env, argv[0], svchp_resource_type, (void**)&svchp_res) &&
        enif_get_resource(env, argv[1], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_is_map(env, argv[2]) &&
        enif_get_uint(env, argv[3], &iters) &&
        enif_get_uint(env, argv[4], &row_off) &&
        enif_get_uint(env, argv[5], &mode) )) {
            return enif_make_badarg(env);
        }
    
    int status = OCIStmtExecute (svchp_res->svchp, stmthp_res->stmthp,
                                 stmthp_res->errhp, iters, row_off,
                                 (OCISnapshot *)0, (OCISnapshot *)0, 
                                 mode);
    if (status) {
        // Fixme, might need to do a rollback
        return reterr(env, stmthp_res->errhp, status);
    }

    
    /* After a successful execute it seems convenient to check
       immediately whether this statement could supply any 
       output rows and if so setting up any OCIDefines needed. */
    
    // Retrieve the statement type
    status = OCIAttrGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                        &stmthp_res->stmt_type, (ub4 *)0,
                        OCI_ATTR_STMT_TYPE, stmthp_res->errhp);
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    }

    if (stmthp_res->stmt_type != OCI_STMT_SELECT && stmthp_res->col_info_retrieved) {
        // Column info already populated by a previous call to OCIStmtExecute
        return enif_make_tuple2(env, ATOM_OK,
                                execute_result_map(env, stmthp_res));
    }


    if (stmthp_res->stmt_type != OCI_STMT_SELECT) {
        return ATOM_OK;
    }
    // Retrieve the column count 
    status = OCIAttrGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                        (dvoid *)&col_count, (ub4 *)0,
                        OCI_ATTR_PARAM_COUNT, stmthp_res->errhp);
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    }

    if (col_count == 0) {
        // No output, we are done
        return ATOM_OK;
    }

    // Keep column info around for future execs of the same statement
    stmthp_res->num_cols = col_count;
    col_info *col_array = enif_alloc(col_count * sizeof(*col_array));
    if (col_array == NULL) {
        return enif_raise_exception(env, ATOM_ENOMEM);
    }
    stmthp_res->col_info = col_array;

    /* Maybe some rows to fetch, setup to receive them and retrieve OCIDefine
     info for the first column
     */
    counter = 1;    
    int param_status = OCIParamGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                            stmthp_res->errhp,
                            (dvoid **)&paramd, counter);

    // if we got a description for the first column, get them all
    while (param_status == OCI_SUCCESS) {
        col_info *column_info = &col_array[counter - 1];
        column_info->definehp = NULL;

        /* Retrieve the datatype attribute */
        status = OCIAttrGet((dvoid*) paramd, (ub4) OCI_DTYPE_PARAM,
                (dvoid*) &col_type,(ub4 *) 0, (ub4) OCI_ATTR_DATA_TYPE,
                (OCIError *) stmthp_res->errhp  );
        if (status) {
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }
        column_info->col_type = col_type;
        // printf("Col %d data_type: %d\r\n", counter, col_type);

        /* Retrieve the column name attribute and put in a locally stored
           erlang binary. Could just have used a text* for this, but convenient
           for sending back to beam */
        col_name_len = 0;
        status = OCIAttrGet((dvoid*) paramd, (ub4) OCI_DTYPE_PARAM,
                            (dvoid**) &col_name, (ub4 *) &col_name_len,
                            (ub4) OCI_ATTR_NAME,
                            (OCIError *) stmthp_res->errhp );
        
        if (status) {
            /* Explicitly free the paramd descriptor. It's not hooked to anything
               that will be garbage collected by Erlang. */
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }
        if (!enif_alloc_binary(col_name_len, &column_info->col_name)) {
            // binary creation failed. out of memory?
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return enif_raise_exception(env, ATOM_ENOMEM);
        }
        memcpy(column_info->col_name.data, col_name, col_name_len);

        /* Retrieve the length semantics for the column */
        char_semantics = 0;
        status = OCIAttrGet((void*) paramd, (ub4) OCI_DTYPE_PARAM,
                            (void*) &char_semantics,(ub4 *) 0,
                            (ub4) OCI_ATTR_CHAR_USED,
                            (OCIError *) stmthp_res->errhp);
        if (status) {
            /* Explicitly free the paramd descriptor. It's not hooked to
               anything that will be garbage collected by Erlang. */
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }
        column_info->char_semantics = char_semantics;

        /* Retrieve the column width in characters */
        col_width = 0;
        if (char_semantics) {
            status = OCIAttrGet((void*) paramd, (ub4) OCI_DTYPE_PARAM,
                                (void*) &col_width, (ub4 *) 0,
                                (ub4) OCI_ATTR_CHAR_SIZE,
                                (OCIError *) stmthp_res->errhp);
            if (status) {
                /* Explicitly free the paramd descriptor. */
                OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
                return reterr(env, stmthp_res->errhp, status);
                }
            column_info->col_size = col_width;
        } else {
            /* Retrieve the column width in bytes */
            status = OCIAttrGet((void*) paramd, (ub4) OCI_DTYPE_PARAM,
                                (void*) &col_width,(ub4 *) 0,
                                (ub4) OCI_ATTR_DATA_SIZE,
                                (OCIError *) stmthp_res->errhp);
            if (status) {
                /* Explicitly free the paramd descriptor. */
                OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
                return reterr(env, stmthp_res->errhp, status);
                }
            column_info->col_size = col_width;
        }
        // printf("Col %d col_width: %d\r\n", counter, col_width);

        /* Retrieve the column scale */
        status = OCIAttrGet((dvoid*) paramd, (ub4) OCI_DTYPE_PARAM,
                (dvoid*) &column_info->col_scale, (ub4 *) 0, (ub4) OCI_ATTR_SCALE,
                (OCIError *) stmthp_res->errhp  );
        if (status) {
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }

        /* Retrieve the column precision */
        status = OCIAttrGet((dvoid*) paramd, (ub4) OCI_DTYPE_PARAM,
                (dvoid*) &column_info->col_precision, (ub4 *) 0, (ub4) OCI_ATTR_SCALE,
                (OCIError *) stmthp_res->errhp  );
        if (status) {
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }


        // FIXME: could also retrieve Character set form and id,
        // column scale, column precision

        /* We have what we need to set up the OCIDefine for this column
           Allocate storage and call DefineByPos */
        valuep = enif_alloc(column_info->col_size);
        column_info->valuep = valuep;
        status = OCIDefineByPos(stmthp_res->stmthp, &definehp,
                                stmthp_res->errhp,
                                counter,
                                (void *)valuep,
                                column_info->col_size,
                                col_type,
                                (void *) &column_info->ind,
                                (ub2 *) NULL, // FIXME: rlenp,
                                (ub2 *) NULL, // FIXME rcodep,
                                OCI_DEFAULT );
        if (status) {
                OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
                return reterr(env, stmthp_res->errhp, status);
                }
        // printf("Col %d OCIDefineByPos OK\r\n", counter);


        counter++;
        param_status = OCIParamGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                                    stmthp_res->errhp,
                                    (dvoid **)&paramd, counter);
    }
    stmthp_res->col_info_retrieved = 1;
    return enif_make_tuple2(env, ATOM_OK, execute_result_map(env, stmthp_res));
}

static ERL_NIF_TERM ociStmtFetch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    stmthp_res *stmthp_res;
    ub4 nrows;
    
    if(!(argc == 2 &&
        enif_get_resource(env, argv[0], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_get_uint(env, argv[1], &nrows) )) {
            return enif_make_badarg(env);
        }
    
    int status = OCIStmtFetch2 (stmthp_res->stmthp, stmthp_res->errhp,
                                nrows,               // Number of rows to return
                                (ub2) OCI_DEFAULT,   // orientation
                                (sb4) 0,             // fetchOffset
                                OCI_DEFAULT);
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    } else {
        ERL_NIF_TERM list = enif_make_list(env, 0);
        for (int i = 0; i < stmthp_res->num_cols; i++) {
            ERL_NIF_TERM term;
            col_info *col = &stmthp_res->col_info[i];
            if (col->ind == (ub2) -1) {
                // returned value is NULL
                list = enif_make_list_cell(env, ATOM_NULL, list);
            } else {
                unsigned char *bin = enif_make_new_binary(env, col->col_size, &term);
                memcpy(bin, col->valuep, col->col_size);
                list = enif_make_list_cell(env, term, list);
            }
        }
        return enif_make_tuple2(env, ATOM_OK, list);
    }
}

static ErlNifFunc nif_funcs[] =
{
    // All functions that execure server round trips MUST be marked as IO_BOUND
    // Knowledge from: https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/oci-function-server-round-trips.html
    {"ociEnvNlsCreate", 0, ociEnvNlsCreate},
    {"ociSessionPoolCreate", 7, ociSessionPoolCreate, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociAuthHandleCreate", 3, ociAuthHandleCreate},
    {"ociSessionGet", 3, ociSessionGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociStmtHandleCreate", 1, ociStmtHandleCreate},
    {"ociStmtPrepare", 2, ociStmtPrepare},
    {"ociStmtExecute", 6, ociStmtExecute, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociBindByName", 6, ociBindByName},
    {"ociStmtFetch", 2, ociStmtFetch, ERL_NIF_DIRTY_JOB_IO_BOUND}
};

static void envhp_res_dtor(ErlNifEnv *env, void *resource) {
    envhp_res *res = (envhp_res*)resource;
    // printf("envhp_res_dtor called\r\n");
    if(res->envhp) {
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR );
        OCIHandleFree(res->envhp, OCI_HTYPE_ENV );
    }
}

static void spoolhp_res_dtor(ErlNifEnv *env, void *resource) {
    spoolhp_res *res = (spoolhp_res*)resource;
    //printf("spoolhp_res_ called\r\n");
    if(res->spoolhp) {
        OCISessionPoolDestroy(res->spoolhp, 
                            res->errhp,
                            OCI_SPD_FORCE );
        OCIHandleFree(res->spoolhp, OCI_HTYPE_SPOOL);
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR);
    }
}

static void authhp_res_dtor(ErlNifEnv *env, void *resource) {
    authhp_res *res = (authhp_res*)resource;
    //printf("authhp_res_ called\r\n");
    if(res->authhp) {
        OCIHandleFree(res->authhp, OCI_HTYPE_AUTHINFO );
    }
}

static void svchp_res_dtor(ErlNifEnv *env, void *resource) {
  svchp_res *res = (svchp_res*)resource;
  //printf("svchp_res_ called\r\n");
  if(res->svchp) {
        // The docs say this will even commit outstanding transactions
        // Do we really want this to happen during garbage collection?
        OCISessionRelease(res->svchp,
                        res->errhp,
                        (OraText *) NULL,
                        (ub4) 0,
                        OCI_SESSRLS_DROPSESS ); // Instant drop as GC'd
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR);
    }
}

static void stmthp_res_dtor(ErlNifEnv *env, void *resource) {
    stmthp_res *res = (stmthp_res*)resource;
    // printf("stmthp_res_ called\r\n");
    if (res->col_info) {
        free_col_info(res->col_info, res->num_cols);
        res->col_info = NULL;
        res->num_cols = 0;
    }
  if(res->stmthp) {
    OCIHandleFree(res->errhp, OCI_HTYPE_ERROR );
    OCIHandleFree(res->stmthp, OCI_HTYPE_STMT );
  }
}

static void bindhp_res_dtor(ErlNifEnv *env, void *resource) {
    bindhp_res *res = (bindhp_res*)resource;
    // printf("bindhp_res_ called\r\n");
    // OCI will free the bind handle when the stmthp is released
    // so here we just need to free the name and value binaries
    enif_release_binary(&res->name);
    enif_release_binary(&res->value);
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
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_NULL = enif_make_atom(env, "NULL");
    ATOM_COLS = enif_make_atom(env, "cols");
    ATOM_ROWIDS = enif_make_atom(env, "rowids");
    ATOM_ENOMEM = enif_make_atom(env, "enomem");
    return 0;
}

ERL_NIF_INIT(erloci_nif_drv,nif_funcs,&load,NULL,NULL,NULL)