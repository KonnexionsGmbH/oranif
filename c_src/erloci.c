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
static ERL_NIF_TERM ATOM_ERROR;
static ERL_NIF_TERM ATOM_NULL;
static ERL_NIF_TERM ATOM_COLS;
static ERL_NIF_TERM ATOM_ROWIDS;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_STATEMENT;
static ERL_NIF_TERM ATOM_PONG;
static ERL_NIF_TERM ATOM_PANG;


#define C_TYPE_MIN 0
#define C_TYPE_TEXT 0
#define C_TYPE_UB1 1
#define C_TYPE_UB2 2
#define C_TYPE_UB4 3
#define C_TYPE_SB1 4
#define C_TYPE_SB2 5
#define C_TYPE_SB4 6
#define C_TYPE_MAX 6

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
    sb2 *indp;              // OUT - array of NULL? indicator placed here
    ub2 *rlenp;             // OUT - array of row lenghts
    void *valuep;           // OUT - array of output values
} col_info;

typedef struct {
    OCIStmt *stmthp;        // statement handle
    OCIRowid *rowidhp;      // Row ID handle for retrieving row ids
    OCIError *errhp;        // per statement error handle
    ub2 stmt_type;          // Retrieved statement type (e.g. OCI_SELECT)
    int col_info_retrieved; // Set on first call to execute
    ub4 num_cols;           // number of define columns for select
    ub4 num_rows_reserved;
    col_info *col_info;     // array of *col_info for define data
} stmthp_res;

// One of these is created for each bind variable. They end up stored in a map
typedef struct {
    OCIBind *bindhp;
    ErlNifBinary name;   // Binding placeholder name
    ub2 dty;             // Oracle data type
    size_t value_sz;     // size of the currently stored value
    size_t alloced_sz;
    void *valuep;
} bindhp_res;

static void free_col_info(col_info *col_info, int num_cols) {
    /* If we re-prepare an existing stmt handle we must manually
    reset the retrieved col info from previous executions.
    Also needed by the GC callbacks when our stmt resource is
    destroyed
    */
   int i;
    for (i = 0; i < num_cols; i++) {
        enif_release_binary(&col_info[i].col_name);
    }
    for (i = 0; i < num_cols; i++) {
        enif_free(col_info[i].indp);
        enif_free(col_info[i].rlenp);
        enif_free(col_info[i].valuep);
    }
    enif_free(col_info);
}

static void envhp_res_dtor(ErlNifEnv *env, void *resource) {
    envhp_res *res = (envhp_res*)resource;
    // printf("envhp_res_dtor called\r\n");
    if(res->envhp) {
        OCIHandleFree(res->envhp, OCI_HTYPE_ENV );
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR );
        res->envhp = NULL;
        res->errhp = NULL;
    }
}

static void spoolhp_res_dtor(ErlNifEnv *env, void *resource) {
    spoolhp_res *res = (spoolhp_res*)resource;
    // printf("spoolhp_res_ called\r\n");
    if(res->spoolhp) {
        OCISessionPoolDestroy(res->spoolhp, 
                            res->errhp,
                            OCI_SPD_FORCE );
        OCIHandleFree(res->spoolhp, OCI_HTYPE_SPOOL);
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR);
        res->spoolhp = NULL;
        res->errhp = NULL;
    }
}

static void authhp_res_dtor(ErlNifEnv *env, void *resource) {
    authhp_res *res = (authhp_res*)resource;
    // printf("authhp_res_ called\r\n");
    if(res->authhp) {
        OCIHandleFree(res->authhp, OCI_HTYPE_AUTHINFO );
        res->authhp = NULL;
    }
}

static void svchp_res_dtor(ErlNifEnv *env, void *resource) {
  svchp_res *res = (svchp_res*)resource;
  // printf("svchp_res_ called\r\n");
  if(res->svchp) {
        // The docs say this will even commit outstanding transactions
        // Do we really want this to happen during garbage collection?
        OCISessionRelease(res->svchp,
                        res->errhp,
                        (OraText *) NULL,
                        (ub4) 0,
                        OCI_SESSRLS_DROPSESS ); // Instant drop as GC'd
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR);
        res->svchp = NULL;
        res->errhp = NULL;
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
        OCIDescriptorFree(res->rowidhp, OCI_DTYPE_ROWID);
        OCIHandleFree(res->errhp, OCI_HTYPE_ERROR );
        OCIHandleFree(res->stmthp, OCI_HTYPE_STMT );
        res->rowidhp = NULL;
        res->errhp = NULL;
        res->stmthp = NULL;
  }
}

static void bindhp_res_dtor(ErlNifEnv *env, void *resource) {
    bindhp_res *res = (bindhp_res*)resource;
    // OCI will free the bind handle when the stmthp is released
    // so here we just need to free the name and value binaries
    enif_release_binary(&res->name);
    if (res->valuep) {
        enif_free(res->valuep);
    }
}

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
    ERL_NIF_TERM map, result_map;
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
        enif_make_map_put(env, new_map, ATOM_STATEMENT,
                          enif_make_int(env, stmthp->stmt_type), &result_map);
        return result_map;
    } else if (stmthp->stmt_type == OCI_STMT_INSERT
               || stmthp->stmt_type == OCI_STMT_UPDATE
               || stmthp->stmt_type == OCI_STMT_DELETE) {
        // Return any updated row_ids
        enif_make_map_put(env, map, ATOM_STATEMENT,
                          enif_make_int(env, stmthp->stmt_type), &result_map);
        return result_map;
    } else {
        enif_make_map_put(env, map, ATOM_STATEMENT,
                          enif_make_int(env, stmthp->stmt_type), &result_map);
        return result_map;
    }
}

static ERL_NIF_TERM ociEnvNlsCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIEnv *envhp = (OCIEnv *)NULL;
    OCIError *errhp = (OCIError *)NULL;
    ub4 charset, ncharset;
    text errbuf[OCI_ERROR_MAXMSG_SIZE2];
    sb4 errcode;

    if(!(argc == 2 &&
        enif_get_uint(env, argv[0], &charset) &&
        enif_get_uint(env, argv[1], &ncharset) )) {
            return enif_make_badarg(env);
        }

    int status = OCIEnvNlsCreate(&envhp,
                                OCI_THREADED,
                                NULL, NULL, NULL, NULL,
                                (size_t) 0,
                                (void **)NULL,
                                (ub2) charset, (ub2) ncharset);
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

static ERL_NIF_TERM ociEnvHandleFree(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *res;

    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&res) )) {
            return enif_make_badarg(env);
        }
    envhp_res_dtor(env, res);
    return ATOM_OK;
}

static ERL_NIF_TERM ociTerminate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCITerminate(OCI_DEFAULT);
    return ATOM_OK;
}

static ERL_NIF_TERM ociNlsGetInfo(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    ub4 item;
    text buf[OCI_NLS_MAXBUFSZ];
    ERL_NIF_TERM binary;

    if(!(argc == 2 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_uint(env, argv[1], &item) )) {
            return enif_make_badarg(env);
        }
    int status = OCINlsGetInfo(envhp_res->envhp, envhp_res->errhp, buf,
                                OCI_NLS_MAXBUFSZ, (ub2) item);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }
    size_t size = strnlen((const char *)buf, OCI_NLS_MAXBUFSZ);
    unsigned char *bin_buf = enif_make_new_binary(env, size, &binary);
    memcpy(bin_buf, buf, size);
    return enif_make_tuple2(env, ATOM_OK, binary);
}

static ERL_NIF_TERM ociNlsCharSetIdToName(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    ub4 charset_id;
    text buf[OCI_NLS_MAXBUFSZ];
    ERL_NIF_TERM binary;

    if(!(argc == 2 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_get_uint(env, argv[1], &charset_id) )) {
            return enif_make_badarg(env);
        }
    int status = OCINlsCharSetIdToName(envhp_res->envhp, buf,
                                       OCI_NLS_MAXBUFSZ, (ub2) charset_id);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }
    size_t size = strnlen((const char *)buf, OCI_NLS_MAXBUFSZ);
    unsigned char *bin_buf = enif_make_new_binary(env, size, &binary);
    memcpy(bin_buf, buf, size);
    return enif_make_tuple2(env, ATOM_OK, binary);
}

static ERL_NIF_TERM ociNlsCharSetNameToId(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    ErlNifBinary name;

    if(!(argc == 2 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) &&
        enif_inspect_binary(env, argv[1], &name) )) {
            return enif_make_badarg(env);
        }
    ub2 charset_id = OCINlsCharSetNameToId(envhp_res->envhp, name.data);
    return enif_make_tuple2(env, ATOM_OK, enif_make_int(env, charset_id));
}

static ERL_NIF_TERM ociCharsetAttrGet(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    envhp_res *envhp_res;
    ub2 charset, ncharset;

    int status;

    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res) )) {
            return enif_make_badarg(env);
        }
    status = OCIAttrGet(envhp_res->envhp, OCI_HTYPE_ENV, &charset, (ub4 *)0,
                        OCI_ATTR_ENV_CHARSET_ID, envhp_res->errhp);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }

    status = OCIAttrGet(envhp_res->envhp, OCI_HTYPE_ENV, &ncharset, (ub4 *)0,
                        OCI_ATTR_ENV_NCHARSET_ID, envhp_res->errhp);
    if (status) {
        return reterr(env, envhp_res->errhp, status);
    }
    
    return enif_make_tuple2(env, ATOM_OK,
                            enif_make_tuple2(env, enif_make_int(env, charset),
                                                  enif_make_int(env, ncharset)));
}

static ERL_NIF_TERM ociAttrGet(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    /* Generic getting of attrs. Only supports handle types we explicitly manage
       and values of types we explicitly support */
    ub4 handle_type, data_type, attr_type;
    ERL_NIF_TERM bin_value; // value when it's text
    // ub4 uint_value;       // value when it's unsigned (erlang only unpacks ub4)
    // sb4 int_value;        // value when it's signed
    ub4 size = 100;         // size of output value
    void *handlep;
    OCIError *errorhp;
    text attributep[100];
    //void *attributep = NULL;     // void * to output value

    if(!(argc == 4 &&
        enif_get_uint(env, argv[1], &handle_type) &&
        enif_get_uint(env, argv[2], &data_type) &&
        data_type >= C_TYPE_MIN &&
        data_type <= C_TYPE_MAX &&
        enif_get_uint(env, argv[3], &attr_type) )) {
            return enif_make_badarg(env);
        }

    switch (handle_type) {
    case OCI_HTYPE_ENV: {
        envhp_res *envhp_res;
        if(!enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res)) {
            return enif_make_badarg(env);
        }
        handlep = envhp_res->envhp;
        errorhp = envhp_res->errhp;
        break;
        }
    }
    int status = OCIAttrGet(handlep, handle_type, attributep, &size, attr_type,
                            errorhp);

    if (status) {
        return reterr(env, errorhp, status);
    }
    unsigned char *bin_buf = enif_make_new_binary(env, size, &bin_value);
    if (size > 100) {
        printf("Trunated OCIAttrGet %d\r\n", size);
        size = 100;
    }
    // printf("Called OCIAttrGet %s\r\n",(text *)attributep);
    memcpy(bin_buf, attributep, size);
    return enif_make_tuple2(env, ATOM_OK, bin_value);
}

static ERL_NIF_TERM ociAttrSet(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    /* Generic setting of attrs. Only supports handle types we explicitly manage
       and values of types we explicitly support */
    ub4 handle_type, data_type, attr_type;
    ErlNifBinary bin_value; // value when it's text
    ub4 uint_value;       // value when it's unsigned (erlang only unpacks ub4)
    sb4 int_value;        // value when it's signed
    ub4 size = 0;         // size of input value - can use 0 except for binaries
    void *handlep;
    OCIError *errorhp;
    void *attributep;     // void * to value

    if(!(argc == 5 &&
        enif_get_uint(env, argv[1], &handle_type) &&
        enif_get_uint(env, argv[2], &data_type) &&
        data_type >= C_TYPE_MIN &&
        data_type <= C_TYPE_MAX &&
        enif_get_uint(env, argv[4], &attr_type) )) {
            return enif_make_badarg(env);
        }
    switch (data_type) {
    case C_TYPE_TEXT:
        if (!enif_inspect_binary(env, argv[3], &bin_value)) {
            return enif_make_badarg(env);
        }
        attributep = (void *)bin_value.data;
        size = bin_value.size;
        break;
    case C_TYPE_SB1:
    case C_TYPE_SB2:
    case C_TYPE_SB4:
        if(!enif_get_int(env, argv[3], &int_value)) {
            return enif_make_badarg(env);
        }
        attributep = (void *)&int_value;
        break;
    case C_TYPE_UB1:
    case C_TYPE_UB2:
    case C_TYPE_UB4:
        if(!enif_get_uint(env, argv[3], &uint_value)) {
            return enif_make_badarg(env);
        }
        attributep = (void *)&uint_value;
        break;
    }

    switch (handle_type) {
    case OCI_HTYPE_ENV: {
        envhp_res *envhp_res;
        if(!enif_get_resource(env, argv[0], envhp_resource_type, (void**)&envhp_res)) {
            return enif_make_badarg(env);
        }
        handlep = envhp_res->envhp;
        errorhp = envhp_res->errhp;
        break;
        }
    }
    
    int status = OCIAttrSet (handlep, handle_type,
                            attributep, size, attr_type,
                            errorhp );
    if (status) {
        return reterr(env, errorhp, status);
    }
    return ATOM_OK;
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

static ERL_NIF_TERM ociSessionPoolDestroy(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    spoolhp_res *spoolhp_res;

    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], spoolhp_resource_type, (void**)&spoolhp_res) )) {
            return enif_make_badarg(env);
        }
    if (spoolhp_res->spoolhp) {
        int status = OCISessionPoolDestroy(spoolhp_res->spoolhp, spoolhp_res->errhp,
                                         OCI_DEFAULT);
        if (status) {
            return reterr(env, spoolhp_res->errhp, status);
        }
        OCIHandleFree(spoolhp_res->spoolhp, OCI_HTYPE_SPOOL);
        OCIHandleFree(spoolhp_res->errhp, OCI_HTYPE_ERROR);
        spoolhp_res->spoolhp = NULL;
        spoolhp_res->errhp = NULL;
    }
    return ATOM_OK;
}

static ERL_NIF_TERM ociPing(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    svchp_res *svchp_res;

    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], svchp_resource_type, (void**)&svchp_res) )) {
            return enif_make_badarg(env);
        }
    int status = OCIPing(svchp_res->svchp, svchp_res->errhp, OCI_DEFAULT);
    if (status) {
        return ATOM_PANG;
    } else {
        return ATOM_PONG;
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

static ERL_NIF_TERM ociSessionRelease(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    svchp_res *svchp_res;

    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], svchp_resource_type, (void**)&svchp_res) )) {
            return enif_make_badarg(env);
        }
    int status = OCISessionRelease(svchp_res->svchp, svchp_res->errhp,
                                    NULL, (ub4) 0, (ub4) OCI_DEFAULT);
    if (status) {
        return reterr(env, svchp_res->errhp, status);
    }
    OCIHandleFree(svchp_res->errhp, OCI_HTYPE_ERROR);
    svchp_res->errhp = NULL;
    svchp_res->svchp = NULL;
    return ATOM_OK;
}

static ERL_NIF_TERM ociStmtHandleCreate(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    OCIStmt *stmthp = (OCIStmt *)NULL;
    OCIRowid *rowidhp = (OCIRowid *)NULL;
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

    status = OCIDescriptorAlloc(envhp_res->envhp, (void**)&rowidhp,
                                    OCI_DTYPE_ROWID, (size_t) 0, (void **)NULL);
    if (status) {
        OCIHandleFree(stmthp, OCI_HTYPE_STMT);
        return enif_raise_exception(env, ATOM_ENOMEM);
    }
    
    /* allocate a per statement error handle to avoid multiple 
       statements having to use the global one.
       The docs suggest one error handle per thread, but we
       don't have control over erlang thread pools */
    status = OCIHandleAlloc(envhp_res->envhp, (void  **)&errhp,
                            OCI_HTYPE_ERROR, 0, (void  **) 0);
    if (status) {
        OCIHandleFree(stmthp, OCI_HTYPE_STMT);
        OCIDescriptorFree(rowidhp, OCI_DTYPE_ROWID);
        enif_raise_exception(env, ATOM_ENOMEM);
    }

    res->stmthp = stmthp;
    res->rowidhp = rowidhp;
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

    int status = OCIStmtPrepare(stmthp_res->stmthp, stmthp_res->errhp,
                                (OraText *) statement.data, (ub4) statement.size,
                                OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    } else {
        return ATOM_OK;
    }
}

static ERL_NIF_TERM ociStmtHandleFree(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    stmthp_res *res;

    if(!(argc == 1 &&
        enif_get_resource(env, argv[0], stmthp_resource_type, (void**)&res) )) {
            return enif_make_badarg(env);
        }
    stmthp_res_dtor(env, res);
    return ATOM_OK;
}

static ERL_NIF_TERM ociBindByName(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    bindhp_res *bindhp_resource;
    stmthp_res *stmthp_res;
    int dty, ind, status;
    int is_new_resource;
    ErlNifBinary name;

    // 
    enum v_type {
        V_INT,
        V_FLOAT,
        V_DOUBLE,
        V_BINARY
    };

    // Vars for each value type.
    enum v_type v_type = V_BINARY;
    int value_int;
    double value_float, value_double;
    ErlNifBinary value_bin;

    ub4 arg_len;             // The size of memory we will need for the value
    // void *valuep = NULL;     // pointer to the value whatever c type it is

    if(!(argc == 6 &&
        enif_get_resource(env, argv[0], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_is_map(env, argv[1]) &&
        enif_inspect_binary(env, argv[2], &name) &&
        enif_get_int(env, argv[3], &ind) &&
        enif_get_int(env, argv[4], &dty))) {
            return enif_make_badarg(env);
        }

    ERL_NIF_TERM original_map = argv[1];
    ERL_NIF_TERM current_map_value;
    if(enif_get_map_value(env, original_map, argv[2], &current_map_value)) {
        if (!enif_get_resource(env, current_map_value, bindhp_resource_type, (void**)&bindhp_resource)) {
            return enif_make_badarg(env);
        }
        is_new_resource = 0;
    } else {
        // No existing bind for this name
        // Create the managed pointer / resource
        bindhp_resource = (bindhp_res *) enif_alloc_resource(bindhp_resource_type, sizeof(bindhp_res));
        if(!bindhp_resource) return enif_make_badarg(env);
        bindhp_resource->bindhp = NULL; // Ask the OCIBindByName call to allocate this
        bindhp_resource->valuep = NULL;
        bindhp_resource->value_sz = 0;
        bindhp_resource->dty = 0;
        bindhp_resource->alloced_sz = 0;
        // Make copy of the name for oci use
        if (!enif_alloc_binary(name.size, &bindhp_resource->name)) {
            return enif_raise_exception(env, ATOM_ENOMEM);
        }
        memcpy(bindhp_resource->name.data, name.data, name.size);
        is_new_resource = 1;
    }

    /* Unpack the value from Erlang, casting to match the SQL_* type
       provided by the user.
       Could be less prescriptive here. Maybe always allow binary data?
     */
    //printf("NAME: %.*s\r\n", (int)name.size, name.data);
    switch (dty) {
        case SQLT_NUM:
        case SQLT_INT:
        case SQLT_UIN:
            if (!enif_get_int(env, argv[5], &value_int)) {
                return enif_make_badarg(env);
            }
            arg_len = sizeof(int);
            v_type = V_INT;
            break;
        case SQLT_FLT:
        case SQLT_BFLOAT:
        case SQLT_IBFLOAT:
            // Could be int or float
            if (!enif_get_double(env, argv[5], &value_double)) {
                if (!enif_get_int(env, argv[5], &value_int)) {
                    return enif_make_badarg(env);
                }
                // user provided an int, cast to float
                value_float = (float)value_int;
                arg_len = sizeof(float);
                v_type = V_FLOAT;
            }
            // User provided a float
            value_float = (float)value_double;
            arg_len = sizeof(float);
            v_type = V_FLOAT;
            /*
            printf("FLOAT: %d ",v_type);
            unsigned char *p = (unsigned char *)&value_float;
            size_t i;
            for (i=0; i < arg_len; ++i)
                printf("%02x ", p[i]);
            printf("\r\n");
            */
            break;
        case SQLT_IBDOUBLE:
        case SQLT_BDOUBLE:
            if (!enif_get_double(env, argv[5], &value_double)) {
                if (!enif_get_int(env, argv[5], &value_int)) {
                    return enif_make_badarg(env);
                }
                // user provided an int, cast to double
                value_double = (double)value_int;
                arg_len = sizeof(double);
                v_type = V_DOUBLE;
            }
            // User provided a double
            arg_len = sizeof(double);
            v_type = V_DOUBLE;
            /*
            printf("DOUBLE: %d ",v_type);
            unsigned char *dp = (unsigned char *)&value_double;
            size_t di;
            for (di=0; di < arg_len; ++di)
                printf("%02x ", dp[di]);
            printf("\r\n");
            */
            break;
        case SQLT_AFC:
        case SQLT_CHR:
        case SQLT_LNG:
        case SQLT_BIN:
            if (!enif_inspect_binary(env, argv[5], &value_bin)) {
                return enif_make_badarg(env);
            }
            arg_len = value_bin.size;
            v_type = V_BINARY;
            break;
        case SQLT_INTERVAL_YM:
        case SQLT_DAT:
        case SQLT_DATE:
        case SQLT_TIMESTAMP:
        case SQLT_TIMESTAMP_LTZ:
        case SQLT_INTERVAL_DS:
        case SQLT_TIMESTAMP_TZ:
        case SQLT_RDD:
            if (!enif_inspect_binary(env, argv[5], &value_bin)) {
                return enif_make_badarg(env);
            }
            arg_len = value_bin.size;
            v_type = V_BINARY;
            break;
        case SQLT_RSET:
            // Ouput ref cursors bind values are ignored.
            break;
        default:
            return enif_make_badarg(env);
    }

    /* If it's an existing binding, the dty of the new value is the same,
       and there is enough space, we can just copy the new value into
       the existing memory and don't need to call OCIBind again.

       Special case this fast path
    */
   if (!is_new_resource &&
        bindhp_resource->dty == dty &&
        bindhp_resource->alloced_sz >= arg_len) {
        switch (v_type) {
        case V_BINARY:
            memcpy(bindhp_resource->valuep, value_bin.data, arg_len);
            bindhp_resource->value_sz = arg_len;
            break;
        case V_INT:
            *(int *)bindhp_resource->valuep = value_int;
            break;
        case V_FLOAT:
            *(float *)bindhp_resource->valuep = value_float;
            break;
        case V_DOUBLE:
            *(double *)bindhp_resource->valuep = value_double;
            break;
        }
        return enif_make_tuple2(env, ATOM_OK, original_map);
    }

    bindhp_resource->dty = dty;

    /* From this point we know we must call OCIBind, but we might
       or might not be able to reuse the allocated memory */

    if (is_new_resource) {
        bindhp_resource->valuep = enif_alloc(arg_len);
        if (!bindhp_resource->valuep) {
            return enif_raise_exception(env, ATOM_ENOMEM);
        }
        bindhp_resource->alloced_sz = arg_len;
    } else if (!is_new_resource && bindhp_resource->alloced_sz < arg_len) {
        void *tmp = enif_realloc(bindhp_resource->valuep, arg_len);
        if (!tmp) {
            return enif_raise_exception(env, ATOM_ENOMEM);
        }
        bindhp_resource->valuep = tmp;
        bindhp_resource->alloced_sz = arg_len;
    } // else we already have enough memory

    /* We have a bindhp struct, reserved memory, and our value, it's
       time to put them together */
    switch (v_type) {
    case V_INT:
        *(int *)bindhp_resource->valuep = value_int;
        bindhp_resource->value_sz = arg_len;
        //printf("VALUE INT: %d with sz %zu\r\n", *(int *)bindhp_resource->valuep, bindhp_resource->value_sz);
        break;
    case V_FLOAT:
        *(float *)bindhp_resource->valuep = value_float;
        bindhp_resource->value_sz = arg_len;
        //printf("VALUE FLOAT: %f with sz %zu\r\n", value_float, bindhp_resource->value_sz);
        break;
    case V_DOUBLE:
        *(double *)bindhp_resource->valuep = value_double;
        bindhp_resource->value_sz = arg_len;
        //printf("VALUE DOUBLE: %f with sz %zu\r\n", *(double *)bindhp_resource->valuep, bindhp_resource->value_sz);
        break;
    case V_BINARY:
        memcpy(bindhp_resource->valuep, value_bin.data, arg_len);
        bindhp_resource->value_sz = arg_len;
        //printf("VALUE BUF: %.*s with sz %zu\r\n", arg_len, value_bin.data, bindhp_resource->value_sz);
    }
    status = OCIBindByName(stmthp_res->stmthp, &bindhp_resource->bindhp,
                stmthp_res->errhp,
                (OraText *) bindhp_resource->name.data, (sword) bindhp_resource->name.size, // Bind name
                bindhp_resource->valuep, (sword) bindhp_resource->value_sz, // Bind value
                (sb2) dty,      // Oracle DataType
                (void *) &ind,  // Indicator variable
                (ub2*) NULL,    // address of actual length
                (ub2*) NULL,    // address of return code
                0,              // max array length for PLSQL indexed tables
                (ub4*) NULL,    // current array length for PLSQL indexed tables
                OCI_DEFAULT);   // mode
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    }
    // If it's a new resource put it in the map
    if (is_new_resource) {
        ERL_NIF_TERM return_map;
        ERL_NIF_TERM nif_res = enif_make_resource(env, bindhp_resource);
        enif_release_resource(bindhp_resource);
        if (!enif_make_map_put(env, original_map, argv[2], nif_res, &return_map)) {
            return enif_raise_exception(env, ATOM_ENOMEM);
        }
        return enif_make_tuple2(env, ATOM_OK, return_map);

    } else {
        return enif_make_tuple2(env, ATOM_OK, original_map);
    }
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

    if (stmthp_res->stmt_type == OCI_STMT_SELECT && stmthp_res->col_info_retrieved) {
        // Column info already populated by a previous call to OCIStmtExecute
        return enif_make_tuple2(env, ATOM_OK,
                                execute_result_map(env, stmthp_res));
    }

    if (stmthp_res->stmt_type == OCI_STMT_INSERT
        || stmthp_res->stmt_type == OCI_STMT_UPDATE
        || stmthp_res->stmt_type == OCI_STMT_DELETE) {
        // Retrieve the row count
        ub4 rc = 0;
        status = OCIAttrGet(stmthp_res->stmthp, OCI_HTYPE_STMT, &rc, 0,
                    OCI_ATTR_ROW_COUNT, stmthp_res->errhp);
        if (status) {
            return reterr(env, stmthp_res->errhp, status);
        }

        if (rc > 0) {
            status = OCIAttrGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                                stmthp_res->rowidhp, 0, OCI_ATTR_ROWID, stmthp_res->errhp);
            if (status) {
                return reterr(env, stmthp_res->errhp, status);
            }
            // printf("ROW COUNT %d\r\n", rc);

            OraText *rowID = NULL;
            ub2 size = 0;
            ERL_NIF_TERM row_id_bin;
            OCIRowidToChar(stmthp_res->rowidhp, rowID, &size, stmthp_res->errhp);
            unsigned char *buf = enif_make_new_binary(env, size, &row_id_bin);
            OCIRowidToChar(stmthp_res->rowidhp, buf, &size, stmthp_res->errhp);
            // printf("ROWD STR %.*s\r\n", size, buf);
        }
    }

    if (stmthp_res->stmt_type != OCI_STMT_SELECT) {
        return enif_make_tuple2(env, ATOM_OK,
                                execute_result_map(env, stmthp_res));
    }
    // From here on it's SELECT only
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
        // We want simple numbers out of the database as VARNUMs for decoding
        // on the Erlang side
        if (col_type == SQLT_NUM || col_type == SQLT_INT || col_type == SQLT_UIN) {
            column_info->col_type = SQLT_VNU;
        }
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
                (dvoid*) &column_info->col_precision, (ub4 *) 0, (ub4) OCI_ATTR_PRECISION,
                (OCIError *) stmthp_res->errhp  );
        if (status) {
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }

        // FIXME: could also retrieve Character set form and id

        /* We have what we need to set up the OCIDefine for this column
           Allocate storage and call DefineByPos */
        if (column_info->col_type == SQLT_VNU) {
            // FIXME: Why do we need to do this to avoid truncation?
            column_info->col_size++;
        }

        column_info->valuep = enif_alloc(column_info->col_size);
        column_info->indp = enif_alloc(sizeof(sb2));
        column_info->rlenp = enif_alloc(sizeof(ub2));

        if (!column_info->valuep || !column_info->indp || !column_info->rlenp) {
            OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
            return reterr(env, stmthp_res->errhp, status);
        }
        memset(column_info->valuep, 0, column_info->col_size);
        memset(column_info->indp, 0, sizeof(sb2));
        memset(column_info->rlenp, 0, sizeof(ub2));

        status = OCIDefineByPos(stmthp_res->stmthp, &definehp,
                                stmthp_res->errhp,
                                counter,
                                (void *)column_info->valuep,
                                column_info->col_size,
                                column_info->col_type,
                                (sb2 *) column_info->indp,
                                (ub2 *) column_info->rlenp,
                                (ub2 *) NULL, // FIXME rcodep,
                                OCI_DEFAULT );
        if (status) {
                OCIDescriptorFree(paramd, OCI_DTYPE_PARAM);
                return reterr(env, stmthp_res->errhp, status);
                }

        counter++;
        // param_status will get us out of the while loop
        param_status = OCIParamGet(stmthp_res->stmthp, OCI_HTYPE_STMT,
                                    stmthp_res->errhp,
                                    (dvoid **)&paramd, counter);
    }
    stmthp_res->col_info_retrieved = 1;
    stmthp_res->num_rows_reserved = 1;
    return enif_make_tuple2(env, ATOM_OK, execute_result_map(env, stmthp_res));
}

static ERL_NIF_TERM ociStmtFetch(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    stmthp_res *stmthp_res;
    ub4 nrows;
    ub4 fetched_rows;
    
    if(!(argc == 2 &&
        enif_get_resource(env, argv[0], stmthp_resource_type, (void**)&stmthp_res) &&
        enif_get_uint(env, argv[1], &nrows) )) {
            return enif_make_badarg(env);
        }
    if (nrows > stmthp_res->num_rows_reserved) {
        // Not enough space for the rows requested, alloc more and rebind
        for (int i = 0; i < stmthp_res->num_cols; i++) {
            col_info *column_info = &stmthp_res->col_info[i];
            void *valuep = enif_realloc(column_info->valuep, column_info->col_size * nrows);
            if (!valuep) {
                return enif_raise_exception(env, ATOM_ENOMEM);
            }
            column_info->valuep = valuep;

            void *indp = enif_realloc(column_info->indp, sizeof(sb2) * nrows);
            if (!indp) {
                return enif_raise_exception(env, ATOM_ENOMEM);
            }
            column_info->indp = indp;

            void *rlenp = enif_realloc(column_info->rlenp, sizeof(ub2) * nrows);
            if (!rlenp) {
                return enif_raise_exception(env, ATOM_ENOMEM);
            }
            column_info->rlenp = rlenp;

            int status = OCIDefineByPos(stmthp_res->stmthp, &column_info->definehp,
                                stmthp_res->errhp,
                                i + 1,
                                (void *)valuep,
                                column_info->col_size,
                                column_info->col_type,
                                (sb2 *) indp,
                                (ub2 *) rlenp,
                                (ub2 *) NULL, // FIXME rcodep,
                                OCI_DEFAULT );
            if (status) {
                return reterr(env, stmthp_res->errhp, status);
                }
        }
        stmthp_res->num_rows_reserved = nrows;
    }
    
    int status = OCIStmtFetch2 (stmthp_res->stmthp, stmthp_res->errhp,
                                nrows,               // Number of rows to return
                                (ub2) OCI_DEFAULT,   // orientation
                                (sb4) 0,             // fetchOffset
                                OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_NO_DATA) {
        return reterr(env, stmthp_res->errhp, status);
    }
    status = OCIAttrGet(stmthp_res->stmthp, (ub4) OCI_HTYPE_STMT,
                            (void*) &fetched_rows,(ub4 *) 0,
                            OCI_ATTR_ROWS_FETCHED,
                            stmthp_res->errhp);
    if (status) {
        return reterr(env, stmthp_res->errhp, status);
    }

    ERL_NIF_TERM col_list = enif_make_list(env, 0);
    // 
    for (int i = 0; i < fetched_rows; i++) {
        ERL_NIF_TERM row_list = enif_make_list(env, 0);
        for (int j = 0; j < stmthp_res->num_cols; j++) {
            col_info *col = &stmthp_res->col_info[j];
            if (*(col->indp + i) == (sb2) -1) {
                // returned value is NULL
                row_list = enif_make_list_cell(env, ATOM_NULL, row_list);
            } else {
                ERL_NIF_TERM term;
                ub2 rlen = *(col->rlenp + i);
                if (col->col_type == SQLT_VNU) rlen++;
                
                unsigned char *bin = enif_make_new_binary(env, rlen, &term);
                memcpy(bin, (char *)col->valuep + i * col->col_size, rlen);

                row_list = enif_make_list_cell(env, term, row_list);
            }
        }
        col_list = enif_make_list_cell(env, row_list, col_list);
    }
    ERL_NIF_TERM finished;
    if (status == OCI_NO_DATA || fetched_rows < nrows) {
        finished = ATOM_TRUE;
    } else {
        finished = ATOM_FALSE;
    }
    return enif_make_tuple3(env, ATOM_OK, col_list, finished);
}

static ErlNifFunc nif_funcs[] =
{
    // All functions that execure server round trips MUST be marked as IO_BOUND
    // Knowledge from: https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnoci/oci-function-server-round-trips.html
    {"ociEnvNlsCreate", 2, ociEnvNlsCreate},
    {"ociEnvHandleFree", 1, ociEnvHandleFree},
    {"ociTerminate", 0, ociTerminate, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociNlsGetInfo", 2, ociNlsGetInfo},
    {"ociNlsCharSetIdToName", 2, ociNlsCharSetIdToName},
    {"ociNlsCharSetNameToId_ll", 2, ociNlsCharSetNameToId},
    {"ociCharsetAttrGet", 1, ociCharsetAttrGet},
    {"ociAttrSet", 5, ociAttrSet},
    {"ociAttrGet", 4, ociAttrGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociSessionPoolCreate", 7, ociSessionPoolCreate, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociSessionPoolDestroy", 1, ociSessionPoolDestroy, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociAuthHandleCreate", 3, ociAuthHandleCreate},
    {"ociPing", 1, ociPing, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociSessionGet", 3, ociSessionGet, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociSessionRelease", 1, ociSessionRelease, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociStmtHandleCreate", 1, ociStmtHandleCreate},
    {"ociStmtPrepare", 2, ociStmtPrepare},
    {"ociStmtHandleFree", 1, ociStmtHandleFree},
    {"ociStmtExecute", 6, ociStmtExecute, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"ociBindByName", 6, ociBindByName},
    {"ociStmtFetch", 2, ociStmtFetch, ERL_NIF_DIRTY_JOB_IO_BOUND}
};


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
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_NULL = enif_make_atom(env, "NULL");
    ATOM_COLS = enif_make_atom(env, "cols");
    ATOM_ROWIDS = enif_make_atom(env, "rowids");
    ATOM_ENOMEM = enif_make_atom(env, "enomem");
    ATOM_STATEMENT = enif_make_atom(env, "statement");
    ATOM_PONG = enif_make_atom(env, "pong");
    ATOM_PANG = enif_make_atom(env, "pang");
    return 0;
}

ERL_NIF_INIT(erloci_drv,nif_funcs,&load,NULL,NULL,NULL)