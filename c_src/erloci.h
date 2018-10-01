#pragma once
#ifndef __ERLOCI_H__
#define __ERLOCI_H__

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
static ERL_NIF_TERM ATOM_OCI_ERROR;
static ERL_NIF_TERM ATOM_NULL;
static ERL_NIF_TERM ATOM_COLS;
static ERL_NIF_TERM ATOM_ROWIDS;
static ERL_NIF_TERM ATOM_ENOMEM;
static ERL_NIF_TERM ATOM_STATEMENT;
static ERL_NIF_TERM ATOM_PONG;
static ERL_NIF_TERM ATOM_PANG;
static ERL_NIF_TERM STRING_FILE;

#define C_TYPE_MIN 0
#define C_TYPE_TEXT 0
#define C_TYPE_UB1 1
#define C_TYPE_UB2 2
#define C_TYPE_UB4 3
#define C_TYPE_SB1 4
#define C_TYPE_SB2 5
#define C_TYPE_SB4 6
#define C_TYPE_MAX 6

typedef struct
{
	OCIEnv *envhp;
	OCIError *errhp;
} envhp_res;

typedef struct
{
	OCISPool *spoolhp;
	OCIError *errhp;
	OraText *poolName;
	ub4 poolNameLen;
} spoolhp_res;

typedef struct
{
	OCIAuthInfo *authhp;
} authhp_res;

typedef struct
{
	OCISvcCtx *svchp;
	OCIError *errhp;
} svchp_res;

// struct to keep the definitions of Define columns around
typedef struct
{
	ub4 col_type;
	ub1 char_semantics;
	ub2 col_size;
	ub1 col_scale;
	ub2 col_precision;
	ErlNifBinary col_name;
	OCIDefine *definehp;
	sb2 *indp;	// OUT - array of NULL? indicator placed here
	ub2 *rlenp;   // OUT - array of row lenghts
	void *valuep; // OUT - array of output values
} col_info;

typedef struct
{
	OCIStmt *stmthp;		// statement handle
	OCIRowid *rowidhp;		// Row ID handle for retrieving row ids
	OCIError *errhp;		// per statement error handle
	ub2 stmt_type;			// Retrieved statement type (e.g. OCI_SELECT)
	int col_info_retrieved; // Set on first call to execute
	ub4 num_cols;			// number of define columns for select
	ub4 num_rows_reserved;
	col_info *col_info; // array of *col_info for define data
} stmthp_res;

// One of these is created for each bind variable. They end up stored in a map
typedef struct
{
	OCIBind *bindhp;
	ErlNifBinary name; // Binding placeholder name
	ub2 dty;		   // Oracle data type
	size_t value_sz;   // size of the currently stored value
	size_t alloced_sz;
	void *valuep;
} bindhp_res;

#define TRACE printf("[%s:%s:%d]\r\n", __FILE__, __FUNCTION__, __LINE__)
#define CALL_TRACE	\
	printf("[%s:%s:%d] called\r\n", __FILE__, __FUNCTION__, __LINE__)
#define RETURNED_TRACE	\
	printf("[%s:%s:%d] returned\r\n", __FILE__, __FUNCTION__, __LINE__)

#define RAISE_OCI_EXCEPTION(__EC, __EB)                                                               \
	enif_raise_exception(                                                                             \
		env,                                                                                          \
		enif_make_tuple5(                                                                             \
			env, ATOM_OCI_ERROR, STRING_FILE, enif_make_int(env, __LINE__), enif_make_int(env, __EC), \
			enif_make_string(env, (const char *)(__EB), ERL_NIF_LATIN1)))

#define RAISE_EXCEPTION(__EB)                                           \
	enif_raise_exception(                                               \
		env,                                                            \
		enif_make_tuple4(                                               \
			env, ATOM_ERROR, STRING_FILE, enif_make_int(env, __LINE__), \
			enif_make_string(env, (const char *)(__EB), ERL_NIF_LATIN1)))

#endif // __ERLOCI_H__
