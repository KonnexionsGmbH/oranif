// compile with:
// gcc varArrayMec.c ../../c_src/odpi/embed/dpi.c -I"../../c_src/opdi/include"

#define p(text)         \
	printf(#text "\n"); \
	fflush(stdout);
#define pf(text)  \
	printf(text); \
	fflush(stdout);
#include <stdio.h>  // printf
#include <string.h> // strlen



#include "../../c_src/odpi/include/dpi.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>

#ifdef _MSC_VER
#if _MSC_VER < 1900
#define PRId64 "I64d"
#define PRIu64 "I64u"
#endif
#endif

#ifndef PRIu64
#include <inttypes.h>
#endif

#define L(_str, ...)                                                \
	printf("[%s:%s:%d]\r\n "_str, __FILE__, __FUNCTION__, __LINE__, \
		   ##__VA_ARGS__);                                          \
	fflush(stdout);

#define LSTR(_var)                                             \
	if (_var.data == NULL)                                     \
	{                                                          \
		L(#_var " is NULL\r\n");                               \
	}                                                          \
	else                                                       \
	{                                                          \
		L(#_var " : '%.*s'\r\n", (int)_var.length, _var.data); \
	}

#define LINT(_var)                            \
	if (_var == NULL)                         \
	{                                         \
		L(#_var " is NULL\r\n");              \
	}                                         \
	else                                      \
	{                                         \
		L(#_var " : %" PRId64 "\r\n", *_var); \
	}

#define TRACE L("TRACE\r\n")

#define E(_str, ...)                                                      \
	printf("[%s:%s:%d] ERROR\r\n "_str, __FILE__, __FUNCTION__, __LINE__, \
		   ##__VA_ARGS__);                                                \
	fflush(stdout);

#define DPI_CHECK_ERROR(__ctx, __exprn)                                 \
	if ((__exprn) < 0)                                                  \
	{                                                                   \
		dpiContext_getError((__ctx)->context, &((__ctx)->errorInfo));   \
		E("DPI_ERROR\r\n %.*s (%s : %s)\r\n",                           \
		  (__ctx)->errorInfo.messageLength, (__ctx)->errorInfo.message, \
		  (__ctx)->errorInfo.fnName, (__ctx)->errorInfo.action);        \
		return -1;                                                      \
	}

typedef struct
{
	dpiContext *context;
	dpiConn *conn;
	dpiErrorInfo errorInfo;
} dpiCtx_res;

extern int connect(
	const char *userName, uint32_t userNameLength,
	const char *password, uint32_t passwordLength,
	const char *connectString, uint32_t connectStringLength,
	dpiCtx_res *ctx);

extern int close(dpiCtx_res *ctx);
extern int commit(dpiCtx_res *ctx);
extern int rollback(dpiCtx_res *ctx);
extern int ping(dpiCtx_res *ctx);

typedef struct
{
	char *data;
	uint32_t length;
} string;

typedef struct
{
	dpiVar *var;
	dpiData *data;
} param;

#define COPY_STR(_var)                                               \
	if (_var.data)                                                   \
	{                                                                \
		DPI_CHECK_ERROR(                                             \
			ctx, dpiVar_setFromBytes(                                \
					 stmtctx->_var.var, 0, _var.data, _var.length)); \
	}                                                                \
	else                                                             \
		stmtctx->_var.data->isNull = 1;

#define COPY_INT(_var)                             \
	if (_var == NULL)                              \
		stmtctx->_var.data->isNull = 1;            \
	else                                           \
	{                                              \
		stmtctx->_var.data->isNull = 0;            \
		stmtctx->_var.data->value.asInt64 = *_var; \
	}

#define NEW_INT_VAR(_var)                                                     \
	DPI_CHECK_ERROR(                                                          \
		ctx, dpiConn_newVar(                                                  \
				 ctx->conn, DPI_ORACLE_TYPE_NUMBER, DPI_NATIVE_TYPE_INT64, 1, \
				 0, 0, 0, NULL, &stmtctx->_var.var, &stmtctx->_var.data))

#define NEW_STR_VAR(_var)                                                      \
	DPI_CHECK_ERROR(                                                           \
		ctx, dpiConn_newVar(                                                   \
				 ctx->conn, DPI_ORACLE_TYPE_VARCHAR, DPI_NATIVE_TYPE_BYTES, 1, \
				 4000, 0, 0, NULL, &stmtctx->_var.var, &stmtctx->_var.data));

#define BIND(_var)               \
	DPI_CHECK_ERROR(             \
		ctx, dpiStmt_bindByName( \
				 stmtctx->stmt, #_var, strlen(#_var), stmtctx->_var.var));


#define SP_INSERT_CSV                      \
	"begin PKG_MEC_IC_CSV.SP_INSERT_CSV (" \
	"	:p_BiHId_In_VarChar2,"               \
	"	:p_BatchSize_In_Integer,"            \
	"	:p_MaxAge_In_Number,"                \
	"	:p_DataHeader_In_VarChar2,"          \
                                           \
	"	:p_RecordNr_In_ArrRecNr,"            \
	"	:p_RecordData_In_ArrRecData,"        \
                                           \
	"	:p_RecCount_InOut_Number,"           \
	"	:p_PreParseErrCount_InOut_Number,"   \
	"	:p_ErrCount_InOut_Number,"           \
	"	:p_DateFc_InOut_Varchar2,"           \
	"	:p_DateLc_InOut_Varchar2,"           \
                                           \
	"	:p_ErrorCode_Out_Number,"            \
	"	:p_ErrorDesc_Out_Varchar2,"          \
	"	:p_ReturnStatus_Out_Number"          \
	"); end;"

typedef struct
{
	dpiStmt *stmt;

	param p_BiHId_In_VarChar2;
	param p_BatchSize_In_Integer;
	param p_MaxAge_In_Number;
	param p_DataHeader_In_VarChar2;

	param p_RecordNr_In_ArrRecNr;
	param p_RecordData_In_ArrRecData;

	param p_RecCount_InOut_Number;
	param p_PreParseErrCount_InOut_Number;
	param p_ErrCount_InOut_Number;
	param p_DateFc_InOut_Varchar2;
	param p_DateLc_InOut_Varchar2;

	param p_ErrorCode_Out_Number;
	param p_ErrorDesc_Out_Varchar2;
	param p_ReturnStatus_Out_Number;
} sp_insert_csv_ctx;

extern int sp_insert_csv(
	dpiCtx_res *ctx,
	sp_insert_csv_ctx *stmtctx);

extern int sp_insert_csv_values(
	dpiCtx_res *ctx,
	sp_insert_csv_ctx *stmtctx,

	string p_BiHId_In_VarChar2,
	int64_t *p_BatchSize_In_Integer,
	int64_t *p_MaxAge_In_Number,
	string p_DataHeader_In_VarChar2,

	const int64_t p_RecordNr_In_ArrRecNr[],
	const string p_RecordData_In_ArrRecData[],

	int64_t *p_RecCount_InOut_Number,
	int64_t *p_PreParseErrCount_InOut_Number,
	int64_t *p_ErrCount_InOut_Number,

	string p_DateFc_InOut_Varchar2,
	string p_DateLc_InOut_Varchar2);

extern int sp_insert_csv_execute(
	dpiCtx_res *ctx,
	sp_insert_csv_ctx stmtctx);

extern int sp_insert_csv_cleanup(
	dpiCtx_res *ctx,
	sp_insert_csv_ctx stmtctx);


int connect(
	const char *userName, uint32_t userNameLength,
	const char *password, uint32_t passwordLength,
	const char *connectString, uint32_t connectStringLength,
	dpiCtx_res *ctx)
{
	dpiErrorInfo e;

	if (dpiContext_create(
			DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &(ctx->context), &(ctx->errorInfo)) < 0)
	{
		E("dpiContext_create");
		return -1;
	}

	DPI_CHECK_ERROR(
		ctx,
		dpiConn_create(
			ctx->context, userName, userNameLength,
			password, passwordLength,
			connectString, connectStringLength,
			NULL, NULL, &(ctx->conn)));

	return 0;
}

int sp_insert_csv(dpiCtx_res *ctx, sp_insert_csv_ctx *stmtctx)
{
	DPI_CHECK_ERROR(
		ctx,
		dpiConn_prepareStmt(
			ctx->conn, 0, SP_INSERT_CSV, strlen(SP_INSERT_CSV), NULL, 0,
			&stmtctx->stmt));

	// In parameters
	NEW_STR_VAR(p_BiHId_In_VarChar2);
	NEW_INT_VAR(p_BatchSize_In_Integer);
	NEW_INT_VAR(p_MaxAge_In_Number);
	NEW_STR_VAR(p_DataHeader_In_VarChar2);

	DPI_CHECK_ERROR(
		ctx,
		dpiConn_newVar(
			ctx->conn, DPI_ORACLE_TYPE_NUMBER, DPI_NATIVE_TYPE_INT64, 1024,
			0, 0, 1, NULL,
			&stmtctx->p_RecordNr_In_ArrRecNr.var,
			&stmtctx->p_RecordNr_In_ArrRecNr.data));

	DPI_CHECK_ERROR(
		ctx,
		dpiConn_newVar(
			ctx->conn, DPI_ORACLE_TYPE_VARCHAR, DPI_NATIVE_TYPE_BYTES, 1024,
			4000, 0, 1, NULL,
			&stmtctx->p_RecordData_In_ArrRecData.var,
			&stmtctx->p_RecordData_In_ArrRecData.data));

	// InOut parameters
	NEW_INT_VAR(p_RecCount_InOut_Number);
	NEW_INT_VAR(p_PreParseErrCount_InOut_Number);
	NEW_INT_VAR(p_ErrCount_InOut_Number);

	NEW_STR_VAR(p_DateFc_InOut_Varchar2);
	NEW_STR_VAR(p_DateLc_InOut_Varchar2);

	// Out parameters
	NEW_INT_VAR(p_ErrorCode_Out_Number);
	NEW_STR_VAR(p_ErrorDesc_Out_Varchar2);
	NEW_INT_VAR(p_ReturnStatus_Out_Number);

	// Binds
	BIND(p_BiHId_In_VarChar2);
	BIND(p_BatchSize_In_Integer);
	BIND(p_MaxAge_In_Number);
	BIND(p_DataHeader_In_VarChar2);
	BIND(p_RecordNr_In_ArrRecNr);
	BIND(p_RecordData_In_ArrRecData);
	BIND(p_RecCount_InOut_Number);
	BIND(p_PreParseErrCount_InOut_Number);
	BIND(p_ErrCount_InOut_Number);
	BIND(p_DateFc_InOut_Varchar2);
	BIND(p_DateLc_InOut_Varchar2);
	BIND(p_ErrorCode_Out_Number);
	BIND(p_ErrorDesc_Out_Varchar2);
	BIND(p_ReturnStatus_Out_Number);

	return 0;
}

int sp_insert_csv_values(
	dpiCtx_res *ctx,
	sp_insert_csv_ctx *stmtctx,

	string p_BiHId_In_VarChar2,
	int64_t *p_BatchSize_In_Integer,
	int64_t *p_MaxAge_In_Number,
	string p_DataHeader_In_VarChar2,

	const int64_t p_RecordNr_In_ArrRecNr[],
	const string p_RecordData_In_ArrRecData[],

	int64_t *p_RecCount_InOut_Number,
	int64_t *p_PreParseErrCount_InOut_Number,
	int64_t *p_ErrCount_InOut_Number,

	string p_DateFc_InOut_Varchar2,
	string p_DateLc_InOut_Varchar2)
{
	// In parameters
	COPY_STR(p_BiHId_In_VarChar2);
	COPY_INT(p_BatchSize_In_Integer);
	p(test 1) int batch_sz = 0;
	if (p_BatchSize_In_Integer == NULL)
	{
		p(batch size in integer == NULL)
			stmtctx->p_RecordNr_In_ArrRecNr.data->isNull = 1;
		stmtctx->p_RecordData_In_ArrRecData.data->isNull = 1;
		p(batch size in integer == NULL end)
	}
	else
	{
		p(batch size in integer NOT NULL)
			batch_sz = *p_BatchSize_In_Integer;

		DPI_CHECK_ERROR(
			ctx,
			dpiVar_setNumElementsInArray(
				stmtctx->p_RecordNr_In_ArrRecNr.var,
				*p_BatchSize_In_Integer));

		DPI_CHECK_ERROR(
			ctx,
			dpiVar_setNumElementsInArray(
				stmtctx->p_RecordData_In_ArrRecData.var,
				*p_BatchSize_In_Integer));
	}
	COPY_INT(p_MaxAge_In_Number);
	COPY_STR(p_DataHeader_In_VarChar2);
	p(pass1234) for (int64_t i = 0; i < batch_sz; i++)
	{
		printf("batch_sz %d", batch_sz);
		fflush(stdout);
		stmtctx->p_RecordNr_In_ArrRecNr.data[i].isNull = 0;
		stmtctx->p_RecordNr_In_ArrRecNr.data[i].value.asInt64 =
			p_RecordNr_In_ArrRecNr[i];
		DPI_CHECK_ERROR(
			ctx,
			dpiVar_setFromBytes(
				stmtctx->p_RecordData_In_ArrRecData.var, i, p_RecordData_In_ArrRecData[i].data,
				p_RecordData_In_ArrRecData[i].length));
	}

	// InOut parameters
	COPY_INT(p_RecCount_InOut_Number);
	COPY_INT(p_PreParseErrCount_InOut_Number);
	COPY_INT(p_ErrCount_InOut_Number);
	p(pass1)
		COPY_STR(p_DateFc_InOut_Varchar2);
	p(pass2)
		COPY_STR(p_DateLc_InOut_Varchar2);
	p(pass3)

		return 0;
}

int sp_insert_csv_execute(dpiCtx_res *ctx, sp_insert_csv_ctx stmtctx)
{
	// Out parameters
	stmtctx.p_ErrorCode_Out_Number.data->isNull = 1;
	stmtctx.p_ErrorDesc_Out_Varchar2.data->isNull = 1;
	stmtctx.p_ReturnStatus_Out_Number.data->isNull = 1;

	uint32_t numQueryColumns;
	DPI_CHECK_ERROR(
		ctx, dpiStmt_execute(stmtctx.stmt, 0, &numQueryColumns));
	L("numQueryColumns %d\r\n", numQueryColumns);

	return 0;
}

int sp_insert_csv_cleanup(dpiCtx_res *ctx, sp_insert_csv_ctx stmtctx)
{
	DPI_CHECK_ERROR(ctx, dpiStmt_release(stmtctx.stmt));

	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_BiHId_In_VarChar2.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_BatchSize_In_Integer.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_MaxAge_In_Number.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_DataHeader_In_VarChar2.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_RecordNr_In_ArrRecNr.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_RecordData_In_ArrRecData.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_RecCount_InOut_Number.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_PreParseErrCount_InOut_Number.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_ErrCount_InOut_Number.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_DateFc_InOut_Varchar2.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_DateLc_InOut_Varchar2.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_ErrorCode_Out_Number.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_ErrorDesc_Out_Varchar2.var));
	DPI_CHECK_ERROR(ctx, dpiVar_release(stmtctx.p_ReturnStatus_Out_Number.var));

	return 0;
}

const char *userName = "scott";
const char *password = "regit";
const char *tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)"
				  "(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
int mainOld(int argc, char **argv)
{

	printf("varArray example.\n");

	dpiCtx_res ctx;
	connect(userName, strlen(userName), password, strlen(password), tns, strlen(tns), &ctx);

	sp_insert_csv_ctx stmt_context;
	sp_insert_csv(&ctx, &stmt_context);

	string p_BiHId_In_VarChar2 = {"meow", strlen("meow")};
	int64_t *p_BatchSize_In_Integer = malloc(sizeof(int64_t));
	*p_BatchSize_In_Integer = 3;
	int64_t *p_MaxAge_In_Number = malloc(sizeof(int64_t));
	*p_MaxAge_In_Number = 100;
	string p_DataHeader_In_VarChar2 = {"somestuff", strlen("somestuff")};
	const int64_t p_RecordNr_In_ArrRecNr[] = {2, 3, 4};
	const string p_RecordData_In_ArrRecData[] = {{"foo", strlen("foo")}, {"bar", strlen("bar")}, {"baz", strlen("baz")}};
	int64_t *p_RecCount_InOut_Number = malloc(sizeof(int64_t));
	*p_RecCount_InOut_Number = 69;
	int64_t *p_PreParseErrCount_InOut_Number = malloc(sizeof(int64_t));
	*p_PreParseErrCount_InOut_Number = 1337;
	int64_t *p_ErrCount_InOut_Number = malloc(sizeof(int64_t));
	*p_ErrCount_InOut_Number = 666;
	string p_DateFc_InOut_Varchar2 = {"bibidibabidiboo", strlen("bibidibabidiboo")};
	string p_DateLc_InOut_Varchar2 = {"whatdoyouget", strlen("whatdoyouget")};

	sp_insert_csv_values(&ctx, &stmt_context, p_BiHId_In_VarChar2, p_BatchSize_In_Integer, p_MaxAge_In_Number,
						 p_DataHeader_In_VarChar2, p_RecordNr_In_ArrRecNr, p_RecordData_In_ArrRecData,
						 p_RecCount_InOut_Number, p_PreParseErrCount_InOut_Number,
						 p_ErrCount_InOut_Number, p_DateFc_InOut_Varchar2, p_DateLc_InOut_Varchar2);

	sp_insert_csv_execute(&ctx, stmt_context);
	printf("done!\n");

	return 0;
}

int main(int argc, char **argv)
{
	int test(int, char **);
	printf("test returned: %d", test(argc, argv));
	return 0;
}

int test(int argc, char **argv)
{

	printf("varArray example.\n");
	dpiCtx_res *ctx = malloc(sizeof(dpiCtx_res));
	connect(userName, strlen(userName), password, strlen(password), tns, strlen(tns), ctx);
	sp_insert_csv_ctx *stmtctx = malloc(sizeof(sp_insert_csv_ctx));
	DPI_CHECK_ERROR(
		ctx,
		dpiConn_prepareStmt(
			ctx->conn, 0, SP_INSERT_CSV, strlen(SP_INSERT_CSV), NULL, 0,
			&stmtctx->stmt));

	// In parameters
	NEW_STR_VAR(p_BiHId_In_VarChar2);
	NEW_INT_VAR(p_BatchSize_In_Integer);
	NEW_INT_VAR(p_MaxAge_In_Number);
	NEW_STR_VAR(p_DataHeader_In_VarChar2);

	DPI_CHECK_ERROR(
		ctx,
		dpiConn_newVar(
			ctx->conn, DPI_ORACLE_TYPE_NUMBER, DPI_NATIVE_TYPE_INT64, 1024,
			0, 0, 1, NULL,
			&stmtctx->p_RecordNr_In_ArrRecNr.var,
			&stmtctx->p_RecordNr_In_ArrRecNr.data));

	DPI_CHECK_ERROR(
		ctx,
		dpiConn_newVar(
			ctx->conn, DPI_ORACLE_TYPE_VARCHAR, DPI_NATIVE_TYPE_BYTES, 1024,
			4000, 0, 1, NULL,
			&stmtctx->p_RecordData_In_ArrRecData.var,
			&stmtctx->p_RecordData_In_ArrRecData.data));

	// InOut parameters
	NEW_INT_VAR(p_RecCount_InOut_Number);
	NEW_INT_VAR(p_PreParseErrCount_InOut_Number);
	NEW_INT_VAR(p_ErrCount_InOut_Number);

	NEW_STR_VAR(p_DateFc_InOut_Varchar2);
	NEW_STR_VAR(p_DateLc_InOut_Varchar2);

	// Out parameters
	NEW_INT_VAR(p_ErrorCode_Out_Number);
	NEW_STR_VAR(p_ErrorDesc_Out_Varchar2);
	NEW_INT_VAR(p_ReturnStatus_Out_Number);

	// Binds
	BIND(p_BiHId_In_VarChar2);
	BIND(p_BatchSize_In_Integer);
	BIND(p_MaxAge_In_Number);
	BIND(p_DataHeader_In_VarChar2);
	BIND(p_RecordNr_In_ArrRecNr);
	BIND(p_RecordData_In_ArrRecData);
	BIND(p_RecCount_InOut_Number);
	BIND(p_PreParseErrCount_InOut_Number);
	BIND(p_ErrCount_InOut_Number);
	BIND(p_DateFc_InOut_Varchar2);
	BIND(p_DateLc_InOut_Varchar2);
	BIND(p_ErrorCode_Out_Number);
	BIND(p_ErrorDesc_Out_Varchar2);
	BIND(p_ReturnStatus_Out_Number);

	string p_BiHId_In_VarChar2 = {"meow", strlen("meow")};
	int64_t *p_BatchSize_In_Integer = malloc(sizeof(int64_t));
	*p_BatchSize_In_Integer = 3;
	int64_t *p_MaxAge_In_Number = malloc(sizeof(int64_t));
	*p_MaxAge_In_Number = 100;
	string p_DataHeader_In_VarChar2 = {"somestuff", strlen("somestuff")};

	const int64_t p_RecordNr_In_ArrRecNr[] = {2, 3, 4};
	const string p_RecordData_In_ArrRecData[] = {{"foo", strlen("foo")}, {"bar", strlen("bar")}, {"baz", strlen("baz")}};
	int64_t *p_RecCount_InOut_Number = malloc(sizeof(int64_t));
	*p_RecCount_InOut_Number = 69;
	int64_t *p_PreParseErrCount_InOut_Number = malloc(sizeof(int64_t));
	*p_PreParseErrCount_InOut_Number = 1337;
	int64_t *p_ErrCount_InOut_Number = malloc(sizeof(int64_t));
	*p_ErrCount_InOut_Number = 666;
	string p_DateFc_InOut_Varchar2 = {"bibidibabidiboo", strlen("bibidibabidiboo")};
	string p_DateLc_InOut_Varchar2 = {"whatdoyouget", strlen("whatdoyouget")};

	{
		// In parameters
		COPY_STR(p_BiHId_In_VarChar2);
		COPY_INT(p_BatchSize_In_Integer);
		p(test 1) int batch_sz = 0;
		if (p_BatchSize_In_Integer == NULL)
		{
			p(batch size in integer == NULL)
				stmtctx->p_RecordNr_In_ArrRecNr.data->isNull = 1;
			stmtctx->p_RecordData_In_ArrRecData.data->isNull = 1;
			p(batch size in integer == NULL end)
		}
		else
		{
			p(batch size in integer NOT NULL)
				batch_sz = *p_BatchSize_In_Integer;

			DPI_CHECK_ERROR(
				ctx,
				dpiVar_setNumElementsInArray(
					stmtctx->p_RecordNr_In_ArrRecNr.var,
					*p_BatchSize_In_Integer));

			DPI_CHECK_ERROR(
				ctx,
				dpiVar_setNumElementsInArray(
					stmtctx->p_RecordData_In_ArrRecData.var,
					*p_BatchSize_In_Integer));
		}
		COPY_INT(p_MaxAge_In_Number);
		COPY_STR(p_DataHeader_In_VarChar2);
		 for (int64_t i = 0; i < batch_sz; i++)
		{
			printf("batch_sz %d", batch_sz);
			fflush(stdout);
			stmtctx->p_RecordNr_In_ArrRecNr.data[i].isNull = 0;
			stmtctx->p_RecordNr_In_ArrRecNr.data[i].value.asInt64 =
				p_RecordNr_In_ArrRecNr[i];
			DPI_CHECK_ERROR(
				ctx,
				dpiVar_setFromBytes(
					stmtctx->p_RecordData_In_ArrRecData.var, i, p_RecordData_In_ArrRecData[i].data,
					p_RecordData_In_ArrRecData[i].length));
		}

		// InOut parameters
		COPY_INT(p_RecCount_InOut_Number);
		COPY_INT(p_PreParseErrCount_InOut_Number);
		COPY_INT(p_ErrCount_InOut_Number);
		p(pass1)
			COPY_STR(p_DateFc_InOut_Varchar2);
		p(pass2)
			COPY_STR(p_DateLc_InOut_Varchar2);
		p(pass3)
	}

	sp_insert_csv_execute(ctx, *stmtctx);
	printf("done!\n");

	return 0;
}
