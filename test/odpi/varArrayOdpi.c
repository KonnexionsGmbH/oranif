// compile with:
// gcc varArrayOdpi.c ../../c_src/odpi/embed/dpi.c -I"../../c_src/opdi/include"

#include <stdio.h>  // printf
#include <string.h> // strlen

#include "../../c_src/odpi/include/dpi.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#ifdef _MSC_VER
#if _MSC_VER < 1900
#define PRId64                  "I64d"
#define PRIu64                  "I64u"
#endif
#endif

#ifndef PRIu64
#include <inttypes.h>
#endif

typedef struct {
    const char *mainUserName;
    uint32_t mainUserNameLength;
    const char *mainPassword;
    uint32_t mainPasswordLength;
    const char *proxyUserName;
    uint32_t proxyUserNameLength;
    const char *proxyPassword;
    uint32_t proxyPasswordLength;
    const char *connectString;
    uint32_t connectStringLength;
    const char *dirName;
    uint32_t dirNameLength;
    dpiContext *context;
} dpiSampleParams;

// connect to the database
dpiConn *dpiSamples_getConn(int withPool, dpiCommonCreateParams *commonParams);

// acquire parameters
dpiSampleParams *dpiSamples_getParams(void);

// acquire SODA database
dpiSodaDb *dpiSamples_getSodaDb(void);

// show error to stderr
int dpiSamples_showError(void);

static dpiContext *gContext = NULL;
static dpiSampleParams gParams;

//-----------------------------------------------------------------------------
// dpiSamples__fatalError() [INTERNAL]
//   Called when a fatal error is encountered from which recovery is not
// possible. This simply prints a message to stderr and exits the program with
// a non-zero exit code to indicate an error.
//-----------------------------------------------------------------------------
static void dpiSamples__fatalError(const char *message)
{
    fprintf(stderr, "FATAL: %s\n", message);
    exit(1);
}


//-----------------------------------------------------------------------------
// dpiSamples__finalize() [INTERNAL]
//   Destroy context upon process exit.
//-----------------------------------------------------------------------------
static void dpiSamples__finalize(void)
{
    dpiContext_destroy(gContext);
}


//-----------------------------------------------------------------------------
// dpiSamples__getEnvValue()
//   Get parameter value from the environment or use supplied default value if
// the value is not set in the environment. Memory is allocated to accommodate
// the value.
//-----------------------------------------------------------------------------
static void dpiSamples__getEnvValue(const char *envName,
        const char *defaultValue, const char **value, uint32_t *valueLength,
        int convertToUpper)
{
    const char *source;
    uint32_t i;
    char *ptr;

    source = getenv(envName);
    if (!source)
        source = defaultValue;
    *valueLength = strlen(source);
    *value = malloc(*valueLength);
    if (!*value)
        dpiSamples__fatalError("Out of memory!");
    memcpy((void*) *value, source, *valueLength);
    if (convertToUpper) {
        ptr = (char*) *value;
        for (i = 0; i < *valueLength; i++)
            ptr[i] = toupper(ptr[i]);
    }
}


//-----------------------------------------------------------------------------
// dpiSamples_getConn()
//   Connect to the database using the supplied parameters. The DPI library
// will also be initialized, if needed.
//-----------------------------------------------------------------------------
dpiConn *dpiSamples_getConn(int withPool, dpiCommonCreateParams *commonParams)
{
    dpiConn *conn;
    dpiPool *pool;

    // perform initialization
    dpiSamples_getParams();

    // create a pool and acquire a connection
    if (withPool) {
        if (dpiPool_create(gContext, gParams.mainUserName,
                gParams.mainUserNameLength, gParams.mainPassword,
                gParams.mainPasswordLength, gParams.connectString,
                gParams.connectStringLength, commonParams, NULL, &pool) < 0) {
            dpiSamples_showError();
            dpiSamples__fatalError("Unable to create pool.");
        }
        if (dpiPool_acquireConnection(pool, NULL, 0, NULL, 0, NULL,
                    &conn) < 0) {
            dpiSamples_showError();
            dpiSamples__fatalError("Unable to acquire connection from pool.");
        }
        dpiPool_release(pool);

    // or create a standalone connection
    } else if (dpiConn_create(gContext, gParams.mainUserName,
            gParams.mainUserNameLength, gParams.mainPassword,
            gParams.mainPasswordLength, gParams.connectString,
            gParams.connectStringLength, commonParams, NULL, &conn) < 0) {
        dpiSamples_showError();
        dpiSamples__fatalError("Unable to create connection.");
    }

    return conn;
}

const char *userName = "scott";
const char *password = "regit";
const char *tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)"
                  "(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
//-----------------------------------------------------------------------------
// dpiSamples_getParams()
//   Get parameters set in the environment. The DPI library will also be
// initialized if needed.
//-----------------------------------------------------------------------------
dpiSampleParams *dpiSamples_getParams(void)
{
    dpiErrorInfo errorInfo;

    if (!gContext) {
        if (dpiContext_create(DPI_MAJOR_VERSION, DPI_MINOR_VERSION, &gContext,
                &errorInfo) < 0) {
            fprintf(stderr, "ERROR: %.*s (%s : %s)\n", errorInfo.messageLength,
                    errorInfo.message, errorInfo.fnName, errorInfo.action);
            dpiSamples__fatalError("Cannot create DPI context.");
        }
        atexit(dpiSamples__finalize);
    }

	gParams.mainUserName = "scott";
	gParams.mainPassword = "regit";
	gParams.connectString = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)"
                  "(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";

	gParams.mainUserNameLength = strlen(gParams.mainUserName);
	gParams.mainPasswordLength = strlen(gParams.mainPassword);
	gParams.connectStringLength = strlen(gParams.connectString);
    /*dpiSamples__getEnvValue("ODPIC_SAMPLES_MAIN_USER", "odpicdemo",
            &gParams.mainUserName, &gParams.mainUserNameLength, 1);
    dpiSamples__getEnvValue("ODPIC_SAMPLES_MAIN_PASSWORD", "welcome",
            &gParams.mainPassword, &gParams.mainPasswordLength, 0);
    dpiSamples__getEnvValue("ODPIC_SAMPLES_PROXY_USER", "odpicdemo_proxy",
            &gParams.proxyUserName, &gParams.proxyUserNameLength, 1);
    dpiSamples__getEnvValue("ODPIC_SAMPLES_PROXY_PASSWORD", "welcome",
            &gParams.proxyPassword, &gParams.proxyPasswordLength, 0);
    dpiSamples__getEnvValue("ODPIC_SAMPLES_CONNECT_STRING", "localhost/orclpdb",
            &gParams.connectString, &gParams.connectStringLength, 0);
    dpiSamples__getEnvValue("ODPIC_SAMPLES_DIR_NAME", "odpicdemo_dir",
            &gParams.dirName, &gParams.dirNameLength, 1);
    gParams.context = gContext;*/

    return &gParams;
}


//-----------------------------------------------------------------------------
// dpiSamples_getSodaDb()
//   Connect to the database and then acquire the SODA database object.
//-----------------------------------------------------------------------------
dpiSodaDb *dpiSamples_getSodaDb(void)
{
    dpiSodaDb *db;
    dpiConn *conn;

    conn = dpiSamples_getConn(0, NULL);
    if (dpiConn_getSodaDb(conn, &db) < 0) {
        dpiSamples_showError();
        dpiSamples__fatalError("Unable to acquire SODA database.");
    }
    dpiConn_release(conn);

    return db;
}


//-----------------------------------------------------------------------------
// dpiSamples_showError()
//   Display the error to stderr.
//-----------------------------------------------------------------------------
int dpiSamples_showError(void)
{
    dpiErrorInfo info;

    dpiContext_getError(gContext, &info);
    fprintf(stderr, "ERROR: %.*s (%s: %s)\n", info.messageLength, info.message,
            info.fnName, info.action);
    return -1;
}

#define SQL_IN    "begin :1 := pkg_TestStringArrays.TestInArrays(:2, :3); end;"
#define SQL_INOUT "begin pkg_TestStringArrays.TestInOutArrays(:1, :2); end;"
#define SQL_OUT   "begin pkg_TestStringArrays.TestOutArrays(:1, :2); end;"
#define SQL_ASSOC "begin pkg_TestStringArrays.TestIndexBy(:1); end;"
#define TYPE_NAME "PKG_TESTSTRINGARRAYS.UDT_STRINGLIST"

static const char *gc_Strings[5] = {
    "Test String 1 (I)",
    "Test String 2 (II)",
    "Test String 3 (III)",
    "Test String 4 (IV)",
    "Test String 5 (V)"
};

//-----------------------------------------------------------------------------
// main()
//-----------------------------------------------------------------------------
int main(int argc, char **argv)
{
    dpiData *returnValue, *numberValue, *arrayValue, *objectValue;
    dpiVar *returnVar, *numberVar, *arrayVar, *objectVar;
    uint32_t numQueryColumns, i, numElementsInArray;
    int32_t elementIndex, nextElementIndex;
    dpiObjectType *objType;
    dpiData elementValue;
    dpiStmt *stmt;
    dpiConn *conn;
    int exists;

    // connect to database
    conn = dpiSamples_getConn(0, NULL);

    // create variable for return value
    if (dpiConn_newVar(conn, DPI_ORACLE_TYPE_NUMBER, DPI_NATIVE_TYPE_INT64, 1,
            0, 0, 0, NULL, &returnVar, &returnValue) < 0)
        return dpiSamples_showError();
	printf("made var. \n");
    // create variable for numeric value passed to procedures
    if (dpiConn_newVar(conn, DPI_ORACLE_TYPE_NUMBER, DPI_NATIVE_TYPE_INT64, 1,
            0, 0, 0, NULL, &numberVar, &numberValue) < 0)
        return dpiSamples_showError();
	printf("made another var. \n");
    // create variable for string array passed to procedures
    // a maximum of 8 elements, each of 60 characters is permitted
	if (dpiConn_newVar(conn, DPI_ORACLE_TYPE_VARCHAR, DPI_NATIVE_TYPE_BYTES, 8,
										 60, 0, 1, NULL, &arrayVar, &arrayValue) < 0)
		return dpiSamples_showError();
	printf("made conn. \n");
	// ************** IN ARRAYS *****************
	// prepare statement for testing in arrays


	if (dpiConn_prepareStmt(conn, 0, SQL_IN, strlen(SQL_IN), NULL, 0,
													&stmt) < 0)
		return dpiSamples_showError();
	printf("prepared stmt. \n");
    // bind return value
    if (dpiStmt_bindByPos(stmt, 1, returnVar) < 0)
        return dpiSamples_showError();
	printf("bound return value. \n");
    // bind in numeric value
    numberValue->isNull = 0;
    numberValue->value.asInt64 = 12;
    if (dpiStmt_bindByPos(stmt, 2, numberVar) < 0)
        return dpiSamples_showError();

    // bind in string array
    for (i = 0; i < 5; i++) {
        if (dpiVar_setFromBytes(arrayVar, i, gc_Strings[i],
                strlen(gc_Strings[i])) < 0)
            return dpiSamples_showError();
    }
		printf("set the array bytes. \n");
    if (dpiVar_setNumElementsInArray(arrayVar, 5) < 0)
        return dpiSamples_showError();
		printf("set num elements call. \n");
    if (dpiStmt_bindByPos(stmt, 3, arrayVar) < 0)
        return dpiSamples_showError();
	printf("bind pos call. \n");
    // perform execution (in arrays with 5 elements)
    if (dpiStmt_execute(stmt, 0, &numQueryColumns) < 0)
        return dpiSamples_showError();
				printf("execute order 66. \n");
    printf("IN array (5 elements): return value is %" PRId64 "\n\n",
            returnValue->value.asInt64);
	printf("perform execution (1). \n");
    // perform execution (in arrays with 0 elements)
    if (dpiVar_setNumElementsInArray(arrayVar, 0) < 0)
        return dpiSamples_showError();
    if (dpiStmt_execute(stmt, 0, &numQueryColumns) < 0)
        return dpiSamples_showError();
    dpiStmt_release(stmt);
    printf("IN array (0 elements): return value is %" PRId64 "\n\n",
            returnValue->value.asInt64);

    // ************** IN/OUT ARRAYS *****************
    // prepare statement for testing in/out arrays
    if (dpiConn_prepareStmt(conn, 0, SQL_INOUT, strlen(SQL_INOUT), NULL, 0,
            &stmt) < 0)
        return dpiSamples_showError();

    // bind in numeric value
    numberValue->value.asInt64 = 5;
    if (dpiStmt_bindByPos(stmt, 1, numberVar) < 0)
        return dpiSamples_showError();

    // bind in array value (use same values as test for in arrays)
    if (dpiVar_setNumElementsInArray(arrayVar, 5) < 0)
        return dpiSamples_showError();
    if (dpiStmt_bindByPos(stmt, 2, arrayVar) < 0)
        return dpiSamples_showError();

    // perform execution (in/out arrays)
    if (dpiStmt_execute(stmt, 0, &numQueryColumns) < 0)
        return dpiSamples_showError();
    dpiStmt_release(stmt);

    // display value of array after procedure call
    if (dpiVar_getNumElementsInArray(arrayVar, &numElementsInArray) < 0)
        return dpiSamples_showError();
    printf("IN/OUT array contents:\n");
    for (i = 0; i < numElementsInArray; i++)
        printf("    [%d] %.*s\n", i + 1, arrayValue[i].value.asBytes.length,
                arrayValue[i].value.asBytes.ptr);
    printf("\n");

    // ************** OUT ARRAYS *****************
    // prepare statement for testing out arrays
    if (dpiConn_prepareStmt(conn, 0, SQL_OUT, strlen(SQL_OUT), NULL, 0,
            &stmt) < 0)
        return dpiSamples_showError();

    // bind in numeric value
    numberValue->value.asInt64 = 7;
    if (dpiStmt_bindByPos(stmt, 1, numberVar) < 0)
        return dpiSamples_showError();

    // bind in array value (value will be overwritten)
    if (dpiStmt_bindByPos(stmt, 2, arrayVar) < 0)
        return dpiSamples_showError();

    // perform execution (out arrays)
    if (dpiStmt_execute(stmt, 0, &numQueryColumns) < 0)
        return dpiSamples_showError();
    dpiStmt_release(stmt);

    // display value of array after procedure call
    if (dpiVar_getNumElementsInArray(arrayVar, &numElementsInArray) < 0)
        return dpiSamples_showError();
    printf("OUT array contents:\n");
    for (i = 0; i < numElementsInArray; i++)
        printf("    [%d] %.*s\n", i + 1, arrayValue[i].value.asBytes.length,
                arrayValue[i].value.asBytes.ptr);
    printf("\n");

    // ************** INDEX-BY ASSOCIATIVE ARRAYS *****************
    // look up object type by name
    if (dpiConn_getObjectType(conn, TYPE_NAME, strlen(TYPE_NAME),
            &objType) < 0)
        return dpiSamples_showError();

    // create new object variable
    if (dpiConn_newVar(conn, DPI_ORACLE_TYPE_OBJECT, DPI_NATIVE_TYPE_OBJECT, 1,
            0, 0, 0, objType, &objectVar, &objectValue) < 0)
        return dpiSamples_showError();

    // prepare statement for testing associative arrays
    if (dpiConn_prepareStmt(conn, 0, SQL_ASSOC, strlen(SQL_ASSOC), NULL, 0,
            &stmt) < 0)
        return dpiSamples_showError();

    // bind array
    if (dpiStmt_bindByPos(stmt, 1, objectVar) < 0)
        return dpiSamples_showError();

    // perform execution (associative arrays)
    if (dpiStmt_execute(stmt, 0, &numQueryColumns) < 0)
        return dpiSamples_showError();
    dpiStmt_release(stmt);

    // display contents of array after procedure call
    if (dpiObject_getFirstIndex(objectValue->value.asObject, &elementIndex,
            &exists) < 0)
        return dpiSamples_showError();
    printf("ASSOCIATIVE array contents:\n");
    while (1) {
        if (dpiObject_getElementValueByIndex(objectValue->value.asObject,
                elementIndex, DPI_NATIVE_TYPE_BYTES, &elementValue) < 0)
            return dpiSamples_showError();
        printf("    [%d] %.*s\n", elementIndex,
                elementValue.value.asBytes.length,
                elementValue.value.asBytes.ptr);
        if (dpiObject_getNextIndex(objectValue->value.asObject, elementIndex,
                &nextElementIndex, &exists) < 0)
            return dpiSamples_showError();
        if (!exists)
            break;
        elementIndex = nextElementIndex;
    }
    printf("\n");

    // clean up
    dpiVar_release(returnVar);
    dpiVar_release(numberVar);
    dpiVar_release(arrayVar);
    dpiVar_release(objectVar);
    dpiObjectType_release(objType);
    dpiConn_release(conn);

    printf("Done.\n");
    return 0;
}

