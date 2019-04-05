// compile with:
// gcc timezones.c ../../c_src/odpi/embed/dpi.c -I"../../c_src/opdi/include"

#include "../../c_src/odpi/include/dpi.h"
#include <stdio.h>  // printf
#include <string.h> // strlen

const char *userName = "scott";
const char *password = "regit";
const char *tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)"
                  "(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))";
int main(int argc, char **argv)
{

    dpiContext *context = NULL;
    dpiConn *conn = NULL;
    dpiErrorInfo error;
    dpiContext_create(3, 0, &context, &error);
    dpiConn_create(context, userName, strlen(userName), password,
                   strlen(password), tns, strlen(tns), NULL, NULL, &conn);

    dpiStmt *alterStmt = NULL; // set time zone to a fixed value
    const char *alterSQL = "ALTER SESSION SET TIME_ZONE='10:55'";
    dpiConn_prepareStmt(conn, 0, alterSQL, strlen(alterSQL), NULL, 0, &alterStmt);
    dpiStmt_execute(alterStmt, DPI_MODE_EXEC_DEFAULT, NULL);

    dpiStmt *dropStmt = NULL; // get rid of the table incase it exists from a previous run
    const char *dropSQL = "DROP TABLE timezones";
    dpiConn_prepareStmt(conn, 0, dropSQL, strlen(dropSQL), NULL, 0, &dropStmt);
    dpiStmt_execute(dropStmt, DPI_MODE_EXEC_DEFAULT, NULL);

    dpiStmt *createStmt = NULL; // create the table
    const char *createSQL = "CREATE TABLE timezones (c_id NUMBER,"
                            "c_tstz TIMESTAMP(9) WITH TIME ZONE)";
    dpiConn_prepareStmt(conn, 0, createSQL, strlen(createSQL), NULL, 0, &createStmt);
    dpiStmt_execute(createStmt, DPI_MODE_EXEC_DEFAULT, NULL);

    dpiStmt *insertStmt = NULL; // insert timestamp with time zone, this works correctly
    const char *insertSQL = "INSERT INTO timezones VALUES(1, "
                            "TIMESTAMP '2003-01-02 3:44:55.66 +7:08')";
    dpiConn_prepareStmt(conn, 0, insertSQL, strlen(insertSQL), NULL, 0, &insertStmt);
    dpiStmt_execute(insertStmt, DPI_MODE_EXEC_DEFAULT, NULL);
    dpiConn_commit(conn);

    dpiStmt *bindStmt = NULL; // insert timestamp with time zone using bind, this doesn't work correctly
    const char *bindSQL = "INSERT INTO timezones VALUES(2, :A)";
    dpiConn_prepareStmt(conn, 0, bindSQL, strlen(bindSQL), NULL, 0, &bindStmt);
    dpiData bindData;
    dpiData_setTimestamp(&bindData, 2003, 1, 2, 3, 44, 55, /*fsec*/ 660000000, /*tzHour*/ 7, /*tzMin*/ 8);
    dpiTimestamp t0 = bindData.value.asTimestamp;
    printf("Binding timestamp:"
           "%4d-%.2d-%.2d %.2d:%.2d:%.2d.%.6d +%.2d:%.2d\n",
           t0.year, t0.month, t0.day, t0.hour, t0.minute,
           t0.second, t0.fsecond, t0.tzHourOffset, t0.tzMinuteOffset);
    dpiStmt_bindValueByPos(bindStmt, 1, DPI_NATIVE_TYPE_TIMESTAMP, &bindData);
    dpiStmt_execute(bindStmt, DPI_MODE_EXEC_DEFAULT, NULL);
    for (int i = 0; i < 2; i++)
    {
        dpiStmt *fetchStmt = NULL; // get resulting timestamp
        char fetchSQL[64];
        sprintf(fetchSQL, "SELECT c_tstz FROM timezones where c_id = %d", i + 1);
        dpiConn_prepareStmt(conn, 0, fetchSQL, strlen(fetchSQL), NULL, 0, &fetchStmt);
        dpiStmt_execute(fetchStmt, DPI_MODE_EXEC_DEFAULT, NULL);
        int found = 0;
        uint32_t bufferRowIndex = 0;
        dpiData *dataFetch = NULL;
        dpiNativeTypeNum type = 0;
        dpiStmt_fetch(fetchStmt, &found, &bufferRowIndex);
        dpiStmt_getQueryValue(fetchStmt, 1, &type, &dataFetch);
        dpiTimestamp t = dataFetch->value.asTimestamp;
        printf("Result timestamp [%d]: "
               "%4d-%.2d-%.2d %.2d:%.2d:%.2d.%.6d +%.2d:%.2d  -  %s\n",
               i + 1, t.year, t.month, t.day, t.hour, t.minute,
               t.second, t.fsecond, t.tzHourOffset, t.tzMinuteOffset,
               i ? "This is the timestamp that was bound" : "This is the timestamp that was inserted directly");
    }
    dpiConn_commit(conn);
    return 0;
}
