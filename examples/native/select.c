/* nix command shell:
    LD_LIBRARY_PATH=priv/ ./c_src/oci_api/select
*/

#ifdef __WIN32__
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "common.h"

int main(int argc, char *argv[])
{
    int ret = init();
    if (ret < 0)
        return ret;

    ret = session();
    if (ret < 0)
        return ret;

    OCIStmt *stmthp = NULL;
    status = OCIHandleAlloc(envhp, (void **)&stmthp, OCI_HTYPE_STMT, 0, NULL);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    char *stmt = "select * from dual where dummy = :A or dummy = :B";
    status = OCIStmtPrepare(stmthp, errhp, (OraText *)stmt, (ub4)strlen(stmt), OCI_NTV_SYNTAX, OCI_DEFAULT);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    OCIBind *bindhpA = NULL;
    int dtyA = 1, indA = 0;
    ub4 value_szA = 1;
    char *valuepA = "X";
    status = OCIBindByName(stmthp, &bindhpA, errhp, (const OraText *)"A", 1, // Bind name
                           (void *)valuepA, value_szA,
                           (sb2)dtyA, (void *)&indA, NULL, NULL, 0, NULL, OCI_DEFAULT);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    OCIBind *bindhpB = NULL;
    int dtyB = 1, indB = 0;
    ub4 value_szB = 1;
    char *valuepB = "Y";
    status = OCIBindByName(stmthp, &bindhpB, errhp, (const OraText *)"B", 1, // Bind name
                           (void *)valuepB, value_szB,
                           (sb2)dtyB, (void *)&indB, NULL, NULL, 0, NULL, OCI_DEFAULT);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    status = OCIStmtExecute(svchp, stmthp, errhp, 0, 0, NULL, NULL, 0);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    ub4 col_count = 0;
    status = OCIAttrGet(stmthp, OCI_HTYPE_STMT, &col_count, NULL, OCI_ATTR_PARAM_COUNT, errhp);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }
    printf("%s:%d column count %d\r\n", __FUNCTION__, __LINE__, col_count);

    OCIParam *paramd = NULL;
    OCIDefine *definehp = NULL;
    OraText valuep[1][100];
    sb2 ind;
    ub2 rlen;
    ub4 pos;
    for (pos = 1; pos <= col_count; pos++)
    {
        status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (void**)&paramd, pos);
        if (status != OCI_SUCCESS)
        {
            LOG_ERROR(NULL, status);
            return -1;
        }
        ub4 col_name_len = 0;
        OraText *col_name;
        status = OCIAttrGet(paramd, OCI_DTYPE_PARAM, &col_name, &col_name_len, OCI_ATTR_NAME, errhp);
        if (status != OCI_SUCCESS)
        {
            LOG_ERROR(NULL, status);
            return -1;
        }
        printf("%s:%d column[%d] %.*s\r\n", __FUNCTION__, __LINE__, pos, col_name_len, col_name);

        status = OCIDefineByPos(stmthp, &definehp, errhp, pos,
                                (void *)valuep[pos],
                                1, 1,
                                &ind, &rlen, NULL, OCI_DEFAULT);
        if (status != OCI_SUCCESS)
        {
            LOG_ERROR(NULL, status);
            return -1;
        }
    }
    status = OCIStmtFetch2(stmthp, errhp, 1, OCI_DEFAULT, 0, OCI_DEFAULT);
    if (status != OCI_SUCCESS && status != OCI_NO_DATA)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    ub4 fetched_rows = 0;
    status = OCIAttrGet(stmthp, OCI_HTYPE_STMT, (void *)&fetched_rows, NULL, OCI_ATTR_ROWS_FETCHED, errhp);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }
    printf("%s:%d fetched_rows %d\r\n", __FUNCTION__, __LINE__, fetched_rows);
    printf("%s:%d valuep[1] %s\r\n", __FUNCTION__, __LINE__, valuep[1]);

    printf("%s:%d SUCCESS\r\n", __FUNCTION__, __LINE__);
    return 0;
}
