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

	char *stmt = "select sysdate from dual";
	status = OCIStmtPrepare(stmthp, errhp, (OraText *)stmt, (ub4)strlen(stmt), OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIStmtExecute(svchp, stmthp, errhp, 0, 0, NULL, NULL, 0);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	ub4 col_count = 0;
	status = OCIAttrGet(stmthp, OCI_HTYPE_STMT, &col_count, NULL, OCI_ATTR_PARAM_COUNT, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	printf("%s:%d column count %d\r\n", __FUNCTION__, __LINE__, col_count);

	OCIParam *paramd = NULL;
	ub4 pos = 1;

	status = OCIParamGet(stmthp, OCI_HTYPE_STMT, errhp, (void **)&paramd, pos);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	ub2 col_type;
	status = OCIAttrGet(paramd, OCI_DTYPE_PARAM, &col_type, NULL, OCI_ATTR_DATA_TYPE, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	ub4 col_name_len = 0;
	OraText *col_name;
	status = OCIAttrGet(paramd, OCI_DTYPE_PARAM, &col_name, &col_name_len, OCI_ATTR_NAME, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	ub4 col_width = 0;
	status = OCIAttrGet(paramd, OCI_DTYPE_PARAM, (void *)&col_width, NULL, OCI_ATTR_DATA_SIZE, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	printf("%s:%d column[%d] %.*s type %d width %d\r\n", __FUNCTION__, __LINE__, pos, col_name_len, col_name, col_type, col_width);

	OCIDefine *definehp = NULL;
	unsigned char valuep[7];
	sb2 ind;
	ub2 rlen;
	status = OCIDefineByPos(stmthp, &definehp, errhp, pos,
							(void *)valuep,
							col_width, SQLT_DAT,
							&ind, &rlen, NULL, OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIStmtFetch2(stmthp, errhp, 1, OCI_DEFAULT, 0, OCI_DEFAULT);
	if (status != OCI_SUCCESS && status != OCI_NO_DATA)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	ub4 fetched_rows = 0;
	status = OCIAttrGet(stmthp, OCI_HTYPE_STMT, (void *)&fetched_rows, NULL, OCI_ATTR_ROWS_FETCHED, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	printf("%s:%d fetched_rows %d\r\n", __FUNCTION__, __LINE__, fetched_rows);
	//*
	printf("%s:%d valuep %d %d %d %d %d %d %d\r\n", __FUNCTION__, __LINE__,
	 valuep[0], valuep[1], valuep[2], valuep[3],
	 valuep[4], valuep[5], valuep[6]);
	/*/
	printf("%s:%d valuep %s\r\n", __FUNCTION__, __LINE__, valuep);
	//*/

	printf("%s:%d SUCCESS\r\n", __FUNCTION__, __LINE__);
	return 0;
}