/* nix command shell:
    LD_LIBRARY_PATH=priv/ ./c_src/oci_api/table_w
*/

#ifdef __WIN32__
#define _CRTDBG_MAP_ALLOC
#include <crtdbg.h>
#endif

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "common.h"

/*
DROP TYPE T_TEXT_ARR;
CREATE OR REPLACE TYPE T_TEXT_ARR AS TABLE OF VARCHAR2(4000);
/

DROP FUNCTION test_fun2;
CREATE OR REPLACE FUNCTION test_fun2(p_text IN T_TEXT_ARR)
RETURN T_TEXT_ARR
IS
p_text_out T_TEXT_ARR := T_TEXT_ARR();
BEGIN
    FOR I IN 1..p_text.COUNT
    LOOP
		p_text_out.extend;
        p_text_out(I) := p_text(I) || p_text(I) || to_char(I);
    END LOOP;
    RETURN(p_text_out);
END test_fun2;
/
*/
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

	char *stmt = "select test_fun2(:p_text) from dual";
	status = OCIStmtPrepare(stmthp, errhp, (OraText *)stmt, (ub4)strlen(stmt), OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCIType *tdo = NULL;
	status = OCITypeByName(envhp, errhp, svchp, NULL, 0,
						   "T_TEXT_ARR", (ub4)strlen("T_TEXT_ARR"),
						   NULL, 0, OCI_DURATION_SESSION,
						   OCI_TYPEGET_ALL, &tdo);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	if (!tdo)
	{
		L("Error - NULL tdo returned\n");
		return -1;
	}

	OCIBind *bindpp = NULL;
	status = OCIBindByName(stmthp, &bindpp, errhp, ":p_text",
						   (sb4)-1, NULL /*void *valuep*/,
						   (sb4)sizeof(OCITable *) /*sb4 value_sz*/,
						   SQLT_NTY /*ub2 dty*/,
						   NULL /*void *indp*/,
						   NULL /*ub2 *alenp*/,
						   NULL /*ub2 *rcodep*/,
						   10 /*ub4 maxarr_len*/,
						   NULL /*ub4 *curelep*/,
						   OCI_DEFAULT /*ub4 mode*/);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCITable *in_tab = NULL;
	status = OCIObjectNew(envhp, errhp, svchp,
						  OCI_TYPECODE_TABLE, tdo,
						  NULL, OCI_DURATION_SESSION,
						  TRUE, &in_tab);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCIString *str;
	status = OCIObjectNew(envhp, errhp, svchp,
						  OCI_TYPECODE_VARCHAR2, NULL,
						  NULL, OCI_DURATION_SESSION,
						  TRUE, &str);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	for (int i = 0; i < 5; ++i)
	{
		status = OCIStringAssignText(envhp, errhp,
									 "TestBikram",
									 (ub4)(strlen("TestBikram")),
									 &str);
		if (status != OCI_SUCCESS)
		{
			LOG_ERROR(errhp, status);
			return -1;
		}

		status = OCICollAppend(envhp, errhp,
							   str,
							   NULL,
							   (OCIColl *)in_tab);
		if (status != OCI_SUCCESS)
		{
			LOG_ERROR(errhp, status);
			return -1;
		}
	}

	status = OCIBindObject(bindpp, errhp, tdo,
						   &in_tab /*void **pgvpp*/,
						   NULL /*ub4 *pvszsp*/,
						   NULL /*void **indpp*/,
						   NULL /*ub4 *indszp*/);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCIDefine *definehp = NULL;
	status = OCIDefineByPos(stmthp, &definehp, errhp, 1,
							NULL, 0, SQLT_NTY,
							NULL, NULL, NULL, OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCITable *tab = NULL;
	status = OCIDefineObject(definehp, errhp, tdo, (dvoid **)&tab,
							 NULL, NULL, NULL);
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
	L("column count %d\r\n", col_count);

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

	L("column[%d] %.*s type %d width %d\r\n", pos, col_name_len, col_name, col_type, col_width);

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
	L("fetched_rows %d\r\n", fetched_rows);

	sb4 tabsz = -1;
	status = OCITableSize(envhp, errhp, tab, &tabsz);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	L("tabsz %d\n", tabsz);

	OCIIter *iterator = NULL;
	status = OCIIterCreate(envhp, errhp, (const OCIColl *)tab, &iterator);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	void *elm = NULL;
	OCIInd *elmInd = NULL;
	boolean eoc;
	OCIString *client_elem;
	text *text_ptr;
	status = OCIIterNext(envhp, errhp, iterator, &elm, &elmInd, &eoc);
	while (!eoc && (status == OCI_SUCCESS))
	{
		client_elem = *((OCIString **)elm);
		text_ptr = OCIStringPtr(envhp, client_elem);
		L("%s\r\n", text_ptr);

		status = OCIIterNext(envhp, errhp, iterator, &elm, &elmInd, &eoc);
	}
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIIterDelete(envhp, errhp, &iterator);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	L("SUCCESS\r\n");
	return 0;
}