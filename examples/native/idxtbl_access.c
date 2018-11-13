/* nix command shell:
    LD_LIBRARY_PATH=priv/ ./c_src/oci_api/idxtbl_access
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
Type ArrRecNr Is Table Of Number(8) Index By Binary_Integer;
Type ArrRecData Is Table Of Varchar2(4000) Index By Binary_Integer;

CREATE OR REPLACE PROCEDURE test_proc1(
    p_RecordNr In SCOTT.PKG_MEC_IC_CSV.ArrRecNr,
    p_RecordData In SCOTT.PKG_MEC_IC_CSV.ArrRecData,
    p_RecCount OUT Number,
    p_outrecorddata OUT scott.pkg_mec_ic_csv.arrrecdata
) IS
BEGIN
    FOR I IN 1..p_RecordData.COUNT
    LOOP
        p_outrecorddata(I) := p_recorddata(I) || ' ' || to_char(p_recordnr(I));
        p_RecCount := I;
    END LOOP;
END;
/

-- set serveroutput on;
DECLARE
    p_recordnr        scott.pkg_mec_ic_csv.arrrecnr;
    p_recorddata      scott.pkg_mec_ic_csv.arrrecdata;
    p_reccount        NUMBER;
    p_outrecorddata   scott.pkg_mec_ic_csv.arrrecdata;
BEGIN
    p_recordnr(1) := 1;
    p_recordnr(2) := 2;
    p_recordnr(3) := 3;
    p_recorddata(1) := 'test1';
    p_recorddata(2) := 'test2';
    p_recorddata(3) := 'test3';
    p_reccount := 1;
    test_proc1(
        p_recordnr        => p_recordnr,
        p_recorddata      => p_recorddata,
        p_reccount        => p_reccount,
        p_outrecorddata   => p_outrecorddata
    );

    dbms_output.put_line('P_RECCOUNT = ' || p_reccount);
    FOR i IN 1..p_reccount LOOP
        dbms_output.put_line('P_OUTRECORDDATA('
         || TO_CHAR(i)
         || ') = '
         || p_outrecorddata(i) );
    END LOOP;

END;
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
	L("session initialized\r\n");

	char *stmt =
		"BEGIN"
		"	TEST_PROC1("
		"		P_RECORDNR => :P_RECORDNR,"
		"		P_RECORDDATA => :P_RECORDDATA,"
		"		P_RECCOUNT => :P_RECCOUNT,"
		"		P_OUTRECORDDATA => :P_OUTRECORDDATA"
		"	);"
		"END;";
	status = OCIStmtPrepare(stmthp, errhp, (OraText *)stmt, (ub4)strlen(stmt), OCI_NTV_SYNTAX, OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	L("Statement prepared : %s\r\n", stmt);

	OCIType *ArrRecNr = NULL;
	status = OCITypeByFullName(envhp, errhp, svchp,
							   "SCOTT.PKG_MEC_IC_CSV.ARRRECNR",
							   (ub4)strlen("SCOTT.PKG_MEC_IC_CSV.ARRRECNR"),
							   NULL, 0, OCI_DURATION_SESSION,
							   OCI_TYPEGET_ALL, &ArrRecNr);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	if (!ArrRecNr)
	{
		L("Error - NULL ArrRecNr returned\n");
		return -1;
	}

	OCIType *ArrRecData = NULL;
	status = OCITypeByFullName(envhp, errhp, svchp,
							   "SCOTT.PKG_MEC_IC_CSV.ARRRECDATA",
							   (ub4)strlen("SCOTT.PKG_MEC_IC_CSV.ARRRECDATA"),
							   NULL, 0, OCI_DURATION_SESSION,
							   OCI_TYPEGET_ALL, &ArrRecData);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	if (!ArrRecData)
	{
		L("Error - NULL ArrRecData returned\n");
		return -1;
	}
	L("Types ArrRecNr and ArrRecData are loaded\r\n");

	OCIBind *bindpp_P_RECORDNR = NULL;
	status = OCIBindByName(stmthp, &bindpp_P_RECORDNR, errhp, ":P_RECORDNR",
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

	OCIBind *bindpp_P_RECORDDATA = NULL;
	status = OCIBindByName(stmthp, &bindpp_P_RECORDDATA, errhp, ":P_RECORDDATA",
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

	OCIBind *bindpp_P_OUTRECORDDATA = NULL;
	status = OCIBindByName(stmthp, &bindpp_P_RECORDDATA, errhp, ":P_OUTRECORDDATA",
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

	OCITable *in_P_RECORDNR = NULL;
	status = OCIObjectNew(envhp, errhp, svchp,
						  OCI_TYPECODE_TABLE, bindpp_P_RECORDNR,
						  NULL, OCI_DURATION_SESSION,
						  TRUE, &in_P_RECORDNR);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCITable *in_P_RECORDDATA = NULL;
	status = OCIObjectNew(envhp, errhp, svchp,
						  OCI_TYPECODE_TABLE, bindpp_P_RECORDDATA,
						  NULL, OCI_DURATION_SESSION,
						  TRUE, &in_P_RECORDDATA);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	OCITable *out_P_OUTRECORDDATA = NULL;
	status = OCIObjectNew(envhp, errhp, svchp,
						  OCI_TYPECODE_TABLE, bindpp_P_OUTRECORDDATA,
						  NULL, OCI_DURATION_SESSION,
						  TRUE, &out_P_OUTRECORDDATA);
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

	OCINumber n;
	for (int i = 0; i < 5; ++i)
	{
		status = OCIStringAssignText(envhp, errhp, "TestBikram",
									 (ub4)(strlen("TestBikram")), &str);
		if (status != OCI_SUCCESS)
		{
			LOG_ERROR(errhp, status);
			return -1;
		}

		status = OCICollAppend(envhp, errhp, str, NULL,
							   (OCIColl *)in_P_RECORDDATA);
		if (status != OCI_SUCCESS)
		{
			LOG_ERROR(errhp, status);
			return -1;
		}

		status = OCINumberFromReal(errhp, &i, sizeof(i), OCI_NUMBER_SIGNED, &n);
		if (status != OCI_SUCCESS)
		{
			LOG_ERROR(errhp, status);
			return -1;
		}

		status = OCICollAppend(envhp, errhp, &n, NULL,
							   (OCIColl *)in_P_RECORDNR);
		if (status != OCI_SUCCESS)
		{
			LOG_ERROR(errhp, status);
			return -1;
		}
	}
	L("5 rows loaded into P_RECORDNR and P_RECORDDATA\r\n");

	status = OCIBindObject(bindpp_P_RECORDNR, errhp, ArrRecNr,
						   &in_P_RECORDNR /*void **pgvpp*/,
						   NULL /*ub4 *pvszsp*/,
						   NULL /*void **indpp*/,
						   NULL /*ub4 *indszp*/);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIBindObject(bindpp_P_RECORDDATA, errhp, ArrRecData,
						   &in_P_RECORDDATA /*void **pgvpp*/,
						   NULL /*ub4 *pvszsp*/,
						   NULL /*void **indpp*/,
						   NULL /*ub4 *indszp*/);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIBindObject(bindpp_P_OUTRECORDDATA, errhp, ArrRecData,
						   &out_P_OUTRECORDDATA /*void **pgvpp*/,
						   NULL /*ub4 *pvszsp*/,
						   NULL /*void **indpp*/,
						   NULL /*ub4 *indszp*/);
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

	sb4 out_P_OUTRECORDDATA_sz = -1;
	status = OCITableSize(envhp, errhp, out_P_OUTRECORDDATA, &out_P_OUTRECORDDATA_sz);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
	L("out_P_OUTRECORDDATA_sz %d\n", out_P_OUTRECORDDATA_sz);

	OCIIter *iterator = NULL;
	status = OCIIterCreate(envhp, errhp, (const OCIColl *)out_P_OUTRECORDDATA, &iterator);
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