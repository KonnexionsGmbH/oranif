/* nix command shell:
    LD_LIBRARY_PATH=$INSTANT_CLIENT_LIB_PATH ./oci_scratchpad/console
*/

#ifdef __WIN32__
#include <crtdbg.h>
#endif

#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <oci.h>
#include <orid.h>

#define LOG_ERROR(__errhp, __status) checkerr(__errhp, __status, __FUNCTION__, __LINE__)

static void checkerr(OCIError *, sword, const char[100], int);

int main(int argc, char *argv[])
{
	const char
		*tns = "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))",
		*usr = "scott",
		*pwd = "regit";

	OCIEnv *envhp = NULL;
	OCIError *errhp = NULL;
	OCISPool *spoolhp = NULL;
	OCISvcCtx *svchp = NULL;
	OCIAuthInfo *authp = NULL;
	sword status = OCI_SUCCESS;

	status = OCIEnvCreate(&envhp, OCI_OBJECT | OCI_THREADED, NULL, NULL, NULL, NULL, 0, NULL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(NULL, status);
		return -1;
	}

	status = OCIHandleAlloc(envhp, (void **)&errhp, OCI_HTYPE_ERROR, (size_t)0, (void **)NULL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(NULL, status);
		return -1;
	}

	status = OCIHandleAlloc((const dvoid *)envhp, (dvoid **)&spoolhp, OCI_HTYPE_SPOOL, 0, NULL);
	if (status != OCI_SUCCESS || spoolhp == NULL)
	{
		LOG_ERROR(NULL, status);
		return -1;
	}

//#define GOOD_CRED_IN_POOL
//#define GOOD_CRED_NOT_IN_POOL

#ifdef GOOD_CRED_IN_POOL
	OraText *poolName = (OraText *)malloc(50);
	ub4 poolNameLen;
	status = OCISessionPoolCreate(
		/* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
		/* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
		/* connStr */ (OraText *)tns, /* connStrLen */ (sb4)strlen(tns),
		/* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
		/* userid */ (OraText *)usr, /* useridLen */ (sb4)strlen(usr),
		/* password */ (OraText *)pwd, /* passwordLen */ (sb4)strlen(pwd),
		/* mode */ OCI_SPC_HOMOGENEOUS);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCISessionGet(
		envhp, errhp, &svchp, /*OCIAuthInfo *authInfop*/ NULL,
		poolName, poolNameLen,
		NULL, 0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
#endif
#ifdef GOOD_CRED_NOT_IN_POOL
	OraText *poolName = (OraText *)malloc(50);
	ub4 poolNameLen;
	status = OCISessionPoolCreate(
		/* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
		/* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
		/* connStr */ (OraText *)tns, /* connStrLen */ (sb4)strlen(tns),
		/* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
		/* userid */ NULL, /* useridLen */ 0,
		/* password */ NULL, /* passwordLen */ 0,
		/* mode */ OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIHandleAlloc(envhp, &authp, OCI_HTYPE_AUTHINFO, 0, NULL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(NULL, status);
		return -1;
	}

	status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, usr, (ub4)strlen(usr), OCI_ATTR_USERNAME, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, pwd, (ub4)strlen(pwd), OCI_ATTR_PASSWORD, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCISessionGet(
		envhp, errhp, &svchp, authp,
		poolName, poolNameLen,
		NULL, 0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
#endif

//#define BAD_CRED_IN_POOL
#define BAD_CRED_NOT_IN_POOL

#ifdef BAD_CRED_IN_POOL
	OraText *poolName = (OraText *)malloc(50);
	ub4 poolNameLen;
	pwd = "badpassword";
	status = OCISessionPoolCreate(
		/* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
		/* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
		/* connStr */ (OraText *)tns, /* connStrLen */ (sb4)strlen(tns),
		/* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
		/* userid */ (OraText *)usr, /* useridLen */ (sb4)strlen(usr),
		/* password */ (OraText *)pwd, /* passwordLen */ (sb4)strlen(pwd),
		/* mode */ OCI_SPC_HOMOGENEOUS);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCISessionGet(
		envhp, errhp, &svchp, /*OCIAuthInfo *authInfop*/ NULL,
		poolName, poolNameLen,
		NULL, 0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
#endif
#ifdef BAD_CRED_NOT_IN_POOL
	OraText *poolName = (OraText *)malloc(50);
	ub4 poolNameLen;
	pwd = "badpassword";
	status = OCISessionPoolCreate(
		/* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
		/* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
		/* connStr */ (OraText *)tns, /* connStrLen */ (sb4)strlen(tns),
		/* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
		/* userid */ NULL, /* useridLen */ 0,
		/* password */ NULL, /* passwordLen */ 0,
		/* mode */ OCI_DEFAULT);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIHandleAlloc(envhp, &authp, OCI_HTYPE_AUTHINFO, 0, NULL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(NULL, status);
		return -1;
	}

	status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, usr, (ub4)strlen(usr), OCI_ATTR_USERNAME, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, pwd, (ub4)strlen(pwd), OCI_ATTR_PASSWORD, errhp);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}

	status = OCISessionGet(
		envhp, errhp, &svchp, authp,
		poolName, poolNameLen,
		NULL, 0, NULL, NULL, NULL, OCI_SESSGET_SPOOL);
	if (status != OCI_SUCCESS)
	{
		LOG_ERROR(errhp, status);
		return -1;
	}
#endif

	printf("%s:%d SUCCESS\r\n", __FUNCTION__, __LINE__);
	return 0;
}

static void checkerr(OCIError *errhp, sword status, const char fun[100], int line)
{
	switch (status)
	{
	case OCI_SUCCESS:
		break;
	case OCI_SUCCESS_WITH_INFO:
		printf("[%s:%d] Error - OCI_SUCCESS_WITH_INFO\n", fun, line);
		break;
	case OCI_NEED_DATA:
		printf("[%s:%d] Error - OCI_NEED_DATA\n", fun, line);
		break;
	case OCI_NO_DATA:
		printf("[%s:%d] Error - OCI_NODATA\n", fun, line);
		break;
	case OCI_ERROR:
	{
		if (errhp != NULL)
		{
			text errbuf[512];
			sb4 errcode = 0;

			memset(errbuf, 0, sizeof(errbuf));
			OCIErrorGet((dvoid *)errhp, (ub4)1, NULL, &errcode,
						errbuf, (ub4)sizeof(errbuf), OCI_HTYPE_ERROR);
			printf("[%s:%d] Error - ORA-%d : %s\n", fun, line, errcode, errbuf);
		}
		else
		{
			printf("[%s:%d] Error - OCI_ERROR\n", fun, line);
		}
		break;
	}
	case OCI_INVALID_HANDLE:
		printf("[%s:%d] Error - OCI_INVALID_HANDLE\n", fun, line);
		break;
	case OCI_STILL_EXECUTING:
		printf("[%s:%d] Error - OCI_STILL_EXECUTE\n", fun, line);
		break;
	case OCI_CONTINUE:
		printf("[%s:%d] Error - OCI_CONTINUE\n", fun, line);
		break;
	default:
		printf("[%s:%d] Error - Unknown %d\n", fun, line, status);
		break;
	}
}
