/* nix command shell:
    LD_LIBRARY_PATH=priv/ ./c_src/oci_api/session
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

#define GOOD_CRED_IN_POOL
        //#define GOOD_CRED_NOT_IN_POOL

#ifdef GOOD_CRED_IN_POOL
    authp = authp;

    OraText *poolName = (OraText *)malloc(50);
    ub4 poolNameLen;
    status = OCISessionPoolCreate(
        /* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
        /* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
        /* connStr */ (OraText *)TNS, /* connStrLen */ (sb4)strlen(TNS),
        /* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
        /* userid */ (OraText *)USR, /* useridLen */ (sb4)strlen(USR),
        /* password */ (OraText *)PWD, /* passwordLen */ (sb4)strlen(PWD),
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
        /* connStr */ (OraText *)TNS, /* connStrLen */ (sb4)strlen(TNS),
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

    status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, USR, (ub4)strlen(USR), OCI_ATTR_USERNAME, errhp);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(errhp, status);
        return -1;
    }

    status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, PWD, (ub4)strlen(PWD), OCI_ATTR_PASSWORD, errhp);
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
    //#define BAD_CRED_NOT_IN_POOL

#if (defined BAD_CRED_IN_POOL || BAD_CRED_NOT_IN_POOL)
    char *pwd = "badpassword";
#endif
#ifdef BAD_CRED_IN_POOL
    OraText *poolName = (OraText *)malloc(50);
    ub4 poolNameLen;
    status = OCISessionPoolCreate(
        /* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
        /* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
        /* connStr */ (OraText *)TNS, /* connStrLen */ (sb4)strlen(TNS),
        /* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
        /* userid */ (OraText *)USR, /* useridLen */ (sb4)strlen(USR),
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
    status = OCISessionPoolCreate(
        /* OCIEnv */ envhp, /* OCIError */ errhp, /* OCISPool */ spoolhp,
        /* poolName */ &poolName, /* poolNameLen */ &poolNameLen,
        /* connStr */ (OraText *)TNS, /* connStrLen */ (sb4)strlen(TNS),
        /* sessMin */ 1, /* sessMax */ 10, /* sessIncr */ 1,
        /* userid */ NULL, /* useridLen */ 0,
        /* password */ NULL, /* passwordLen */ 0,
        /* mode */ OCI_DEFAULT);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(errhp, status);
        return -1;
    }

    status = OCIHandleAlloc(envhp, (void **)&authp, OCI_HTYPE_AUTHINFO, 0, NULL);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, (void *)USR, (ub4)strlen(USR), OCI_ATTR_USERNAME, errhp);
    if (status != OCI_SUCCESS)
    {
        LOG_ERROR(errhp, status);
        return -1;
    }

    status = OCIAttrSet(authp, OCI_HTYPE_AUTHINFO, (void *)pwd, (ub4)strlen(pwd), OCI_ATTR_PASSWORD, errhp);
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
