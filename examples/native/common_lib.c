#ifdef __WIN32__
#include <crtdbg.h>
#endif

#define _CRTDBG_MAP_ALLOC
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "common.h"

OCIEnv *envhp = NULL;
OCIError *errhp = NULL;
OCISPool *spoolhp = NULL;
OCISvcCtx *svchp = NULL;
OCIAuthInfo *authp = NULL;
sword status = OCI_SUCCESS;

int init(void)
{
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

    status = OCIHandleAlloc(envhp, (void **)&spoolhp, OCI_HTYPE_SPOOL, 0, NULL);
    if (status != OCI_SUCCESS || spoolhp == NULL)
    {
        LOG_ERROR(NULL, status);
        return -1;
    }

    return 0;
}

int session(void)
{
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

    return 0;
}

void checkerr(OCIError *errorhp, sword sts, const char fun[100], int line)
{
    switch (sts)
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
        if (errorhp != NULL)
        {
            text errbuf[512];
            sb4 errcode = 0;

            memset(errbuf, 0, sizeof(errbuf));
            OCIErrorGet((dvoid *)errorhp, (ub4)1, NULL, &errcode,
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
        printf("[%s:%d] Error - Unknown %d\n", fun, line, sts);
        break;
    }
}