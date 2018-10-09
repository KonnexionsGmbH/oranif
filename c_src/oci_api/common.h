#ifndef _COMMON_H_
#define _COMMON_H_

#include <oci.h>
#include <orid.h>

#define TNS "(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)"\
           "(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))"
#define USR "scott"
#define PWD	"regit"

extern OCIEnv *envhp;
extern OCIError *errhp;
extern OCISPool *spoolhp;
extern OCISvcCtx *svchp;
extern OCIAuthInfo *authp;
extern sword status;

#define LOG_ERROR(__errhp, __status) checkerr(__errhp, __status, __FUNCTION__, __LINE__)

extern int init(void);
extern int session(void);
extern void checkerr(OCIError *, sword, const char[100], int);

#endif // _COMMON_H_
