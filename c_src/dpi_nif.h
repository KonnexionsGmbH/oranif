#ifndef _DPI_NIF_H_
#define _DPI_NIF_H_
#include "erl_nif.h"
#include "string.h"
#include "stdio.h"
#include "dpi.h"

#ifdef ORANIF_DEBUG

#if ORANIF_DEBUG > 5
#define TRACE                                                   \
    printf("[%s:%s:%d]\r\n", __FILE__, __FUNCTION__, __LINE__); \
    fflush(stdout)
#else
#define TRACE
#endif

#if ORANIF_DEBUG > 4
#define RETURNED_TRACE                                             \
    printf("[%s:%s:%d] <-\r\n", __FILE__, __FUNCTION__, __LINE__); \
    fflush(stdout)
#else
#define RETURNED_TRACE
#endif

#if ORANIF_DEBUG > 3
#define CALL_TRACE                                                 \
    printf("[%s:%s:%d] ->\r\n", __FILE__, __FUNCTION__, __LINE__); \
    fflush(stdout)
#else
#define CALL_TRACE
#endif

#if ORANIF_DEBUG > 2
#ifndef __WIN32__
#define D(_str, ...) L("[D] " _str, ##__VA_ARGS__);
#else
#define D(_str, ...) L("[D] " _str, __VA_ARGS__)
#endif
#else
#define D(_Format, ...)
#endif

#if ORANIF_DEBUG > 1
#ifndef __WIN32__
#define I(_str, ...) L("[I] " _str, ##__VA_ARGS__);
#else
#define I(_str, ...) L("[I] " _str, __VA_ARGS__)
#endif
#else
#define I(_Format, ...)
#endif

#if ORANIF_DEBUG > 0
#ifndef __WIN32__
#define W(_str, ...) L("[W] " _str, ##__VA_ARGS__);
#else
#define W(_str, ...) L("[W] " _str, __VA_ARGS__)
#endif
#else
#define W(_Format, ...)
#endif

#if ORANIF_DEBUG > -1
#ifndef __WIN32__
#define E(_str, ...) L("[E] " _str, ##__VA_ARGS__);
#else
#define E(_str, ...) L("[E] " _str, __VA_ARGS__)
#endif
#else
#define E(_Format, ...)
#endif

#ifndef __WIN32__ // *nix
#define L(_Format, ...)                                            \
    printf("[%s:%s:%d] "_Format, __FILE__, __FUNCTION__, __LINE__, \
           ##__VA_ARGS__);                                         \
    fflush(stdout)
#else // __WIN32__
#define L(_Format, ...)                                            \
    printf("[%s:%s:%d] "_Format, __FILE__, __FUNCTION__, __LINE__, \
           __VA_ARGS__);                                           \
    fflush(stdout)
#endif // __WIN32__

#else // no ORANIF_DEBUG

#define TRACE
#define CALL_TRACE
#define RETURNED_TRACE
#define D(_Format, ...)
#define I(_Format, ...)
#define W(_Format, ...)
#define E(_Format, ...)
#define L(_Format, ...)

#endif // ORANIF_DEBUG

#define RAISE_STR_EXCEPTION(__EB) \
    RAISE_EXCEPTION(enif_make_string(env, (const char *)(__EB), ERL_NIF_LATIN1))

#define RAISE_EXCEPTION(__T)                                     \
    {                                                            \
        RETURNED_TRACE;                                          \
        return enif_raise_exception(                             \
            env,                                                 \
            enif_make_tuple4(                                    \
                env, ATOM_ERROR,                                 \
                enif_make_string(env, __FILE__, ERL_NIF_LATIN1), \
                enif_make_int(env, __LINE__), (__T)));           \
    }

#define CHECK_ARGCOUNT(_Count)                               \
    CALL_TRACE;                                              \
    if (argc != _Count)                                      \
    {                                                        \
        RAISE_STR_EXCEPTION(                                 \
            "Wrong number of arguments. Required " #_Count); \
    }

#define BADARG_EXCEPTION(_idx, _type) \
    RAISE_STR_EXCEPTION("Unable to retrieve " _type " from arg" #_idx);

#define DEF_RES(_res)                                                \
    _res##_type = enif_open_resource_type(                           \
        env, NULL, #_res, _res##_res_dtor, ERL_NIF_RT_CREATE, NULL); \
    if (!_res##_type)                                                \
    {                                                                \
        E("Failed to open resource type \"" #_res "\"");             \
        RETURNED_TRACE;                                              \
        return -1;                                                   \
    }

extern ERL_NIF_TERM ATOM_OK;
extern ERL_NIF_TERM ATOM_NULL;
extern ERL_NIF_TERM ATOM_TRUE;
extern ERL_NIF_TERM ATOM_FALSE;
extern ERL_NIF_TERM ATOM_ERROR;
extern ERL_NIF_TERM ATOM_ENOMEM;

#define DEF_NIF(_fun, _arity) \
    {                         \
#_fun, _arity, _fun   \
    }

#define IOB_NIF(_fun, _arity)                           \
    {                                                   \
#_fun, _arity, _fun, ERL_NIF_DIRTY_JOB_IO_BOUND \
    }

#define DPI_NIF_FUN(_fun) \
    ERL_NIF_TERM _fun(    \
        ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])

extern ERL_NIF_TERM dpiErrorInfoMap(ErlNifEnv *, dpiErrorInfo);
#define RAISE_EXCEPTION_ON_DPI_ERROR(_ctx, _exprn)    \
    if (DPI_FAILURE == (_exprn))                      \
    {                                                 \
        dpiErrorInfo __err;                           \
        dpiContext_getError(_ctx, &__err);            \
        RAISE_EXCEPTION(dpiErrorInfoMap(env, __err)); \
    }

#define RAISE_EXCEPTION_ON_DPI_ERROR_RESOURCE(_ctx, _exprn, _opt_res, _res) \
    if (DPI_FAILURE == (_exprn))                                            \
    {                                                                       \
        dpiErrorInfo __err;                                                 \
        if (_opt_res)                                                       \
            RELEASE_RESOURCE(_opt_res, _res);                               \
        dpiContext_getError(_ctx, &__err);                                  \
        RAISE_EXCEPTION(dpiErrorInfoMap(env, __err));                       \
    }

#define CASE_MACRO2STR(_Macro, _StrVar) \
    case _Macro:                        \
        _StrVar = #_Macro;              \
        break

#define A2M(_macro, _atom, _assign)                             \
    if (enif_is_identical(enif_make_atom(env, #_macro), _atom)) \
    (_assign) = _macro

#define M2A(_macro, _assign)                    \
    case _macro:                                \
        _assign = enif_make_atom(env, #_macro); \
        break


char *strdup(const char *s1);
#define STRING_SIZE 128
typedef struct linkedList{
    char string[STRING_SIZE];
    struct linkedList *next;
 } llist;

void removeNode(llist** head, char* value, unsigned length);
void addNode(llist** head, char* value, unsigned length);
typedef struct
{
    ErlNifMutex *lock;
    unsigned long dpiContext_count;
    unsigned long dpiConn_count;
    unsigned long dpiStmt_count;
    unsigned long dpiData_count;
    unsigned long dpiDataPtr_count;
    unsigned long dpiVar_count;
    llist* resList;
} oranif_st;

#define ALLOC_RESOURCE(_var, _dpiType)                                       \
    {                                                                        \
        oranif_st *st = (oranif_st *)enif_priv_data(env);                    \
        enif_mutex_lock(st->lock);                                           \
        _var = enif_alloc_resource(_dpiType##_type, sizeof(_dpiType##_res)); \
        st->_dpiType##_count++;                                              \
        enif_mutex_unlock(st->lock);                                         \
    }

#define RELEASE_RESOURCE(_var, _dpiType)                  \
    {                                                     \
        oranif_st *st = (oranif_st *)enif_priv_data(env); \
        enif_mutex_lock(st->lock);                        \
        enif_release_resource(_var);                      \
        st->_dpiType##_count--;                           \
        enif_mutex_unlock(st->lock);                      \
    }

// binary: a string that is NOT null terminated (gotten right out of erl binary)
// len: the length of said binary. The null-terminated string is made inside
#define ALLOC_RESOURCE_N(_var, _dpiType, _binary, _len)                      \
    {                                                                           \
        oranif_st *st = (oranif_st *)enif_priv_data(env);                    \
        enif_mutex_lock(st->lock);                                           \
        _var = enif_alloc_resource(_dpiType##_type, sizeof(_dpiType##_res)); \
        st->_dpiType##_count++;                                              \
        addNode(&st->resList, _binary, _len);                                                   \
        llist *head = st->resList;                                           \
        while (head)                                                         \
        {                                                                    \
            head = head->next;                                               \
        }                                                                    \
        enif_mutex_unlock(st->lock);                                         \
    }

#define RELEASE_RESOURCE_N(_var, _dpiType, _binary, _len)                    \
    {                                                                        \
        oranif_st *st = (oranif_st *)enif_priv_data(env);                    \
        enif_mutex_lock(st->lock);                                           \
        enif_release_resource(_var);                                         \
        st->_dpiType##_count--;                                                             \
        removeNode(&st->resList, _binary, _len);                                        \
        llist *head = st->resList;                                           \
        while (head)                                                         \
        {                                                                    \
            head = head->next;                                               \
        }                                                                    \
        enif_mutex_unlock(st->lock);                                         \
    }

#endif // _DPI_NIF_H_
