#ifndef _DPI_NIF_H_
#define _DPI_NIF_H_
#include "erl_nif.h"

#include "stdio.h"
#include "dpi.h"

#define TRACE                                                   \
    printf("[%s:%s:%d]\r\n", __FILE__, __FUNCTION__, __LINE__); \
    fflush(stdout)

#define CALL_TRACE                                                     \
    printf("[%s:%s:%d] called\r\n", __FILE__, __FUNCTION__, __LINE__); \
    fflush(stdout)

#define RETURNED_TRACE                                                   \
    printf("[%s:%s:%d] returned\r\n", __FILE__, __FUNCTION__, __LINE__); \
    fflush(stdout)

#ifndef __WIN32__
#define L(_Format, ...)                                            \
    printf("[%s:%s:%d] "_Format, __FILE__, __FUNCTION__, __LINE__, \
           ##__VA_ARGS__);                                         \
    fflush(stdout)

#define E(_str, ...) \
    L("ERROR: " _str, ##__VA_ARGS__);
#else
#define L(_Format, ...)                                            \
    printf("[%s:%s:%d] "_Format, __FILE__, __FUNCTION__, __LINE__, \
           __VA_ARGS__);                                           \
    fflush(stdout)

#define E(_str, ...) \
    L("ERROR: " _str, __VA_ARGS__)
#endif

#define RAISE_STR_EXCEPTION(__EB) \
    RAISE_EXCEPTION(enif_make_string(env, (const char *)(__EB), ERL_NIF_LATIN1))

#define RAISE_EXCEPTION(__T)                                                  \
    enif_raise_exception(                                                     \
        env,                                                                  \
        enif_make_tuple4(                                                     \
            env, ATOM_ERROR, enif_make_string(env, __FILE__, ERL_NIF_LATIN1), \
            enif_make_int(env, __LINE__), (__T)))

#define CHECK_ARGCOUNT(_Count)                                       \
    TRACE;                                                           \
    if (argc != _Count)                                              \
    {                                                                \
        return enif_raise_exception(                                 \
            env,                                                     \
            enif_make_string(                                        \
                env, "Wrong number of arguments. Required " #_Count, \
                ERL_NIF_LATIN1));                                    \
    }

#define BADARG_EXCEPTION(_idx, _type)                           \
    enif_raise_exception(                                       \
        env,                                                    \
        enif_make_string(                                       \
            env, "Unable to retrieve " _type " from arg" #_idx, \
            ERL_NIF_LATIN1))

#define DEF_RES(_res)                                                \
    _res##_type = enif_open_resource_type(                           \
        env, NULL, #_res, _res##_res_dtor, ERL_NIF_RT_CREATE, NULL); \
    if (!_res##_type)                                                \
    {                                                                \
        E("Failed to open resource type \"" #_res "\"");             \
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

extern ERL_NIF_TERM dpiErrorInfoMap(ErlNifEnv*, dpiErrorInfo);
#define RAISE_EXCEPTION_ON_DPI_ERROR(_ctx, _exprn, _opt_res) \
    if (DPI_FAILURE == (_exprn))                             \
    {                                                        \
        dpiErrorInfo __err;                                  \
        if (_opt_res)                                        \
            enif_release_resource(_opt_res);                 \
        dpiContext_getError(_ctx, &__err);                   \
        return RAISE_EXCEPTION(dpiErrorInfoMap(env, __err)); \
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

#endif // _DPI_NIF_H_
