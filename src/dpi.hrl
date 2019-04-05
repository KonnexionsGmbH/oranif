-ifndef(_DPI_HRL_).
-define(_DPI_HRL_, true).

% macros
-define(GUARD_NULL_STR(_X),     _X == null orelse is_binary(_X)).
-define(GUARD_NULL_INT(_X),     _X == null orelse is_integer(_X)).
-define(GUARD_ARRY_LEN(_X, _Y), _X == null orelse length(_X) == _Y).

-define(NIF_NOT_LOADED,
    exit({nif_library_not_loaded, ?MODULE, ?FUNCTION_NAME})
).

-define(G_NIF_FUN(__Prefix1, __Prefix2, __Fn, __ArgsIn, __mode),
	{__Prefix1??__Fn, __Prefix2??__Fn"_nif", __ArgsIn, __mode}
).

-define(CF(__Fn, __ArgsIn), ?G_NIF_FUN("c", "dpiC", __Fn, __ArgsIn, undefined)).
-define(SF(__Fn, __ArgsIn), ?G_NIF_FUN("s", "dpiS", __Fn, __ArgsIn, undefined)).
-define(DF(__Fn, __ArgsIn), ?G_NIF_FUN("d", "dpiD", __Fn, __ArgsIn, undefined)).
-define(QF(__Fn, __ArgsIn), ?G_NIF_FUN("q", "dpiQ", __Fn, __ArgsIn, undefined)).
-define(VF(__Fn, __ArgsIn), ?G_NIF_FUN("v", "dpiV", __Fn, __ArgsIn, undefined)).
-define(OF(__Fn, __ArgsIn), ?G_NIF_FUN("o", "dpiO", __Fn, __ArgsIn, undefined)).

-define(CF(__Fn, __ArgsIn, __mode), ?G_NIF_FUN("c", "dpiC", __Fn, __ArgsIn, __mode)).
-define(SF(__Fn, __ArgsIn, __mode), ?G_NIF_FUN("s", "dpiS", __Fn, __ArgsIn, __mode)).
-define(DF(__Fn, __ArgsIn, __mode), ?G_NIF_FUN("d", "dpiD", __Fn, __ArgsIn, __mode)).
-define(QF(__Fn, __ArgsIn, __mode), ?G_NIF_FUN("q", "dpiQ", __Fn, __ArgsIn, __mode)).
-define(VF(__Fn, __ArgsIn, __mode), ?G_NIF_FUN("v", "dpiV", __Fn, __ArgsIn, __mode)).
-define(OF(__Fn, __ArgsIn, __mode), ?G_NIF_FUN("o", "dpiO", __Fn, __ArgsIn, __mode)).

-endif. % _DPI_HRL_
