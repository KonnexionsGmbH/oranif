-ifndef(_DPI_HRL_).
-define(_DPI_HRL_, true).

% macros
-define(GUARD_NULL_STR(_X),     _X == null orelse is_binary(_X)).
-define(GUARD_NULL_INT(_X),     _X == null orelse is_integer(_X)).
-define(GUARD_ARRY_LEN(_X, _Y), _X == null orelse length(_X) == _Y).

-define(NIF_NOT_LOADED,
    exit({nif_library_not_loaded, ?MODULE, ?FUNCTION_NAME})
).

-endif. % _DPI_HRL_
