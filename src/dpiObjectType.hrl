-ifndef(_DPI_OBJECTTYPE_HRL_).
-define(_DPI_OBJECTTYPE_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiObjectType.html

-nifs({dpiQueryInfo, [
    ?OF(bjectType_addRef, [reference]),
    ?OF(bjectType_createObject, [reference], create),
    ?OF(bjectType_getAttributes, [reference, integer]),
    ?OF(bjectType_getInfo, [reference]),
    ?OF(bjectType_release, [reference], delete)
]}).

-endif. % _DPI_OBJECTTYPE_HRL_
