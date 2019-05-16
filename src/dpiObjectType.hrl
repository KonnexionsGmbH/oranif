-ifndef(_DPI_OBJECTTYPE_HRL_).
-define(_DPI_OBJECTTYPE_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiObjectType.html

-nifs({dpiQueryInfo, [
    {objectType_addRef, [reference]},
    {objectType_createObject, [reference]},
    {objectType_getAttributes, [reference, integer]},
    {objectType_getInfo, [reference]},
    {objectType_release, [reference]}
]}).

-endif. % _DPI_OBJECTTYPE_HRL_
