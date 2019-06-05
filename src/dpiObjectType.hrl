-ifndef(_DPI_OBJECTTYPE_HRL_).
-define(_DPI_OBJECTTYPE_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiObjectType.html

-nifs({dpiQueryInfo, [
    {objectType_release, [reference]}
]}).

-endif. % _DPI_OBJECTTYPE_HRL_
