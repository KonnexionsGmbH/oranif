-ifndef(_DPI_QUERYINFO_HRL_).
-define(_DPI_QUERYINFO_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/structs/dpiQueryInfo.html

-nifs({dpiQueryInfo, [
    {queryInfo_get, [reference]},
    {queryInfo_delete, [reference]}
]}).

-endif. % _DPI_QUERYINFO_HRL_
