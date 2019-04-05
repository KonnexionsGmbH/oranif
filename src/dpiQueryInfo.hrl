-ifndef(_DPI_QUERYINFO_HRL_).
-define(_DPI_QUERYINFO_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/structs/dpiQueryInfo.html

-nifs({dpiQueryInfo, [
    ?QF(ueryInfo_get, [reference]),
    ?QF(ueryInfo_delete, [reference], delete)
]}).

-endif. % _DPI_QUERYINFO_HRL_
