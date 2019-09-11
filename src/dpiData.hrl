-ifndef(_DPI_DATA_HRL_).
-define(_DPI_DATA_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiData.html

-nifs({dpiData, [
    {data_getBytes, [reference]},
    {data_getInt64, [reference]},
    {data_setBytes, [reference, binary]},
    {data_setInt64, [reference, integer]},
    {data_setIntervalDS,
        [reference, integer, integer, integer, integer, integer]},
    {data_setIntervalYM, [reference, integer, integer]},
    {data_setTimestamp,
        [reference, integer, integer, integer, integer, integer, integer,
         integer, integer, integer]},
    {data_ctor, []},
    {data_ctor_n, [binary]},
    {data_get, [reference]},
    {data_setIsNull, [reference, atom]},
    {data_release, [reference]},
    {data_release_n, [reference, binary]}
]}).

-endif. % _DPI_DATA_HRL_
