-ifndef(_DPI_DATA_HRL_).
-define(_DPI_DATA_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiData.html

-nifs({dpiData, [
    {data_getBytes, [reference]},
    {data_getInt64, [reference]},
    {data_getDouble, [reference]},
    {data_setBytes, [reference, binary]},
    {data_setInt64, [reference, integer]},
    {data_setDouble, [reference, float]},
    {data_setIntervalDS,
        [reference, integer, integer, integer, integer, integer]},
    {data_setIntervalYM, [reference, integer, integer]},
    {data_setTimestamp,
        [reference, integer, integer, integer, integer, integer, integer,
         integer, integer, integer]},
    {data_ctor, []},
    {data_get, [reference]},
    {data_setIsNull, [reference, atom]},
    {data_release, [reference]}
]}).

-endif. % _DPI_DATA_HRL_
