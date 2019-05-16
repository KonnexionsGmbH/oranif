-ifndef(_DPI_DATA_HRL_).
-define(_DPI_DATA_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiData.html

-nifs({dpiData, [
    {data_getBool, [reference]},
    {data_getBytes, [reference]},
    {data_getDouble, [reference]},
    {data_getFloat, [reference]},
    {data_getInt64, [reference]},
    {data_getIntervalDS, [reference]},
    {data_getIntervalYM, [reference]},
    {data_getLOB, [reference]},
    {data_getObject, [reference]},
    {data_getStmt, [reference]},
    {data_getTimestamp, [reference]},
    {data_getUint64, [reference]},
    {data_setBool, [reference, atom]},
    {data_setBytes, [reference, binary]},
    {data_setDouble, [reference, float]},
    {data_setFloat, [reference, float]},
    {data_setInt64, [reference, integer]},
    {data_setIntervalDS,
        [reference, integer, integer, integer, integer, integer]},
    {data_setIntervalYM, [reference, integer, integer]},
    {data_setLOB, [reference, reference]},
    {data_setObject, [reference, reference]},
    {data_setStmt, [reference, reference]},
    {data_setTimestamp,
        [reference, integer, integer, integer, integer, integer, integer,
         integer, integer, integer]},
    {data_setUint64, [reference, integer]},
    {data_ctor, []},
    {data_get, [reference]},
    {data_setIsNull, [reference, atom]},
    {data_release, [reference]}
]}).

-endif. % _DPI_DATA_HRL_
