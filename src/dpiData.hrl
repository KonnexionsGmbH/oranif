-ifndef(_DPI_DATA_HRL_).
-define(_DPI_DATA_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiData.html

-nifs({dpiData, [
    ?DF(ata_getBool, [reference]),
    ?DF(ata_getBytes, [reference]),
    ?DF(ata_getDouble, [reference]),
    ?DF(ata_getFloat, [reference]),
    ?DF(ata_getInt64, [reference]),
    ?DF(ata_getIntervalDS, [reference]),
    ?DF(ata_getIntervalYM, [reference]),
    ?DF(ata_getLOB, [reference]),
    ?DF(ata_getObject, [reference]),
    ?DF(ata_getStmt, [reference]),
    ?DF(ata_getTimestamp, [reference]),
    ?DF(ata_getUint64, [reference]),
    ?DF(ata_setBool, [reference, atom]),
    ?DF(ata_setBytes, [reference, binary]),
    ?DF(ata_setDouble, [reference, float]),
    ?DF(ata_setFloat, [reference, float]),
    ?DF(ata_setInt64, [reference, integer]),
    ?DF(ata_setIntervalDS,
        [reference, integer, integer, integer, integer, integer]),
    ?DF(ata_setIntervalYM, [reference, integer, integer]),
    ?DF(ata_setLOB, [reference, reference]),
    ?DF(ata_setObject, [reference, reference]),
    ?DF(ata_setStmt, [reference, reference]),
    ?DF(ata_setTimestamp,
        [reference, integer, integer, integer, integer, integer, integer,
         integer, integer, integer]),
    ?DF(ata_setUint64, [reference, integer]),
    ?DF(ata_ctor, [], create),
    ?DF(ata_get, [reference]),
    ?DF(ata_setIsNull, [reference, atom]),
    ?DF(ata_release, [reference], delete)
]}).

-endif. % _DPI_DATA_HRL_
