-ifndef(_DPI_VAR_HRL_).
-define(_DPI_VAR_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiVar.html

-nifs({dpiVar, [
    ?VF(ar_addRef, [reference]),
    ?VF(ar_copyData, [reference, integer, reference, integer]),
    ?VF(ar_getNumElementsInArray, [reference]),
    ?VF(ar_getReturnedData, [reference, integer]),
    ?VF(ar_getSizeInBytes, [reference]),
    ?VF(ar_release, [reference], delete),
    ?VF(ar_setFromBytes, [reference, integer, binary]),
    ?VF(ar_setFromLob, [reference, integer, reference]),
    ?VF(ar_setFromObject, [reference, integer, reference]),
    ?VF(ar_setFromRowid, [reference, integer, reference]),
    ?VF(ar_setFromStmt, [reference, integer, reference]),
    ?VF(ar_setNumElementsInArray, [reference, integer])
]}).

-endif. % _DPI_VAR_HRL_
