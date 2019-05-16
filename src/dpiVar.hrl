-ifndef(_DPI_VAR_HRL_).
-define(_DPI_VAR_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiVar.html

-nifs({dpiVar, [
    {var_addRef, [reference]},
    {var_copyData, [reference, integer, reference, integer]},
    {var_getNumElementsInArray, [reference]},
    {var_getReturnedData, [reference, integer]},
    {var_getSizeInBytes, [reference]},
    {var_release, [reference]},
    {var_setFromBytes, [reference, integer, binary]},
    {var_setFromLob, [reference, integer, reference]},
    {var_setFromObject, [reference, integer, reference]},
    {var_setFromRowid, [reference, integer, reference]},
    {var_setFromStmt, [reference, integer, reference]},
    {var_setNumElementsInArray, [reference, integer]}
]}).

-endif. % _DPI_VAR_HRL_
