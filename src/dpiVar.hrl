-ifndef(_DPI_VAR_HRL_).
-define(_DPI_VAR_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiVar.html

-nifs({dpiVar, [
    {var_release, [reference]},
    {var_setFromBytes, [reference, integer, binary]},
    {var_setNumElementsInArray, [reference, integer]}
]}).

-endif. % _DPI_VAR_HRL_
