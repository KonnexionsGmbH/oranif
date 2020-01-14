-ifndef(_DPI_LOB_HRL_).
-define(_DPI_LOB_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiLob.html

-nifs({dpiLob, [
    {lob_release, [reference]},
    {lob_setFromBytes, [reference, binary]},
    {lob_readBytes, [reference, integer, integer]}
    
]}).

-endif. % _DPI_LOB_HRL_
