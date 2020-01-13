-ifndef(_DPI_LOB_HRL_).
-define(_DPI_LOB_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiLob.html

-nifs({dpiLob, [
    {lob_release, [reference]}
]}).

-endif. % _DPI_LOB_HRL_
