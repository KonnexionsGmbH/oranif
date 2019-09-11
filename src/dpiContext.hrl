-ifndef(_DPI_CONTEXT_HRL_).
-define(_DPI_CONTEXT_HRL_, true).

-include("dpi.hrl").

% see : https://oracle.github.io/odpi/doc/functions/dpiContext.html
-nifs({dpiContext, [
    {context_create, [integer, integer]},
    {context_create_n, [integer, integer, binary]},
    {context_destroy, [reference]},
    {context_destroy_n, [reference, binary]},
    {context_getClientVersion, [reference]}
]}).

-endif. % _DPI_CONTEXT_HRL_
