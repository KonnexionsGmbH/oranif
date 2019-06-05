-ifndef(_DPI_CONTEXT_HRL_).
-define(_DPI_CONTEXT_HRL_, true).

-include("dpi.hrl").

% see : https://oracle.github.io/odpi/doc/functions/dpiContext.html
-nifs({dpiContext, [
    {context_create, [integer, integer]},
    {context_destroy, [reference]},
    {context_getClientVersion, [reference]},
    {context_getError, [reference]}
]}).

-endif. % _DPI_CONTEXT_HRL_
