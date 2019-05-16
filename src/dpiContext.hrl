-ifndef(_DPI_CONTEXT_HRL_).
-define(_DPI_CONTEXT_HRL_, true).

-include("dpi.hrl").

% see : https://oracle.github.io/odpi/doc/functions/dpiContext.html
-nifs({dpiContext, [
    {context_create, [integer, integer]},
    {context_destroy, [reference]},
    {context_getClientVersion, [reference]},
    {context_getError, [reference]},
    {context_initCommonCreateParams, [reference]},
    {context_initConnCreateParams, [reference]},
    {context_initPoolCreateParams, [reference]},
    {context_initSodaOperOptions, [reference]},
    {context_initSubscrCreateParams, [reference]},
    {context_testfunc, [list, map, reference, port, integer]}
]}).

-endif. % _DPI_CONTEXT_HRL_
