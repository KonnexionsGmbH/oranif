-ifndef(_DPI_CONTEXT_HRL_).
-define(_DPI_CONTEXT_HRL_, true).

-include("dpi.hrl").

% see : https://oracle.github.io/odpi/doc/functions/dpiContext.html
-nifs({dpiContext, [
    ?CF(ontext_create, [integer, integer], create),
    ?CF(ontext_destroy, [reference], delete),
    ?CF(ontext_getClientVersion, [reference]),
    ?CF(ontext_getError, [reference]),
    ?CF(ontext_initCommonCreateParams, [reference]),
    ?CF(ontext_initConnCreateParams, [reference]),
    ?CF(ontext_initPoolCreateParams, [reference]),
    ?CF(ontext_initSodaOperOptions, [reference]),
    ?CF(ontext_initSubscrCreateParams, [reference]),
    ?CF(ontext_testfunc, [list, map, reference, port, integer])
]}).

-endif. % _DPI_CONTEXT_HRL_
