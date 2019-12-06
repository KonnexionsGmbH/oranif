-ifndef(_DPI_STMT_HRL_).
-define(_DPI_STMT_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiSTMT.html

-nifs({dpiStmt, [
    {stmt_bindByName, [reference, binary, reference]},
    {stmt_bindByPos, [reference, integer, reference]},
    {stmt_bindValueByName, [reference, binary, term, term]},
    {stmt_bindValueByPos, [reference, integer, term, term]},
    {stmt_define, [reference, integer, reference]},
    {stmt_defineValue, [reference, integer, atom, atom, integer, atom, term]}, %% atom is bool, last argument is actually binary, but it's optional
    {stmt_execute, [reference, list]},
    {stmt_executeMany, [reference, list, integer]},
    {stmt_fetch, [reference]},
    {stmt_fetchRows, [reference, integer]},
    {stmt_getQueryInfo, [reference, integer]},
    {stmt_getQueryValue, [reference, integer]},
    {stmt_close, [reference, binary]},
    {stmt_getNumQueryColumns, [reference]},
    {stmt_getInfo, [reference]}
]}).

-endif. % _DPI_STMT_HRL_
