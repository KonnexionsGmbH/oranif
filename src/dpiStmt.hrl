-ifndef(_DPI_STMT_HRL_).
-define(_DPI_STMT_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiSTMT.html

-nifs({dpiStmt, [
    {stmt_addRef, [reference]},
    {stmt_bindByName, [reference, binary, reference]},
    {stmt_bindByPos, [reference, integer, reference]},
    {stmt_bindValueByName, [reference, binary, term, term]},
    {stmt_bindValueByPos, [reference, integer, term, term]},
    {stmt_close, [reference, binary]},
    {stmt_define, [reference, integer, reference]},
    {stmt_defineValue, [reference, integer, atom, atom, integer, atom, term]}, %% atom is bool, last argument is actually binary, but it's optional
    {stmt_execute, [reference, list]},
    {stmt_executeMany, [reference, binary, integer]},
    {stmt_fetch, [reference]},
    {stmt_fetchRows, [reference, integer]},
    {stmt_getBatchErrorCount, [reference]},
    {stmt_getBatchErrors, [reference, integer, reference]},
    {stmt_getBindCount, [reference]},
    {stmt_getBindNames, [reference]},
    {stmt_getFetchArraySize, [reference]},
    {stmt_getImplicitResult, [reference]},
    {stmt_getInfo, [reference]},
    {stmt_getNumQueryColumns, [reference]},
    {stmt_getQueryInfo, [reference, integer]},
    {stmt_getQueryValue, [reference, integer]},
    {stmt_getRowCount, [reference]},
    {stmt_getRowCounts, [reference]},
    {stmt_getSubscrQueryId, [reference]},
    {stmt_release, [reference]},
    {stmt_scroll, [reference, binary, integer, integer]},
    {stmt_setFetchArraySize, [reference, integer]}
]}).

-endif. % _DPI_STMT_HRL_
