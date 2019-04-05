-ifndef(_DPI_STMT_HRL_).
-define(_DPI_STMT_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiSTMT.html

-nifs({dpiStmt, [
    ?SF(tmt_addRef, [reference]),
    ?SF(tmt_bindByName, [reference, binary, reference]),
    ?SF(tmt_bindByPos, [reference, integer, reference]),
    ?SF(tmt_bindValueByName, [reference, binary, term, term]),
    ?SF(tmt_bindValueByPos, [reference, integer, term, term]),
    ?SF(tmt_close, [reference, binary]),
    ?SF(tmt_define, [reference, integer, reference]),
    ?SF(tmt_defineValue, [reference, integer, atom, atom, integer, atom, term]), %% atom is bool, last argument is actually binary, but it's optional
    ?SF(tmt_execute, [reference, list]),
    ?SF(tmt_executeMany, [reference, binary, integer]),
    ?SF(tmt_fetch, [reference]),
    ?SF(tmt_fetchRows, [reference, integer]),
    ?SF(tmt_getBatchErrorCount, [reference]),
    ?SF(tmt_getBatchErrors, [reference, integer, reference]),
    ?SF(tmt_getBindCount, [reference]),
    ?SF(tmt_getBindNames, [reference]),
    ?SF(tmt_getFetchArraySize, [reference]),
    ?SF(tmt_getImplicitResult, [reference]),
    ?SF(tmt_getInfo, [reference]),
    ?SF(tmt_getNumQueryColumns, [reference]),
    ?SF(tmt_getQueryInfo, [reference, integer], create),
    ?SF(tmt_getQueryValue, [reference, integer], create),
    ?SF(tmt_getRowCount, [reference]),
    ?SF(tmt_getRowCounts, [reference]),
    ?SF(tmt_getSubscrQueryId, [reference]),
    ?SF(tmt_release, [reference], delete),
    ?SF(tmt_scroll, [reference, binary, integer, integer]),
    ?SF(tmt_setFetchArraySize, [reference, integer])
]}).

-endif. % _DPI_STMT_HRL_
