-ifndef(_DPI_CONN_HRL_).
-define(_DPI_CONN_HRL_, true).

-include("dpi.hrl").

% see: https://oracle.github.io/odpi/doc/public_functions/dpiConn.html

-nifs({dpiConn, [
    {conn_close, [reference, list, binary]},
    {conn_close_n, [reference, list, binary, binary]},
    {conn_commit, [reference]},
    {conn_create, [reference, binary, binary, binary, {map, null}, {map, null}]},
    {conn_create_n, [reference, binary, binary, binary, {map, null}, {map, null}, binary]},
    {conn_getServerVersion, [reference]},
    {conn_newVar, [reference, atom, atom, integer, integer, atom, atom, atom]}, %% bools are to be checked if atom true|false in NIF-C code
    {conn_newVar_n, [reference, atom, atom, integer, integer, atom, atom, atom, binary]}, %% bools are to be checked if atom true|false in NIF-C code
    {conn_ping, [reference]},
    {conn_prepareStmt, [reference, atom, binary, binary]}, %% bool to be checked if atom true|false in NIF-C code
    {conn_rollback, [reference]},
    {conn_setClientIdentifier, [reference, binary]}
]}).

-endif. % _DPI_CONN_HRL_
