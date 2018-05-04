-module(erloci_nif).

-export([test_column_types/2, test_select/2]).

%% Basic set of test calls to verify progress during dev
-spec test_column_types(binary(), binary()) -> ok.
test_column_types(User, Pass) ->
        Envhp = erloci_nif_drv:ociEnvCreate(),
        Spoolhp = erloci_nif_drv:ociSpoolHandleCreate(Envhp),
        {ok, Spool} = erloci_nif_drv:ociSessionPoolCreate(Envhp, Spoolhp,
        %    <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>,
            <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>,
            2, 10, 1, User, Pass),
        {ok, Authhp} = erloci_nif_drv:ociAuthHandleCreate(Envhp, User, Pass),
        {ok, Svchp} = erloci_nif_drv:ociSessionGet(Envhp, Authhp, Spool),
        {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
        ok = erloci_nif_drv:ociStmtPrepare(Envhp, Stmthp, <<"select * from testtable">>),
        ok = erloci_nif_drv:ociStmtExecute(Envhp, Svchp, Stmthp, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT')),
        {ok, Rows} = erloci_nif_drv:ociStmtFetch(Envhp, Stmthp, 1).


-spec test_select(binary(), binary()) -> ok.
test_select(User, Pass) ->
    Envhp = erloci_nif_drv:ociEnvCreate(),
    Spoolhp = erloci_nif_drv:ociSpoolHandleCreate(Envhp),
    {ok, Spool} = erloci_nif_drv:ociSessionPoolCreate(Envhp, Spoolhp,
    %    <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.43)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>,
        <<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=XE)))">>,
        2, 10, 1, User, Pass),
    {ok, Authhp} = erloci_nif_drv:ociAuthHandleCreate(Envhp, User, Pass),
    {ok, Svchp} = erloci_nif_drv:ociSessionGet(Envhp, Authhp, Spool),
    {ok, Stmthp} = erloci_nif_drv:ociStmtHandleCreate(Envhp),
    ok = erloci_nif_drv:ociStmtPrepare(Envhp, Stmthp, <<"select * from dual where dummy = :A or dummy = :B">>),
    {ok, _BindVar} = erloci_nif_drv:ociBindByName(Envhp, Stmthp, <<"A">>, 0, erloci_nif_drv:sql_type('SQLT_CHR'), <<"X">>),
    {ok, _BindVar2} = erloci_nif_drv:ociBindByName(Envhp, Stmthp, <<"B">>, 0, erloci_nif_drv:sql_type('SQLT_CHR'), <<"Y">>),
    ok = erloci_nif_drv:ociStmtExecute(Envhp, Svchp, Stmthp, 0, 0, erloci_nif_drv:oci_mode('OCI_DEFAULT')).
    %% {ok, Rows} = erloci_nif_drv:ociStmtFetch(Envhp, Stmthp, 1).


