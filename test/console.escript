#!/usr/bin/env escript
%% -*- erlang -*-
%%! -env LD_LIBRARY_PATH priv/ -smp enable -pa _build/default/lib/erloci/ebin

-define(TNS,<<
	"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
  	"(PROTOCOL=tcp)(HOST=192.168.1.43)(PORT=1521)))"
  	"(CONNECT_DATA=(SERVICE_NAME=XE)))"
>>).
-define(LANG, "GERMAN_SWITZERLAND.AL32UTF8").
-define(USR, <<"scott">>).
-define(PWD, <<"regit">>).

-include("../src/oci.hrl").

main(_) ->
	{Language, Country, Charset} = erloci:parse_lang(?LANG),

    E1 =  erloci:ociEnvNlsCreate(0,0),
    {ok, CharsetId} = erloci:ociNlsCharSetNameToId(E1, Charset),
    ok = erloci:ociEnvHandleFree(E1),
    Envhp =  erloci:ociEnvNlsCreate(CharsetId,CharsetId),
    ok = erloci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, Language, ?OCI_ATTR_ENV_NLS_LANGUAGE),
    ok = erloci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, Country, ?OCI_ATTR_ENV_NLS_TERRITORY),
    {ok, Spoolhp} =  erloci:ociSessionPoolCreate(Envhp, ?TNS, 2, 10, 1, null,
                                                 null),

    {ok, Authhp} =  erloci:ociAuthHandleCreate(Envhp, ?USR, ?PWD),
    {ok, Svchp} =  erloci:ociSessionGet(Envhp, Authhp, Spoolhp),

    {ok, Stmthp} = erloci:ociStmtHandleCreate(Envhp),
    ok = erloci:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A or dummy = :B">>),
    {ok, BindVars1} = erloci:ociBindByName(Stmthp, #{}, <<"A">>, ?SQLT_CHR, <<"X">>),
    {ok, BindVars2} = erloci:ociBindByName(Stmthp, BindVars1, <<"B">>,  ?SQLT_CHR, <<"Y">>),
    {ok, #{statement := ?OCI_STMT_SELECT, cols := [{<<"DUMMY">>,?SQLT_CHR,1,_,0}]}} =  erloci:ociStmtExecute(Svchp, Stmthp, BindVars2, 0, 0, ?OCI_DEFAULT),
    {ok, [[<<"X">>]], _} = erloci:ociStmtFetch(Stmthp, 1).