#!/usr/bin/env escript
%% -*- erlang -*-
%%! -env LD_LIBRARY_PATH priv/ -smp enable -pa _build/default/lib/oci/ebin

-define(TNS,<<
	"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
  	"(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))"
  	"(CONNECT_DATA=(SERVICE_NAME=XE)))"
>>).
-define(LANG, "GERMAN_SWITZERLAND.AL32UTF8").
-define(USR, <<"scott">>).
-define(PWD, <<"regit">>).

-include("../src/oci.hrl").

main(_) ->
	{Language, Country, Charset} = oci:parse_lang(?LANG),

    E1 =  oci:ociEnvNlsCreate(0,0),
    {ok, CharsetId} = oci:ociNlsCharSetNameToId(E1, Charset),
    ok = oci:ociEnvHandleFree(E1),
    Envhp =  oci:ociEnvNlsCreate(CharsetId,CharsetId),
    ok = oci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, Language, ?OCI_ATTR_ENV_NLS_LANGUAGE),
    ok = oci:ociAttrSet(Envhp, ?OCI_HTYPE_ENV, text, Country, ?OCI_ATTR_ENV_NLS_TERRITORY),
    {ok, Spoolhp} =  oci:ociSessionPoolCreate(Envhp, ?TNS, 2, 10, 1, null,
                                                 null),

    {ok, Authhp} =  oci:ociAuthHandleCreate(Envhp, ?USR, ?PWD),
    {ok, Svchp} =  oci:ociSessionGet(Envhp, Authhp, Spoolhp),

    {ok, Stmthp} = oci:ociStmtHandleCreate(Envhp),
    ok = oci:ociStmtPrepare(Stmthp, <<"select * from dual where dummy = :A or dummy = :B">>),
    {ok, BindVars1} = oci:ociBindByName(Stmthp, #{}, <<"A">>, ?SQLT_CHR, <<"X">>),
    {ok, BindVars2} = oci:ociBindByName(Stmthp, BindVars1, <<"B">>,  ?SQLT_CHR, <<"Y">>),
    {ok, #{statement := ?OCI_STMT_SELECT, cols := [{<<"DUMMY">>,?SQLT_CHR,1,_,0}]}} =  oci:ociStmtExecute(Svchp, Stmthp, BindVars2, 0, 0, ?OCI_DEFAULT),
    {ok, [[<<"X">>]], _} = oci:ociStmtFetch(Stmthp, 1).