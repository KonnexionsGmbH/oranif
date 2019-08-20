#!/user/bin/escript
%% -*- erlang -*-
%%! -name console@127.0.0.1 -setcookie console -pa _build/default/lib/oranif/ebin

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).
-define(TNS,
	<<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
	"(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1522)))"
	"(CONNECT_DATA=(SERVICE_NAME=XE)))">>
).
main([]) ->
	dpi:load_unsafe(),
	Context = dpi:context_create(?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION),
    Conn = dpi:conn_create(
		Context, <<"scott">>, <<"regit">>, ?TNS,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
	),
	Sql = <<"insert into test (col2) values ('efg') returning rowid into :rid">>,
	Stmt = dpi:conn_prepareStmt(Conn, false, Sql, <<>>),
	io:format("statement : ~p~n", [Stmt]),
 	#{var := Var, data := [Data]} = dpi:conn_newVar(
		Conn, 'DPI_ORACLE_TYPE_ROWID', 'DPI_NATIVE_TYPE_ROWID',
		1, 100, true, false, null
    ),
	ok = dpi:stmt_bindByName(Stmt, <<"rid">>, Var),
	0 = dpi:stmt_execute(Stmt, []),
	DataVal = dpi:data_get(Data),
	io:format("data : ~p~n", [DataVal]),
	ok = dpi:data_release(Data),
	ok = dpi:var_release(Var),
	ok = dpi:stmt_close(Stmt, <<>>),
	ok = dpi:conn_close(Conn, [], <<>>),
	ok = dpi:context_destroy(Context),
	io:format("DONE~n"),
	halt(1).
