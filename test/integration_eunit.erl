-module(integration_eunit).
-include_lib("eunit/include/eunit.hrl").

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

-define(EXEC_STMT(_Conn, _Sql),
    (fun() ->
        __Stmt = dpiCall(Safe, conn_prepareStmt, [_Conn, false, _Sql, <<"">>]),
        R = (catch dpiCall(Safe, stmt_execute, [__Stmt, []])),
        catch dpiCall(Safe, stmt_close, [__Stmt, <<>>]),
        R
    end)()
).

%% gets a value out of a fetched set, compares it using an assertation,
%% then cleans is up again
assert_getQueryValue(Safe, Stmt, Index, Value) ->
    #{data := QueryValueRef} = dpiCall(Safe, stmt_getQueryValue, [Stmt, Index]),
    ?assertEqual(Value, dpiCall(Safe, data_get, [QueryValueRef])),
	dpiCall(Safe, data_release, [QueryValueRef]),
    ok.


assert_getQueryInfo(Safe, Stmt, Index, Value, Atom) ->
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, Index]),
    ?assertEqual(Value, maps:get(Atom, dpiCall(Safe, queryInfo_get, [QueryInfoRef]))),
	dpiCall(Safe, queryInfo_delete, [QueryInfoRef]),
    ok.

extract_getQueryValue(Safe, Stmt, Index) ->
    #{data := QueryValueRef} = dpiCall(Safe, stmt_getQueryValue, [Stmt, Index]),
    Result = dpiCall(Safe, data_get, [QueryValueRef]),
	dpiCall(Safe, data_release, [QueryValueRef]),
    Result.

extract_getQueryInfo(Safe, Stmt, Index, Atom) ->
    QueryInfoRef = dpiCall(Safe, stmt_getQueryInfo, [Stmt, Index]),
    Result = maps:get(Atom, dpiCall(Safe, queryInfo_get, [QueryInfoRef])),
	dpiCall(Safe, queryInfo_delete, [QueryInfoRef]),
    Result.


iozip(List) -> lists:zip(List, lists:seq(1, length(List))).

simple_fetch({Safe, _Context, Conn}) ->
    SQL = <<"select 12345, 2, 4, 8.5, 'miau' from dual">>,
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, SQL, <<"">>]),
    Query_cols = dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_fetch, [Stmt]),
    #{nativeTypeNum := Type, data := Result} =
        dpiCall(Safe, stmt_getQueryValue, [Stmt, 1]),
    ?assertEqual(12345.0, dpiCall(Safe, data_get, [Result])),
    ?assertEqual(Type, 'DPI_NATIVE_TYPE_DOUBLE'),
    ?assertEqual(Query_cols, 5),
    dpiCall(Safe, data_release, [Result]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]).

create_insert_select_drop({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"select 12345, 2, 4, 8.5, 'miau' from dual">>),
    ?EXEC_STMT(Conn, <<"select 12345, 2, 4, 844.5, 'miau' from dual">>),
    ?EXEC_STMT(Conn, <<"drop table test_dpi1">>),
    CountSQL = <<"select count (table_name) from user_tables where table_name = 'TEST_DPI1'">>, %% SQL that evaluates to 1.0 or 0.0 depending on whether test_dpi exists
    Stmt_Exist = dpiCall(Safe, conn_prepareStmt, [Conn, false, CountSQL, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt_Exist, []]),
    dpiCall(Safe, stmt_fetch, [Stmt_Exist]),
    #{data := TblCount, nativeTypeNum := _Type} = dpiCall(Safe, stmt_getQueryValue, [Stmt_Exist, 1]),
    ?assertEqual(0.0, dpiCall(Safe, data_get, [TblCount])), %% the table was dropped so it shouldn't exist at this port
    dpiCall(Safe, data_release, [TblCount]),
    dpiCall(Safe, stmt_close, [Stmt_Exist, <<>>]),
    ?EXEC_STMT(Conn, <<"create table test_dpi1(a integer, b integer, c integer)">>),
     
    Stmt_Exist2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, CountSQL, <<"">>]),  
    dpiCall(Safe, stmt_execute, [Stmt_Exist2, []]),
    dpiCall(Safe, stmt_fetch, [Stmt_Exist2]),
    #{data := TblCount2} = dpiCall(Safe, stmt_getQueryValue, [Stmt_Exist2, 1]),
    ?assertEqual(1.0, dpiCall(Safe, data_get, [TblCount2])), %% the table was created so it should exists now
    dpiCall(Safe, data_release, [TblCount2]),
    dpiCall(Safe, stmt_close, [Stmt_Exist2, <<>>]),
    ?EXEC_STMT(Conn, <<"insert into test_dpi1 values (1, 1337, 5)">>),

    Stmt_fetch = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi1">>, <<"">>]),
    Query_cols = dpiCall(Safe, stmt_execute, [Stmt_fetch, []]),
    dpiCall(Safe, stmt_fetch, [Stmt_fetch]),
    #{data := Query_refResult} = dpiCall(Safe, stmt_getQueryValue, [Stmt_fetch, 2]),
    ?assertEqual(3, Query_cols), %% one row (3 cols) has been added to the new table
    ?EXEC_STMT(Conn, <<"drop table test_dpi1">>),

    Stmt_Exist3 = dpiCall(Safe, conn_prepareStmt, [Conn, false,CountSQL, <<"">>]),  
    dpiCall(Safe, stmt_execute, [Stmt_Exist3, []]),
    dpiCall(Safe, stmt_fetch, [Stmt_Exist3]),
    #{data := TblCount3} = dpiCall(Safe, stmt_getQueryValue, [Stmt_Exist3, 1]),
    ?assertEqual(0.0,  dpiCall(Safe, data_get, [TblCount3])), %% the table was dropped again
    ?assertEqual(1337.0, dpiCall(Safe, data_get, [Query_refResult])),
    dpiCall(Safe, data_release, [TblCount3]),
    dpiCall(Safe, data_release, [Query_refResult]),
    dpiCall(Safe, stmt_close, [Stmt_Exist3, <<>>]),
    dpiCall(Safe, stmt_close, [Stmt_fetch, <<>>]).


truncate_table({Safe, _Context, Conn}) ->
    ?EXEC_STMT(Conn, <<"drop table test_dpi2">>),
    ?EXEC_STMT(Conn, <<"create table test_dpi2(a integer, b integer, c integer)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (1, 2, 3)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (4, 5, 6)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (7, 8, 9)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (2, 3, 5)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (7, 11, 13)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (17, 19, 23)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (29, 31, 37)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (1, 1, 2)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (3, 5, 8)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi2 values (13, 21, 34)">>),

    Stmt_fetch = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select count(*) from test_dpi2">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt_fetch, []]),
    dpiCall(Safe, stmt_fetch, [Stmt_fetch]),
    #{data := Query_refResult} = dpiCall(Safe, stmt_getQueryValue, [Stmt_fetch, 1]),
    ?assertEqual(10.0, dpiCall(Safe, data_get, [Query_refResult])),
    dpiCall(Safe, data_release, [Query_refResult]),
    dpiCall(Safe, stmt_close, [Stmt_fetch, <<>>]),
    ?EXEC_STMT(Conn, <<"truncate table test_dpi2">>),

    Stmt_fetch2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select count(*) from test_dpi2">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt_fetch2, []]),
    dpiCall(Safe, stmt_fetch, [Stmt_fetch2]),
    #{data := Query_refResult2} = dpiCall(Safe, stmt_getQueryValue, [Stmt_fetch2, 1]),
    ?assertEqual(0.0,  dpiCall(Safe, data_get, [Query_refResult2])),
    dpiCall(Safe, data_release, [Query_refResult2]),
    dpiCall(Safe, stmt_close, [Stmt_fetch2, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi2">>).

drop_nonexistent_table({Safe, Context, Conn}) -> 
	?EXEC_STMT(Conn, <<"drop table test_dpi3">>),
    {'EXIT', {{error, _File, _Line, Map}, _StackTrace}} = ?EXEC_STMT(Conn, <<"drop table test_dpi3">>),
    ?assertEqual(false, maps:get(isRecoverable, Map)).

update_where({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi4">>), %% drop if exists

    %% make and fill new table
    ?EXEC_STMT(Conn, <<"create table test_dpi4(a integer, b integer, c integer, d integer, e integer)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi4 values (1, 2, 3, 4, 5)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi4 values (6, 7, 8, 9, 10)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi4 values (11, 12, 13, 14, 15)">>),

    %% update some values
    ?EXEC_STMT(Conn, <<"update test_dpi4 set A = 7, B = B * 10 where D > 9">>),
    ?EXEC_STMT(Conn, <<"update test_dpi4 set C = C * -1, A = B + C, E = 777 where E < 13">>),
    ?EXEC_STMT(Conn, <<"update test_dpi4 set B = D * A, A = D * D, E = E - B where C < -5">>),

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi4">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),

    dpiCall(Safe, stmt_fetch, [Stmt]),  %% fetch the first row
    assert_getQueryValue(Safe, Stmt, 1, 5.0),
    assert_getQueryValue(Safe, Stmt, 2, 2.0),
    assert_getQueryValue(Safe, Stmt, 3, -3.0),
    assert_getQueryValue(Safe, Stmt, 4, 4.0),
    assert_getQueryValue(Safe, Stmt, 5, 777.0),

    dpiCall(Safe, stmt_fetch, [Stmt]),  %% fetch the second row
    assert_getQueryValue(Safe, Stmt, 1, 81.0),
    assert_getQueryValue(Safe, Stmt, 2, 135.0),
    assert_getQueryValue(Safe, Stmt, 3, -8.0),
    assert_getQueryValue(Safe, Stmt, 4, 9.0),
    assert_getQueryValue(Safe, Stmt, 5, 770.0),

    dpiCall(Safe, stmt_fetch, [Stmt]),  %% fetch the third row
    assert_getQueryValue(Safe, Stmt, 1, 7.0),
    assert_getQueryValue(Safe, Stmt, 2, 120.0),
    assert_getQueryValue(Safe, Stmt, 3, 13.0),
    assert_getQueryValue(Safe, Stmt, 4, 14.0),
    assert_getQueryValue(Safe, Stmt, 5, 15.0),

    dpiCall(Safe, stmt_close, [Stmt, <<>>]),

    %% drop that table again
    ?EXEC_STMT(Conn, <<"drop table test_dpi4">>).

select_from_where({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi5">>), %% drop if exists

    %% make and fill new table
    ?EXEC_STMT(Conn, <<"create table test_dpi5(a integer, b integer, c integer)">>), %% drop if exists
    ?EXEC_STMT(Conn, <<"insert into test_dpi5 values (1, 2, 3)">>), %% drop if exists
    ?EXEC_STMT(Conn, <<"insert into test_dpi5 values (4, 5, 6)">>), %% drop if exists
    ?EXEC_STMT(Conn, <<"insert into test_dpi5 values (3, 99, 44)">>), %% drop if exists

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi5 where B = 5">>, <<"">>]),
    Query_cols = dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertEqual(3, Query_cols),
    dpiCall(Safe, stmt_fetch, [Stmt]),
    assert_getQueryValue(Safe, Stmt, 1, 4.0),
    assert_getQueryValue(Safe, Stmt, 2, 5.0),
    assert_getQueryValue(Safe, Stmt, 3, 6.0),

    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi5 t1 inner join test_dpi5 t2 on t1.A = t2.C">>, <<"">>]),
    Query_cols2 = dpiCall(Safe, stmt_execute, [Stmt2, []]),
    ?assertEqual(6, Query_cols2),
    dpiCall(Safe, stmt_fetch, [Stmt2]),

    assert_getQueryValue(Safe, Stmt2, 1, 3.0),
    assert_getQueryValue(Safe, Stmt2, 2, 99.0),
    assert_getQueryValue(Safe, Stmt2, 3, 44.0),
    assert_getQueryValue(Safe, Stmt2, 4, 1.0),
    assert_getQueryValue(Safe, Stmt2, 5, 2.0),
    assert_getQueryValue(Safe, Stmt2, 6, 3.0),

    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
    Stmt3 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select t1.a, t2.b, t1.c - t2.a * t2.b from test_dpi5 t1 full join test_dpi5 t2 on t1.C > t2.B">>, <<"">>]),
    Query_cols3 = dpiCall(Safe, stmt_execute, [Stmt3, []]),
    ?assertEqual(3, Query_cols3),

    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, 1.0),
    assert_getQueryValue(Safe, Stmt3, 2, 2.0),
    assert_getQueryValue(Safe, Stmt3, 3, 1.0),

    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, 4.0),
    assert_getQueryValue(Safe, Stmt3, 2, 2.0),
    assert_getQueryValue(Safe, Stmt3, 3, 4.0),

    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, 4.0),
    assert_getQueryValue(Safe, Stmt3, 2, 5.0),
    assert_getQueryValue(Safe, Stmt3, 3, -14.0),
    
    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, 3.0),
    assert_getQueryValue(Safe, Stmt3, 2, 2.0),
    assert_getQueryValue(Safe, Stmt3, 3, 42.0),
    
    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, 3.0),
    assert_getQueryValue(Safe, Stmt3, 2, 5.0),
    assert_getQueryValue(Safe, Stmt3, 3, 24.0),

    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, null),
    assert_getQueryValue(Safe, Stmt3, 2, 99.0),
    assert_getQueryValue(Safe, Stmt3, 3, null),

    dpiCall(Safe, stmt_close, [Stmt3, <<>>]),
    %% drop that table again
    ?EXEC_STMT(Conn, <<"drop table test_dpi5">>).


get_column_names({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi6">>), 

    %% make and fill new table
    ?EXEC_STMT(Conn, <<"create table test_dpi6 (a integer, b integer, c varchar(32))">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi6 values (10, 20, 'miau')">>), 
    
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select a, b as xyz, a+b, b / a, c, 'foobar' from test_dpi6">>, <<"">>]),
    ?assertEqual(6, dpiCall(Safe, stmt_execute, [Stmt, []])),

    assert_getQueryInfo(Safe, Stmt, 1, "A", name),
    assert_getQueryInfo(Safe, Stmt, 2, "XYZ", name),
    assert_getQueryInfo(Safe, Stmt, 3, "A+B", name),
    assert_getQueryInfo(Safe, Stmt, 4, "B/A", name),
    assert_getQueryInfo(Safe, Stmt, 5, "C", name),
    assert_getQueryInfo(Safe, Stmt, 6, "'FOOBAR'", name),
    
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    %% drop that table again
    ?EXEC_STMT(Conn, <<"drop table test_dpi6">>).

bind_by_pos({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi7">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi7 (a integer, b integer, c integer)">>), 
    
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi7 values (:A, :B, :C)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setInt64, [BindData, 1337]),
    dpiCall(Safe, stmt_bindValueByPos, [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData]),
    dpiCall(Safe, data_setInt64, [BindData, 100]),     % data can be recycled
    dpiCall(Safe, stmt_bindValueByPos, [Stmt, 2, 'DPI_NATIVE_TYPE_INT64', BindData]),
    dpiCall(Safe, data_setInt64, [BindData, 323]),
    dpiCall(Safe, stmt_bindValueByPos, [Stmt, 3, 'DPI_NATIVE_TYPE_INT64', BindData]),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select a, b, c from test_dpi7">>, <<"">>]),
    Query_cols = dpiCall(Safe, stmt_execute, [Stmt2, []]),
    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 1337.0),
    assert_getQueryValue(Safe, Stmt2, 2, 100.0),
    assert_getQueryValue(Safe, Stmt2, 3, 323.0),
    
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
    ?assertEqual(Query_cols, 3),
    ?EXEC_STMT(Conn, <<"drop table test_dpi7">>).

bind_by_name({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi8">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi8 (a integer, b integer, c integer)">>), 
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi8 values (:First, :Second, :Third)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setInt64, [BindData, 222]),
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"Second">>, 'DPI_NATIVE_TYPE_INT64', BindData]),
    dpiCall(Safe, data_setInt64, [BindData, 111]),
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"First">>, 'DPI_NATIVE_TYPE_INT64', BindData]),
    dpiCall(Safe, data_setInt64, [BindData, 323]),
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"Third">>, 'DPI_NATIVE_TYPE_INT64', BindData]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select a, b, c from test_dpi8">>, <<"">>]),
    Query_cols = dpiCall(Safe, stmt_execute, [Stmt2, []]),

    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 111.0),
    assert_getQueryValue(Safe, Stmt2, 2, 222.0),
    assert_getQueryValue(Safe, Stmt2, 3, 323.0),

    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
    ?assertEqual(Query_cols, 3),
    ?EXEC_STMT(Conn, <<"drop table test_dpi8">>).

in_binding({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi9">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi9 (a integer, b integer, c varchar(32))">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi9 values (1, 8, 'test')">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi9 values (2, 9, 'foo')">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi9 values (3, 8, 'rest')">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi9 values (4, 7, 'food')">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi9 values (5, 6, 'fest')">>), 
    dpiCall(Safe, conn_commit, [Conn]),
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi9 where c like :A">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setBytes, [BindData, <<"fo%">>]),
    dpiCall(Safe, stmt_bindValueByPos, [Stmt, 1, 'DPI_NATIVE_TYPE_BYTES', BindData]),   %% match 'foo' and 'food'
    dpiCall(Safe, stmt_execute, [Stmt, []]),

    dpiCall(Safe, stmt_fetch, [Stmt]),

    assert_getQueryValue(Safe, Stmt, 1, 2.0),
    assert_getQueryValue(Safe, Stmt, 2, 9.0),
    Safe, 
    dpiCall(Safe, stmt_fetch, [Stmt]),
    assert_getQueryValue(Safe, Stmt, 1, 4.0),
    assert_getQueryValue(Safe, Stmt, 2, 7.0),

    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),

    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi9 where c like :A">>, <<"">>]),
    BindData2 = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setBytes, [BindData2, <<"%est">>]),
    dpiCall(Safe, stmt_bindValueByPos, [Stmt2, 1, 'DPI_NATIVE_TYPE_BYTES', BindData2]),   %% match 'test', 'rest' and 'fest'
    dpiCall(Safe, stmt_execute, [Stmt2, []]),

    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 1.0),
    assert_getQueryValue(Safe, Stmt2, 2, 8.0),
    
    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 3.0),
    assert_getQueryValue(Safe, Stmt2, 2, 8.0),

    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 5.0),
    assert_getQueryValue(Safe, Stmt2, 2, 6.0),

    dpiCall(Safe, data_release, [BindData2]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi10">>).

bind_datatypes({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi10">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi10 (a TIMESTAMP(9) WITH TIME ZONE, b INTERVAL DAY TO SECOND , c INTERVAL YEAR TO MONTH )">>),
    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi10 values (:First, :Second, :Third)">>, <<"">>]),
    BindData = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setTimestamp, [BindData, 7, 6, 5, 4, 3, 2, 1234, 5, 30]),
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"First">>, 'DPI_NATIVE_TYPE_TIMESTAMP',  BindData]),
    dpiCall(Safe, data_setIntervalDS, [BindData, 7, 9, 14, 13, 20000]),
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"Second">>, 'DPI_NATIVE_TYPE_INTERVAL_DS', BindData]),
    dpiCall(Safe, data_setIntervalYM, [BindData, 13, 8]),
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"Third">>, 'DPI_NATIVE_TYPE_INTERVAL_YM',  BindData]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    dpiCall(Safe, conn_commit, [Conn]),
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select a, b, c from test_dpi10">>, <<"">>]),
    Query_cols = dpiCall(Safe, stmt_execute, [Stmt2, []]),

    dpiCall(Safe, stmt_fetch, [Stmt2]),

    QueryValueRef = maps:get(data, (dpiCall(Safe, stmt_getQueryValue, [Stmt2, 1]))),
    ?assertEqual(maps:remove(tzMinuteOffset, maps:remove(tzHourOffset, dpiCall(Safe, data_get, [QueryValueRef]))), 
    #{fsecond => 1234, second => 2, minute => 3, hour => 4, day => 5, month => 6, year => 7}),
    assert_getQueryValue(Safe, Stmt2, 2, #{fseconds => 20000, seconds => 13, minutes => 14, hours => 9, days => 7 }),
    assert_getQueryValue(Safe, Stmt2, 3, #{months => 8, years => 13}),
    
    dpiCall(Safe, data_release, [QueryValueRef]),
    dpiCall(Safe, data_release, [BindData]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
    ?assertEqual(Query_cols, 3),
    ?EXEC_STMT(Conn, <<"drop table test_dpi10">>).

tz_test({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table timezones">>), 
    ?EXEC_STMT(Conn, <<"ALTER SESSION SET TIME_ZONE='-7:13'">>), 
    ?EXEC_STMT(Conn, <<"CREATE TABLE timezones (c_id NUMBER, c_tstz TIMESTAMP(9) WITH TIME ZONE)">>),
    ?EXEC_STMT(Conn, <<"INSERT INTO timezones VALUES(1, TIMESTAMP '2003-01-02 3:44:55 -8:00')">>),

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"INSERT INTO timezones VALUES(2, :A)">>, <<"">>]),
    TimestampData = dpiCall(Safe, data_ctor, []),
    dpiCall(Safe, data_setTimestamp, [TimestampData, 2003, 1, 2, 3, 44, 56, 123456, 22, 8]), % timezones are discarded when doing the value bind, this is by ODPI design
    dpiCall(Safe, stmt_bindValueByName, [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_TIMESTAMP', TimestampData]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    dpiCall(Safe, conn_commit, [Conn]),
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select c_tstz from timezones where c_id = 2">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt2, []]),
    dpiCall(Safe, stmt_fetch, [Stmt2]),
    %% time zone here is NOT the 8h 22m set in the data_setTimestamp, but the timezone from "ALTER SESSION SET TIME_ZONE='-7:13'"
    TZData = maps:get(data, (dpiCall(Safe, stmt_getQueryValue, [Stmt2, 1]))),
    ?assertEqual(#{fsecond => 123456, second => 56, minute => 44, hour => 3, day => 2, month => 1, year => 2003, tzMinuteOffset => -13, tzHourOffset => -7 },   dpiCall(Safe, data_get, [TZData])),
    dpiCall(Safe, data_release, [TZData]),
    dpiCall(Safe, data_release, [TimestampData]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]).

fail_stmt_released_too_early({Safe, Context, Conn}) -> 
    Failure = fun()->
        SQL = <<"select 12345, 2, 4, 8.5, 'miau' from dual">>,
        Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, SQL, <<"">>]),
        dpiCall(Safe, stmt_close, [Stmt, <<>>]),
        Query_cols = dpiCall(Safe, stmt_execute, [Stmt, []]),
        dpiCall(Safe, stmt_fetch, [Stmt]),
        #{nativeTypeNum := Type, data := Result} = dpiCall(Safe, stmt_getQueryValue, [Stmt, 1]),
        ?assertEqual(Result, 12345.0),
        ?assertEqual(Type, 'DPI_NATIVE_TYPE_DOUBLE'),
        ?assertEqual(Query_cols, 5),
        dpiCall(Safe, data_release, [Result]),
        dpiCall(Safe, stmt_close, [Stmt, <<>>]),
        ?_assert(true)
    end,
    try Failure(Context, Conn) of
        _ -> throw("Negative Test failure: test succeeded, but shouldn't have")
    catch
        throw: _ ->  ?_assert(true);
        exit: _ ->  ?_assert(true);
        error: _ ->  ?_assert(true)

    end.

define_type({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi11">>),
    ?EXEC_STMT(Conn, <<"create table test_dpi11 (a integer)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi11 values(123)">>),

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select a from test_dpi11">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_fetch, [Stmt]),
    assert_getQueryValue(Safe, Stmt, 1, 123.0),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),

    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select a from test_dpi11">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt2, []]),
    dpiCall(Safe, stmt_defineValue, [Stmt2, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null]),
    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 123),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]).

iterate({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi12">>), 
    

     LittleBobbyTables = [[ "FOO", "BAR", "BAZ", "QUX", "QUUX"  ],
                          [ 33,    66,    99,    333,   666     ],
                          [ 1337,  69,    58008, 420,   9001    ],
                          [ 2,     4,     8,     16,    32      ],
                          [ 2,     3,     5,     7,     11      ],
                          [ 1,     1,     2,     3,     5       ]],
    ?EXEC_STMT(Conn, <<"create table test_dpi12 (FOO integer, BAR integer, BAZ integer, QUX integer, QUUX integer)">>),
    [_ | Content] = LittleBobbyTables,
    InsertRow = fun(Row) ->
        Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"insert into test_dpi12 values (:A, :B, :C, :D, :E )">>, <<"">>]),
        [
        begin
            BindData = dpiCall(Safe, data_ctor, []),
            dpiCall(Safe, data_setInt64, [BindData, Value]),
            dpiCall(Safe, stmt_bindValueByPos, [Stmt, Pos, 'DPI_NATIVE_TYPE_INT64', BindData]),
            dpiCall(Safe, data_release, [BindData])
        end    
        || {Value, Pos} <-iozip(Row)],
        dpiCall(Safe, stmt_execute, [Stmt, []]),
        dpiCall(Safe, stmt_close, [Stmt, <<>>])
    end,

    [InsertRow(X) || X <- Content],

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi12">>, <<"">>]),
    Length = dpiCall(Safe, stmt_execute, [Stmt, []]),
    [dpiCall(Safe, stmt_defineValue, [Stmt, X, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])
    || X <- lists:seq(1, Length)],    
    Rec = fun Rec(Result) ->
        case maps:get(found,dpiCall(Safe, stmt_fetch, [Stmt])) of
        true ->
            Resultset = [extract_getQueryValue(Safe, Stmt, X) || X <-  lists:seq(1, Length)],
            Rec(Result ++ [Resultset]);
        false -> Result
        end
    end,
    R = [[extract_getQueryInfo(Safe, Stmt, X, name) || X <-  lists:seq(1, Length)]] ++ Rec([]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    ?assertEqual(LittleBobbyTables, R).

commit_rollback({Safe, _Context, Conn}) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi13">>), 
    
    ?EXEC_STMT(Conn, <<"create table test_dpi13 (a integer)">>),   %% contains 0 rows
    ?EXEC_STMT(Conn, <<"insert into test_dpi13 values(123)">>),    %% contains 1 row
    dpiCall(Safe, conn_commit, [Conn]),

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select count(*) from test_dpi13">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, stmt_fetch, [Stmt]),
    assert_getQueryValue(Safe, Stmt, 1, 1.0),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),

    ?EXEC_STMT(Conn, <<"insert into test_dpi13 values(456)">>),    %% contains 2 rows
    
    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select count(*) from test_dpi13">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt2, []]),
    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 2.0),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),

    dpiCall(Safe, conn_rollback, [Conn]),                            %% contains 1 row again
    
    Stmt3 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select count(*) from test_dpi13">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt3, []]),
    dpiCall(Safe, stmt_fetch, [Stmt3]),
    assert_getQueryValue(Safe, Stmt3, 1, 1.0),
    dpiCall(Safe, stmt_close, [Stmt3, <<>>]).

ping_close({Safe, _Context, Conn}) -> 
    % valid connection: ping succeeds
    ok = dpiCall(Safe, conn_ping, [Conn]),
    % invalidate connection
    ok = dpiCall(Safe, conn_close, [Conn, [], <<"">>]),
    % now the ping fails
    ?_assertException(
        error, {error, _File, _Line, _}, dpiCall(Safe, conn_ping, [Conn])
    ).

var_define({Safe, _Context, Conn}) -> 
    %% the variables need to be of at least size 100 when used with stmt_fetch
    %% because it will try to fetch 100 rows per default, even if there aren't as many
    #{var := Var1, data := DataRep1} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    #{var := Var2, data := DataRep2} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    #{var := Var3, data := DataRep3} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    #{var := Var4, data := DataRep4} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    #{var := Var5, data := DataRep5} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),

    ?EXEC_STMT(Conn, <<"drop table test_dpi14">>), %% remake table, fill with test data
    ?EXEC_STMT(Conn, <<"create table test_dpi14(a integer, b integer, c integer, d integer, e integer)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi14 values(1, 4, 9, 16, 25)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi14 values(123, 456, 579, 1035, 1614)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi14 values(121, 12321, 1234321, 123454321, 12345654321)">>), 

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi14">>, <<"">>]),
    5 = dpiCall(Safe, stmt_execute, [Stmt, []]),
    ok = dpiCall(Safe, stmt_define, [Stmt, 1, Var1]),    %% results will be fetched to the vars
    ok = dpiCall(Safe, stmt_define, [Stmt, 2, Var2]),
    ok = dpiCall(Safe, stmt_define, [Stmt, 3, Var3]),
    ok = dpiCall(Safe, stmt_define, [Stmt, 4, Var4]),
    ok = dpiCall(Safe, stmt_define, [Stmt, 5, Var5]),
    dpiCall(Safe, stmt_fetch, [Stmt]), %% do the fetch, this fetches all rows now, not just one

    [A11, A12, A13 | _] = DataRep1, %% match the entries for the three rows
    [A21, A22, A23 | _] = DataRep2, %% the other 97 are null entries
    [A31, A32, A33 | _] = DataRep3, %% we don't care about those
    [A41, A42, A43 | _] = DataRep4,
    [A51, A52, A53, NullEntry | _] = DataRep5, %% get a null entry, too

    ?assertEqual(1.0, dpiCall(Safe, data_get, [A11])),
    ?assertEqual(4.0, dpiCall(Safe, data_get, [A21])),
    ?assertEqual(9.0, dpiCall(Safe, data_get, [A31])),
    ?assertEqual(16.0, dpiCall(Safe, data_get, [A41])),
    ?assertEqual(25.0, dpiCall(Safe, data_get, [A51])),

    ?assertEqual(123.0, dpiCall(Safe, data_get, [A12])),
    ?assertEqual(456.0, dpiCall(Safe, data_get, [A22])),
    ?assertEqual(579.0, dpiCall(Safe, data_get, [A32])),
    ?assertEqual(1035.0, dpiCall(Safe, data_get, [A42])),
    ?assertEqual(1614.0, dpiCall(Safe, data_get, [A52])),

    ?assertEqual(121.0, dpiCall(Safe, data_get, [A13])),
    ?assertEqual(12321.0, dpiCall(Safe, data_get, [A23])),
    ?assertEqual(1234321.0, dpiCall(Safe, data_get, [A33])),
    ?assertEqual(123454321.0, dpiCall(Safe, data_get, [A43])),
    ?assertEqual(12345654321.0, dpiCall(Safe, data_get, [A53])),

    ?assertEqual(null, dpiCall(Safe, data_get, [NullEntry])),

    [dpiCall(Safe, data_release, [X]) || X <- DataRep1],
    dpiCall(Safe, var_release, [Var1]),
    [dpiCall(Safe, data_release, [X]) || X <- DataRep2],
    dpiCall(Safe, var_release, [Var2]),
    [dpiCall(Safe, data_release, [X]) || X <- DataRep3],
    dpiCall(Safe, var_release, [Var3]),
    [dpiCall(Safe, data_release, [X]) || X <- DataRep4],
    dpiCall(Safe, var_release, [Var4]),
    [dpiCall(Safe, data_release, [X]) || X <- DataRep5],
    dpiCall(Safe, var_release, [Var5]),

    dpiCall(Safe, stmt_close, [Stmt, <<>>]).

var_bind({Safe, _Context, Conn}) -> 
    #{var := Var1, data := DataRep1} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    #{var := Var2, data := DataRep2} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),

    ?EXEC_STMT(Conn, <<"drop table test_dpi15">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi15(a integer, b integer, c integer, d integer, e integer)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi15 values(1, 2, 3, 4, 5)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi15 values(6, 7, 8, 9, 10)">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi15 values(1, 2, 4, 8, 16)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi15 values(1, 2, 3, 5, 7)">>), 

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select 2, 3 from dual">>, <<"">>]),
    dpiCall(Safe, stmt_execute, [Stmt, []]),
    ok = dpiCall(Safe, stmt_define, [Stmt, 1, Var1]),
    ok = dpiCall(Safe, stmt_define, [Stmt, 2, Var2]),
    dpiCall(Safe, stmt_fetch, [Stmt]),
    ?assertEqual(2.0, dpiCall(Safe, data_get, [hd(DataRep1)])),
    ?assertEqual(3.0, dpiCall(Safe, data_get, [hd(DataRep2)])),

    Stmt2 = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi15 where b = :A and c = :B">>, <<"">>]),

    ok = dpiCall(Safe, stmt_bindByName, [Stmt2, <<"A">>, Var1]),
    ok = dpiCall(Safe, stmt_bindByPos, [Stmt2, 2, Var2]),
    5 = dpiCall(Safe, stmt_execute, [Stmt2, []]),
    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 1.0),
    assert_getQueryValue(Safe, Stmt2, 2, 2.0),
    assert_getQueryValue(Safe, Stmt2, 3, 3.0),
    assert_getQueryValue(Safe, Stmt2, 4, 4.0),
    assert_getQueryValue(Safe, Stmt2, 5, 5.0),

    dpiCall(Safe, stmt_fetch, [Stmt2]),
    assert_getQueryValue(Safe, Stmt2, 1, 1.0),
    assert_getQueryValue(Safe, Stmt2, 2, 2.0),
    assert_getQueryValue(Safe, Stmt2, 3, 3.0),
    assert_getQueryValue(Safe, Stmt2, 4, 5.0),
    assert_getQueryValue(Safe, Stmt2, 5, 7.0),

    [dpiCall(Safe, data_release, [X]) || X <- DataRep1],
    dpiCall(Safe, var_release, [Var1]),
    [dpiCall(Safe, data_release, [X]) || X <- DataRep2],
    dpiCall(Safe, var_release, [Var2]),
    
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]).

var_setFromBytes({Safe, _Context, Conn}) -> 
    #{var := Var, data := DataRep} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100, true, false, null]),

    ?EXEC_STMT(Conn, <<"drop table test_dpi16">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi16(a integer, b varchar(32))">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi16 values(10, 'foobar')">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi16 values(20, 'qwert')">>),
    ?EXEC_STMT(Conn, <<"insert into test_dpi16 values(30, 'poipoi')">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi16 values(40, 'zalgo')">>), 

    dpiCall(Safe, var_setFromBytes, [Var, 0, <<"%oi%">>]),

    Stmt = dpiCall(Safe, conn_prepareStmt, [Conn, false, <<"select * from test_dpi16 where b like :A">>, <<"">>]),
    ok = dpiCall(Safe, stmt_bindByPos, [Stmt, 1, Var]),
    2 = dpiCall(Safe, stmt_execute, [Stmt, []]),

    dpiCall(Safe, stmt_fetch, [Stmt]),
    assert_getQueryValue(Safe, Stmt, 1, 30.0),
    assert_getQueryValue(Safe, Stmt, 2, <<"poipoi">>),

    [dpiCall(Safe, data_release, [X]) || X <- DataRep],
    dpiCall(Safe, var_release, [Var]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]).

set_get_data_ptr({Safe, _Context, Conn}) -> 

    #{var := IntVar, data := [IntData]} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0, true, false, null]),
    dpiCall(Safe, data_setInt64, [IntData, 12345]),
    12345 = dpiCall(Safe, data_get, [IntData]),
    dpiCall(Safe, data_release, [IntData]),
    dpiCall(Safe, var_release, [IntVar]),

    #{var := DSVar, data := [DSData]} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS', 1, 0, true, false, null]),
    dpiCall(Safe, data_setIntervalDS, [DSData, 12, 5, 4, 3, 2]),
    #{days := 12, hours := 5, minutes := 4, seconds := 3, fseconds := 2 } = dpiCall(Safe, data_get, [DSData]),
    dpiCall(Safe, data_release, [DSData]),
    dpiCall(Safe, var_release, [DSVar]),

    #{var := YMVar, data := [YMData]} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 0, true, false, null]),
    dpiCall(Safe, data_setIntervalYM, [YMData, 1990, 8]),
    #{years := 1990, months := 8} = dpiCall(Safe, data_get, [YMData]),
    dpiCall(Safe, data_release, [YMData]),
    dpiCall(Safe, var_release, [YMVar]),

    #{var := TSVar, data := [TSData]} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP', 1, 0, true, false, null]),
    dpiCall(Safe, data_setTimestamp, [TSData, 1990, 8, 22, 22, 12, 54, 3, 4, 5]),
    #{year := 1990, month := 8, day := 22, hour := 22, minute := 12, second := 54, fsecond := 3, tzHourOffset := 4, tzMinuteOffset := 5} = dpiCall(Safe, data_get, [TSData]),
    dpiCall(Safe, data_release, [TSData]),
    dpiCall(Safe, var_release, [TSVar]),

    #{var := StrVar, data := [StrData]} = dpiCall(Safe, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100, true, false, null]),
    %% according to https://oracle.github.io/odpi/doc/functions/dpiData.html dpiData_setBytes should NOT be used for vars, so var_setFromBytes is used instead
    dpiCall(Safe, var_setFromBytes, [StrVar, 0, <<"abc">>]),
    <<"abc">> = dpiCall(Safe, data_get, [StrData]),
    dpiCall(Safe, data_release, [StrData]),
    dpiCall(Safe, var_release, [StrVar]).

data_is_null({Safe, _Context, Conn}) ->
    #{var := Var, data := [Data]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0,
            true, false, null
        ]
    ),

    dpiCall(Safe, data_setInt64, [Data, 12345]),
    % confirm that "Data" works correctly with actual values
    12345 = dpiCall(Safe, data_get, [Data]),
    dpiCall(Safe, data_setIsNull, [Data, true]),
    null = dpiCall(Safe, data_get, [Data]),
    dpiCall(Safe, data_setInt64, [Data, 54321]),
    % assert that "Data" can be set again properly after having been null
    ?assertEqual(54321, dpiCall(Safe, data_get, [Data])),
    % NOT setting it to null
    dpiCall(Safe, data_setIsNull, [Data, false]),
    % assert that, after not setting it to null, it indeed didn't get set to
    % null
    ?assertEqual(54321, dpiCall(Safe, data_get, [Data])),

    % maybe have a test of setting it to null and back to not null and see if
    % the original value is still there, but that would require making a
    % guarantee about null values that probably doesn't exist on Oracle level
    dpiCall(Safe, data_release, [Data]),
    dpiCall(Safe, var_release, [Var]).

var_array({Safe, _Context, Conn}) ->
    #{var := Var, data := DataRep} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ok = dpiCall(Safe, var_setNumElementsInArray, [Var, 100]),

    dpiCall(Safe, var_setFromBytes, [Var, 0, <<"abc">>]),
    dpiCall(Safe, var_setFromBytes, [Var, 1, <<"bar">>]),
    dpiCall(Safe, var_setFromBytes, [Var, 2, <<"baz">>]),

    [A, B, C | _] = DataRep,
    ?assertEqual(<<"abc">>, dpiCall(Safe, data_get, [A])),
    ?assertEqual(<<"bar">>, dpiCall(Safe, data_get, [B])),
    ?assertEqual(<<"baz">>, dpiCall(Safe, data_get, [C])),
    
    [dpiCall(Safe, data_release, [X]) || X <- DataRep],
    dpiCall(Safe, var_release, [Var]),

    ?EXEC_STMT(Conn, <<"drop table test_dpi17">>), 
    ?EXEC_STMT(Conn, <<"CREATE Or REPLACE TYPE namearray AS VARRAY(3) OF VARCHAR2(32)">>), 
    ?EXEC_STMT(Conn, <<"CREATE or replace TYPE footype AS OBJECT(foo integer, bar integer)">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi17(a FOOTYPE)">>), 
    ?EXEC_STMT(Conn, <<"insert into test_dpi17 values(footype(2, 4))">>),
    dpiCall(Safe, conn_commit, [Conn]).

client_server_version({Safe, Context, Conn}) -> 
    #{
        releaseNum := CRNum, versionNum := CVNum, fullVersionNum := CFNum
    } = dpiCall(Safe, context_getClientVersion, [Context]),

    ?assert(CRNum == 3 orelse CRNum == 2),
    ?assert(CVNum == 18 orelse CVNum == 12),
    ?assert(CFNum == 1803000000 orelse CFNum == 1202000000),

    ?assertMatch(
        #{
            releaseNum := 2, versionNum := 11, fullVersionNum := 1102000200,
            portReleaseNum := 2, portUpdateNum := 0,
            releaseString := "Oracle Database 11g Express Edition Release"
                             " 11.2.0.2.0 - 64bit Production"
        },
        dpiCall(Safe, conn_getServerVersion, [Conn])
    ).

-define(GET_QUERY_VALUE(_Stmt, _Index, _Value),
    (fun() ->
        #{data := __QueryValueRef} = dpiCall(
            Safe, stmt_getQueryValue,[_Stmt, _Index]
        ),
        ?assertEqual(_Value, dpiCall(Safe, data_get, [__QueryValueRef])),
	    dpiCall(Safe, data_release, [__QueryValueRef])
    end)()
).

catch_error_message({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(Safe, conn_prepareStmt, [
        Conn, false, <<"select 'miaumiau' from unexistingTable">>, <<"">>
    ]),
    ?assertError(
        {error, _File, _Line,
            #{
                message := "ORA-00942: table or view does not exist",
                fnName := "dpiStmt_execute"
            }
        },
        begin
            A = dpiCall(Safe, stmt_execute, [Stmt, []]),
            ?debugFmt("The call on line ~p should have thrown an exception, but didn't!", [?LINE - 1]),
            A
        end
    ).

catch_error_message_conn({Safe, _, _}) -> 
    #{user := User, tns := TNS} = getConfig(),
    Context = dpiCall(Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertError(
        {error, _File, _Line,
            #{
                message :=
                    "ORA-01017: invalid username/password; logon denied",
                fnName := "dpiConn_create"
            }
        },
        dpiCall(Safe, conn_create, [
            Context, User, <<"someBadPassword">>, TNS, #{}, #{}
        ])
    ).

get_num_query_cols({Safe, _Context, Conn}) -> 
    Stmt = dpiCall(
        Safe, conn_prepareStmt, [
            Conn, false, <<"select 12345 from dual">>, <<"">>
        ]
    ),
    1 = dpiCall(Safe, stmt_execute, [Stmt, []]),
    ?assertEqual(1, dpiCall(Safe, stmt_getNumQueryColumns, [Stmt])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),

    Stmt2 = dpiCall(
        Safe, conn_prepareStmt, [
            Conn, false, <<"select 1, 2, 3, 4, 5 from dual">>, <<"">>
        ]
    ),
    5 = dpiCall(Safe, stmt_execute, [Stmt2, []]),
    ?assertEqual(5, dpiCall(Safe, stmt_getNumQueryColumns, [Stmt2])),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]).

-define(TESTPROCEDURE, "ERLOCI_TEST_PROCEDURE").
stored_procedure({Safe, _Context, Conn}) -> 
    #{var := Var1, data := [DataRep1]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0,
            false, false, null
        ]
    ),
    #{var := Var2, data := [DataRep2]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_LONG_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1,
            100, false, false, null
        ]
    ),
    #{var := Var3, data := [DataRep3]} = dpiCall(
        Safe, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0,
            false, false, null
        ]
    ),

    dpiCall(Safe, data_setInt64, [DataRep1, 50]),
    dpiCall(Safe, var_setFromBytes, [Var2, 0, <<"1             ">>]),
    dpiCall(Safe, data_setInt64, [DataRep3, 3]),
    
    Stmt = dpiCall(
        Safe, conn_prepareStmt, [Conn, false,
            << "create or replace procedure "?TESTPROCEDURE
               "(p_first in number, p_second in out varchar2, p_result out number)
                is
                begin
                    p_result := p_first + to_number(p_second);
                    p_second := 'The sum is ' || to_char(p_result);
                end "?TESTPROCEDURE";">>,
            <<"">>
        ]
    ),
    0 = dpiCall(Safe, stmt_execute, [Stmt, []]),
    dpiCall(Safe, conn_commit, [Conn]),
    Stmt2 = dpiCall(
        Safe, conn_prepareStmt, [
            Conn, false,
            <<"begin "?TESTPROCEDURE"(:p_first,:p_second,:p_result); end;">>,
            <<"">>
        ]
    ),
    ok = dpiCall(Safe, stmt_bindByName, [Stmt2, <<"p_first">>, Var1]),
    ok = dpiCall(Safe, stmt_bindByName, [Stmt2, <<"p_second">>, Var2]),
    ok = dpiCall(Safe, stmt_bindByName, [Stmt2, <<"p_result">>, Var3]),

    % before executing the procedure, hast the old value
    ?assertEqual(3, dpiCall(Safe, data_get, [DataRep3])),
    dpiCall(Safe, stmt_execute, [Stmt2, []]),
    % now has the new value of 50 added to the char value of "1             "
    ?assertEqual(51,dpiCall(Safe, data_get, [DataRep3])),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]),
    dpiCall(Safe, stmt_close, [Stmt2, <<>>]),
 
    dpiCall(Safe, data_release, [DataRep1]),
    dpiCall(Safe, var_release, [Var1]),
    dpiCall(Safe, data_release, [DataRep2]),
    dpiCall(Safe, var_release, [Var2]),
    dpiCall(Safe, data_release, [DataRep3]),
    dpiCall(Safe, var_release, [Var3]).

ref_cursor({Safe, _Context, Conn}) -> 
    CreateStmt = dpiCall(Safe, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure "?TESTPROCEDURE"
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select CURRENT_TIMESTAMP from dual;
            end "?TESTPROCEDURE";">>,
        <<"">>
    ]),
    ?assertEqual(0, dpiCall(Safe, stmt_execute, [CreateStmt, []])),
    ?assertEqual(ok, dpiCall(Safe, stmt_close, [CreateStmt, <<>>])),

    #{var := VarStmt, data := [DataStmt]} = dpiCall(Safe, conn_newVar, [
        Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0,
        false, false, null
    ]),
    Stmt = dpiCall(Safe, conn_prepareStmt, [
        Conn, false, <<"begin "?TESTPROCEDURE"(:cursor); end;">>, <<"">>
    ]),
    ok = dpiCall(Safe, stmt_bindByName, [Stmt, <<"cursor">>, VarStmt]),

    dpiCall(Safe, stmt_execute, [Stmt, []]),
    RefCursor = dpiCall(Safe, data_get, [DataStmt]),
    ?assertMatch(#{found := true}, dpiCall(Safe, stmt_fetch, [RefCursor])),
    ?assertEqual(1, dpiCall(Safe, stmt_getNumQueryColumns, [RefCursor])),
    Result = get_column_values(Safe, RefCursor, 1, 1),
    ?assertMatch(
        [#{
            day := _, fsecond := _, hour := _, minute := _, month := _,
            second := _, tzHourOffset := _, tzMinuteOffset := _, year := _
        } | _ ],
        Result
    ),

    dpiCall(Safe, stmt_execute, [Stmt, []]),
    RefCursor1 = dpiCall(Safe, data_get, [DataStmt]),
    ?assertMatch(#{found := false}, dpiCall(Safe, stmt_fetch, [RefCursor1])),
    ?assertEqual(1, dpiCall(Safe, stmt_getNumQueryColumns, [RefCursor1])),
    Result1 = get_column_values(Safe, RefCursor1, 1, 1),
    ?assertMatch(
        [#{
            day := _, fsecond := _, hour := _, minute := _, month := _,
            second := _, tzHourOffset := _, tzMinuteOffset := _, year := _
        } | _ ],
        Result1
    ),

    ?assertEqual(RefCursor, RefCursor1),
    ?assertEqual(Result, Result1),

    dpiCall(Safe, data_release, [DataStmt]),
    dpiCall(Safe, var_release, [VarStmt]),
    dpiCall(Safe, stmt_close, [Stmt, <<>>]).

-define(SLAVE, oranif_slave).
setup(Safe) ->
    if
        Safe -> ok = dpi:load(?SLAVE);
        true -> ok = dpi:load_unsafe()
    end,
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Context = dpiCall(
        Safe, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    Connnnection = dpiCall(
        Safe, conn_create, [
            Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
        ]
    ),
    if
        Safe ->
            SlaveNode = list_to_existing_atom(
                re:replace(
                    atom_to_list(node()), ".*(@.*)", atom_to_list(?SLAVE)++"\\1",
                    [{return, list}]
                )
            ),
            pong = net_adm:ping(SlaveNode),
            {Safe, SlaveNode, Context, Connnnection};
        true -> {Safe, Context, Connnnection}
    end.

cleanup({Safe, _SlaveNode, Context, Connnnection}) ->
    cleanup({Safe, Context, Connnnection});
cleanup({Safe, Context, Connnnection}) ->
    dpiCall(Safe, conn_release, [Connnnection]),
    dpiCall(Safe, context_destroy, [Context]),
    if Safe -> dpiCall(Safe, unload, []); true -> ok end.

-define(F(__Fn), {??__Fn, fun __Fn/1}).

-define(STATEMENT_TESTS, [
    ?F(simple_fetch),
    ?F(create_insert_select_drop),
    ?F(truncate_table),
    ?F(drop_nonexistent_table),
    ?F(update_where),
    ?F(select_from_where),
    ?F(get_column_names),
    ?F(bind_by_pos),
    ?F(bind_by_name),
    ?F(in_binding),
    ?F(bind_datatypes),
    ?F(fail_stmt_released_too_early),
    ?F(tz_test),
    ?F(define_type),
    ?F(iterate),
    ?F(commit_rollback),
    ?F(var_define),
    ?F(var_bind),
    ?F(var_setFromBytes),
    ?F(set_get_data_ptr),
    ?F(data_is_null),
    ?F(var_array),
    ?F(client_server_version),
    ?F(catch_error_message),
    ?F(catch_error_message_conn),
    ?F(get_num_query_cols),
    ?F(stored_procedure),
    ?F(ref_cursor)
]).

-define(CONNECTION_TESTS, [
    ?F(ping_close)
]).

unsafe_statements_test_() ->
    %% tests that can share a valid connections
    {
        setup,
        fun() -> setup(false) end,
        fun cleanup/1,
        oraniftst(?STATEMENT_TESTS)
    }.

safe_statements_test_() ->
    {
        setup,
        fun() -> setup(true) end,
        fun cleanup/1,
        oraniftst(?STATEMENT_TESTS)
    }.

unsafe_connection_test_() ->
    {
        setup,
        fun() -> setup(false) end,
        fun cleanup/1,
        oraniftst(?CONNECTION_TESTS)
    }.

oraniftst(TestFuns) ->
    fun
        ({Safe, SlaveNode, Context, Connnnection}) ->
            [{
                "slave_"++Title,
                fun() ->
                    put(dpi_node, SlaveNode),
                    TestFun({Safe, Context, Connnnection})
                end
            } || {Title, TestFun} <- TestFuns];
        (Ctx) ->
            [{Title, fun() -> TestFun(Ctx) end} || {Title, TestFun} <- TestFuns]
    end.

%-------------------------------------------------------------------------------
% Internal functions
%-------------------------------------------------------------------------------

dpiCall(true, F, A) -> dpi:safe(dpi, F, A);
dpiCall(false, F, A) -> apply(dpi, F, A).

getConfig() ->
    case file:get_cwd() of
        {ok, Cwd} ->
            ConnectConfigFile = filename:join(
                lists:reverse(
                    ["connect.config", "test"
                        | lists:reverse(filename:split(Cwd))]
                )
            ),
            case file:consult(ConnectConfigFile) of
                {ok, [Params]} when is_map(Params) -> Params;
                {ok, Params} ->
                    ?debugFmt("bad config (expected map) ~p", [Params]),
                    error(badconfig);
                {error, Reason} ->
                    ?debugFmt("~p", [Reason]),
                    error(Reason)
            end;
        {error, Reason} ->
            ?debugFmt("~p", [Reason]),
            error(Reason)
    end.

get_column_values(_Safe, _Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_values(Safe, Stmt, ColIdx, Limit) ->
    #{data := Data} = dpiCall(Safe, stmt_getQueryValue, [Stmt, ColIdx]),
    [dpiCall(Safe, data_get, [Data])
     | get_column_values(Safe, Stmt, ColIdx + 1, Limit)].
