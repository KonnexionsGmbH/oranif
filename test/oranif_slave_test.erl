-module(oranif_slave_test).
-include_lib("eunit/include/eunit.hrl").

-define(SLAVE_NAME, dpi_slave).

execStmt(Conn, Sql) ->
    _Stmt = (catch dpi:conn_prepareStmt(Conn, false, Sql, <<"">>)),
    catch dpi:stmt_execute(_Stmt, []),
    catch dpi:stmt_release(_Stmt),
    ok.

%% gets a value out of a fetched set, compares it using an assertation,
%% then cleans is up again
assert_getQueryValue(Stmt, Index, Value) ->
    QueryValueRef = maps:get(data, (dpi:stmt_getQueryValue(Stmt, Index))),
    ?assertEqual(Value, dpi:data_get(QueryValueRef)),
	dpi:data_release(QueryValueRef),
    ok.


assert_getQueryInfo(Stmt, Index, Value, Atom) ->
    QueryInfoRef = dpi:stmt_getQueryInfo(Stmt, Index),
    ?assertEqual(Value, maps:get(Atom, dpi:queryInfo_get(QueryInfoRef))),
	dpi:queryInfo_delete(QueryInfoRef),
    ok.

extract_getQueryValue(Stmt, Index) ->
    QueryValueRef = maps:get(data, (dpi:stmt_getQueryValue(Stmt, Index))),
    Result = dpi:data_get(QueryValueRef),
	dpi:data_release(QueryValueRef),
    Result.

extract_getQueryInfo(Stmt, Index, Atom) ->
    QueryInfoRef = dpi:stmt_getQueryInfo(Stmt, Index),
    Result = maps:get(Atom, dpi:queryInfo_get(QueryInfoRef)),
	dpi:queryInfo_delete(QueryInfoRef),
    Result.

bindByPos(Stmt, Pos, Type, SetFun, Value) ->
    BindData = dpi:data_ctor(),
    SetFun(BindData, Value),
    dpi:stmt_bindValueByPos(Stmt, Pos, Type, BindData),
    dpi:data_release(BindData),
    ok.

iota(1, Tail) -> [1]++Tail;
iota(Number, Tail) -> iota(Number-1, [Number]++Tail).
iota(Number) -> iota(Number-1, [Number]).

iozip(List) -> lists:zip(List, iota(length(List))).

simple_fetch({_Context, Conn}) ->
    SQL = <<"select 12345, 2, 4, 8.5, 'miau' from dual">>,
    Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt, []),
    dpi:stmt_fetch(Stmt),
    #{nativeTypeNum := Type, data := Result} = dpi:stmt_getQueryValue(Stmt, 1),
    ?assertEqual(12345.0, dpi:data_get(Result)),
    ?assertEqual(Type, 'DPI_NATIVE_TYPE_DOUBLE'),
    ?assertEqual(Query_cols, 5),
    dpi:data_release(Result),
    dpi:stmt_release(Stmt).

create_insert_select_drop({_Context, Conn}) -> 
    execStmt(Conn, <<"select 12345, 2, 4, 8.5, 'miau' from dual">>),
    execStmt(Conn, <<"select 12345, 2, 4, 844.5, 'miau' from dual">>),
    execStmt(Conn, <<"drop table test_dpi1">>),
    CountSQL = <<"select count (table_name) from user_tables where table_name = 'TEST_DPI1'">>, %% SQL that evaluates to 1.0 or 0.0 depending on whether test_dpi exists
    Stmt_Exist = dpi:conn_prepareStmt(Conn, false, CountSQL, <<"">>),
    dpi:stmt_execute(Stmt_Exist, []),
    dpi:stmt_fetch(Stmt_Exist),
    #{data := TblCount, nativeTypeNum := _Type} = dpi:stmt_getQueryValue(Stmt_Exist, 1),
    ?assertEqual(0.0, dpi:data_get(TblCount)), %% the table was dropped so it shouldn't exist at this port
    dpi:data_release(TblCount),
    dpi:stmt_release(Stmt_Exist),
    execStmt(Conn, <<"create table test_dpi1(a integer, b integer, c integer)">>),
     
    Stmt_Exist2 = dpi:conn_prepareStmt(Conn, false, CountSQL, <<"">>),  
    dpi:stmt_execute(Stmt_Exist2, []),
    dpi:stmt_fetch(Stmt_Exist2),
    #{data := TblCount2} = dpi:stmt_getQueryValue(Stmt_Exist2, 1),
    ?assertEqual(1.0, dpi:data_get(TblCount2)), %% the table was created so it should exists now
    dpi:data_release(TblCount2),
    dpi:stmt_release(Stmt_Exist2),
    execStmt(Conn, <<"insert into test_dpi1 values (1, 1337, 5)">>),

    Stmt_fetch = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi1">>, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt_fetch, []),
    dpi:stmt_fetch(Stmt_fetch),
    #{data := Query_refResult} = dpi:stmt_getQueryValue(Stmt_fetch, 2),
    ?assertEqual(3, Query_cols), %% one row (3 cols) has been added to the new table
    execStmt(Conn, <<"drop table test_dpi1">>),

    Stmt_Exist3 = dpi:conn_prepareStmt(Conn, false,CountSQL, <<"">>),  
    dpi:stmt_execute(Stmt_Exist3, []),
    dpi:stmt_fetch(Stmt_Exist3),
    #{data := TblCount3} = dpi:stmt_getQueryValue(Stmt_Exist3, 1),
    ?assertEqual(0.0,  dpi:data_get(TblCount3)), %% the table was dropped again
    ?assertEqual(1337.0, dpi:data_get(Query_refResult)),
    dpi:data_release(TblCount3),
    dpi:data_release(Query_refResult),
    dpi:stmt_release(Stmt_Exist3),
    dpi:stmt_release(Stmt_fetch).


truncate_table({_Context, Conn}) ->
    execStmt(Conn, <<"drop table test_dpi2">>),
    execStmt(Conn, <<"create table test_dpi2(a integer, b integer, c integer)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (1, 2, 3)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (4, 5, 6)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (7, 8, 9)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (2, 3, 5)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (7, 11, 13)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (17, 19, 23)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (29, 31, 37)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (1, 1, 2)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (3, 5, 8)">>),
    execStmt(Conn, <<"insert into test_dpi2 values (13, 21, 34)">>),

    Stmt_fetch = dpi:conn_prepareStmt(Conn, false, <<"select count(*) from test_dpi2">>, <<"">>),
    dpi:stmt_execute(Stmt_fetch, []),
    dpi:stmt_fetch(Stmt_fetch),
    #{data := Query_refResult} = dpi:stmt_getQueryValue(Stmt_fetch, 1),
    ?assertEqual(10.0, dpi:data_get(Query_refResult)),
    dpi:data_release(Query_refResult),
    dpi:stmt_release(Stmt_fetch),
    execStmt(Conn, <<"truncate table test_dpi2">>),

    Stmt_fetch2 = dpi:conn_prepareStmt(Conn, false, <<"select count(*) from test_dpi2">>, <<"">>),
    dpi:stmt_execute(Stmt_fetch2, []),
    dpi:stmt_fetch(Stmt_fetch2),
    #{data := Query_refResult2} = dpi:stmt_getQueryValue(Stmt_fetch2, 1),
    ?assertEqual(0.0,  dpi:data_get(Query_refResult2)),
    dpi:data_release(Query_refResult2),
    dpi:stmt_release(Stmt_fetch2),
    execStmt(Conn, <<"drop table test_dpi2">>).

drop_nonexistent_table({Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi3">>),
    execStmt(Conn, <<"drop table test_dpi3">>),
    ?assertEqual(false, maps:get(isRecoverable, dpi:context_getError(Context))).

update_where({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi4">>), %% drop if exists

    %% make and fill new table
    execStmt(Conn, <<"create table test_dpi4(a integer, b integer, c integer, d integer, e integer)">>),
    execStmt(Conn, <<"insert into test_dpi4 values (1, 2, 3, 4, 5)">>),
    execStmt(Conn, <<"insert into test_dpi4 values (6, 7, 8, 9, 10)">>),
    execStmt(Conn, <<"insert into test_dpi4 values (11, 12, 13, 14, 15)">>),

    %% update some values
    execStmt(Conn, <<"update test_dpi4 set A = 7, B = B * 10 where D > 9">>),
    execStmt(Conn, <<"update test_dpi4 set C = C * -1, A = B + C, E = 777 where E < 13">>),
    execStmt(Conn, <<"update test_dpi4 set B = D * A, A = D * D, E = E - B where C < -5">>),

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi4">>, <<"">>),
    dpi:stmt_execute(Stmt, []),

    dpi:stmt_fetch(Stmt),  %% fetch the first row
    assert_getQueryValue(Stmt, 1, 5.0),
    assert_getQueryValue(Stmt, 2, 2.0),
    assert_getQueryValue(Stmt, 3, -3.0),
    assert_getQueryValue(Stmt, 4, 4.0),
    assert_getQueryValue(Stmt, 5, 777.0),

    dpi:stmt_fetch(Stmt),  %% fetch the second row
    assert_getQueryValue(Stmt, 1, 81.0),
    assert_getQueryValue(Stmt, 2, 135.0),
    assert_getQueryValue(Stmt, 3, -8.0),
    assert_getQueryValue(Stmt, 4, 9.0),
    assert_getQueryValue(Stmt, 5, 770.0),

    dpi:stmt_fetch(Stmt),  %% fetch the third row
    assert_getQueryValue(Stmt, 1, 7.0),
    assert_getQueryValue(Stmt, 2, 120.0),
    assert_getQueryValue(Stmt, 3, 13.0),
    assert_getQueryValue(Stmt, 4, 14.0),
    assert_getQueryValue(Stmt, 5, 15.0),

    dpi:stmt_release(Stmt),

    %% drop that table again
    execStmt(Conn, <<"drop table test_dpi4">>).

select_from_where({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi5">>), %% drop if exists

    %% make and fill new table
    execStmt(Conn, <<"create table test_dpi5(a integer, b integer, c integer)">>), %% drop if exists
    execStmt(Conn, <<"insert into test_dpi5 values (1, 2, 3)">>), %% drop if exists
    execStmt(Conn, <<"insert into test_dpi5 values (4, 5, 6)">>), %% drop if exists
    execStmt(Conn, <<"insert into test_dpi5 values (3, 99, 44)">>), %% drop if exists

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi5 where B = 5">>, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt, []),
    ?assertEqual(3, Query_cols),
    dpi:stmt_fetch(Stmt),
    assert_getQueryValue(Stmt, 1, 4.0),
    assert_getQueryValue(Stmt, 2, 5.0),
    assert_getQueryValue(Stmt, 3, 6.0),

    dpi:stmt_release(Stmt),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi5 t1 inner join test_dpi5 t2 on t1.A = t2.C">>, <<"">>),
    Query_cols2 = dpi:stmt_execute(Stmt2, []),
    ?assertEqual(6, Query_cols2),
    dpi:stmt_fetch(Stmt2),

    assert_getQueryValue(Stmt2, 1, 3.0),
    assert_getQueryValue(Stmt2, 2, 99.0),
    assert_getQueryValue(Stmt2, 3, 44.0),
    assert_getQueryValue(Stmt2, 4, 1.0),
    assert_getQueryValue(Stmt2, 5, 2.0),
    assert_getQueryValue(Stmt2, 6, 3.0),

    dpi:stmt_release(Stmt2),
    Stmt3 = dpi:conn_prepareStmt(Conn, false, <<"select t1.a, t2.b, t1.c - t2.a * t2.b from test_dpi5 t1 full join test_dpi5 t2 on t1.C > t2.B">>, <<"">>),
    Query_cols3 = dpi:stmt_execute(Stmt3, []),
    ?assertEqual(3, Query_cols3),

    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, 1.0),
    assert_getQueryValue(Stmt3, 2, 2.0),
    assert_getQueryValue(Stmt3, 3, 1.0),

    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, 4.0),
    assert_getQueryValue(Stmt3, 2, 2.0),
    assert_getQueryValue(Stmt3, 3, 4.0),

    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, 4.0),
    assert_getQueryValue(Stmt3, 2, 5.0),
    assert_getQueryValue(Stmt3, 3, -14.0),
    
    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, 3.0),
    assert_getQueryValue(Stmt3, 2, 2.0),
    assert_getQueryValue(Stmt3, 3, 42.0),
    
    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, 3.0),
    assert_getQueryValue(Stmt3, 2, 5.0),
    assert_getQueryValue(Stmt3, 3, 24.0),

    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, null),
    assert_getQueryValue(Stmt3, 2, 99.0),
    assert_getQueryValue(Stmt3, 3, null),

    dpi:stmt_release(Stmt3),
    %% drop that table again
    execStmt(Conn, <<"drop table test_dpi5">>).


get_column_names({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi6">>), 

    %% make and fill new table
    execStmt(Conn, <<"create table test_dpi6 (a integer, b integer, c varchar(32))">>), 
    execStmt(Conn, <<"insert into test_dpi6 values (10, 20, 'miau')">>), 
    
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select a, b as xyz, a+b, b / a, c, 'foobar' from test_dpi">>, <<"">>),
    ?assertEqual(6, dpi:stmt_execute(Stmt, [])),

    assert_getQueryInfo(Stmt, 1, "A", name),
    assert_getQueryInfo(Stmt, 2, "XYZ", name),
    assert_getQueryInfo(Stmt, 3, "A+B", name),
    assert_getQueryInfo(Stmt, 4, "B/A", name),
    assert_getQueryInfo(Stmt, 5, "C", name),
    assert_getQueryInfo(Stmt, 6, "'FOOBAR'", name),
    
    dpi:stmt_release(Stmt),
    %% drop that table again
    execStmt(Conn, <<"drop table test_dpi6">>).

bind_by_pos({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi7">>), 
    execStmt(Conn, <<"create table test_dpi7 (a integer, b integer, c integer)">>), 
    
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"insert into test_dpi7 values (:A, :B, :C)">>, <<"">>),
    BindData = dpi:data_ctor(),
    dpi:data_setInt64(BindData, 1337),
    dpi:stmt_bindValueByPos(Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData),
    dpi:data_setInt64(BindData, 100),     % data can be recycled
    dpi:stmt_bindValueByPos(Stmt, 2, 'DPI_NATIVE_TYPE_INT64', BindData),
    dpi:data_setInt64(BindData, 323),
    dpi:stmt_bindValueByPos(Stmt, 3, 'DPI_NATIVE_TYPE_INT64', BindData),
    dpi:data_release(BindData),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_release(Stmt),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select a, b, c from test_dpi7">>, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt2, []),
    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 1337.0),
    assert_getQueryValue(Stmt2, 2, 100.0),
    assert_getQueryValue(Stmt2, 3, 323.0),
    
    dpi:stmt_release(Stmt2),
    ?assertEqual(Query_cols, 3),
    execStmt(Conn, <<"drop table test_dpi7">>).

bind_by_name({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi8">>), 
    execStmt(Conn, <<"create table test_dpi8 (a integer, b integer, c integer)">>), 
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"insert into test_dpi8 values (:First, :Second, :Third)">>, <<"">>),
    BindData = dpi:data_ctor(),
    dpi:data_setInt64(BindData, 222),
    dpi:stmt_bindValueByName(Stmt, <<"Second">>, 'DPI_NATIVE_TYPE_INT64', BindData),
    dpi:data_setInt64(BindData, 111),
    dpi:stmt_bindValueByName(Stmt, <<"First">>, 'DPI_NATIVE_TYPE_INT64', BindData),
    dpi:data_setInt64(BindData, 323),
    dpi:stmt_bindValueByName(Stmt, <<"Third">>, 'DPI_NATIVE_TYPE_INT64', BindData),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_release(Stmt),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select a, b, c from test_dpi8">>, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt2, []),

    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 111.0),
    assert_getQueryValue(Stmt2, 2, 222.0),
    assert_getQueryValue(Stmt2, 3, 323.0),

    dpi:data_release(BindData),
    dpi:stmt_release(Stmt2),
    ?assertEqual(Query_cols, 3),
    execStmt(Conn, <<"drop table test_dpi8">>).

in_binding({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi9">>), 
    execStmt(Conn, <<"create table test_dpi9 (a integer, b integer, c varchar(32))">>), 
    execStmt(Conn, <<"insert into test_dpi9 values (1, 8, 'test')">>), 
    execStmt(Conn, <<"insert into test_dpi9 values (2, 9, 'foo')">>), 
    execStmt(Conn, <<"insert into test_dpi9 values (3, 8, 'rest')">>), 
    execStmt(Conn, <<"insert into test_dpi9 values (4, 7, 'food')">>), 
    execStmt(Conn, <<"insert into test_dpi9 values (5, 6, 'fest')">>), 
    dpi:conn_commit(Conn),
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi9 where c like :A">>, <<"">>),
    BindData = dpi:data_ctor(),
    dpi:data_setBytes(BindData, <<"fo%">>),
    dpi:stmt_bindValueByPos(Stmt, 1, 'DPI_NATIVE_TYPE_BYTES', BindData),   %% match 'foo' and 'food'
    dpi:stmt_execute(Stmt, []),

    dpi:stmt_fetch(Stmt),

    assert_getQueryValue(Stmt, 1, 2.0),
    assert_getQueryValue(Stmt, 2, 9.0),
    
    dpi:stmt_fetch(Stmt),
    assert_getQueryValue(Stmt, 1, 4.0),
    assert_getQueryValue(Stmt, 2, 7.0),

    dpi:data_release(BindData),
    dpi:stmt_release(Stmt),

    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi9 where c like :A">>, <<"">>),
    BindData2 = dpi:data_ctor(),
    dpi:data_setBytes(BindData2, <<"%est">>),
    dpi:stmt_bindValueByPos(Stmt2, 1, 'DPI_NATIVE_TYPE_BYTES', BindData2),   %% match 'test', 'rest' and 'fest'
    dpi:stmt_execute(Stmt2, []),

    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 1.0),
    assert_getQueryValue(Stmt2, 2, 8.0),
    
    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 3.0),
    assert_getQueryValue(Stmt2, 2, 8.0),

    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 5.0),
    assert_getQueryValue(Stmt2, 2, 6.0),

    dpi:data_release(BindData2),
    dpi:stmt_release(Stmt2),
    execStmt(Conn, <<"drop table test_dpi10">>).

bind_datatypes({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi10">>), 
    execStmt(Conn, <<"create table test_dpi10 (a TIMESTAMP(9) WITH TIME ZONE, b INTERVAL DAY TO SECOND , c INTERVAL YEAR TO MONTH )">>),
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"insert into test_dpi10 values (:First, :Second, :Third)">>, <<"">>),
    BindData = dpi:data_ctor(),
    dpi:data_setTimestamp(BindData, 7, 6, 5, 4, 3, 2, 1234, 5, 30),
    dpi:stmt_bindValueByName(Stmt, <<"First">>, 'DPI_NATIVE_TYPE_TIMESTAMP',  BindData),
    dpi:data_setIntervalDS(BindData, 7, 9, 14, 13, 20000),
    dpi:stmt_bindValueByName(Stmt, <<"Second">>, 'DPI_NATIVE_TYPE_INTERVAL_DS', BindData),
    dpi:data_setIntervalYM(BindData, 13, 8),
    dpi:stmt_bindValueByName(Stmt, <<"Third">>, 'DPI_NATIVE_TYPE_INTERVAL_YM',  BindData),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_release(Stmt),
    dpi:conn_commit(Conn),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select a, b, c from test_dpi10">>, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt2, []),

    dpi:stmt_fetch(Stmt2),

    QueryValueRef = maps:get(data, (dpi:stmt_getQueryValue(Stmt2, 1))),
    ?assertEqual(maps:remove(tzMinuteOffset, maps:remove(tzHourOffset, dpi:data_get(QueryValueRef))), 
    #{fsecond => 1234, second => 2, minute => 3, hour => 4, day => 5, month => 6, year => 7}),
    assert_getQueryValue(Stmt2, 2, #{fseconds => 20000, seconds => 13, minutes => 14, hours => 9, days => 7 }),
    assert_getQueryValue(Stmt2, 3, #{months => 8, years => 13}),
    
    dpi:data_release(QueryValueRef),
    dpi:data_release(BindData),
    dpi:stmt_release(Stmt2),
    ?assertEqual(Query_cols, 3),
    execStmt(Conn, <<"drop table test_dpi10">>).

tz_test({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table timezones">>), 
    execStmt(Conn, <<"ALTER SESSION SET TIME_ZONE='-7:13'">>), 
    execStmt(Conn, <<"CREATE TABLE timezones (c_id NUMBER, c_tstz TIMESTAMP(9) WITH TIME ZONE)">>),
    execStmt(Conn, <<"INSERT INTO timezones VALUES(1, TIMESTAMP '2003-01-02 3:44:55 -8:00')">>),

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"INSERT INTO timezones VALUES(2, :A)">>, <<"">>),
    TimestampData = dpi:data_ctor(),
    dpi:data_setTimestamp(TimestampData, 2003, 1, 2, 3, 44, 56, 123456, 22, 8), % timezones are discarded when doing the value bind, this is by ODPI design
    dpi:stmt_bindValueByName(Stmt, <<"A">>, 'DPI_NATIVE_TYPE_TIMESTAMP', TimestampData),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_release(Stmt),
    dpi:conn_commit(Conn),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select c_tstz from timezones where c_id = 2">>, <<"">>),
    dpi:stmt_execute(Stmt2, []),
    dpi:stmt_fetch(Stmt2),
    %% time zone here is NOT the 8h 22m set in the data_setTimestamp, but the timezone from "ALTER SESSION SET TIME_ZONE='-7:13'"
    TZData = maps:get(data, (dpi:stmt_getQueryValue(Stmt2, 1))),
    ?assertEqual(#{fsecond => 123456, second => 56, minute => 44, hour => 3, day => 2, month => 1, year => 2003, tzMinuteOffset => -13, tzHourOffset => -7 },   dpi:data_get(TZData)),
    dpi:data_release(TZData),
    dpi:data_release(TimestampData),
    dpi:stmt_release(Stmt2).

fail_stmt_released_too_early({Context, Conn}) -> 
    Failure = fun()->
        SQL = <<"select 12345, 2, 4, 8.5, 'miau' from dual">>,
        Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
        dpi:stmt_release(Stmt),
        Query_cols = dpi:stmt_execute(Stmt, []),
        dpi:stmt_fetch(Stmt),
        #{nativeTypeNum := Type, data := Result} = dpi:stmt_getQueryValue(Stmt, 1),
        ?assertEqual(Result, 12345.0),
        ?assertEqual(Type, 'DPI_NATIVE_TYPE_DOUBLE'),
        ?assertEqual(Query_cols, 5),
        dpi:data_release(Result),
        dpi:stmt_release(Stmt),
        ?_assert(true)
    end,
    try Failure(Context, Conn) of
        _ -> throw("Negative Test failure: test succeeded, but shouldn't have")
    catch
        throw: _ ->  ?_assert(true);
        exit: _ ->  ?_assert(true);
        error: _ ->  ?_assert(true)

    end.

define_type({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi11">>),
    execStmt(Conn, <<"create table test_dpi11 (a integer)">>),
    execStmt(Conn, <<"insert into test_dpi11 values(123)">>),

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select a from test_dpi11">>, <<"">>),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_fetch(Stmt),
    assert_getQueryValue(Stmt, 1, 123.0),
    dpi:stmt_release(Stmt),

    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select a from test_dpi11">>, <<"">>),
    dpi:stmt_execute(Stmt2, []),
    dpi:stmt_defineValue(Stmt2, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null),
    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 123),
    dpi:stmt_release(Stmt2).

iterate({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi12">>), 
    

     LittleBobbyTables = [[ "FOO", "BAR", "BAZ", "QUX", "QUUX"  ],
                          [ 33,    66,    99,    333,   666     ],
                          [ 1337,  69,    58008, 420,   9001    ],
                          [ 2,     4,     8,     16,    32      ],
                          [ 2,     3,     5,     7,     11      ],
                          [ 1,     1,     2,     3,     5       ]],
    execStmt(Conn, <<"create table test_dpi12 (FOO integer, BAR integer, BAZ integer, QUX integer, QUUX integer)">>),
    [_ | Content] = LittleBobbyTables,
    InsertRow = fun(Row) ->
        Stmt = dpi:conn_prepareStmt(Conn, false, <<"insert into test_dpi12 values (:A, :B, :C, :D, :E )">>, <<"">>),
        [ bindByPos(Stmt, Pos, 'DPI_NATIVE_TYPE_INT64', fun dpi:data_setInt64/2, Value)|| {Value, Pos} <-iozip(Row)],
        dpi:stmt_execute(Stmt, []),
        dpi:stmt_release(Stmt)
    end,

    [InsertRow(X) || X <- Content],

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi12">>, <<"">>),
    Length = dpi:stmt_execute(Stmt, []),
    [dpi:stmt_defineValue(Stmt, X, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null)
    || X <- iota(Length)],    
    Rec = fun Rec(Result) ->
        case maps:get(found,dpi:stmt_fetch(Stmt)) of
        true ->
            Resultset = [extract_getQueryValue(Stmt, X) || X <- iota(Length)],
            Rec(Result ++ [Resultset]);
        false -> Result
        end
    end,
    R = [[extract_getQueryInfo(Stmt, X, name) || X <- iota(Length)]] ++ Rec([]),
    dpi:stmt_release(Stmt),
    ?assertEqual(LittleBobbyTables, R).

commit_rollback({_Context, Conn}) -> 
    execStmt(Conn, <<"drop table test_dpi13">>), 
    
    execStmt(Conn, <<"create table test_dpi13 (a integer)">>),   %% contains 0 rows
    execStmt(Conn, <<"insert into test_dpi13 values(123)">>),    %% contains 1 row
    dpi:conn_commit(Conn),

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select count(*) from test_dpi13">>, <<"">>),
    dpi:stmt_execute(Stmt, []),
    dpi:stmt_fetch(Stmt),
    assert_getQueryValue(Stmt, 1, 1.0),
    dpi:stmt_release(Stmt),

    execStmt(Conn, <<"insert into test_dpi13 values(456)">>),    %% contains 2 rows
    
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select count(*) from test_dpi13">>, <<"">>),
    dpi:stmt_execute(Stmt2, []),
    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 2.0),
    dpi:stmt_release(Stmt2),

    dpi:conn_rollback(Conn),                            %% contains 1 row again
    
    Stmt3 = dpi:conn_prepareStmt(Conn, false, <<"select count(*) from test_dpi13">>, <<"">>),
    dpi:stmt_execute(Stmt3, []),
    dpi:stmt_fetch(Stmt3),
    assert_getQueryValue(Stmt3, 1, 1.0),
    dpi:stmt_release(Stmt3).

ping_close({Context, Conn}) -> 
    ok = dpi:conn_ping(Conn),               %% valid connection: ping succeeds
    ok = dpi:conn_close(Conn, [], <<"">>),  %% invalidate connection
    ?_assertException(error, {error, _File, _Line, _}, dpi:conn_ping(Conn)). %% now the ping fails

var_define({_Context, Conn}) -> 
    %% the variables need to be of at least size 100 when used with stmt_fetch
    %% because it will try to fetch 100 rows per default, even if there aren't as many
    #{var := Var1, data := DataRep1} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),
    #{var := Var2, data := DataRep2} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),
    #{var := Var3, data := DataRep3} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),
    #{var := Var4, data := DataRep4} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),
    #{var := Var5, data := DataRep5} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),

    execStmt(Conn, <<"drop table test_dpi14">>), %% remake table, fill with test data
    execStmt(Conn, <<"create table test_dpi14(a integer, b integer, c integer, d integer, e integer)">>), 
    execStmt(Conn, <<"insert into test_dpi14 values(1, 4, 9, 16, 25)">>), 
    execStmt(Conn, <<"insert into test_dpi14 values(123, 456, 579, 1035, 1614)">>), 
    execStmt(Conn, <<"insert into test_dpi14 values(121, 12321, 1234321, 123454321, 12345654321)">>), 

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi14">>, <<"">>),
    5 = dpi:stmt_execute(Stmt, []),
    ok = dpi:stmt_define(Stmt, 1, Var1),    %% results will be fetched to the vars
    ok = dpi:stmt_define(Stmt, 2, Var2),
    ok = dpi:stmt_define(Stmt, 3, Var3),
    ok = dpi:stmt_define(Stmt, 4, Var4),
    ok = dpi:stmt_define(Stmt, 5, Var5),
    dpi:stmt_fetch(Stmt), %% do the fetch, this fetches all rows now, not just one

    [A11, A12, A13 | _] = DataRep1, %% match the entries for the three rows
    [A21, A22, A23 | _] = DataRep2, %% the other 97 are null entries
    [A31, A32, A33 | _] = DataRep3, %% we don't care about those
    [A41, A42, A43 | _] = DataRep4,
    [A51, A52, A53, NullEntry | _] = DataRep5, %% get a null entry, too

    ?assertEqual(1.0, dpi:data_get(A11)),
    ?assertEqual(4.0, dpi:data_get(A21)),
    ?assertEqual(9.0, dpi:data_get(A31)),
    ?assertEqual(16.0, dpi:data_get(A41)),
    ?assertEqual(25.0, dpi:data_get(A51)),

    ?assertEqual(123.0, dpi:data_get(A12)),
    ?assertEqual(456.0, dpi:data_get(A22)),
    ?assertEqual(579.0, dpi:data_get(A32)),
    ?assertEqual(1035.0, dpi:data_get(A42)),
    ?assertEqual(1614.0, dpi:data_get(A52)),

    ?assertEqual(121.0, dpi:data_get(A13)),
    ?assertEqual(12321.0, dpi:data_get(A23)),
    ?assertEqual(1234321.0, dpi:data_get(A33)),
    ?assertEqual(123454321.0, dpi:data_get(A43)),
    ?assertEqual(12345654321.0, dpi:data_get(A53)),

    ?assertEqual(null, dpi:data_get(NullEntry)),

    dpi:var_release(Var1),
    [dpi:data_release(X) || X <- DataRep1],
    dpi:var_release(Var2),
    [dpi:data_release(X) || X <- DataRep2],
    dpi:var_release(Var3),
    [dpi:data_release(X) || X <- DataRep3],
    dpi:var_release(Var4),
    [dpi:data_release(X) || X <- DataRep4],
    dpi:var_release(Var5),
    [dpi:data_release(X) || X <- DataRep5],

    dpi:stmt_release(Stmt).

var_bind({_Context, Conn}) -> 
    #{var := Var1, data := DataRep1} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),
    #{var := Var2, data := DataRep2} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),

    execStmt(Conn, <<"drop table test_dpi15">>), 
    execStmt(Conn, <<"create table test_dpi15(a integer, b integer, c integer, d integer, e integer)">>), 
    execStmt(Conn, <<"insert into test_dpi15 values(1, 2, 3, 4, 5)">>), 
    execStmt(Conn, <<"insert into test_dpi15 values(6, 7, 8, 9, 10)">>),
    execStmt(Conn, <<"insert into test_dpi15 values(1, 2, 4, 8, 16)">>), 
    execStmt(Conn, <<"insert into test_dpi15 values(1, 2, 3, 5, 7)">>), 

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select 2, 3 from dual">>, <<"">>),
    dpi:stmt_execute(Stmt, []),
    ok = dpi:stmt_define(Stmt, 1, Var1),
    ok = dpi:stmt_define(Stmt, 2, Var2),
    dpi:stmt_fetch(Stmt),
    ?assertEqual(2.0, dpi:data_get(lists:nth(1, DataRep1))),
    ?assertEqual(3.0, dpi:data_get(lists:nth(1, DataRep2))),

    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi15 where b = :A and c = :B">>, <<"">>),

    ok = dpi:stmt_bindByName(Stmt2, <<"A">>, Var1),
    ok = dpi:stmt_bindByPos(Stmt2, 2, Var2),
    5 = dpi:stmt_execute(Stmt2, []),
    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 1.0),
    assert_getQueryValue(Stmt2, 2, 2.0),
    assert_getQueryValue(Stmt2, 3, 3.0),
    assert_getQueryValue(Stmt2, 4, 4.0),
    assert_getQueryValue(Stmt2, 5, 5.0),

    dpi:stmt_fetch(Stmt2),
    assert_getQueryValue(Stmt2, 1, 1.0),
    assert_getQueryValue(Stmt2, 2, 2.0),
    assert_getQueryValue(Stmt2, 3, 3.0),
    assert_getQueryValue(Stmt2, 4, 5.0),
    assert_getQueryValue(Stmt2, 5, 7.0),

    dpi:var_release(Var1),
    [dpi:data_release(X) || X <- DataRep1],
    dpi:var_release(Var2),
    [dpi:data_release(X) || X <- DataRep2],
    
    dpi:stmt_release(Stmt),
    dpi:stmt_release(Stmt2).

var_setFromBytes({_Context, Conn}) -> 
    #{var := Var, data := DataRep} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100, true, false, null),

    execStmt(Conn, <<"drop table test_dpi16">>), 
    execStmt(Conn, <<"create table test_dpi16(a integer, b varchar(32))">>), 
    execStmt(Conn, <<"insert into test_dpi16 values(10, 'foobar')">>), 
    execStmt(Conn, <<"insert into test_dpi16 values(20, 'qwert')">>),
    execStmt(Conn, <<"insert into test_dpi16 values(30, 'poipoi')">>), 
    execStmt(Conn, <<"insert into test_dpi16 values(40, 'zalgo')">>), 

    dpi:var_setFromBytes(Var, 0, <<"%oi%">>),

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi16 where b like :A">>, <<"">>),
    ok = dpi:stmt_bindByPos(Stmt, 1, Var),
    2 = dpi:stmt_execute(Stmt, []),

    dpi:stmt_fetch(Stmt),
    assert_getQueryValue(Stmt, 1, 30.0),
    assert_getQueryValue(Stmt, 2, <<"poipoi">>),

    dpi:var_release(Var),
    [dpi:data_release(X) || X <- DataRep],
    dpi:stmt_release(Stmt).

set_get_data_ptr({_Context, Conn}) -> 

    #{var := IntVar, data := [IntData]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0, true, false, null),
    dpi:data_setInt64(IntData, 12345),
    12345 = dpi:data_get(IntData),
    dpi:var_release(IntVar),
    dpi:data_release(IntData),

    #{var := DSVar, data := [DSData]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS', 1, 0, true, false, null),
    dpi:data_setIntervalDS(DSData, 12, 5, 4, 3, 2),
    #{days := 12, hours := 5, minutes := 4, seconds := 3, fseconds := 2 } = dpi:data_get(DSData),
    dpi:var_release(DSVar),
    dpi:data_release(DSData),

    #{var := YMVar, data := [YMData]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 0, true, false, null),
    dpi:data_setIntervalYM(YMData, 1990, 8),
    #{years := 1990, months := 8} = dpi:data_get(YMData),
    dpi:var_release(YMVar),
    dpi:data_release(YMData),

    #{var := TSVar, data := [TSData]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP', 1, 0, true, false, null),
    dpi:data_setTimestamp(TSData, 1990, 8, 22, 22, 12, 54, 3, 4, 5),
    #{year := 1990, month := 8, day := 22, hour := 22, minute := 12, second := 54, fsecond := 3, tzHourOffset := 4, tzMinuteOffset := 5} = dpi:data_get(TSData),
    dpi:var_release(TSVar),
    dpi:data_release(TSData),

    #{var := StrVar, data := [StrData]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100, true, false, null),
    %% according to https://oracle.github.io/odpi/doc/functions/dpiData.html dpiData_setBytes should NOT be used for vars, so var_setFromBytes is used instead
    dpi:var_setFromBytes(StrVar, 0, <<"abc">>),
    <<"abc">> = dpi:data_get(StrData),
    dpi:var_release(StrVar),
    dpi:data_release(StrData).

data_is_null({_Context, Conn}) ->
    #{var := Var, data := [Data]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0, true, false, null),

    dpi:data_setInt64(Data, 12345),
    12345 = dpi:data_get(Data),  %% confirm that "Data" works correctly with actual values
    dpi:data_setIsNull(Data, true),
    null = dpi:data_get(Data),
    dpi:data_setInt64(Data, 54321),
    54321 = dpi:data_get(Data),  %% assert that "Data" can be set again properly after having been null
    dpi:data_setIsNull(Data, false), % NOT setting it to null
    54321 = dpi:data_get(Data),  %% assert that, after not setting it to null, it indeed didn't get set to null

    %% maybe have a test of setting it to null and back to not null and see if the original value is still there,
    %% but that would require making a guarantee about null values that probably doesn't exist on Oracle level
    dpi:data_release(Data),
    dpi:var_release(Var).

var_array({_Context, Conn}) ->
    
    #{var := Var, data := DataRep} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100, true, true, null),
    ok = dpi:var_setNumElementsInArray(Var, 100),

    dpi:var_setFromBytes(Var, 0, <<"abc">>),
    dpi:var_setFromBytes(Var, 1, <<"bar">>),
    dpi:var_setFromBytes(Var, 2, <<"baz">>),

    [A, B, C | _] = DataRep,
    ?assertEqual(<<"abc">>, dpi:data_get(A)),
    ?assertEqual(<<"bar">>, dpi:data_get(B)),
    ?assertEqual(<<"baz">>, dpi:data_get(C)),
    
    dpi:var_release(Var),
    [dpi:data_release(X) || X <- DataRep],

    execStmt(Conn, <<"drop table test_dpi17">>), 
    execStmt(Conn, <<"CREATE Or REPLACE TYPE namearray AS VARRAY(3) OF VARCHAR2(32)">>), 
    execStmt(Conn, <<"CREATE or replace TYPE footype AS OBJECT(foo integer, bar integer)">>), 
    execStmt(Conn, <<"create table test_dpi17(a FOOTYPE)">>), 
    execStmt(Conn, <<"insert into test_dpi17 values(footype(2, 4))">>),
    dpi:conn_commit(Conn).

client_server_version({Context, Conn}) -> 
    #{releaseNum := CRNum, versionNum := CVNum, fullVersionNum := CFNum} = dpi:context_getClientVersion(Context),
    true = (CRNum == 3) or (CRNum == 2),
    true = (CVNum == 18) or (CVNum == 12),
    true = (CFNum == 1803000000) or (CFNum == 1202000000),

    ?assertMatch(
        #{
            releaseNum := 2, versionNum := 11, fullVersionNum := 1102000200,
            portReleaseNum := 2, portUpdateNum := 0,
            releaseString := "Oracle Database 11g Express Edition Release"
                             " 11.2.0.2.0 - 64bit Production"
        },
        dpi:conn_getServerVersion(Conn)
    ).

simple_fetch_no_assert({_Context, Conn}) -> 
    SQL = <<"select 12345, 2, 4, 8.5, 'miau' from dual">>,
    Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt, []),
    dpi:stmt_fetch(Stmt),
    #{nativeTypeNum := Type, data := Result} = dpi:stmt_getQueryValue(Stmt, 1),
    12345.0 = dpi:data_get(Result),
    Type = 'DPI_NATIVE_TYPE_DOUBLE',
    Query_cols = 5,
    dpi:data_release(Result),
    dpi:stmt_release(Stmt),
    ok.

var_bind_no_assert({_Context, Conn}) -> 

    Assert_getQueryValue = fun (Stmt, Index, Value) ->
        QueryValueRef = maps:get(data, (dpi:stmt_getQueryValue(Stmt, Index))),
        Value = dpi:data_get(QueryValueRef),
	    dpi:data_release(QueryValueRef),
        ok end,

    #{var := Var1, data := DataRep1} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),
    #{var := Var2, data := DataRep2} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null),

    execStmt(Conn, <<"drop table test_dpi15">>), 
    execStmt(Conn, <<"create table test_dpi15(a integer, b integer, c integer, d integer, e integer)">>), 
    execStmt(Conn, <<"insert into test_dpi15 values(1, 2, 3, 4, 5)">>), 
    execStmt(Conn, <<"insert into test_dpi15 values(6, 7, 8, 9, 10)">>),
    execStmt(Conn, <<"insert into test_dpi15 values(1, 2, 4, 8, 16)">>), 
    execStmt(Conn, <<"insert into test_dpi15 values(1, 2, 3, 5, 7)">>), 

    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select 2, 3 from dual">>, <<"">>),
    dpi:stmt_execute(Stmt, []),
    ok = dpi:stmt_define(Stmt, 1, Var1),
    ok = dpi:stmt_define(Stmt, 2, Var2),
    dpi:stmt_fetch(Stmt),
    2.0 = dpi:data_get(lists:nth(1, DataRep1)),
    3.0 = dpi:data_get(lists:nth(1, DataRep2)),

    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select * from test_dpi15 where b = :A and c = :B">>, <<"">>),

    ok = dpi:stmt_bindByName(Stmt2, <<"A">>, Var1),
    ok = dpi:stmt_bindByPos(Stmt2, 2, Var2),
    5 = dpi:stmt_execute(Stmt2, []),
    dpi:stmt_fetch(Stmt2),
    Assert_getQueryValue(Stmt2, 1, 1.0),
    Assert_getQueryValue(Stmt2, 2, 2.0),
    Assert_getQueryValue(Stmt2, 3, 3.0),
    Assert_getQueryValue(Stmt2, 4, 4.0),
    Assert_getQueryValue(Stmt2, 5, 5.0),

    dpi:stmt_fetch(Stmt2),
    Assert_getQueryValue(Stmt2, 1, 1.0),
    Assert_getQueryValue(Stmt2, 2, 2.0),
    Assert_getQueryValue(Stmt2, 3, 3.0),
    Assert_getQueryValue(Stmt2, 4, 5.0),
    Assert_getQueryValue(Stmt2, 5, 7.0),

    dpi:var_release(Var1),
    [dpi:data_release(X) || X <- DataRep1],
    dpi:var_release(Var2),
    [dpi:data_release(X) || X <- DataRep2],
    
    dpi:stmt_release(Stmt),
    dpi:stmt_release(Stmt2),

    ok.

distributed(_) ->
    ok = dpi:load(slave),
    ?debugFmt("Slave: ~p ~n", [get(dpi_node)]),

    #{tns := Tns, user := User, password := Password} = getConfig(),
    Context = dpi:safe(dpi, context_create, [3, 0]),
    Conn = dpi:safe(dpi, conn_create, [Context, User, Password, Tns, #{}, #{}]),

    ok = dpi:safe(fun simple_fetch_no_assert/1,[{Context, Conn}]),
    ok = dpi:safe(fun var_bind_no_assert/1,[{Context, Conn}]).

catch_error_message({_Context, Conn}) -> 
    Stmt = dpi:conn_prepareStmt(
        Conn, false, <<"select 'miaumiau' from unexistingTable">>, <<"">>
    ),
    ?assertError(
        {error, _File, _Line,
            #{
                message := "ORA-00942: table or view does not exist",
                fnName := "dpiStmt_execute"
            }
        },
        dpi:stmt_execute(Stmt, [])
    ).

catch_error_message_conn(_) -> 
    #{user := User, tns := TNS} = getConfig(),
    Context = dpi:context_create(3, 0),
    ?assertError(
        {error, _File, _Line,
            #{
                message :=
                    "ORA-01017: invalid username/password; logon denied",
                fnName := "dpiConn_create"
            }
        },
        dpi:conn_create(Context, User, <<"someBadPassword">>, TNS, #{}, #{})
    ).

get_num_query_cols({_Context, Conn}) -> 
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"select 12345 from dual">>, <<"">>),
    1 = dpi:stmt_execute(Stmt, []),
    ?assertEqual(1, dpi:stmt_getNumQueryColumns(Stmt)),
    dpi:stmt_release(Stmt),

    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"select 1, 2, 3, 4, 5 from dual">>, <<"">>),
    5 = dpi:stmt_execute(Stmt2, []),
    ?assertEqual(5, dpi:stmt_getNumQueryColumns(Stmt2)),
    dpi:stmt_release(Stmt2).

-define(TESTPROCEDURE, "ERLOCI_TEST_PROCEDURE").
stored_procedure({_Context, Conn}) -> 
    #{var := Var1, data := [DataRep1]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0, false, false, null),
    #{var := Var2, data := [DataRep2]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_LONG_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100, false, false, null),
    #{var := Var3, data := [DataRep3]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 0, false, false, null),

    dpi:data_setInt64(DataRep1, 50),
    dpi:var_setFromBytes(Var2, 0, <<"1             ">>),
    dpi:data_setInt64(DataRep3, 3),
    
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"
        create or replace procedure "
        ?TESTPROCEDURE
        "(p_first in number, p_second in out varchar2, p_result out number)
        is
        begin
            p_result := p_first + to_number(p_second);
            p_second := 'The sum is ' || to_char(p_result);
        end "?TESTPROCEDURE";
        ">>, <<"">>),
    0 = dpi:stmt_execute(Stmt, []),
    dpi:conn_commit(Conn),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"begin "?TESTPROCEDURE"(:p_first,:p_second,:p_result); end;">>, <<"">>),
    ok = dpi:stmt_bindByName(Stmt2, <<"p_first">>, Var1),
    ok = dpi:stmt_bindByName(Stmt2, <<"p_second">>, Var2),
    ok = dpi:stmt_bindByName(Stmt2, <<"p_result">>, Var3),

    ?assertEqual(3,dpi:data_get(DataRep3)), %% before executing the procedure, hast the old value
    dpi:stmt_execute(Stmt2, []),
    ?assertEqual(51,dpi:data_get(DataRep3)), %% now has the new value of 50 added to the char value of "1             "
    dpi:stmt_release(Stmt),
    dpi:stmt_release(Stmt2),
 
    dpi:var_release(Var1),
    dpi:data_release(DataRep1),
    dpi:var_release(Var2),
    dpi:data_release(DataRep2),
    dpi:var_release(Var3),
    dpi:data_release(DataRep3).

ref_cursor({_Context, Conn}) -> 
    Get_column_values = fun Get_column_values(_Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
                            Get_column_values(Stmt, ColIdx, Limit) ->
                                #{data := Data} = dpi:stmt_getQueryValue(Stmt, ColIdx),
                                [dpi:data_get(Data) | Get_column_values(Stmt, ColIdx + 1, Limit)] end,
    
    #{var := Var1, data := [DataRep1]} = dpi:conn_newVar(Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0, false, false, null),
    Stmt = dpi:conn_prepareStmt(Conn, false, <<"
       create or replace procedure "
        ?TESTPROCEDURE
        "(p_cur out sys_refcursor)
        is
        begin
            open p_cur for select 123 from dual;
        end "?TESTPROCEDURE";
        ">>, <<"">>),
    0 = dpi:stmt_execute(Stmt, []),
    dpi:conn_commit(Conn),
    Stmt2 = dpi:conn_prepareStmt(Conn, false, <<"begin "?TESTPROCEDURE"(:cursor); end;">>, <<"">>),
    ok = dpi:stmt_bindByName(Stmt2, <<"cursor">>, Var1),
    dpi:stmt_execute(Stmt2, []),
    RefCursor = dpi:data_get(DataRep1),
    {true, [Result]} = 
        case dpi:stmt_fetch(RefCursor) of
            #{found := true} ->
                NumCols = dpi:stmt_getNumQueryColumns(RefCursor),
                {true, Get_column_values(RefCursor, 1, NumCols)};
            #{found := false} ->
                {false, []}
        end,
    ?assertEqual(123.0, Result),
    
    dpi:stmt_release(Stmt),
    dpi:stmt_release(Stmt2),
    dpi:var_release(Var1),
    dpi:data_release(DataRep1).

start() ->
     {Tns, User, Password} = getTnsUserPass(),
     Context = dpi:context_create(3, 0),
     try
        Conn = dpi:conn_create(
            Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"},
            #{}
        ),
        [Context, Conn]
    catch
        error:{error, CSrc, Line, Details} ->
            ?debugFmt(
                "[~s:~p] ERROR ~p", [CSrc, Line, Details]),
            throw(Details#{csrc => CSrc, line => Line});
        Class:Exception ->
            ?debugFmt(
                "Class ~p, Exception ~p, Context ~p",
                [Class, Exception, Context]
            ),
            throw({Class, Exception})
    end.

s() ->
     ?debugMsg("Performing setup."),
     %ok = dpi:load(?SLAVE_NAME),
     ok = dpi:load_unsafe(),
     ?debugMsg("Performed setup."),
     ok.

stop([Context, Conn]) ->
    %?debugMsg("Teardown of test, but there is nothing to do (dtors should take care of freeing the resources)"),
    dpi:conn_release(Conn),
    dpi:context_destroy(Context),
    ok.

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

statement_test_() ->
    
    %% tests that all share one context and connection
    {
        setup, fun start/0, fun stop/1,
        {with, [
            fun simple_fetch/1,
            fun create_insert_select_drop/1, 
            fun truncate_table/1,

            fun drop_nonexistent_table/1,
            fun update_where/1,             
            fun select_from_where/1,

            fun get_column_names/1,         
            fun bind_by_pos/1,                
            fun bind_by_name/1,             
            fun in_binding/1,

            fun bind_datatypes/1,

            fun fail_stmt_released_too_early/1,
            fun tz_test/1,

            fun define_type/1,
            fun iterate/1,
            fun commit_rollback/1,
            fun var_define/1,
            fun var_bind/1,
            fun var_setFromBytes/1,
            fun set_get_data_ptr/1,
            fun data_is_null/1,
            fun var_array/1,
            fun client_server_version/1,
            fun distributed/1,
            fun catch_error_message/1,
            fun catch_error_message_conn/1,
            fun get_num_query_cols/1,
            fun stored_procedure/1,
            fun ref_cursor/1
        ]}
    }.

connection_test_() ->
    {
        foreach, fun start/0, fun stop/1, [
            fun ping_close/1
        ]
    }.
