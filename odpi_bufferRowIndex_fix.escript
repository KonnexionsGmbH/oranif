#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -pa _build/default/lib/oranif/ebin/


-define(TNS, <<"">>).
-define(USER, <<"foo">>).
-define(PSWD, <<"bar">>).

-define(TEST_SQL, <<"select * from foo a, foo b, foo c, foo d">>). %selects 10000 rows

% this function executes SQL, fire-and-forget
% it can be used for setup purposes.
% the result is caught because it can throw an expection even if we want that
% per example dropping a table that might not exist as part of setup
% which would raise an exception if the table didn't actually exist
executeSQL(Conn, SQL) ->
    Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<>>),
    catch dpi:stmt_execute(Stmt, []),
    dpi:stmt_close(Stmt, <<"">>).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% DDERL DRIVER EXTRACT
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dpi_fetch_rows( Conn, Statement, BlockSize) ->
    get_rows_prepare(Conn, Statement, BlockSize, []).

%% initalizes things that need to be done before getting the rows
%% it finds out how many columns they are and what types all those columns are
%% then it makes and defines dpiVars for every column where it's necessary because stmt_getQueryValue() can't be used for those cols
%% and calls get_rows to fetch all the results of the query
get_rows_prepare(Conn, Stmt, NRows, Acc)->
    NumCols = dpi:stmt_getNumQueryColumns(Stmt),    % get number of cols returned by the Stmt
    Types = [
        begin
            Qinfo = maps:get(typeInfo, dpi:stmt_getQueryInfo(Stmt, Col)), % get the info and extract the map within the map
            #{defaultNativeTypeNum := DefaultNativeTypeNum,
            oracleTypeNum := OracleTypeNum} = Qinfo,    % match the map to get the wanted native/ora data type atoms
            {OracleTypeNum, DefaultNativeTypeNum, Col} % put those types into a tuple and add the column number
        end
        || Col <- lists:seq(1, NumCols)], % make a list of types that each row has. Each entry is a tuple of Oratype and nativetype. Also includes the col count

    VarsDatas = [
            begin
                case NativeType of 'DPI_NATIVE_TYPE_DOUBLE' ->  % if the type is a double, make a variable for it, but say that the native type is bytes
                        % if stmt_getQueryValue() is used to get the values, then they will have their "correct" type. But doubles need to be
                        % fetched as a binary in order to avoid a rounding error that would occur if they were transformed from their internal decimal
                        % representation to double. Therefore, stmt_getQueryValue() can't be used for this, so a variable needs to be made because
                        % the data has to be fetched using define so the value goes into the data and then retrieving the values from the data
                        #{var := Var, data := Datas} = dpi:conn_newVar(Conn, OraType, 'DPI_NATIVE_TYPE_BYTES', 100, 0, false, false, null),
                        ok = dpi:stmt_define(Stmt, Col, Var),    %% results will be fetched to the vars and go into the data
                        {Var, Datas, OraType}; % put the variable and its data list into a tuple
                    'DPI_NATIVE_TYPE_LOB' ->
                        #{var := Var, data := Datas} = dpi:conn_newVar(Conn, OraType, 'DPI_NATIVE_TYPE_LOB', 100, 0, false, false, null),
                        ok = dpi:stmt_define(Stmt, Col, Var),    %% results will be fetched to the vars and go into the data
                        {Var, Datas, OraType}; % put the variable and its data list into a tuple
                    _else -> noVariable % when no variable needs to be made for the type, just put an atom signlizing that no variable was made and stmt_getQueryValue() can be used to get the values
                end
            end
            || {OraType, NativeType, Col} <- Types], % make a list of {Var, Datas} tuples. Var is the dpiVar handle, Datas is the list of Data handles in that respective Var  
R = get_rows(Conn, Stmt, NRows, Acc, VarsDatas), % gets all the results from the query
    [begin
        case VarDatas of {Var, Datas} -> % if there is a variable (which was made to fetch a double as a binary)
                [dpi:data_release(Data)|| Data <- Datas], % loop through the list of datas and release them all
                dpi:var_release(Var); % now release the variable
            _else -> nop % if no variable was made, then nothing needs to be done here
        end
    end || VarDatas <- VarsDatas], % clean up eventual variables that may have been made
R. % return query results

%% this recursive function fetches all the rows. It does so by calling yet another recursive function that fetches all the fields in a row.
get_rows(_Conn, _, 0, Acc, _VarsDatas) -> {lists:reverse(Acc), false};
get_rows(Conn, Stmt, NRows, Acc, VarsDatas) ->
    case dpi:stmt_fetch(Stmt) of % try to fetch a row
        #{found := true, bufferRowIndex := Index} -> % got a row: get the values in that row and then do the recursive call to try to get another row
            get_rows(Conn, Stmt, NRows -1, [get_column_values(Conn, Stmt, 1, VarsDatas, Index+1) | Acc], VarsDatas); % recursive call
        #{found := false} -> % no more rows: that was all of them
            {lists:reverse(Acc), true} % reverse the list so it's in the right order again after it was pieced together the other way around
    end.

%% get all the fields in one row
get_column_values(_Conn, _Stmt, ColIdx, VarsDatas, _RowIndex) when ColIdx > length(VarsDatas) -> [];
get_column_values(Conn, Stmt, ColIdx, VarsDatas, RowIndex) ->
    %io:format("varsdatas ColIdx ~p ~p~n", [VarsDatas, ColIdx]),
    case lists:nth(ColIdx, VarsDatas) of % get the entry that is either a {Var, Datas} tuple or noVariable if no variable was made for this column
        {_Var, Datas, OraType} -> % if a variable was made for this column: the value was fetched into the variable's data object, so get it from there
    %io:format("rowindex ~p nth ~p~n", [RowIndex, (catch lists:nth(RowIndex, Datas))]),    
    %io:format("datas ~p~n", [Datas]),
            Value = dpi:data_get(lists:nth(RowIndex, Datas)), % get the value out of that data variable
            ValueFixed = case OraType of % depending on the ora type, the value might have to be changed into a different format so it displays properly
                'DPI_ORACLE_TYPE_BLOB' -> list_to_binary(lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Value)])); % turn binary to hex string
                _Else -> Value end, % the value is already in the correct format for most types, so do nothing

            [ValueFixed | get_column_values(Conn, Stmt, ColIdx + 1, VarsDatas, RowIndex)]; % recursive call
        noVariable -> % if no variable has been made then that means that the value can be fetched with stmt_getQueryValue()
            #{data := Data} = dpi:stmt_getQueryValue(Stmt, ColIdx), % get the value 
            Value = dpi:data_get(Data), % take the value from this freshly made data
            dpi:data_release(Data), % release this new data object
            [Value | get_column_values(Conn, Stmt, ColIdx + 1, VarsDatas, RowIndex)]; % recursive call
        Else ->
            io:format("ERROR! Invalid variable term of ~p~n!", [Else])
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% END OF DDERL DRIVER EXTRACT
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    
main(_) ->
    % set up dpi, context and conn
    ok = dpi:load_unsafe(),
    Ctx = dpi:context_create(3, 0),
    Conn = dpi:conn_create(Ctx, ?USER, ?PSWD, ?TNS, #{}, #{}),

    % make a table with 10 rows
    executeSQL(Conn, <<"drop table foo">>),
    executeSQL(Conn, <<"create table foo (bar int)">>),
    executeSQL(Conn, <<"insert into foo values (1)">>),
    executeSQL(Conn, <<"insert into foo values (2)">>),
    executeSQL(Conn, <<"insert into foo values (3)">>),
    executeSQL(Conn, <<"insert into foo values (4)">>),
    executeSQL(Conn, <<"insert into foo values (5)">>),
    executeSQL(Conn, <<"insert into foo values (6)">>),
    executeSQL(Conn, <<"insert into foo values (7)">>),
    executeSQL(Conn, <<"insert into foo values (8)">>),
    executeSQL(Conn, <<"insert into foo values (9)">>),
    executeSQL(Conn, <<"insert into foo values (10)">>),


    Stmt = dpi:conn_prepareStmt(Conn, false, ?TEST_SQL, <<>>),
    Ret = dpi:stmt_execute(Stmt, []),
    R = dpi_fetch_rows( Conn, Stmt, 99999999),
    io:format("Fetched ~p Ret ~p~n", [R, Ret]),
    halt(1).

