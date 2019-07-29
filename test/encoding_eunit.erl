-module(encoding_eunit).
-include_lib("eunit/include/eunit.hrl").

load_test() -> 
    ?assertEqual(ok, dpi:load_unsafe()),
    Context = dpi:context_create(3, 0),
    #{user := User, password := Pass, tns := TNS} = getConfig(),
    Conn = dpi:conn_create(Context, User, Pass, TNS, #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}),
    Original = <<"äëïöü ñç"/utf8>>,
    %SQL = <<"select '汉字' from dual">>,
    SQL = <<"select 'äëïöü ñç' from dual"/utf8>>,
    Stmt = dpi:conn_prepareStmt(Conn, false, SQL, <<"">>),
    Query_cols = dpi:stmt_execute(Stmt, []),
    dpi:stmt_fetch(Stmt),
    #{nativeTypeNum := Type, data := Result} =
        dpi:stmt_getQueryValue(Stmt, 1),
    D = dpi:data_get(Result),
    ?debugFmt("Original ~w Got ~w", [Original, D]),
    %?assertEqual(<<"汉字">>, D),

    ?assertEqual(<<"äëïöü ñç"/utf8>>, D),
    %?assertEqual(Type, 'DPI_NATIVE_TYPE_DOUBLE'),
    ?assertEqual(Query_cols, 1),
    dpi:data_release(Result),
    dpi:stmt_close(Stmt, <<>>),
    myeh.




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
