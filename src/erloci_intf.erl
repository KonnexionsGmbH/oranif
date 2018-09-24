-module(erloci_intf).

%% External API to OCI Driver

-export([ociNlsGetInfo/2, ociCharsetAttrGet/1, ociAttrSet/5, ociAttrGet/4,
         ociStmtExecute/6, ociBindByName/5]).

-export([parse_lang/1]).

-spec ociNlsGetInfo(Envhp :: reference(),
                    Item :: atom()) -> {ok, Info :: binary()}
                                          | {error, binary()}.
ociNlsGetInfo(Envhp, Item) ->
    ItemId = erloci_int:nls_info_to_int(Item),
    erloci:ociNlsGetInfo(Envhp, ItemId).

-spec ociCharsetAttrGet(Envhp :: reference()) ->
    {ok, {Charset :: integer(), NCharset :: integer()}}
    | {error, binary()}.
ociCharsetAttrGet(Envhp) ->
    case erloci:ociCharsetAttrGet(Envhp) of
        {ok, {CharsetId, NCharsetId}} ->
            {ok, Name} = erloci:ociNlsCharSetIdToName(Envhp, CharsetId),
            {ok, NName} = erloci:ociNlsCharSetIdToName(Envhp, NCharsetId),
            {ok, #{charset => {CharsetId, Name},
                   ncharset => {NCharsetId, NName}}};
        Err ->
            Err
    end.

-spec ociAttrSet(Handle :: reference(),
                HandleType :: atom(),
                CDataType :: atom(),
                Value :: integer() | binary(),
                AttrType :: atom()) ->
                    ok | {error, binary()}.
ociAttrSet(Handle, HandleType, CDataTpe, Value, AttrType) ->
    HandleTypeInt = erloci_int:handle_type_to_int(HandleType),
    CDataTypeInt = erloci_int:c_type(CDataTpe),
    AttrTypeInt = erloci_int:attr_name_to_int(AttrType),
    erloci:ociAttrSet(Handle, HandleTypeInt, CDataTypeInt, Value, AttrTypeInt).

-spec ociAttrGet(Handle :: reference(),
                HandleType :: atom(),
                CDataTpe :: atom(),
                AttrType :: atom()) ->
        {ok, Value :: integer() | binary()} | {error, binary()}.
ociAttrGet(Handle, HandleType, CDataTpe, AttrType) ->
    HandleTypeInt = erloci_int:handle_type_to_int(HandleType),
    CDataTypeInt = erloci_int:c_type(CDataTpe),
    AttrTypeInt = erloci_int:attr_name_to_int(AttrType),
    erloci:ociAttrGet(Handle, HandleTypeInt, CDataTypeInt, AttrTypeInt).

%%
-spec ociStmtExecute(Svchp :: reference(),
                    Stmthp :: reference(),
                    BindVars :: map(),
                    Iters :: pos_integer(),
                    RowOff :: pos_integer(),
                    Mode :: atom()) -> ok | {error, binary()}.
ociStmtExecute(Svchp, Stmthp, BindVars, Iters, RowOff, Mode) ->
    ModeInt = erloci_int:oci_mode(Mode),
    case erloci:ociStmtExecute(Svchp, Stmthp, BindVars, Iters, RowOff, ModeInt) of
        {ok, #{statement := Stmt} = Map} ->
            Map2 = case Map of
                #{cols := Cols} ->
                    Cs2 = [{Value,erloci_int:int_to_sql_type(Type),Size,Precision,Scale} ||
                            {Value,Type, Size,Precision,Scale} <- Cols],
                    maps:put(cols, Cs2, Map);
                _ -> 
                    Map
                end,
            {ok, maps:put(statement, erloci_int:int_to_stmt_type(Stmt), Map2)};
        Else ->
            Else
    end.

-spec ociBindByName(Stmthp :: reference(),
                    BindVars :: map(),
                    BindVarName :: binary(),
                    SqlType :: atom(),
                    BindVarValue :: term() | 'NULL') ->
                                        {ok, BindVars2 :: map()}
                                        | {error, binary()}.
ociBindByName(Stmthp, BindVars, BindVarName, SqlType, BindVarValue) when
                                                    is_reference(Stmthp),
                                                    is_map(BindVars),
                                                    is_binary(BindVarName),
                                                    is_atom(SqlType) ->
    IntType = erloci_int:sql_type_to_int(SqlType),
    case BindVarValue of
        'NULL' ->
            erloci:ociBindByName(Stmthp, BindVars, BindVarName, -1, IntType, <<>>);
        _ ->
            erloci:ociBindByName(Stmthp, BindVars, BindVarName, 0, IntType, BindVarValue)
    end.

parse_lang(Lang) when is_list(Lang) ->
    case string:tokens(Lang, "._") of
        [Language, Country, Charset] ->
            {list_to_binary(Language),
                list_to_binary(Country),
                list_to_binary(Charset)};
        Else ->
            Else
    end;
parse_lang(Lang) when is_binary(Lang) ->
    case binary:split(Lang, [<<".">>, <<"_">>], [global]) of
        [Language, Country, Charset] ->
            {Language, Country, Charset};
        Else ->
                Else
    end.
