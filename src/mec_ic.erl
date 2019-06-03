-module(mec_ic).

% public safe apis (when NIF is loaded in slave)
-export([connect/4, close/1, commit/1, rollback/1, ping/1, connect_internal/3, close_internal/2]).

-export([sp_insert_csv/1, sp_insert_csv_values/10, sp_insert_csv_execute/0,
         sp_insert_csv_cleanup/0]).

-export([sp_update_header/1, sp_update_header_values/8,
         sp_update_header_execute/0, sp_update_header_cleanup/0]).

-export([sp_insert_header/1, sp_insert_header_values/12,
         sp_insert_header_execute/0, sp_insert_header_cleanup/0]).


-export([sp_insert_csv_internal/1, sp_insert_csv_values_internal/11, sp_insert_csv_execute_internal/1,
         sp_insert_csv_cleanup_internal/1]).

-export([sp_update_header_internal/1, sp_update_header_values_internal/9,
         sp_update_header_execute_internal/1, sp_update_header_cleanup_internal/1]).

-export([sp_insert_header_internal/1, sp_insert_header_values_internal/13,
         sp_insert_header_execute_internal/1, sp_insert_header_cleanup_internal/1]).

-export([commit_internal/2]).

-define(GUARD_NULL_STR(_X), (_X == null orelse is_binary(_X))).
-define(GUARD_NULL_INT(_X), (_X == null orelse is_integer(_X))).
-define(GUARD_ARRY_LEN(_X, _Y), (_X == null orelse length(_X) == _Y)).

% records for each PL/SQL API state
-record(csvctx, {
    stmt, context, conn, array_size = 0,
    binary_size = 0,
    
    p_BatchSize_In_Integer_Var, p_BatchSize_In_Integer_Data,
    p_BiHId_In_VarChar2_Var, p_BiHId_In_VarChar2_Data,
    p_MaxAge_In_Number_Var, p_MaxAge_In_Number_Data,
    p_DataHeader_In_VarChar2_Var, p_DataHeader_In_VarChar2_Data,

    p_RecCount_InOut_Number_Var, p_RecCount_InOut_Number_Data,
    p_PreParseErrCount_InOut_Number_Var, p_PreParseErrCount_InOut_Number_Data,
    p_ErrCount_InOut_Number_Var, p_ErrCount_InOut_Number_Data,
    p_DateFc_InOut_Varchar2_Var, p_DateFc_InOut_Varchar2_Data,
    p_DateLc_InOut_Varchar2_Var, p_DateLc_InOut_Varchar2_Data,
    p_ErrorCode_Out_Number_Var, p_ErrorCode_Out_Number_Data,
    p_ErrorDesc_Out_Varchar2_Var, p_ErrorDesc_Out_Varchar2_Data,
    p_ReturnStatus_Out_Number_Var, p_ReturnStatus_Out_Number_Data,
    p_RecordNr_In_ArrRecNr_Var, p_RecordNr_In_ArrRecNr_Data,
    p_RecordData_In_ArrRecData_Var, p_RecordData_In_ArrRecData_Data
}).

-record(uhctx, {
    stmt, context,
    
    p_BiHId_In_Varchar2_Var, p_BiHId_In_Varchar2_Data,
    p_MaxAge_In_Number_Var, p_MaxAge_In_Number_Data,
    p_DataHeader_In_VarChar2_Var, p_DataHeader_In_VarChar2_Data,
    p_RecCount_In_Number_Var, p_RecCount_In_Number_Data,
    p_PreParseErrCount_In_Number_Var, p_PreParseErrCount_In_Number_Data,
    p_ErrCount_In_Number_Var, p_ErrCount_In_Number_Data,
    p_DateFc_In_Varchar2_Var, p_DateFc_In_Varchar2_Data,
    p_DateLc_In_Varchar2_Var, p_DateLc_In_Varchar2_Data,

    p_ErrorCode_Out_Number_Var, p_ErrorCode_Out_Number_Data,
    p_ErrorDesc_Out_Varchar2_Var, p_ErrorDesc_Out_Varchar2_Data,
    p_ReturnStatus_Out_Number_Var, p_ReturnStatus_Out_Number_Data
}).

-record(ihctx, {
    stmt, context,

    p_ErrorCode_Out_Number_Var, p_ErrorCode_Out_Number_Data,
	p_ErrorDesc_Out_Varchar2_Var, p_ErrorDesc_Out_Varchar2_Data,
    p_ReturnStatus_Out_Number_Var, p_ReturnStatus_Out_Number_Data,

    p_BIH_ID_InOut_VarChar2_Var, p_BIH_ID_InOut_VarChar2_Data,
    p_BIH_DEMO_In_Number_Var, p_BIH_DEMO_In_Number_Data,
    p_BIH_FILESEQ_In_Number_Var, p_BIH_FILESEQ_In_Number_Data,
    p_BIH_FILENAME_In_Varchar2_Var, p_BIH_FILENAME_In_Varchar2_Data,
    p_BIH_FILEDATE_In_Varchar2_Var, p_BIH_FILEDATE_In_Varchar2_Data,
    p_BIH_MAPID_In_Varchar2_Var, p_BIH_MAPID_In_Varchar2_Data,
    p_AppName_In_Varchar2_Var, p_AppName_In_Varchar2_Data,
    p_AppVer_In_Varchar2_Var, p_AppVer_In_Varchar2_Data,
    p_Thread_In_Varchar2_Var, p_Thread_In_Varchar2_Data,
    p_JobId_In_Number_Var, p_JobId_In_Number_Data,
    p_HostName_In_Varchar2_Var, p_HostName_In_Varchar2_Data,
    p_Status_In_Varchar_Var, p_Status_In_Varchar_Data
}).

%==============================================================================
%   Common DB interface
%==============================================================================

connect(User, Password, ConStr, SlaveName)
    when is_binary(User), is_binary(Password), is_binary(ConStr)
->
    ok = dpi:load(SlaveName),
    dpi:safe(fun ?MODULE:connect_internal/3, [User, Password, ConStr]).
connect_internal(User, Password, ConStr) ->
    try
        Context = dpi:context_create(3, 0),
        Conn = dpi:conn_create(Context, User, Password, ConStr, #{}, #{}), 
        {ok, #{context => Context, conn => Conn}}
    catch
        Class:Exception ->
            {error,
            #{class => Class, exception => Exception,
              message =>"Bad Context"}}
    end.

close(#{conn := Conn, context := Ctx}) ->
    dpi:safe(fun ?MODULE:close_internal/2, [Conn, Ctx]).
close_internal(Conn, Context)->
    try
        ok = dpi:conn_release(Conn),
        ok = dpi:context_destroy(Context)
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

commit(#{context := Ctx, conn := Conn}) ->
    dpi:safe(fun ?MODULE:commit_internal/2, [Ctx, Conn]).
commit_internal(Ctx, Conn) ->
   try
       dpi:conn_commit(Conn)
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

rollback(#{context := Ctx, conn := Conn}) -> 
    try
        ok = dpi:safe(fun dpi:conn_rollback/1, [Conn])
    catch
        Class:Exception -> 
            DpiError = getError(Ctx),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

ping(#{context := Ctx, conn := Conn}) -> 
    try
        dpi:safe(fun dpi:conn_ping/1, [Conn])
    catch
        Class:Exception -> 
            DpiError = getError(Ctx),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

%------------------------------------------------------------------------------
%   sp_insert_csv
%------------------------------------------------------------------------------

-define(SP_INSERT_CSV, 
    <<
	    "begin PKG_MEC_IC_CSV.SP_INSERT_CSV ("
	    " :p_BiHId_In_VarChar2,"
	    " :p_BatchSize_In_Integer,"
	    " :p_MaxAge_In_Number,"
	    " :p_DataHeader_In_VarChar2,"
	    " :p_RecordNr_In_ArrRecNr,"
	    " :p_RecordData_In_ArrRecData,"
	    " :p_RecCount_InOut_Number,"
	    " :p_PreParseErrCount_InOut_Number,"
	    " :p_ErrCount_InOut_Number,"
	    " :p_DateFc_InOut_Varchar2,"
	    " :p_DateLc_InOut_Varchar2,"
	    " :p_ErrorCode_Out_Number,"
	    " :p_ErrorDesc_Out_Varchar2,"
	    " :p_ReturnStatus_Out_Number"
	    "); end;"
    >>
).
sp_insert_csv(Map) ->
    {ok, Csvctx} = dpi:safe(fun ?MODULE:sp_insert_csv_internal/1, [Map]),
    put(csvctx, Csvctx),
    ok.
sp_insert_csv_internal(#{conn := Conn, context := Context}) ->
    try
        Stmt = dpi:conn_prepareStmt(Conn, false, ?SP_INSERT_CSV, <<"">>),

        {P_BiHId_In_VarChar2_Var, P_BiHId_In_VarChar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_BiHId_In_VarChar2">>),
        {P_BatchSize_In_Integer_Var, P_BatchSize_In_Integer_Data} =
            make_and_bind_int_var(Conn, Stmt, <<"p_BatchSize_In_Integer">>),
        {P_MaxAge_In_Number_Var, P_MaxAge_In_Number_Data} =
            make_and_bind_int_var(Conn, Stmt, <<"p_MaxAge_In_Number">>),
        {P_DataHeader_In_VarChar2_Var, P_DataHeader_In_VarChar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_DataHeader_In_VarChar2">>),

        {P_RecCount_InOut_Number_Var, P_RecCount_InOut_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_RecCount_InOut_Number">>),

        {P_PreParseErrCount_InOut_Number_Var,
            P_PreParseErrCount_InOut_Number_Data} = make_and_bind_int_var(
            Conn, Stmt, <<"p_PreParseErrCount_InOut_Number">>
        ),

        {P_ErrCount_InOut_Number_Var, P_ErrCount_InOut_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_ErrCount_InOut_Number">>),

        {P_DateFc_InOut_Varchar2_Var, P_DateFc_InOut_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_DateFc_InOut_Varchar2">>),

        {P_DateLc_InOut_Varchar2_Var, P_DateLc_InOut_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_DateLc_InOut_Varchar2">>),

        {P_ErrorCode_Out_Number_Var, P_ErrorCode_Out_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_ErrorCode_Out_Number">>),

        {P_ErrorDesc_Out_Varchar2_Var, P_ErrorDesc_Out_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_ErrorDesc_Out_Varchar2">>),

        {P_ReturnStatus_Out_Number_Var, P_ReturnStatus_Out_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_ReturnStatus_Out_Number">>),

            Csvctx = 
            #csvctx{
                stmt = Stmt, conn = Conn, context = Context,

                p_BiHId_In_VarChar2_Var = P_BiHId_In_VarChar2_Var,
                p_BiHId_In_VarChar2_Data = P_BiHId_In_VarChar2_Data,
                p_BatchSize_In_Integer_Var = P_BatchSize_In_Integer_Var,
                p_BatchSize_In_Integer_Data = P_BatchSize_In_Integer_Data,
                p_MaxAge_In_Number_Var = P_MaxAge_In_Number_Var,
                p_MaxAge_In_Number_Data = P_MaxAge_In_Number_Data,
                p_DataHeader_In_VarChar2_Var = P_DataHeader_In_VarChar2_Var,
                p_DataHeader_In_VarChar2_Data = P_DataHeader_In_VarChar2_Data,
                p_RecCount_InOut_Number_Var = P_RecCount_InOut_Number_Var,
                p_RecCount_InOut_Number_Data = P_RecCount_InOut_Number_Data,
                p_PreParseErrCount_InOut_Number_Var =
                    P_PreParseErrCount_InOut_Number_Var,
                p_PreParseErrCount_InOut_Number_Data =
                    P_PreParseErrCount_InOut_Number_Data,
                p_ErrCount_InOut_Number_Var = P_ErrCount_InOut_Number_Var,
                p_ErrCount_InOut_Number_Data = P_ErrCount_InOut_Number_Data,
                p_DateFc_InOut_Varchar2_Var = P_DateFc_InOut_Varchar2_Var,
                p_DateFc_InOut_Varchar2_Data = P_DateFc_InOut_Varchar2_Data,
                p_DateLc_InOut_Varchar2_Var = P_DateLc_InOut_Varchar2_Var,
                p_DateLc_InOut_Varchar2_Data = P_DateLc_InOut_Varchar2_Data,
                p_ErrorCode_Out_Number_Var = P_ErrorCode_Out_Number_Var,
                p_ErrorCode_Out_Number_Data = P_ErrorCode_Out_Number_Data,
                p_ErrorDesc_Out_Varchar2_Var = P_ErrorDesc_Out_Varchar2_Var,
                p_ErrorDesc_Out_Varchar2_Data = P_ErrorDesc_Out_Varchar2_Data,
                p_ReturnStatus_Out_Number_Var = P_ReturnStatus_Out_Number_Var,
                p_ReturnStatus_Out_Number_Data = P_ReturnStatus_Out_Number_Data
            },
        {ok, Csvctx}
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_insert_csv_values(
    P_BiHId_In_VarChar2,
    P_MaxAge_In_Number,
    P_DataHeader_In_VarChar2,
    P_RecordNr_In_ArrRecNr,
    P_RecordData_In_ArrRecData,
    P_RecCount_InOut_Number,
    P_PreParseErrCount_InOut_Number,
    P_ErrCount_InOut_Number,
    P_DateFc_InOut_Varchar2,
    P_DateLc_InOut_Varchar2
) ->
    put(
        csvctx,
        dpi:safe(
            fun ?MODULE:sp_insert_csv_values_internal/11,
            [
                get(csvctx),
                P_BiHId_In_VarChar2,
                P_MaxAge_In_Number,
                P_DataHeader_In_VarChar2,
                P_RecordNr_In_ArrRecNr,
                P_RecordData_In_ArrRecData,
                P_RecCount_InOut_Number,
                P_PreParseErrCount_InOut_Number,
                P_ErrCount_InOut_Number,
                P_DateFc_InOut_Varchar2,
                P_DateLc_InOut_Varchar2
            ]
        )
    ).
sp_insert_csv_values_internal(
    Ctx0,
    P_BiHId_In_VarChar2,
    P_MaxAge_In_Number,
    P_DataHeader_In_VarChar2,
    P_RecordNr_In_ArrRecNr,
    P_RecordData_In_ArrRecData,
    P_RecCount_InOut_Number,
    P_PreParseErrCount_InOut_Number,
    P_ErrCount_InOut_Number,
    P_DateFc_InOut_Varchar2,
    P_DateLc_InOut_Varchar2
)
    when
        ?GUARD_NULL_STR(P_BiHId_In_VarChar2) andalso
        ?GUARD_NULL_INT(P_MaxAge_In_Number) andalso
        ?GUARD_NULL_STR(P_DataHeader_In_VarChar2) andalso
        ?GUARD_NULL_INT(P_RecCount_InOut_Number) andalso
        ?GUARD_NULL_INT(P_PreParseErrCount_InOut_Number) andalso
        ?GUARD_NULL_INT(P_ErrCount_InOut_Number) andalso
        ?GUARD_NULL_STR(P_DateFc_InOut_Varchar2) andalso
        ?GUARD_NULL_STR(P_DateLc_InOut_Varchar2) andalso
        length(P_RecordNr_In_ArrRecNr) == length(P_RecordData_In_ArrRecData)
-> 
    try       
        Ctx = sp_insert_csv_var_realloc(
            Ctx0, P_RecordNr_In_ArrRecNr, P_RecordData_In_ArrRecData
        ),
        
        ok = dpi:stmt_bindByName(
            Ctx#csvctx.stmt, <<"p_RecordNr_In_ArrRecNr">>,
            Ctx#csvctx.p_RecordNr_In_ArrRecNr_Var
        ),
        ok = dpi:stmt_bindByName(
            Ctx#csvctx.stmt, <<"p_RecordData_In_ArrRecData">>,
            Ctx#csvctx.p_RecordData_In_ArrRecData_Var
        ),

        dpi:var_setNumElementsInArray(Ctx#csvctx.p_RecordNr_In_ArrRecNr_Var,
            length(P_RecordNr_In_ArrRecNr)),
        dpi:var_setNumElementsInArray(Ctx#csvctx.p_RecordData_In_ArrRecData_Var,
            length(P_RecordData_In_ArrRecData)),

        set_var_bin(
            P_BiHId_In_VarChar2, Ctx#csvctx.p_BiHId_In_VarChar2_Data,
            Ctx#csvctx.p_BiHId_In_VarChar2_Var
        ),
        ok = dpi:data_setInt64(
            Ctx#csvctx.p_MaxAge_In_Number_Data, P_MaxAge_In_Number
        ),
        set_var_bin(
            P_DataHeader_In_VarChar2, Ctx#csvctx.p_BiHId_In_VarChar2_Data,
            Ctx#csvctx.p_BiHId_In_VarChar2_Var
        ),
        set_var_bin(
            P_DataHeader_In_VarChar2, Ctx#csvctx.p_DataHeader_In_VarChar2_Data,
            Ctx#csvctx.p_DataHeader_In_VarChar2_Var
        ),

        IndexList = lists:seq(0, length(P_RecordData_In_ArrRecData) - 1),

        % #csvctx.p_RecordNr_In_ArrRecNr_Data may be bigger than
        % length(P_RecordData_In_ArrRecData) so we need to only use first
        % 1 - length(P_RecordData_In_ArrRecData) from this list
        DataList = lists:sublist(
            Ctx#csvctx.p_RecordNr_In_ArrRecNr_Data,
            length(P_RecordNr_In_ArrRecNr)
        ),

        lists:foreach(
            fun({Idx, {ArrRecNr, Entry, Value}}) ->
                ok = dpi:data_setIsNull(ArrRecNr, false),
                ok = dpi:data_setInt64(ArrRecNr, Entry),
                ok = dpi:var_setFromBytes(
                    Ctx#csvctx.p_RecordData_In_ArrRecData_Var, Idx, Value
                )
            end,
            lists:zip(
                IndexList,
                lists:zip3(
                    DataList, P_RecordNr_In_ArrRecNr,
                    P_RecordData_In_ArrRecData
                )
            )
        ),
        
        ok = dpi:data_setInt64(
            Ctx#csvctx.p_RecCount_InOut_Number_Data, P_RecCount_InOut_Number
        ),
        ok = dpi:data_setInt64(
            Ctx#csvctx.p_ErrCount_InOut_Number_Data, P_ErrCount_InOut_Number
        ),
        ok = dpi:data_setInt64(
            Ctx#csvctx.p_PreParseErrCount_InOut_Number_Data,
            P_PreParseErrCount_InOut_Number
        ),
        ok = dpi:var_setFromBytes(
            Ctx#csvctx.p_DateFc_InOut_Varchar2_Var, 0, P_DateFc_InOut_Varchar2
        ),
        ok = dpi:var_setFromBytes(
            Ctx#csvctx.p_DateLc_InOut_Varchar2_Var, 0, P_DateLc_InOut_Varchar2
        ),
        %if Ctx /= Ctx0 -> put(csvctx, Ctx); true -> ok end,
        Ctx
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx0#csvctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_insert_csv_execute() ->
    dpi:safe(fun ?MODULE:sp_insert_csv_execute_internal/1,[get(csvctx)]).
sp_insert_csv_execute_internal(Ctx) -> 
    try
        dpi:stmt_execute(Ctx#csvctx.stmt, []),
        #{ 
            p_DateFc_InOut_Varchar2 => 
                dpi:data_get(Ctx#csvctx.p_DateFc_InOut_Varchar2_Data),
            p_DateLc_InOut_Varchar2 => 
                dpi:data_get(Ctx#csvctx.p_DateLc_InOut_Varchar2_Data),
            p_ErrCount_InOut_Number => 
                dpi:data_get(Ctx#csvctx.p_ErrCount_InOut_Number_Data),
            p_RecCount_InOut_Number => 
                dpi:data_get(Ctx#csvctx.p_RecCount_InOut_Number_Data),
            p_ErrorCode_Out_Number => 
                dpi:data_get(Ctx#csvctx.p_ErrorCode_Out_Number_Data),
            p_ErrorDesc_Out_Varchar2 => 
                dpi:data_get(Ctx#csvctx.p_ErrorDesc_Out_Varchar2_Data),
            p_ReturnStatus_Out_Number => 
                dpi:data_get(Ctx#csvctx.p_ReturnStatus_Out_Number_Data)
        }
    catch
        Class:Exception -> 
            DpiError = getError(Ctx#csvctx.context),
        {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_insert_csv_cleanup() ->
    dpi:safe(fun ?MODULE:sp_insert_csv_cleanup_internal/1, [get(csvctx)]).
sp_insert_csv_cleanup_internal(Ctx) -> 
    try
        ok = dpi:stmt_release(Ctx#csvctx.stmt),
        ok = dpi:var_release(Ctx#csvctx.p_BiHId_In_VarChar2_Var),
        ok = dpi:data_release(Ctx#csvctx.p_BiHId_In_VarChar2_Data),
        ok = dpi:var_release(Ctx#csvctx.p_BatchSize_In_Integer_Var),
        ok = dpi:data_release(Ctx#csvctx.p_BatchSize_In_Integer_Data),
        ok = dpi:var_release(Ctx#csvctx.p_MaxAge_In_Number_Var),
        ok = dpi:data_release(Ctx#csvctx.p_MaxAge_In_Number_Data),
        ok = dpi:var_release(Ctx#csvctx.p_DataHeader_In_VarChar2_Var),
        ok = dpi:data_release(Ctx#csvctx.p_DataHeader_In_VarChar2_Data),
        ok = dpi:var_release(Ctx#csvctx.p_RecordNr_In_ArrRecNr_Var),
        ok = oks([dpi:data_release(X)
                    || X <- Ctx#csvctx.p_RecordNr_In_ArrRecNr_Data]),
        ok = dpi:var_release(Ctx#csvctx.p_RecordData_In_ArrRecData_Var),
        ok = oks([dpi:data_release(X)
                    || X <- Ctx#csvctx.p_RecordData_In_ArrRecData_Data]),
        ok = dpi:var_release(Ctx#csvctx.p_RecCount_InOut_Number_Var),
        ok = dpi:data_release(Ctx#csvctx.p_RecCount_InOut_Number_Data),
        ok = dpi:var_release(Ctx#csvctx.p_PreParseErrCount_InOut_Number_Var),
        ok = dpi:data_release(Ctx#csvctx.p_PreParseErrCount_InOut_Number_Data),
        ok = dpi:var_release(Ctx#csvctx.p_ErrCount_InOut_Number_Var),
        ok = dpi:data_release(Ctx#csvctx.p_ErrCount_InOut_Number_Data),
        ok = dpi:var_release(Ctx#csvctx.p_DateFc_InOut_Varchar2_Var),
        ok = dpi:data_release(Ctx#csvctx.p_DateFc_InOut_Varchar2_Data),
        ok = dpi:var_release(Ctx#csvctx.p_DateLc_InOut_Varchar2_Var),
        ok = dpi:data_release(Ctx#csvctx.p_DateLc_InOut_Varchar2_Data),
        ok = dpi:var_release(Ctx#csvctx.p_ErrorCode_Out_Number_Var),
        ok = dpi:data_release(Ctx#csvctx.p_ErrorCode_Out_Number_Data),
        ok = dpi:var_release(Ctx#csvctx.p_ErrorDesc_Out_Varchar2_Var),
        ok = dpi:data_release(Ctx#csvctx.p_ErrorDesc_Out_Varchar2_Data),
        ok = dpi:var_release(Ctx#csvctx.p_ReturnStatus_Out_Number_Var),
        ok = dpi:data_release(Ctx#csvctx.p_ReturnStatus_Out_Number_Data)
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#csvctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

%------------------------------------------------------------------------------
%   sp_update_header
%------------------------------------------------------------------------------

-define(SP_UPDATE_HEADER,
    <<
	    "begin PKG_MEC_IC_CSV.SP_UPDATE_HEADER ("
	    " :p_BiHId_In_Varchar2,"
	    " :p_MaxAge_In_Number,"
	    " :p_DataHeader_In_VarChar2,"
	    " :p_RecCount_In_Number,"
	    " :p_PreParseErrCount_In_Number,"
	    " :p_ErrCount_In_Number,"
	    " :p_DateFc_In_Varchar2,"
	    " :p_DateLc_In_Varchar2,"
	    " :p_ErrorCode_Out_Number,"
	    " :p_ErrorDesc_Out_Varchar2,"
	    " :p_ReturnStatus_Out_Number"
	    "); end;"
    >>
).
sp_update_header(Map) ->
    case dpi:safe(fun ?MODULE:sp_update_header_internal/1,[Map]) of
        {error, _} = Error -> Error;
        Uhctx -> put(uhctx, Uhctx)
    end.
sp_update_header_internal(#{conn := Conn, context := Context}) ->
    try
        Stmt = dpi:conn_prepareStmt(Conn, false, ?SP_UPDATE_HEADER, <<"">>),
        
        {P_BiHId_In_Varchar2_Var, P_BiHId_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_BiHId_In_VarChar2">>),
        {P_MaxAge_In_Number_Var, P_MaxAge_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_MaxAge_In_Number">>),
        {P_DataHeader_In_VarChar2_Var, P_DataHeader_In_VarChar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_DataHeader_In_VarChar2">>),
        {P_RecCount_In_Number_Var, P_RecCount_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_RecCount_In_Number">>),
        {P_PreParseErrCount_In_Number_Var, P_PreParseErrCount_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt,
            <<"p_PreParseErrCount_In_Number">>),
        {P_ErrCount_In_Number_Var, P_ErrCount_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_ErrCount_In_Number">>),
        {P_DateFc_In_Varchar2_Var, P_DateFc_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_DateFc_In_Varchar2">>),
        {P_DateLc_In_Varchar2_Var, P_DateLc_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_DateLc_In_Varchar2">>),

        {P_ErrorCode_Out_Number_Var, P_ErrorCode_Out_Number_Data} =
            make_and_bind_int_var(Conn, Stmt, <<"p_ErrorCode_Out_Number">>),
        {P_ErrorDesc_Out_Varchar2_Var, P_ErrorDesc_Out_Varchar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_ErrorDesc_Out_Varchar2">>),
        {P_ReturnStatus_Out_Number_Var, P_ReturnStatus_Out_Number_Data} =
            make_and_bind_int_var(Conn, Stmt, <<"p_ReturnStatus_Out_Number">>),

        Uhctx = 
            #uhctx{
                stmt = Stmt,
                context = Context,
                p_BiHId_In_Varchar2_Var = P_BiHId_In_Varchar2_Var,
                p_BiHId_In_Varchar2_Data = P_BiHId_In_Varchar2_Data,
                p_MaxAge_In_Number_Var = P_MaxAge_In_Number_Var,
                p_MaxAge_In_Number_Data = P_MaxAge_In_Number_Data,
                p_DataHeader_In_VarChar2_Var = P_DataHeader_In_VarChar2_Var,
                p_DataHeader_In_VarChar2_Data = P_DataHeader_In_VarChar2_Data,
                p_RecCount_In_Number_Var = P_RecCount_In_Number_Var,
                p_RecCount_In_Number_Data = P_RecCount_In_Number_Data,
                p_PreParseErrCount_In_Number_Var =
                    P_PreParseErrCount_In_Number_Var,
                p_PreParseErrCount_In_Number_Data =
                    P_PreParseErrCount_In_Number_Data,
                p_ErrCount_In_Number_Var = P_ErrCount_In_Number_Var,
                p_ErrCount_In_Number_Data = P_ErrCount_In_Number_Data,
                p_DateFc_In_Varchar2_Var = P_DateFc_In_Varchar2_Var,
                p_DateFc_In_Varchar2_Data = P_DateFc_In_Varchar2_Data,
                p_DateLc_In_Varchar2_Var = P_DateLc_In_Varchar2_Var,
                p_DateLc_In_Varchar2_Data = P_DateLc_In_Varchar2_Data,
                p_ErrorCode_Out_Number_Var = P_ErrorCode_Out_Number_Var,
                p_ErrorCode_Out_Number_Data =  P_ErrorCode_Out_Number_Data,
                p_ErrorDesc_Out_Varchar2_Var = P_ErrorDesc_Out_Varchar2_Var,
                p_ErrorDesc_Out_Varchar2_Data = P_ErrorDesc_Out_Varchar2_Data,
                p_ReturnStatus_Out_Number_Var = P_ReturnStatus_Out_Number_Var,
                p_ReturnStatus_Out_Number_Data = P_ReturnStatus_Out_Number_Data
            },
        Uhctx
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.


sp_update_header_values(
    P_BiHId_In_Varchar2,
    P_MaxAge_In_Number,
    P_DataHeader_In_VarChar2,
    P_RecCount_In_Number,
    P_PreParseErrCount_In_Number,
    P_ErrCount_In_Number,
    P_DateFc_In_Varchar2,
    P_DateLc_In_Varchar2
) ->
    dpi:safe(fun ?MODULE:sp_update_header_values_internal/9,[
        get(uhctx),
        P_BiHId_In_Varchar2,    
        P_MaxAge_In_Number,
        P_DataHeader_In_VarChar2,
        P_RecCount_In_Number,
        P_PreParseErrCount_In_Number,
        P_ErrCount_In_Number,
        P_DateFc_In_Varchar2,
        P_DateLc_In_Varchar2]
    ).
sp_update_header_values_internal(
    Ctx,
    P_BiHId_In_Varchar2,    
    P_MaxAge_In_Number,
    P_DataHeader_In_VarChar2,
    P_RecCount_In_Number,
    P_PreParseErrCount_In_Number,
    P_ErrCount_In_Number,
    P_DateFc_In_Varchar2,
    P_DateLc_In_Varchar2
)
    when
        ?GUARD_NULL_STR(P_BiHId_In_Varchar2) andalso
        ?GUARD_NULL_INT(P_MaxAge_In_Number) andalso
        ?GUARD_NULL_STR(P_DataHeader_In_VarChar2) andalso
        ?GUARD_NULL_INT(P_RecCount_In_Number) andalso
        ?GUARD_NULL_INT(P_PreParseErrCount_In_Number) andalso
        ?GUARD_NULL_INT(P_ErrCount_In_Number) andalso
        ?GUARD_NULL_STR(P_DateFc_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_DateLc_In_Varchar2)
-> 
    try
        set_var_bin(
            P_BiHId_In_Varchar2, Ctx#uhctx.p_BiHId_In_Varchar2_Data,
            Ctx#uhctx.p_BiHId_In_Varchar2_Var
        ),
        ok = dpi:data_setInt64(
            Ctx#uhctx.p_MaxAge_In_Number_Data, P_MaxAge_In_Number
        ),
        set_var_bin(
            P_DataHeader_In_VarChar2, Ctx#uhctx.p_DataHeader_In_VarChar2_Data,
            Ctx#uhctx.p_DataHeader_In_VarChar2_Var
        ),
        ok = dpi:data_setInt64(
            Ctx#uhctx.p_RecCount_In_Number_Data, P_RecCount_In_Number
        ),
        ok = dpi:data_setInt64(
            Ctx#uhctx.p_PreParseErrCount_In_Number_Data,
            P_PreParseErrCount_In_Number
        ),
        ok = dpi:data_setInt64(
            Ctx#uhctx.p_ErrCount_In_Number_Data, P_ErrCount_In_Number
        ),
         set_var_bin(
            P_DateFc_In_Varchar2, Ctx#uhctx.p_DateFc_In_Varchar2_Data,
            Ctx#uhctx.p_DateFc_In_Varchar2_Var
        ),
        set_var_bin(
            P_DateLc_In_Varchar2, Ctx#uhctx.p_DateLc_In_Varchar2_Data,
            Ctx#uhctx.p_DateLc_In_Varchar2_Var
        ), 
    ok
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#uhctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_update_header_execute() ->
    dpi:safe(fun ?MODULE:sp_update_header_execute_internal/1,[get(uhctx)]).
sp_update_header_execute_internal(Ctx) ->
    try
        dpi:stmt_execute(Ctx#uhctx.stmt, []),
        #{    
            p_ReturnStatus_Out_Number => 
                dpi:data_get(Ctx#uhctx.p_ReturnStatus_Out_Number_Data),
            p_ErrorDesc_Out_Varchar2 => 
                dpi:data_get(Ctx#uhctx.p_ErrorDesc_Out_Varchar2_Data),
            p_ErrorCode_Out_Number => 
                dpi:data_get(Ctx#uhctx.p_ErrorCode_Out_Number_Data)
        }
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#uhctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_update_header_cleanup() ->
    dpi:safe(fun ?MODULE:sp_update_header_cleanup_internal/1,[get(uhctx)]).
sp_update_header_cleanup_internal(Ctx) ->
    try
        ok = dpi:stmt_release(Ctx#uhctx.stmt),
        ok = dpi:var_release(Ctx#uhctx.p_BiHId_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#uhctx.p_BiHId_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#uhctx.p_MaxAge_In_Number_Var),
        ok = dpi:data_release(Ctx#uhctx.p_MaxAge_In_Number_Data),
        ok = dpi:var_release(Ctx#uhctx.p_DataHeader_In_VarChar2_Var),
        ok = dpi:data_release(Ctx#uhctx.p_DataHeader_In_VarChar2_Data),
        ok = dpi:var_release(Ctx#uhctx.p_RecCount_In_Number_Var),
        ok = dpi:data_release(Ctx#uhctx.p_RecCount_In_Number_Data),
        ok = dpi:var_release(Ctx#uhctx.p_PreParseErrCount_In_Number_Var),
        ok = dpi:data_release(Ctx#uhctx.p_PreParseErrCount_In_Number_Data),
        ok = dpi:var_release(Ctx#uhctx.p_ErrCount_In_Number_Var),
        ok = dpi:data_release(Ctx#uhctx.p_ErrCount_In_Number_Data),
        ok = dpi:var_release(Ctx#uhctx.p_DateFc_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#uhctx.p_DateFc_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#uhctx.p_DateLc_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#uhctx.p_DateLc_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#uhctx.p_ErrorCode_Out_Number_Var),
        ok = dpi:data_release(Ctx#uhctx.p_ErrorCode_Out_Number_Data),
        ok = dpi:var_release(Ctx#uhctx.p_ErrorDesc_Out_Varchar2_Var),
        ok = dpi:data_release(Ctx#uhctx.p_ErrorDesc_Out_Varchar2_Data),
        ok = dpi:var_release(Ctx#uhctx.p_ReturnStatus_Out_Number_Var),
        ok = dpi:data_release(Ctx#uhctx.p_ReturnStatus_Out_Number_Data)
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#uhctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

%------------------------------------------------------------------------------
%   sp_insert_header
%------------------------------------------------------------------------------

-define(SP_INSERT_HEADER,
    <<
	    "begin PKG_COMMON_MAPPING.SP_INSERT_HEADER ("
	    " :p_BIH_ID_InOut_VarChar2,"
	    " :p_BIH_DEMO_In_Number,"
	    " :p_BIH_FILESEQ_In_Number,"
	    " :p_BIH_FILENAME_In_Varchar2,"
	    " :p_BIH_FILEDATE_In_Varchar2,"
	    " :p_BIH_MAPID_In_Varchar2,"
	    " :p_AppName_In_Varchar2,"
	    " :p_AppVer_In_Varchar2,"
	    " :p_Thread_In_Varchar2,"
	    " :p_JobId_In_Number,"
	    " :p_HostName_In_Varchar2,"
	    " :p_Status_In_Varchar,"
	    " :p_ErrorCode_Out_Number,"
	    " :p_ErrorDesc_Out_Varchar2,"
	    " :p_ReturnStatus_Out_Number"
	    "); end;"
    >>
).

sp_insert_header(Map) ->
    case dpi:safe(fun ?MODULE:sp_insert_header_internal/1, [Map]) of
        {error, _} = Error -> Error;
        Ihctx -> put(ihctx, Ihctx)
    end.
sp_insert_header_internal(#{conn := Conn, context := Context}) ->
    try
        Stmt = dpi:conn_prepareStmt(Conn, false, ?SP_INSERT_HEADER, <<"">>),
        {P_BIH_DEMO_In_Number_Var, P_BIH_DEMO_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_BIH_DEMO_In_Number">>),
        {P_BIH_FILESEQ_In_Number_Var, P_BIH_FILESEQ_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_BIH_FILESEQ_In_Number">>),
        {P_BIH_FILENAME_In_Varchar2_Var, P_BIH_FILENAME_In_Varchar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_BIH_FILENAME_In_Varchar2">>),
        {P_BIH_FILEDATE_In_Varchar2_Var, P_BIH_FILEDATE_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_BIH_FILEDATE_In_Varchar2">>),
        {P_BIH_MAPID_In_Varchar2_Var, P_BIH_MAPID_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_BIH_MAPID_In_Varchar2">>),
        {P_AppName_In_Varchar2_Var, P_AppName_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_AppName_In_Varchar2">>),
        {P_AppVer_In_Varchar2_Var, P_AppVer_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_AppVer_In_Varchar2">>),
        {P_Thread_In_Varchar2_Var, P_Thread_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_Thread_In_Varchar2">>),
        {P_JobId_In_Number_Var, P_JobId_In_Number_Data} = 
            make_and_bind_int_var(Conn, Stmt, <<"p_JobId_In_Number">>),
        {P_HostName_In_Varchar2_Var, P_HostName_In_Varchar2_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_HostName_In_Varchar2">>),
        {P_Status_In_Varchar_Var, P_Status_In_Varchar_Data} = 
            make_and_bind_str_var(Conn, Stmt, <<"p_Status_In_Varchar">>),

        {P_BIH_ID_InOut_VarChar2_Var, P_BIH_ID_InOut_VarChar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_BIH_ID_InOut_VarChar2">>),
        {P_ErrorCode_Out_Number_Var, P_ErrorCode_Out_Number_Data} =
            make_and_bind_int_var(Conn, Stmt, <<"p_ErrorCode_Out_Number">>),
        {P_ErrorDesc_Out_Varchar2_Var, P_ErrorDesc_Out_Varchar2_Data} =
            make_and_bind_str_var(Conn, Stmt, <<"p_ErrorDesc_Out_Varchar2">>),
        {P_ReturnStatus_Out_Number_Var, P_ReturnStatus_Out_Number_Data} =
            make_and_bind_int_var(Conn, Stmt, <<"p_ReturnStatus_Out_Number">>),
            Ihctx = 
            #ihctx{
                stmt = Stmt, context = Context,

                p_BIH_ID_InOut_VarChar2_Var = P_BIH_ID_InOut_VarChar2_Var,
                p_BIH_ID_InOut_VarChar2_Data = P_BIH_ID_InOut_VarChar2_Data,
                 p_BIH_DEMO_In_Number_Var= P_BIH_DEMO_In_Number_Var,
                p_BIH_DEMO_In_Number_Data = P_BIH_DEMO_In_Number_Data,
                p_BIH_FILESEQ_In_Number_Var = P_BIH_FILESEQ_In_Number_Var,
                p_BIH_FILESEQ_In_Number_Data = P_BIH_FILESEQ_In_Number_Data,
                p_BIH_FILENAME_In_Varchar2_Var = P_BIH_FILENAME_In_Varchar2_Var,
                p_BIH_FILENAME_In_Varchar2_Data =
                    P_BIH_FILENAME_In_Varchar2_Data,
                p_BIH_FILEDATE_In_Varchar2_Var = P_BIH_FILEDATE_In_Varchar2_Var,
                p_BIH_FILEDATE_In_Varchar2_Data =
                    P_BIH_FILEDATE_In_Varchar2_Data,
                p_BIH_MAPID_In_Varchar2_Var = P_BIH_MAPID_In_Varchar2_Var,
                p_BIH_MAPID_In_Varchar2_Data = P_BIH_MAPID_In_Varchar2_Data,
                p_AppName_In_Varchar2_Var = P_AppName_In_Varchar2_Var,
                p_AppName_In_Varchar2_Data = P_AppName_In_Varchar2_Data,
                p_AppVer_In_Varchar2_Var = P_AppVer_In_Varchar2_Var,
                p_AppVer_In_Varchar2_Data = P_AppVer_In_Varchar2_Data,
                p_Thread_In_Varchar2_Var = P_Thread_In_Varchar2_Var,
                p_Thread_In_Varchar2_Data = P_Thread_In_Varchar2_Data,
                p_JobId_In_Number_Var = P_JobId_In_Number_Var,
                p_JobId_In_Number_Data = P_JobId_In_Number_Data,
                p_HostName_In_Varchar2_Var = P_HostName_In_Varchar2_Var,
                p_HostName_In_Varchar2_Data = P_HostName_In_Varchar2_Data,
                p_Status_In_Varchar_Var = P_Status_In_Varchar_Var,
                p_Status_In_Varchar_Data = P_Status_In_Varchar_Data,
                p_ErrorCode_Out_Number_Var = P_ErrorCode_Out_Number_Var,
                p_ErrorCode_Out_Number_Data = P_ErrorCode_Out_Number_Data,
                p_ErrorDesc_Out_Varchar2_Var = P_ErrorDesc_Out_Varchar2_Var,
                p_ErrorDesc_Out_Varchar2_Data = P_ErrorDesc_Out_Varchar2_Data,
                p_ReturnStatus_Out_Number_Var = P_ReturnStatus_Out_Number_Var,
                p_ReturnStatus_Out_Number_Data = P_ReturnStatus_Out_Number_Data
            },
        Ihctx
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_insert_header_values(
    P_BIH_ID_InOut_VarChar2,
    P_BIH_DEMO_In_Number,
    P_BIH_FILESEQ_In_Number,
    P_BIH_FILENAME_In_Varchar2,
    P_BIH_FILEDATE_In_Varchar2,
    P_BIH_MAPID_In_Varchar2,
    P_AppName_In_Varchar2,
    P_AppVer_In_Varchar2,
    P_Thread_In_Varchar2,
    P_JobId_In_Number,
    P_HostName_In_Varchar2,
    P_Status_In_Varchar
) ->
    dpi:safe(fun ?MODULE:sp_insert_header_values_internal/13,[
        get(ihctx),
        P_BIH_ID_InOut_VarChar2,
        P_BIH_DEMO_In_Number,
        P_BIH_FILESEQ_In_Number,
        P_BIH_FILENAME_In_Varchar2,
        P_BIH_FILEDATE_In_Varchar2,
        P_BIH_MAPID_In_Varchar2,
        P_AppName_In_Varchar2,
        P_AppVer_In_Varchar2,
        P_Thread_In_Varchar2,
        P_JobId_In_Number,
        P_HostName_In_Varchar2,
        P_Status_In_Varchar]
    ).
sp_insert_header_values_internal(
    Ctx,
    P_BIH_ID_InOut_VarChar2,
    P_BIH_DEMO_In_Number,
    P_BIH_FILESEQ_In_Number,
    P_BIH_FILENAME_In_Varchar2,
    P_BIH_FILEDATE_In_Varchar2,
    P_BIH_MAPID_In_Varchar2,
    P_AppName_In_Varchar2,
    P_AppVer_In_Varchar2,
    P_Thread_In_Varchar2,
    P_JobId_In_Number,
    P_HostName_In_Varchar2,
    P_Status_In_Varchar
)
    when
        ?GUARD_NULL_STR(P_BIH_ID_InOut_VarChar2) andalso
        ?GUARD_NULL_INT(P_BIH_DEMO_In_Number) andalso
        ?GUARD_NULL_INT(P_BIH_FILESEQ_In_Number) andalso
        ?GUARD_NULL_STR(P_BIH_FILENAME_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_BIH_FILEDATE_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_BIH_MAPID_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_AppName_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_AppVer_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_Thread_In_Varchar2) andalso
        ?GUARD_NULL_INT(P_JobId_In_Number) andalso
        ?GUARD_NULL_STR(P_HostName_In_Varchar2) andalso
        ?GUARD_NULL_STR(P_Status_In_Varchar)
    -> 
    try
        set_var_bin(
            P_BIH_ID_InOut_VarChar2, Ctx#ihctx.p_BIH_ID_InOut_VarChar2_Data,
            Ctx#ihctx.p_BIH_ID_InOut_VarChar2_Var
        ),
        set_var_bin(
            P_BIH_FILENAME_In_Varchar2,
            Ctx#ihctx.p_BIH_FILENAME_In_Varchar2_Data,
            Ctx#ihctx.p_BIH_FILENAME_In_Varchar2_Var
        ),

        ok = dpi:data_setInt64(
            Ctx#ihctx.p_BIH_DEMO_In_Number_Data, P_BIH_DEMO_In_Number
        ),
        ok = dpi:data_setInt64(
            Ctx#ihctx.p_BIH_FILESEQ_In_Number_Data, P_BIH_FILESEQ_In_Number
        ),

        set_var_bin(
            P_BIH_FILEDATE_In_Varchar2,
            Ctx#ihctx.p_BIH_FILEDATE_In_Varchar2_Data,
            Ctx#ihctx.p_BIH_FILEDATE_In_Varchar2_Var
        ),
        set_var_bin(
            P_BIH_MAPID_In_Varchar2, Ctx#ihctx.p_BIH_MAPID_In_Varchar2_Data,
            Ctx#ihctx.p_BIH_MAPID_In_Varchar2_Var
        ),
        set_var_bin(
            P_AppName_In_Varchar2, Ctx#ihctx.p_AppName_In_Varchar2_Data,
            Ctx#ihctx.p_AppName_In_Varchar2_Var
        ),
        set_var_bin(
            P_AppVer_In_Varchar2, Ctx#ihctx.p_AppVer_In_Varchar2_Data,
            Ctx#ihctx.p_AppVer_In_Varchar2_Var
        ),
        set_var_bin(
            P_Thread_In_Varchar2, Ctx#ihctx.p_Thread_In_Varchar2_Data,
            Ctx#ihctx.p_Thread_In_Varchar2_Var
        ),
        ok = dpi:data_setInt64(
            Ctx#ihctx.p_JobId_In_Number_Data, P_JobId_In_Number
        ),
        set_var_bin(
            P_HostName_In_Varchar2, Ctx#ihctx.p_HostName_In_Varchar2_Data,
            Ctx#ihctx.p_HostName_In_Varchar2_Var
        ),
        set_var_bin(
            P_Status_In_Varchar, Ctx#ihctx.p_Status_In_Varchar_Data,
            Ctx#ihctx.p_Status_In_Varchar_Var
        )
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#ihctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.
            


sp_insert_header_execute() ->
    dpi:safe(fun ?MODULE:sp_insert_header_execute_internal/1,[get(ihctx)]).
sp_insert_header_execute_internal(Ctx) -> 
    try
        dpi:data_setIsNull(Ctx#ihctx.p_ErrorDesc_Out_Varchar2_Data, false),
        dpi:stmt_execute(Ctx#ihctx.stmt, []),
        #{
            p_BIH_ID_InOut_VarChar2 => 
                dpi:data_get(Ctx#ihctx.p_BIH_ID_InOut_VarChar2_Data),
            p_ErrorCode_Out_Number => 
                dpi:data_get(Ctx#ihctx.p_ErrorCode_Out_Number_Data),
            p_ErrorDesc_Out_Varchar2 => 
                dpi:data_get(Ctx#ihctx.p_ErrorDesc_Out_Varchar2_Data),
            p_ReturnStatus_Out_Number => 
                dpi:data_get(Ctx#ihctx.p_ReturnStatus_Out_Number_Data)
        }
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#ihctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

sp_insert_header_cleanup() ->
    dpi:safe(fun ?MODULE:sp_insert_header_cleanup_internal/1,[get(ihctx)]).
sp_insert_header_cleanup_internal(Ctx) -> 
    try
        ok = dpi:stmt_release(Ctx#ihctx.stmt),
        ok = dpi:var_release(Ctx#ihctx.p_BIH_ID_InOut_VarChar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_BIH_ID_InOut_VarChar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_BIH_DEMO_In_Number_Var),
        ok = dpi:data_release(Ctx#ihctx.p_BIH_DEMO_In_Number_Data),
        ok = dpi:var_release(Ctx#ihctx.p_BIH_FILESEQ_In_Number_Var),
        ok = dpi:data_release(Ctx#ihctx.p_BIH_FILESEQ_In_Number_Data),
        ok = dpi:var_release(Ctx#ihctx.p_BIH_FILENAME_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_BIH_FILENAME_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_BIH_FILEDATE_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_BIH_FILEDATE_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_BIH_MAPID_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_BIH_MAPID_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_AppName_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_AppName_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_AppVer_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_AppVer_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_Thread_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_Thread_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_JobId_In_Number_Var),
        ok = dpi:data_release(Ctx#ihctx.p_JobId_In_Number_Data),
        ok = dpi:var_release(Ctx#ihctx.p_HostName_In_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_HostName_In_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_Status_In_Varchar_Var),
        ok = dpi:data_release(Ctx#ihctx.p_Status_In_Varchar_Data),
        ok = dpi:var_release(Ctx#ihctx.p_ErrorCode_Out_Number_Var),
        ok = dpi:data_release(Ctx#ihctx.p_ErrorCode_Out_Number_Data),
        ok = dpi:var_release(Ctx#ihctx.p_ErrorDesc_Out_Varchar2_Var),
        ok = dpi:data_release(Ctx#ihctx.p_ErrorDesc_Out_Varchar2_Data),
        ok = dpi:var_release(Ctx#ihctx.p_ReturnStatus_Out_Number_Var),
        ok = dpi:data_release(Ctx#ihctx.p_ReturnStatus_Out_Number_Data)
    catch
        Class:Exception -> 
            DpiError = dpi:context_getError(Ctx#ihctx.context),
            {error, DpiError#{class => Class, exception => Exception}}
    end.

%------------------------------------------------------------------------------
%   private
%------------------------------------------------------------------------------

set_var_bin(null, Data, _Var)  -> dpi:data_setIsNull(Data, true);
set_var_bin(Value, _Data, Var) -> dpi:var_setFromBytes(Var, 0, Value).

%% makes an int variable and binds it to the statement by the provided name
make_and_bind_int_var(Conn, Stmt, Name)->
    #{var := Var, data := [Data]} =
        dpi:conn_newVar(
            Conn, 'DPI_ORACLE_TYPE_NUMBER', 'DPI_NATIVE_TYPE_INT64', 1, 0,
            false, false, undefined
        ),
    ok = dpi:stmt_bindByName(Stmt, Name, Var),
    {Var, Data}.

%% makes a string variable and binds it to the statement by the provided name
make_and_bind_str_var(Conn, Stmt, Name)->
    #{var := Var, data := [Data]} =
        dpi:conn_newVar(
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 4000,
            false, false, undefined
        ),
    ok = dpi:stmt_bindByName(Stmt, Name, Var),
    {Var, Data}.

%% turns a list of ok atoms to one ok atom to help with list comprehension
oks(ok) -> ok;
oks([ok]) -> ok;
oks([ok | T]) -> oks(T);
oks(_) -> nok.

sp_insert_csv_var_realloc(
    Ctx, P_RecordNr_In_ArrRecNr, P_RecordData_In_ArrRecData
) ->
    % if array is longer than the last
    NewArrayLen = max(Ctx#csvctx.array_size, length(P_RecordNr_In_ArrRecNr)),

    % length of the biggest binary in the list
    NewMaxBinSz = lists:max(
        [Ctx#csvctx.binary_size
         | [byte_size(Bin) || Bin <- P_RecordData_In_ArrRecData]]
    ),
    
    ok = dpi:data_setInt64(Ctx#csvctx.p_BatchSize_In_Integer_Data, NewArrayLen),

    case {
            NewArrayLen > Ctx#csvctx.array_size orelse
                NewMaxBinSz > Ctx#csvctx.binary_size,
            Ctx#csvctx.p_RecordNr_In_ArrRecNr_Var
    } of
        % subsequent no re-alloc
        {false, _} -> Ctx;
        % first time
        {_, undefined} ->
            make_In_ArrRecNr_ArrRecData(Ctx, NewArrayLen, NewMaxBinSz);

        % new arrays are bigger than the last
        {true, VarRecordNr} ->
            #csvctx{
                p_RecordNr_In_ArrRecNr_Data = DataRecordNr,
                p_RecordData_In_ArrRecData_Var = VarRecordData,
                p_RecordData_In_ArrRecData_Data = DataRecordData
            } = Ctx,

            % release the old
            ok = dpi:var_release(VarRecordNr),
            ok = oks([dpi:data_release(X) || X <- DataRecordNr]),
            ok = dpi:var_release(VarRecordData),
            ok = oks([dpi:data_release(X) || X <- DataRecordData]),

            % make new
            make_In_ArrRecNr_ArrRecData(Ctx, NewArrayLen, NewMaxBinSz)
    end.

make_In_ArrRecNr_ArrRecData(Ctx, NewArrayLen, NewMaxBinSz) ->
    #{
        var := P_RecordNr_In_ArrRecNr_Var,
        data := P_RecordNr_In_ArrRecNr_Data
    } = dpi:conn_newVar(
        Ctx#csvctx.conn, 'DPI_ORACLE_TYPE_NUMBER', 'DPI_NATIVE_TYPE_INT64',
        NewArrayLen, 0, false, true, undefined
    ),

    #{
        var := P_RecordData_In_ArrRecData_Var,
        data := P_RecordData_In_ArrRecData_Data
    } = dpi:conn_newVar(
        Ctx#csvctx.conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES',
        NewArrayLen, NewMaxBinSz, false, true, undefined
    ),

    Ctx#csvctx{
        p_RecordNr_In_ArrRecNr_Var = P_RecordNr_In_ArrRecNr_Var,
        p_RecordNr_In_ArrRecNr_Data = P_RecordNr_In_ArrRecNr_Data,
        p_RecordData_In_ArrRecData_Var = P_RecordData_In_ArrRecData_Var,
        p_RecordData_In_ArrRecData_Data = P_RecordData_In_ArrRecData_Data,
        array_size = NewArrayLen, binary_size = NewMaxBinSz
    }.

getError(Ctx) ->
    dpi:safe(fun dpi:context_getError/1, [Ctx]).
