%
% Given a module defined as
%
% 	-module(moduleX).
%	-compile({parse_transform, dpi_transform}).
% 	-nif([{funtion, function_nif, [integer, referance, ...]}, ...]).
%   ...
% 	-nif([...]).
%
% this parse_transform replaces each -nif atribute with
%
% 	-export([funtion/Arity, function_nif/Arity, ...]).
%	  ...
% 	-spec(function(integer(), ...) -> term()).
%	  function(Arg1, ...) when is_integer(Arg1), ... ->
% 		  gen_server:call(
%         	{dpi, get(dpi_slave)},
%         	{function_nif, [Arg1, ...]}
%     	).
%
%	  -spec(function_nif(integer(), ...) -> term()).
%	  function_nif(Arg1, ...) when is_integer(Arg1), ... ->
% 		  exit({nif_library_not_loaded, dpi, function_nif}).
%

-module(dpi_transform).

-export([parse_transform/2]).

-define(L(_F, _A), io:format("[~p:~p] "_F, [?MODULE, ?LINE | _A])).
-define(L(_S), ?L(_S, [])).
-define(N(_F, _A), ?L("[~p] "_F, [get(nif_class) | _A])).
-define(N(_S), ?N(_S, [])).

parse_transform(Forms, Options) ->
    % Look for nif definition : {attribute,_,nifs,[...]}
    case lists:keyfind(nifs, 3, Forms) of
        false ->
            ?L("no more -nifs([...]) found~n"),
            ?L("transform complete~n"),
            Forms;
        {attribute, Line, nifs, {NifClass, NifDefns0}} ->
            put(nif_class, NifClass),
            ?N(
                "===> processing -nif attribute, found ~p interfaces...~n",
                [length(NifDefns0)]
            ),
            NifDefns = process_nif(NifDefns0),

            % replace the -nif(...) attribute with corresponding exports
            % reuse the same line number
            Exports = nif_exports(NifDefns, Line),
            Forms1 = lists:keyreplace(nifs, 3, Forms, Exports),
            ?N("NIF exports inserted~n"),

            Forms2 = insert_nif_fa(Forms1, NifDefns),
            ?N("fa/2 clauses for this NIF are added~n"),

            % generate and append the function stubs to forms
            % move {eof, _} to the end
            {value, {eof, LastLine}, Forms3} = lists:keytake(eof, 1, Forms2),
            Stubs = nif_stubs(NifDefns, LastLine),

            % each NifDefn adds four lines of code :
            % 	Fn spec, Fn defn, fn_nif spec and Fn_nif defn
            NewLastLine = LastLine + 4 * length(NifDefns),
            Forms4 = Forms3 ++ Stubs ++ [{eof, NewLastLine}],
            ?N("<=== function stubs inserted~n"),
            parse_transform(Forms4, Options)
    end.

% converting
%	[{"FN", "FN_nif", ArgsList}, ...]
% to
%	[{'FN', 'FN_nif', Arity, ArgsList}, ...]
process_nif(NifDefns) ->
    [{list_to_atom(Fun), list_to_atom(FunNif), length(Args),
      Args, Mode}
     || {Fun, FunNif, Args, Mode} <- NifDefns].

% inserts the fa generated from -nif to a forms list
% if no existing fa is present throws exception!
insert_nif_fa(Form, NifDefns) ->
    case lists:keyfind(fa, 3, Form) of
        false ->
            error({abort, no_fa2_stub_found});
        % locates the existing fa
        {function, LineExisting, fa, 2, Clauses} ->
            NewClauses = insert_fa(Clauses, NifDefns),
            % replace the clauses in the new form
            lists:keyreplace(
                fa, 3, Form,
                {function, LineExisting, fa, 2, NewClauses}
            )
    end.

% additional NifDefns are inserted into existing fs clauses
insert_fa(Clauses, NifDefns) ->
    [{clause, Line, LastParams, LastExtra, LastBody} | Rest] =
        lists:reverse(Clauses),

    %% build the list of new clauses
    {LastLine, NewClauses} = lists:foldl(
        %
        % piece together one fa clause (resulting this erlang code)
        %
        %  fa(NameNif, [Args1, Arg2, ...]) -> NameNif(Arg1, Arg2, ...);
        %
        fun ({_Name, NameNif, _Arity, Args, _Mode}, {L, NC}) ->
            Params = params(Args, Line),
            {L + 1,
            [{clause, L,
                [{atom, L, NameNif},
                    cons(Params, L)],
                [],
                [{call, L, {atom, L, NameNif}, Params}]}
            | NC]}
        end,
        {Line, []}, NifDefns
    ),

    % insert the new clauses before the last error clause
    lists:reverse([
        {clause, LastLine, LastParams, LastExtra, LastBody}
        | NewClauses ++ Rest
    ]).

% generating
%  	code : -export([FN/Arity, FN_nif/Arity, ...]).
%  	form : {attribute, Line, export, [{FN, Arity}, {FN_nif, Arity}, ...]}
% from
% 		[{FN, FN_nif, Arity, Args}, ...]
nif_exports(NifDefns, Line) ->
    nif_exports(NifDefns, Line, []).

nif_exports([], Line, NewNifDefns) ->
    {attribute, Line, export, NewNifDefns};
nif_exports([{Fun, FunNif, Arity, _Args, _Mode}
         | NifDefns],
        Line, NewNifDefns) ->
    nif_exports(NifDefns, Line,
        [{Fun, Arity}, {FunNif, Arity}
         | NewNifDefns]).

nif_stubs([], _Line) -> [];
nif_stubs([{Fun, FunNif, Arity, Args, Mode} | Rest], Line) ->
    FunParams = params(Args, Line + 1),
    FunGuards = guards(Args, FunParams, Line + 1),
    FunNifParams = params(Args, Line + 3),
    FunNifGuards = guards(Args, FunNifParams, Line + 3),
    ?N("constructing ~p\n", [FunNif]),

    [
    % -spec(Fun(Arg, ...) -> term()).
    {attribute, Line, spec,
        {{Fun, Arity},
            [{type, Line, 'fun',
                [
                    {type, Line, product, specparams(Args, Line)},
                    {type, Line, term, []}
                ]
            }]
        }
    },

    % -spec(FunNif(Arg, ...) -> term()).
    {attribute, Line + 2, spec,
        {{FunNif, Arity},
            [{type, Line + 2, 'fun',
                [
                    {type, Line + 2, product, specparams(Args, Line + 2)},
                    {type, Line + 2, term, []}
                ]
            }]
        }
    },

    % FunNif(Arg1, ...) when is_X(Arg1), ... -> ?NIF_NOT_LOADED.
    {function, Line + 3, FunNif, Arity,
        [{clause, Line + 3, FunNifParams,
            case FunNifGuards of
                [] -> [];
                _ -> [FunNifGuards]
            end,
            [{call, Line + 3, {atom, Line + 3, exit},
                [{tuple, Line + 3,
                    [
                        {atom, Line + 3, nif_library_not_loaded},
                        {atom, Line + 3, dpi},
                        {atom, Line + 3, FunNif}
                    ]
                }]
            }]
        }]
    },

    {function, Line, Fun, Arity,
     [rpc_or_gen(Mode, Line, FunNif, FunParams, FunGuards)]}
     | nif_stubs(Rest, Line + 4)].

% Fun(Arg1, ...) ->
%     case gen_server:call(
%         {dpi, get(dpi_slave)},
%         {Fun_nif, [Arg1, ...], Mode}
%     ) of
%        {'EXIT', Error} -> error(Error);
%        Result -> Result
%     end
rpc_or_gen(Mode, Line, FunNif, FunParams, FunGuards)
    when Mode =:= create; Mode =:= delete
->
    L = Line + 1,
    {clause, L, FunParams,
        if length(FunGuards) > 0 -> [FunGuards]; true -> [] end,
        [{'case', L,
            {call, L,
                {remote, L, {atom, L, gen_server}, {atom, L, call}},
                [{tuple, L,
                    [{atom, L, dpi},
                        {call, L, {atom, L, get}, [{atom, L, dpi_slave}]}]
                },
                {tuple, L,
                    [{atom, L, FunNif}, cons(FunParams, L), {atom, L, Mode}]
                },
                {atom, L, infinity}]
            },
            [{clause, L, [{tuple,L,[{atom,L,'EXIT'},{var,L,'Error'}]}], [],
                [{call,L,{atom,L,error},[{var,L,'Error'}]}]},
            {clause, L, [{var, L,'Result'}], [], [{var, L,'Result'}]}]
        }]
    };
% Fun(Arg1, ...) ->
%     rpc_call(get(dpi_slave), dpi, fa, [FunNif, Arg1, ...])
rpc_or_gen(_, Line, FunNif, FunParams, FunGuards) ->
    L = Line + 1,
    {clause, L, FunParams,
        if length(FunGuards) > 0 -> [FunGuards]; true -> [] end,
        [{call, L, {atom, L, rpc_call},
            [{call, L, {atom, L, get}, [{atom, L, dpi_slave}]},
                {atom, L, dpi}, {atom, L, fa},
                {cons, L, {atom, L, FunNif},
                    {cons, Line, cons(FunParams, L), {nil, Line}}}]
        }]
    }.

specparams([], _L) -> [];
specparams([{T, A} | Types], L) ->
    [{type, L, union, [{type, L, T, any}, type(A, L)]}
     | specparams(Types, L)];
specparams([T | Types], L) ->
    [{type, L, T, []} | specparams(Types, L)].

params(Types, Line) ->
    [{var, Line,
        list_to_atom(if T =:= term -> "_";
            true -> ""
        end
            ++ "Arg" ++ integer_to_list(I))}
    || {I, T}  <- lists:zip(lists:seq(1, length(Types)), Types)].

guards([], [], _L) -> [];
% terms has no guards
guards([term | Types], [_ | Params], L) ->
    guards(Types, Params, L);
guards([{T, A} | Types], [P | Params], L) ->
    [{op, L, 'orelse', {call, L, {atom, L, guard(T)}, [P]},
      {op, L, '=:=', P, type(A, L)}}
     | guards(Types, Params, L)];
guards([T | Types], [P | Params], L) ->
    [{call, L, {atom, L, guard(T)}, [P]} | guards(Types,
        Params, L)].

cons([], Line) -> {nil, Line};
cons([Param | Params], Line) ->
    {cons, Line, Param, cons(Params, Line)}.

guard(pid) -> is_pid;
guard(map) -> is_map;
guard(atom) -> is_atom;
guard(port) -> is_port;
guard(list) -> is_list;
guard(float) -> is_float;
guard(binary) -> is_binary;
guard(integer) -> is_integer;
guard(reference) -> is_reference.

type(A, Line) when is_atom(A) -> {atom, Line, A};
type(A, Line) when is_integer(A) -> {integer, Line, A};
type(A, Line) when is_float(A) -> {float, Line, A};
type(A, Line) when is_list(A) ->
    case io_lib:printable_list(A) of
      true -> {string, Line, A};
      _ -> error({unsupported_type, A})
    end;
type(A, _Line) -> error({unsupported_type, A}).
