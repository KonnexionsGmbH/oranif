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

            % generate and append the function stubs to forms
            % move {eof, _} to the end
            {value, {eof, LastLine}, Forms2} = lists:keytake(eof, 1, Forms1),
            Stubs = nif_stubs(NifDefns, LastLine),

            % each NifDefn adds four lines of code :
            % 	Fn spec, Fn defn, fn_nif spec and Fn_nif defn
            NewLastLine = LastLine + 4 * length(NifDefns),
            Forms3 = Forms2 ++ Stubs ++ [{eof, NewLastLine}],
            ?N("<=== function stubs inserted~n"),
            parse_transform(Forms3, Options)
    end.

% converting
%	[{"FN", "FN_nif", ArgsList}, ...]
% to
%	[{'FN', 'FN_nif', Arity, ArgsList}, ...]
process_nif(NifDefns) ->
    [{Fun, length(Args),
      Args}
     || {Fun, Args} <- NifDefns].

% generating
%  	code : -export([FN/Arity, FN_nif/Arity, ...]).
%  	form : {attribute, Line, export, [{FN, Arity}, {FN_nif, Arity}, ...]}
% from
% 		[{FN, FN_nif, Arity, Args}, ...]
nif_exports(NifDefns, Line) ->
    nif_exports(NifDefns, Line, []).

nif_exports([], Line, NewNifDefns) ->
    {attribute, Line, export, NewNifDefns};
nif_exports([{Fun, Arity, _Args}
         | NifDefns],
        Line, NewNifDefns) ->
    nif_exports(NifDefns, Line,
        [{Fun, Arity}
         | NewNifDefns]).

nif_stubs([], _Line) -> [];
nif_stubs([{Fun, Arity, Args} | Rest], Line) ->
    FunParams = params(Args, Line + 1),
    FunGuards = guards(Args, FunParams, Line + 1),

    ?N("constructing ~p\n", [Fun]),

    [


    % -spec(FunNif(Arg, ...) -> term()).
    {attribute, Line + 2, spec,
        {{Fun, Arity},
            [{type, Line + 2, 'fun',
                [
                    {type, Line + 2, product, specparams(Args, Line + 2)},
                    {type, Line + 2, term, []}
                ]
            }]
        }
    },

    % FunNif(Arg1, ...) when is_X(Arg1), ... -> ?NIF_NOT_LOADED.
    {function, Line + 3, Fun, Arity,
        [{clause, Line + 3, FunParams,
            case FunGuards of
                [] -> [];
                _ -> [FunGuards]
            end,
            [{call, Line + 3, {atom, Line + 3, exit},
                [{tuple, Line + 3,
                    [
                        {atom, Line + 3, nif_library_not_loaded},
                        {atom, Line + 3, dpi},
                        {atom, Line + 3, Fun}
                    ]
                }]
            }]
        }]
    }
     | nif_stubs(Rest, Line + 4)].

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
