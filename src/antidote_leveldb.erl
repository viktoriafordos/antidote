-module(antidote_leveldb).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
    get_db_name/2,
    get/2,
    put/3,
    close_and_destroy/2,
    close/1]).

%% @doc given the base an the partition number, returns the name of the DB
-spec get_db_name(atom(), non_neg_integer()) -> atom().
get_db_name(Base, Partition) ->
    atom_to_list(Base) ++ "-" ++ integer_to_list(Partition).

close_and_destroy(Ref, Name) ->
    eleveldb:close(Ref),
    eleveldb:destroy(Name, []).

close(Ref) ->
    eleveldb:close(Ref).

%% @doc returns the value of Key, in the eleveldb Ref
-spec get(eleveldb:db_ref(), atom() | binary()) -> term() | not_found.
get(Ref, Key) ->
    AKey = case is_binary(Key) of
               true -> Key;
               false -> atom_to_binary(Key, utf8)
           end,
    case eleveldb:get(Ref, AKey, []) of
        {ok, Res} ->
            binary_to_term(Res);
        not_found ->
            not_found
    end.

%% @doc puts the Value associated to Key in eleveldb Ref
-spec put(eleveldb:db_ref(), atom() | binary(), any()) -> ok | {error, any()}.
put(Ref, Key, Value) ->
    AKey = case is_binary(Key) of
               true -> Key;
               false -> atom_to_binary(Key, utf8)
           end,
    ATerm = case is_binary(Value) of
                true -> Value;
                false -> term_to_binary(Value)
            end,
    eleveldb:put(Ref, AKey, ATerm, []).
