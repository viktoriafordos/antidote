-module(antidote_db_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([get_op/2,
    get_snap/2,
    put_op/3,
    put_snap/3,
    close_and_destroy/1]).

-export([init/1,
    handle_command/3,
    handle_coverage/4,
    handle_exit/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_command/3,
    handle_handoff_data/2,
    encode_handoff_item/2,
    is_empty/1,
    terminate/2,
    delete/1]).

-record(state, {
    partition :: partition_id(),
    ops_db :: eleveldb:db_ref(),
    snapshots_db :: eleveldb:db_ref()}).

%% TODO return correctly in an error case
init([Partition]) ->
    %% Open Ops DB
    OpsDB = case eleveldb:open(get_db_name(ops_db, Partition), [{create_if_missing, true}]) of
                {ok, OpsDB1} -> OpsDB1;
                {error, Error} -> {error, Error}
            end,
    %% Open Snapshots DB
    SnapshotsDB = case eleveldb:open(get_db_name(snapshots_db, Partition), [{create_if_missing, true}]) of
                      {ok, SnapshotsDB1} -> SnapshotsDB1;
                      {error, Error1} -> {error, Error1}
                  end,
    %% Check of there where any errors
    case element(1, OpsDB) == error or element(1, SnapshotsDB) == error of
        true -> {stop, {error, OpsDB, SnapshotsDB}, undefined};
        false -> {ok, #state{partition = Partition, ops_db = OpsDB, snapshots_db = SnapshotsDB}}
    end.

handle_command({get_op, Key}, _Sender, State = #state{ops_db = Ops}) ->
    {reply, internal_get(Ops, Key), State};

handle_command({get_snap, Key}, _Sender, State = #state{snapshots_db = Snaps}) ->
    {reply, internal_get(Snaps, Key), State};

handle_command({put_op, Key, Op}, _Sender, State = #state{ops_db = Ops}) ->
    {reply, internal_put(Ops, Key, Op), State};

handle_command({put_snap, Key, Snap}, _Sender, State = #state{snapshots_db = Snaps}) ->
    {reply, internal_put(Snaps, Key, Snap), State};

handle_command({close_and_destroy}, _Sender, State = #state{partition = Partition, ops_db = Ops, snapshots_db = Snaps}) ->
    eleveldb:close(Ops),
    eleveldb:close(Snaps),
    eleveldb:destroy(get_db_name(ops_db, Partition), []),
    eleveldb:destroy(get_db_name(snapshots_db, Partition), []),
    {reply, ok, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_coverage(_Request, _, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_, _Reason, State) ->
    {noreply, State}.

handoff_starting(_, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_, State) ->
    {ok, State}.

handle_handoff_command(_Request, _Sender, State) ->
    {stop, not_implemented, State}.

handle_handoff_data(_, State) ->
    {stop, not_implemented, State}.

encode_handoff_item(_Key, _Value) ->
    erlang:error(not_implemented).

is_empty(State = #state{ops_db = Ops, snapshots_db = Snaps}) ->
    {eleveldb:is_empty(Ops) and eleveldb:is_empty(Snaps), State}.

terminate(_Reason, _State = #state{ops_db = Ops, snapshots_db = Snaps}) ->
    eleveldb:close(Ops),
    eleveldb:close(Snaps),
    ok.

delete(State) ->
    {ok, State}.

%% @doc given the base an the partition number, returns the name of the DB
-spec get_db_name(atom(), non_neg_integer()) -> atom().
get_db_name(Base, Partition) ->
    atom_to_list(Base) ++ "-" ++ integer_to_list(Partition).

get_op(Node, Key) ->
    riak_core_vnode_master:sync_command(Node,
        {get_op, Key},
        ?ANTIDOTE_DB_MASTER,
        infinity).

get_snap(Node, Key) ->
    riak_core_vnode_master:sync_command(Node,
        {get_snap, Key},
        ?ANTIDOTE_DB_MASTER,
        infinity).

put_op(Node, Key, Value) ->
    riak_core_vnode_master:sync_command(Node,
        {put_op, Key, Value},
        ?ANTIDOTE_DB_MASTER,
        infinity).

put_snap(Node, Key, Value) ->
    riak_core_vnode_master:sync_command(Node,
        {put_snap, Key, Value},
        ?ANTIDOTE_DB_MASTER,
        infinity).

close_and_destroy(Node) ->
    riak_core_vnode_master:sync_command(Node,
        {close_and_destroy},
        ?ANTIDOTE_DB_MASTER,
        infinity).

%% @doc returns the value of Key, in the eleveldb Ref
-spec internal_get(eleveldb:db_ref(), atom() | binary()) -> term() | not_found.
internal_get(Ref, Key) ->
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
-spec internal_put(eleveldb:db_ref(), atom() | binary(), any()) -> ok | {error, any()}.
internal_put(Ref, Key, Value) ->
    AKey = case is_binary(Key) of
               true -> Key;
               false -> atom_to_binary(Key, utf8)
           end,
    ATerm = case is_binary(Value) of
                true -> Value;
                false -> term_to_binary(Value)
            end,
    eleveldb:put(Ref, AKey, ATerm, []).
