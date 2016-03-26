%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
%% Number of snapshots to keep after GC
-define(SNAPSHOT_MIN, 3).
%% Number of ops to keep before GC
-define(OPS_THRESHOLD, 50).
%% The first 3 elements in operations list are meta-data
%% First is the key
%% Second is a tuple {current op list size, max op list size}
%% Thrid is a counter that assigns each op 1 larger than the previous
%% Fourth is where the list of ops start
-define(FIRST_OP, 4).
%% If after the op GC there are only this many or less spaces
%% free in the op list then increase the list size
-define(RESIZE_THRESHOLD, 5).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
    check_tables_ready/0,
    read/6,
    store_ss/4,
    update/3,
    belongs_to_snapshot_op/3]).

%% Callbacks
-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).


-record(state, {
  partition :: partition_id()}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(antidote_db:antidote_db(), key(), type(), snapshot_time(), txid(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(AntidoteDB, Key, Type, SnapshotTime, TxId, _Partition) ->
    internal_read(AntidoteDB, Key, Type, SnapshotTime, TxId).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(antidote_db:antidote_db(), key(), clocksi_payload()) -> ok | {error, reason()}.
update(AntidoteDB, Key, DownstreamOp) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, AntidoteDB, Key, DownstreamOp},
                                        materializer_vnode_master).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(antidote_db:antidote_db(), key(), snapshot(), snapshot_time()) -> ok.
store_ss(AntidoteDB, Key, Snapshot, CommitTime) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:command(IndexNode, {store_ss, AntidoteDB, Key, Snapshot, CommitTime},
                                        materializer_vnode_master).

init([Partition]) ->
    {ok, #state{partition=Partition}}.

%% @doc The tables holding the updates and snapshots are shared with concurrent
%%      readers, allowing them to be non-blocking and concurrent.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).


check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result = riak_core_vnode_master:sync_command({Partition,Node},
						 {check_ready},
						 materializer_vnode_master,
						 infinity),
    case Result of
	true ->
	    check_table_ready(Rest);
	false ->
	    false
    end.

handle_command({check_ready},_Sender,State = #state{partition=_Partition}) ->
    {reply, true, State};

handle_command({read, AntidoteDB, Key, Type, SnapshotTime, TxId}, _Sender,
               State = #state{partition=Partition})->
    {reply, read(AntidoteDB, Key, Type, SnapshotTime, TxId,Partition), State};

handle_command({update, AntidoteDB, Key, DownstreamOp}, _Sender, State)->
    insert_op(AntidoteDB, Key, DownstreamOp),
    {reply, ok, State};

handle_command({store_ss, AntidoteDB, Key, Snapshot, CommitTime}, _Sender, State)->
    internal_store_ss(AntidoteDB, Key,Snapshot,CommitTime),
    {noreply, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

%% TODO FIX THIS
%%handle_handoff_command(?FOLD_REQ{foldfun=_Fun, acc0=_Acc0}, _Sender, State) ->
handle_handoff_command(_F, _Sender, State) ->
%%    F = fun(Key, A) ->
%%		[Key1|_]=tuple_to_list(Key),
%%                Fun(Key1, Key, A)
%%        end,
%%    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, [], State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

%% TODO FIX THIS
handle_handoff_data(_Data, State) ->
%%    {_Key, Operation} = binary_to_term(Data),
%%    true = ets:insert(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {false, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%---------------- Internal Functions -------------------%%

-spec internal_store_ss(antidote_db:antidote_db(), key(), snapshot(),
    snapshot_time()) -> ok | error.
internal_store_ss(AntidoteDB, Key, Snapshot, CommitTime) ->
    antidote_db_wrapper:put_snapshot(AntidoteDB, Key, CommitTime, Snapshot).

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(antidote_db:antidote_db(), key(), type(), snapshot_time(), txid() | ignore) -> {ok, snapshot()} | {error, no_snapshot}.
internal_read(AntidoteDB, Key, Type, MinSnapshotTime, TxId) ->
    {{LastOp, LatestSnapshot}, SnapshotCommitTime} = case antidote_db_wrapper:get_snapshot(AntidoteDB, Key, MinSnapshotTime) of
                 {error, not_found} ->
                     %% First time reading this key, store an empty snapshot in the cache
                     BlankSS = {0, clocksi_materializer:new(Type)},
                     case TxId of
                         ignore ->
                             internal_store_ss(AntidoteDB, Key, BlankSS, vectorclock:new());
                         _ ->
                             materializer_vnode:store_ss(AntidoteDB, Key, BlankSS, vectorclock:new())
                     end,
                     {BlankSS, vectorclock:new()};
                 {ok, Snapshot1, SnapshotTime} ->
                     {Snapshot1, vectorclock:from_list(SnapshotTime)}
             end,
    OPS = antidote_db_wrapper:get_ops(AntidoteDB, Key, SnapshotCommitTime, MinSnapshotTime),
    case length(OPS) of
	0 ->
	    {ok, LatestSnapshot};
	_Len ->
	    case clocksi_materializer:materialize(Type, LatestSnapshot, LastOp, SnapshotCommitTime, MinSnapshotTime, OPS, TxId) of
		{ok, Snapshot, NewLastOp, CommitTime, NewSS} ->
		    %% the following checks for the case there were no snapshots and there were operations, but none was applicable
		    %% for the given snapshot_time
		    %% But is the snapshot not safe?
		    case CommitTime of
			ignore ->
			    {ok, Snapshot};
			_ ->
			    case NewSS of
				%% Only store the snapshot if it would be at the end of the list and has new operations added to the
				%% previous snapshot
				true ->
				    case TxId of
					ignore ->
					    internal_store_ss(AntidoteDB, Key,{NewLastOp,Snapshot},CommitTime);
					_ ->
					    materializer_vnode:store_ss(AntidoteDB, Key,{NewLastOp,Snapshot},CommitTime)
				    end;
				_ ->
				    ok
			    end,
			    {ok, Snapshot}
		    end;
		{error, Reason} ->
		    {error, Reason}
	    end
    end.

%% Should be called doesn't belong in SS
%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec belongs_to_snapshot_op(snapshot_time() | ignore, commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot_op(ignore, {_OpDc,_OpCommitTime}, _OpSs) ->
    true;
belongs_to_snapshot_op(SSTime, {OpDc,OpCommitTime}, OpSs) ->
    OpSs1 = dict:store(OpDc,OpCommitTime,OpSs),
    not vectorclock:le(OpSs1,SSTime).

%% @doc Inserts an operation
-spec insert_op(antidote_db:antidote_db(), key(), clocksi_payload()) -> ok.
insert_op(AntidoteDB, Key, DownstreamOp)->
    ok = antidote_db_wrapper:put_op(AntidoteDB, Key, [DownstreamOp#clocksi_payload.commit_time], {1, DownstreamOp}).

-ifdef(TEST).

%% @doc Testing belongs_to_snapshot returns true when a commit time
%% is smaller than a snapshot time
belongs_to_snapshot_test()->
	CommitTime1a= 1,
	CommitTime2a= 1,
	CommitTime1b= 1,
	CommitTime2b= 7,
	SnapshotClockDC1 = 5,
	SnapshotClockDC2 = 5,
	CommitTime3a= 5,
	CommitTime4a= 5,
	CommitTime3b= 10,
	CommitTime4b= 10,

	SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
	?assertEqual(true, belongs_to_snapshot_op(
			     vectorclock:from_list([{1, CommitTime1a},{2,CommitTime1b}]), {1, SnapshotClockDC1}, SnapshotVC)),
	?assertEqual(true, belongs_to_snapshot_op(
			     vectorclock:from_list([{1, CommitTime2a},{2,CommitTime2b}]), {2, SnapshotClockDC2}, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot_op(
			      vectorclock:from_list([{1, CommitTime3a},{2,CommitTime3b}]), {1, SnapshotClockDC1}, SnapshotVC)),
	?assertEqual(false, belongs_to_snapshot_op(
			      vectorclock:from_list([{1, CommitTime4a},{2,CommitTime4b}]), {2, SnapshotClockDC2}, SnapshotVC)).


%% @doc This tests to make sure no updates are lost
no_ops_lost_test() ->
    eleveldb:destroy("no_ops_lost_test", []),
    {ok, AntidoteDB} = antidote_db:new("no_ops_lost_test"),
    Key = mycount,
    DC1 = 'antidote@127.0.0.1',
    Type = riak_dt_gcounter,

    %% Make 10 snapshots

    {ok, Res0} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,2}]),ignore),
    ?assertEqual(0, Type:value(Res0)),

    insert_op(AntidoteDB, Key, generate_payload(10,11,Res0,a1)),
    {ok, Res1} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,12}]),ignore),
    ?assertEqual(1, Type:value(Res1)),

    insert_op(AntidoteDB, Key, generate_payload(20,21,Res1,a2)),
    {ok, Res2} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,22}]),ignore),
    ?assertEqual(2, Type:value(Res2)),

    insert_op(AntidoteDB, Key, generate_payload(30,31,Res2,a3)),
    {ok, Res3} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,32}]),ignore),
    ?assertEqual(3, Type:value(Res3)),

    insert_op(AntidoteDB, Key, generate_payload(40,41,Res3,a4)),
    {ok, Res4} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,42}]),ignore),
    ?assertEqual(4, Type:value(Res4)),

    insert_op(AntidoteDB, Key, generate_payload(50,51,Res4,a5)),
    {ok, Res5} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,52}]),ignore),
    ?assertEqual(5, Type:value(Res5)),

    insert_op(AntidoteDB, Key, generate_payload(60,61,Res5,a6)),
    {ok, Res6} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,62}]),ignore),
    ?assertEqual(6, Type:value(Res6)),

    insert_op(AntidoteDB, Key, generate_payload(70,71,Res6,a7)),
    {ok, Res7} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,72}]),ignore),
    ?assertEqual(7, Type:value(Res7)),

    insert_op(AntidoteDB, Key, generate_payload(80,81,Res7,a8)),
    {ok, Res8} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,82}]),ignore),
    ?assertEqual(8, Type:value(Res8)),

    insert_op(AntidoteDB, Key, generate_payload(90,91,Res8,a9)),
    {ok, Res9} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,92}]),ignore),
    ?assertEqual(9, Type:value(Res9)),

    insert_op(AntidoteDB, Key, generate_payload(100,101,Res9,a10)),

    %% Insert some new values

    insert_op(AntidoteDB, Key, generate_payload(15,111,Res1,a11)),
    insert_op(AntidoteDB, Key, generate_payload(16,121,Res1,a12)),

    {ok, Res10} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,102}]),ignore),
    ?assertEqual(10, Type:value(Res10)),

    insert_op(AntidoteDB, Key, generate_payload(102,131,Res9,a13)),

    %% Be sure you didn't loose any updates
    {ok, Res13} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,142}]),ignore),
    ?assertEqual(13, Type:value(Res13)),

    antidote_db:close_and_destroy(AntidoteDB, "no_ops_lost_test").

%%print_DB(Ref) ->
%%    io:format("----------------------------~n"),
%%    eleveldb:fold(
%%        Ref,
%%        fun({K, _V}, AccIn) ->
%%            io:format("~p ~n", [binary_to_term(K)]),
%%            AccIn
%%        end,
%%        [],
%%        []),
%%    io:format("----------------------------~n").

%% @doc This tests to make sure operation lists can be large and resized
large_list_test() ->
    eleveldb:destroy("large_list_test", []),
    {ok, AntidoteDB} = antidote_db:new("large_list_test"),
    Key = mycount,
    DC1 = 1,
    Type = riak_dt_gcounter,

    %% Make 1000 updates to grow the list, without generating a snapshot to perform the gc
    {ok, Res0} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1, 2}]), ignore),
    ?assertEqual(0, Type:value(Res0)),

    lists:foreach(fun(Val) ->
        insert_op(AntidoteDB, Key, generate_payload(10, 11 + Val, Res0, Val))
                  end, lists:seq(1, 1000)),
%%
%%    print_DB(AntidoteDB),

    {ok, Res1000} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore),
    ?assertEqual(1000, Type:value(Res1000)),

    %% Now check everything is ok as the list shrinks from generating new snapshots
    lists:foreach(fun(Val) ->
        insert_op(AntidoteDB, Key, generate_payload(10 + Val, 11 + Val, Res0, Val)),
        {ok, Res} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore),
        ?assertEqual(Val, Type:value(Res))
                  end, lists:seq(1001, 1100)),

    antidote_db:close_and_destroy(AntidoteDB, "large_list_test").

generate_payload(SnapshotTime,CommitTime,Prev,Name) ->
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 'antidote@127.0.0.1',

    {ok,Op1} = Type:update(increment, Name, Prev),
    #clocksi_payload{key = Key,
		     type = Type,
		     op_param = {merge, Op1},
		     snapshot_time = vectorclock:from_list([{DC1,SnapshotTime}]),
		     commit_time = {DC1,CommitTime},
		     txid = 1
		    }.



seq_write_test() ->
    eleveldb:destroy("seq_write_test", []),
    {ok, AntidoteDB} = antidote_db:new("seq_write_test"),

    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 'antidote@127.0.0.1',
    S1 = Type:new(),

    %% Insert one increment
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    insert_op(AntidoteDB, Key, DownstreamOp1),
    {ok, Res1} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,16}]),ignore),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok,Op2} = Type:update(increment, a, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC1,16}]),
                      commit_time = {DC1,20},
                      txid=2},

    insert_op(AntidoteDB, Key, DownstreamOp2),
    {ok, Res2} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,21}]), ignore),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,16}]), ignore),
    ?assertEqual(1, Type:value(ReadOld)),

    antidote_db:close_and_destroy(AntidoteDB, "seq_write_test").

multipledc_write_test() ->
    eleveldb:destroy("multipledc_write_test", []),
    {ok, AntidoteDB} = antidote_db:new("multipledc_write_test"),

    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 'antidote@127.0.0.1',
    DC2 = 'antidote@127.0.0.2',
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,10}, {DC2,0}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    insert_op(AntidoteDB, Key, DownstreamOp1),
    {ok, Res1} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,16},{DC2,0}]), ignore),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok,Op2} = Type:update(increment, b, Res1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = {merge, Op2},
                      snapshot_time=vectorclock:from_list([{DC2,16}, {DC1,16}]),
                      commit_time = {DC2,20},
                      txid=2},

    insert_op(AntidoteDB, Key, DownstreamOp2),
    {ok, Res2} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,15}, {DC2,15}]), ignore),
    ?assertEqual(1, Type:value(ReadOld)),

    antidote_db:close_and_destroy(AntidoteDB, "multipledc_write_test").

concurrent_write_test() ->
    eleveldb:destroy("concurrent_write_test", []),
    {ok, AntidoteDB} = antidote_db:new("concurrent_write_test"),

    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = {merge, Op1},
                                     snapshot_time = vectorclock:from_list([{DC1,0}, {DC2,0}]),
                                     commit_time = {DC2, 1},
                                     txid = 1
                                    },
    insert_op(AntidoteDB, Key, DownstreamOp1),
    {ok, Res1} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC2,1}, {DC1,0}]), ignore),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:update(increment, b, S1),
    DownstreamOp2 = #clocksi_payload{ key = Key,
				      type = Type,
				      op_param = {merge, Op2},
				      snapshot_time=vectorclock:from_list([{DC1,0}, {DC2,0}]),
				      commit_time = {DC1, 1},
				      txid=2},
    insert_op(AntidoteDB, Key, DownstreamOp2),

    %% Read different snapshots
    {ok, ReadDC1} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,1}, {DC2, 0}]), ignore),
    ?assertEqual(1, Type:value(ReadDC1)),
    io:format("Result1 = ~p", [ReadDC1]),
    {ok, ReadDC2} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC1,0},{DC2,1}]), ignore),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(AntidoteDB, Key, Type, vectorclock:from_list([{DC2,1}, {DC1,1}]), ignore),
    ?assertEqual(2, Type:value(Res2)),

    antidote_db:close_and_destroy(AntidoteDB, "concurrent_write_test").

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
    eleveldb:destroy("read_nonexisting_key_test", []),
    {ok, AntidoteDB} = antidote_db:new("read_nonexisting_key_test"),
    Type = riak_dt_gcounter,
    {ok, ReadResult} = internal_read(AntidoteDB, key, Type, vectorclock:from_list([{dc1,1}, {dc2, 0}]), ignore),
    ?assertEqual(0, Type:value(ReadResult)),

    antidote_db:close_and_destroy(AntidoteDB, "concurrent_write_test").

-endif.
