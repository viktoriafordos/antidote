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


-define(SNAPSHOT_THRESHOLD, 10).
-define(SNAPSHOT_MIN, 5).
-define(OPS_THRESHOLD, 50).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
    check_tables_ready/0,
    read/5,
    store_ss/3,
    update/2,
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


-record(state, {partition :: partition_id()}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), snapshot_time(), txid(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, TxId,_Partition) ->
    internal_read(Key, Type, SnapshotTime, TxId).

%%@doc write operation to antidote DB for future read, updates are stored
%%     one at a time into the antidote DB tables
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    Node = get_node_for_key(Key),
    riak_core_vnode_master:sync_command(Node, {update, Key, DownstreamOp},
                                        materializer_vnode_master).

%%@doc write snapshot to antidote DB for future read, snapshots are stored
%%     one at a time into the antidote DB table
-spec store_ss(key(), snapshot(), snapshot_time()) -> ok.
store_ss(Key, Snapshot, CommitTime) ->
    Node = get_node_for_key(Key),
    riak_core_vnode_master:command(Node, {store_ss,Key, Snapshot, CommitTime},
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

handle_command({read, Key, Type, SnapshotTime, TxId}, _Sender,
               State = #state{partition=Partition})->
    {reply, read(Key, Type, SnapshotTime, TxId,Partition), State};

handle_command({update, Key, DownstreamOp}, _Sender, State)->
    true = op_insert_gc(Key,DownstreamOp),
    {reply, ok, State};

handle_command({store_ss, Key, Snapshot, CommitTime}, _Sender, State)->
    internal_store_ss(Key,Snapshot,CommitTime),
    {noreply, State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

%% TODO CHANGE THIIIIIS
handle_handoff_command(?FOLD_REQ{foldfun=_Fun, acc0=_Acc0} ,
                       _Sender, State) ->
%%    F = fun({Key,Operation}, A) ->
%%                Fun(Key, Operation, A)
%%        end,
%%    Acc = eleveldb:fold(OpsCache, F, Acc0, []),
    {reply, [], State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Key, Operation} = binary_to_term(Data),
    ok = antidote_db_vnode:put_op(get_node_for_key(Key), Key, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).


%% TODO CHANGE THIIIIIS
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

-spec internal_store_ss(key(), snapshot(), snapshot_time()) -> true.
internal_store_ss(Key, Snapshot, CommitTime) ->
    SnapshotDict = case antidote_db_vnode:get_snap(get_node_for_key(Key), Key) of
                       not_found ->
                           vector_orddict:new();
                       SnapshotDictA ->
                           SnapshotDictA
                   end,
    SnapshotDict1 = vector_orddict:insert_bigger(CommitTime, Snapshot, SnapshotDict),
    snapshot_insert_gc(Key, SnapshotDict1).


%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), snapshot_time(), txid() | ignore) -> {ok, snapshot()} | {error, no_snapshot}.
internal_read(Key, Type, MinSnapshotTime, TxId) ->
    Result = case antidote_db_vnode:get_snap(get_node_for_key(Key), Key) of
		not_found ->
		     %% First time reading this key, store an empty snapshot in the antidote DB
		     BlankSS = {0,clocksi_materializer:new(Type)},
		     case TxId of
			 ignore ->
			     internal_store_ss(Key,BlankSS,vectorclock:new());
			 _ ->
			     materializer_vnode:store_ss(Key,BlankSS,vectorclock:new())
		     end,
		     {BlankSS,ignore,true};
         SnapshotDict ->
		     case vector_orddict:get_smaller(MinSnapshotTime, SnapshotDict) of
			 {undefined, _IsF} ->
			     {error, no_snapshot};
			 {{SCT, LS},IsF}->
			     {LS,SCT,IsF}
		     end
	     end,
    {Length,Ops,{LastOp,LatestSnapshot},SnapshotCommitTime,IsFirst} =
	case Result of
	    {error, no_snapshot} ->
		LogId = log_utilities:get_logid_from_key(Key),
		[Node] = log_utilities:get_preflist_from_key(Key),
		Res = logging_vnode:get(Node, {get, LogId, MinSnapshotTime, Type, Key}),
		Res;
	    {LatestSnapshot1,SnapshotCommitTime1,IsFirst1} ->
		case antidote_db_vnode:get_op(get_node_for_key(Key), Key) of
			not_found ->
			{0, [], LatestSnapshot1,SnapshotCommitTime1,IsFirst1};
		    {Length1, Ops1} ->
			{Length1,Ops1,LatestSnapshot1,SnapshotCommitTime1,IsFirst1}
		end
	end,
    case Length of
	0 ->
	    {ok, LatestSnapshot};
	_Len ->
	    case clocksi_materializer:materialize(Type, LatestSnapshot, LastOp, SnapshotCommitTime, MinSnapshotTime, Ops, TxId) of
		{ok, Snapshot, NewLastOp, CommitTime, NewSS} ->
		    %% the following checks for the case there were no snapshots and there were operations, but none was applicable
		    %% for the given snapshot_time
		    %% But is the snapshot not safe?
		    case CommitTime of
			ignore ->
			    {ok, Snapshot};
			_ ->
			    case NewSS and IsFirst of
				%% Only store the snapshot if it would be at the end of the list and has new operations added to the
				%% previous snapshot
				true ->
				    case TxId of
					ignore ->
					    internal_store_ss(Key,{NewLastOp,Snapshot},CommitTime);
					_ ->
					    materializer_vnode:store_ss(Key,{NewLastOp,Snapshot},CommitTime)
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

%% @doc Operation to insert a Snapshot in the antidote DB and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(key(), vector_orddict:vector_orddict()) -> true.
snapshot_insert_gc(Key, SnapshotDict) ->
    %% Should check op size here also, when run from op gc
    case (vector_orddict:size(SnapshotDict)) >= ?SNAPSHOT_THRESHOLD of
        true ->
            %% snapshots are no longer totally ordered
            Node = get_node_for_key(Key),
            PrunedSnapshots = vector_orddict:sublist(SnapshotDict, 1, ?SNAPSHOT_MIN),
            FirstOp = vector_orddict:last(PrunedSnapshots),
            {CT, _S} = FirstOp,
            CommitTime = lists:foldl(fun({CT1, _ST}, Acc) ->
                vectorclock:keep_min(CT1, Acc)
                                     end, CT, vector_orddict:to_list(PrunedSnapshots)),
            {Length, OpsDict} = case antidote_db_vnode:get_op(Node, Key) of
                                    not_found ->
                                        {0, []};
                                    {Len, Dict} ->
                                        {Len, Dict}
                                end,
            {NewLength, PrunedOps} = prune_ops({Length, OpsDict}, CommitTime),
            ok = antidote_db_vnode:put_snap(Node, Key, PrunedSnapshots),
            ok = antidote_db_vnode:put_op(Node, Key, {NewLength, PrunedOps});
        false ->
            ok = antidote_db_vnode:put_snap(get_node_for_key(Key), Key, SnapshotDict)
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops({non_neg_integer(),list()}, snapshot_time())-> {non_neg_integer(),list()}.
prune_ops({_Len,OpsDict}, Threshold)->
%% should write custom function for this in the vector_orddict
%% or have to just traverse the entire list?
%% since the list is ordered, can just stop when all values of
%% the op is smaller (i.e. not concurrent)
%% So can add a stop function to ordered_filter
%% Or can have the filter function return a tuple, one vale for stopping
%% one for including
    Res = lists:filter(fun({_OpId,Op}) ->
			       OpCommitTime=Op#clocksi_payload.commit_time,
			       (belongs_to_snapshot_op(Threshold,OpCommitTime,Op#clocksi_payload.snapshot_time))
		       end, OpsDict),
    NewOps = case Res of
		 [] ->
		     [First|_Rest] = OpsDict,
		     [First];
		 _ ->
		     Res
	     end,
    {length(NewOps),NewOps}.

%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(key(), clocksi_payload()) -> true.
op_insert_gc(Key, DownstreamOp)->
    Node = get_node_for_key(Key),
    {Length,OpsDict,NewId} = case antidote_db_vnode:get_op(Node, Key) of
				 not_found->
				     {0,[],1};
                 {Len, [{PrevId,First}|Rest]}->
                     {Len,[{PrevId,First}|Rest],PrevId+1}
		       end,
    case (Length)>=?OPS_THRESHOLD of
        true ->
            Type=DownstreamOp#clocksi_payload.type,
            SnapshotTime=DownstreamOp#clocksi_payload.snapshot_time,
            {_, _} = internal_read(Key, Type, SnapshotTime, ignore),
	        %% Have to get the new ops dict because the interal_read can change it
	        [{_, {Length1,OpsDict1}}] = antidote_db_vnode:get_op(Node, Key),
            OpsDict2=[{NewId,DownstreamOp} | OpsDict1],
            ok = antidote_db_vnode:put_op(Node, Key, {Length1 + 1, OpsDict2}),
            true;
        false ->
            OpsDict1=[{NewId,DownstreamOp} | OpsDict],
            ok = antidote_db_vnode:put_op(Node, Key, {Length + 1,OpsDict1}),
            true
    end.

get_node_for_key(Key) ->
    hd(log_utilities:get_preflist_from_key(Key)).

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


%% TODO FIX TESTS
%%
%% @doc This tests to make sure when garbage collection happens, no updates are lost
%%gc_test() ->
%%    Key = mycount,
%%    DC1 = 1,
%%    Type = riak_dt_gcounter,
%%
%%    %% Make 10 snapshots
%%
%%    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1,2}]),ignore),
%%    ?assertEqual(0, Type:value(Res0)).

%%    op_insert_gc(Key, generate_payload(10,11,Res0,a1)),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,12}]),ignore),
%%    ?assertEqual(1, Type:value(Res1)),
%%
%%    op_insert_gc(Key, generate_payload(20,21,Res1,a2)),
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,22}]),ignore),
%%    ?assertEqual(2, Type:value(Res2)).

%%    op_insert_gc(Key, generate_payload(30,31,Res2,a3)),
%%    {ok, Res3} = internal_read(Key, Type, vectorclock:from_list([{DC1,32}]),ignore),
%%    ?assertEqual(3, Type:value(Res3)),
%%
%%    op_insert_gc(Key, generate_payload(40,41,Res3,a4)),
%%    {ok, Res4} = internal_read(Key, Type, vectorclock:from_list([{DC1,42}]),ignore),
%%    ?assertEqual(4, Type:value(Res4)),
%%
%%    op_insert_gc(Key, generate_payload(50,51,Res4,a5)),
%%    {ok, Res5} = internal_read(Key, Type, vectorclock:from_list([{DC1,52}]),ignore),
%%    ?assertEqual(5, Type:value(Res5)),
%%
%%    op_insert_gc(Key, generate_payload(60,61,Res5,a6)),
%%    {ok, Res6} = internal_read(Key, Type, vectorclock:from_list([{DC1,62}]),ignore),
%%    ?assertEqual(6, Type:value(Res6)),
%%
%%    op_insert_gc(Key, generate_payload(70,71,Res6,a7)),
%%    {ok, Res7} = internal_read(Key, Type, vectorclock:from_list([{DC1,72}]),ignore),
%%    ?assertEqual(7, Type:value(Res7)),
%%
%%    op_insert_gc(Key, generate_payload(80,81,Res7,a8)),
%%    {ok, Res8} = internal_read(Key, Type, vectorclock:from_list([{DC1,82}]),ignore),
%%    ?assertEqual(8, Type:value(Res8)),
%%
%%    op_insert_gc(Key, generate_payload(90,91,Res8,a9)),
%%    {ok, Res9} = internal_read(Key, Type, vectorclock:from_list([{DC1,92}]),ignore),
%%    ?assertEqual(9, Type:value(Res9)),
%%
%%    op_insert_gc(Key, generate_payload(100,101,Res9,a10)),
%%
%%    %% Insert some new values
%%
%%    op_insert_gc(Key, generate_payload(15,111,Res1,a11)),
%%    op_insert_gc(Key, generate_payload(16,121,Res1,a12)),
%%
%%    %% Trigger the clean
%%    {ok, Res10} = internal_read(Key, Type, vectorclock:from_list([{DC1,102}]),ignore),
%%    ?assertEqual(10, Type:value(Res10)),
%%
%%    op_insert_gc(Key, generate_payload(102,131,Res9,a13)),
%%
%%    %% Be sure you didn't loose any updates
%%    {ok, Res13} = internal_read(Key, Type, vectorclock:from_list([{DC1,142}]),ignore),
%%    ?assertEqual(13, Type:value(Res13)),
%%
%%    close_and_destroy(OpsCache, SnapshotCache).
%%
%%generate_payload(SnapshotTime,CommitTime,Prev,Name) ->
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = 1,
%%
%%    {ok,Op1} = Type:update(increment, Name, Prev),
%%    #clocksi_payload{key = Key,
%%		     type = Type,
%%		     op_param = {merge, Op1},
%%		     snapshot_time = vectorclock:from_list([{DC1,SnapshotTime}]),
%%		     commit_time = {DC1,CommitTime},
%%		     txid = 1
%%		    }.

%%seq_write_test() ->
%%    {ok, OpsCache} = eleveldb:open("OpsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    {ok, SnapshotCache} = eleveldb:open("SnapshotsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = 1,
%%    S1 = Type:new(),
%%
%%    %% Insert one increment
%%    {ok,Op1} = Type:update(increment, a, S1),
%%    DownstreamOp1 = #clocksi_payload{key = Key,
%%                                     type = Type,
%%                                     op_param = {merge, Op1},
%%                                     snapshot_time = vectorclock:from_list([{DC1,10}]),
%%                                     commit_time = {DC1, 15},
%%                                     txid = 1
%%                                    },
%%    op_insert_gc(Key,DownstreamOp1),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]),ignore),
%%    ?assertEqual(1, Type:value(Res1)),
%%
%%    %% Insert second increment
%%    {ok,Op2} = Type:update(increment, a, Res1),
%%    DownstreamOp2 = DownstreamOp1#clocksi_payload{
%%                      op_param = {merge, Op2},
%%                      snapshot_time=vectorclock:from_list([{DC1,16}]),
%%                      commit_time = {DC1,20},
%%                      txid=2},
%%
%%    op_insert_gc(Key,DownstreamOp2),
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,21}]), ignore),
%%    ?assertEqual(2, Type:value(Res2)),
%%
%%    %% Read old version
%%    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}]), ignore),
%%    ?assertEqual(1, Type:value(ReadOld)),
%%
%%    close_and_destroy(OpsCache, SnapshotCache).
%%
%%multipledc_write_test() ->
%%    {ok, OpsCache} = eleveldb:open("OpsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    {ok, SnapshotCache} = eleveldb:open("SnapshotsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = 1,
%%    DC2 = 2,
%%    S1 = Type:new(),
%%
%%    %% Insert one increment in DC1
%%    {ok,Op1} = Type:update(increment, a, S1),
%%    DownstreamOp1 = #clocksi_payload{key = Key,
%%                                     type = Type,
%%                                     op_param = {merge, Op1},
%%                                     snapshot_time = vectorclock:from_list([{DC2,0}, {DC1,10}]),
%%                                     commit_time = {DC1, 15},
%%                                     txid = 1
%%                                    },
%%    op_insert_gc(Key,DownstreamOp1),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16},{DC2,0}]), ignore),
%%    ?assertEqual(1, Type:value(Res1)),
%%
%%    %% Insert second increment in other DC
%%    {ok,Op2} = Type:update(increment, b, Res1),
%%    DownstreamOp2 = DownstreamOp1#clocksi_payload{
%%                      op_param = {merge, Op2},
%%                      snapshot_time=vectorclock:from_list([{DC2,16}, {DC1,16}]),
%%                      commit_time = {DC2,20},
%%                      txid=2},
%%
%%    op_insert_gc(Key,DownstreamOp2),
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore),
%%    ?assertEqual(2, Type:value(Res2)),
%%
%%    %% Read old version
%%    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,15}, {DC2,15}]), ignore),
%%    ?assertEqual(1, Type:value(ReadOld)),
%%
%%    close_and_destroy(OpsCache, SnapshotCache).
%%
%%concurrent_write_test() ->
%%    {ok, OpsCache} = eleveldb:open("OpsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    {ok, SnapshotCache} = eleveldb:open("SnapshotsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    Key = mycount,
%%    Type = riak_dt_gcounter,
%%    DC1 = local,
%%    DC2 = remote,
%%    S1 = Type:new(),
%%
%%    %% Insert one increment in DC1
%%    {ok,Op1} = Type:update(increment, a, S1),
%%    DownstreamOp1 = #clocksi_payload{key = Key,
%%                                     type = Type,
%%                                     op_param = {merge, Op1},
%%                                     snapshot_time = vectorclock:from_list([{DC1,0}, {DC2,0}]),
%%                                     commit_time = {DC2, 1},
%%                                     txid = 1
%%                                    },
%%    op_insert_gc(Key,DownstreamOp1),
%%    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2,1}, {DC1,0}]), ignore),
%%    ?assertEqual(1, Type:value(Res1)),
%%
%%    %% Another concurrent increment in other DC
%%    {ok, Op2} = Type:update(increment, b, S1),
%%    DownstreamOp2 = #clocksi_payload{ key = Key,
%%				      type = Type,
%%				      op_param = {merge, Op2},
%%				      snapshot_time=vectorclock:from_list([{DC1,0}, {DC2,0}]),
%%				      commit_time = {DC1, 1},
%%				      txid=2},
%%    op_insert_gc(Key,DownstreamOp2),
%%
%%    %% Read different snapshots
%%    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1,1}, {DC2, 0}]), ignore),
%%    ?assertEqual(1, Type:value(ReadDC1)),
%%    io:format("Result1 = ~p", [ReadDC1]),
%%    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1,0},{DC2,1}]), ignore),
%%    io:format("Result2 = ~p", [ReadDC2]),
%%    ?assertEqual(1, Type:value(ReadDC2)),
%%
%%    %% Read snapshot including both increments
%%    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2,1}, {DC1,1}]), ignore),
%%    ?assertEqual(2, Type:value(Res2)),
%%
%%    close_and_destroy(OpsCache, SnapshotCache).
%%
%%%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%%%% E.g., for a gcounter, return 0.
%%read_nonexisting_key_test() ->
%%    {ok, OpsCache} = eleveldb:open("OpsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    {ok, SnapshotCache} = eleveldb:open("SnapshotsDBTest", [{create_if_missing, true}, {error_if_exists, true}]),
%%    Type = riak_dt_gcounter,
%%    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1,1}, {dc2, 0}]), ignore),
%%    ?assertEqual(0, Type:value(ReadResult)),
%%
%%    close_and_destroy(OpsCache, SnapshotCache).
%%
%%close_and_destroy(RefDB1,RefDB2) ->
%%    eleveldb:close(RefDB1),
%%    eleveldb:close(RefDB2),
%%    eleveldb:destroy("OpsDBTest", []),
%%    eleveldb:destroy("SnapshotsDBTest", []).
%%

-endif.
