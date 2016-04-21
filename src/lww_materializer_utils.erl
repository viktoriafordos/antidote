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
-module(lww_materializer_utils).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of ops to keep before GC
-define(OPS_THRESHOLD, 60).
-define(OPS_MIN_THRESHOLD, 10).
-define(OPS_LIST_THRESHOLD, 20).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([
         op_insert_gc/3,
         internal_read/5
        ]).

op_insert_gc(Key, DownstreamOp, OpsCache) ->
    {DC, _CT} = DownstreamOp#clocksi_payload.commit_time,
    {OpsLen, OpsDict} = case ets:lookup(OpsCache,Key) of
                  [] -> {0, dict:new()};
                  [{Key, Dict}] -> Dict
              end,
    NewOpsDict = dict:update(DC, fun({Len, OpList}) ->
                                         {Len+1, [DownstreamOp | OpList ]}
                                 end, {1,[DownstreamOp]}, OpsDict),
    %PrunedDict = gc_ops(NewOpsDict), %% GC if needed
    case OpsLen > ?OPS_THRESHOLD of
        true -> 
            {NewLen, PrunedDict} = gc_ops(NewOpsDict, ?OPS_MIN_THRESHOLD, ?OPS_LIST_THRESHOLD),
            ets:insert(OpsCache, {Key, {NewLen, PrunedDict}});
        false ->
            ets:insert(OpsCache, {Key, {OpsLen+1, NewOpsDict}})
    end.
    
    
internal_read(Key, Type, SnapshotTime, _TxId, OpsCache) ->
    case ets:lookup(OpsCache, Key) of
        [] -> {ok, Type:new()};
        [{Key, {_Len, OpsDict}}] ->
            Version = get_version(OpsDict, SnapshotTime),
            case Version of 
                ignore -> {ok, Type:new()};
                _ -> 
                    {ok, Snapshot} = materializer:update_snapshot(Type, Type:new(), Version#clocksi_payload.op_param),
                    {ok, Snapshot}
            end
    end.

get_version(OpsDict, SnapshotTime) ->
    %% Take one version from all DCids
    VersionList = dict:fold(fun(_K, Val, VList) ->
                                    {_Len, OpList} = Val,
                                    NewList = lists:dropwhile(fun(Op) ->
                                                                      not_in_snapshot(Op, SnapshotTime)
                                                              end, OpList),
                                    case NewList of 
                                        [] -> VList;
                                        [H|_] -> [H | VList]
                                    end
                            end, [], OpsDict),
    %% Take the one with maximim (scalar) commit-time, since we need LWW.
    {OpVersion, _CT} = lists:foldl(fun(Op, {CurMax, MaxCT}) ->
                              {_DC,CT} = Op#clocksi_payload.commit_time,
                              case CT > MaxCT of
                                  true -> {Op, CT};
                                  false -> {CurMax, MaxCT}
                              end
                      end,
                      {ignore, 0}, %% give a minimum, this will be overwritten anyway
                      VersionList),
    OpVersion.

not_in_snapshot(Op, SnapshotTime) ->
    {DC, CT} = Op#clocksi_payload.commit_time,
    OpSS = Op#clocksi_payload.snapshot_time,
    belongs_to_snapshot_op(SnapshotTime, {DC, CT}, OpSS). %% returns true if op not in snapshot

%% Should be called doesn't belong in SS
%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec belongs_to_snapshot_op(snapshot_time() | ignore, commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot_op(ignore, {_OpDc,_OpCommitTime}, _OpSs) ->
    true;
belongs_to_snapshot_op(SSTime, {OpDc,OpCommitTime}, OpSs) ->
    OpSs1 = dict:store(OpDc,OpCommitTime,OpSs),
    not vectorclock:le(OpSs1,SSTime).

%% Keeps the first OPS_MIN_THRESHOLD elements of list of ops per DCid.
gc_ops(OpsDict, Min, Max) ->
    PrunedDict = dict:map(
                   fun(_K, Val) ->
                           {Len, OpList} = Val,
                           case Len > Max of
                               true -> 
                                   {PrunedOps, _Rest} = lists:split(Min, OpList),
                                   {Min, PrunedOps};
                               false ->
                                   Val
                           end                                 
                   end,  OpsDict),
    Len = dict:fold(
            fun(_K, {Len, _OpList}, Acc) ->
                    Acc+Len
            end, 0, PrunedDict),
    {Len, PrunedDict}.

-ifdef(TEST).

generate_payload(SnapshotTime, CommitTime, Val) ->
    Key = myreg,
    Type = riak_dt_lwwreg,

    {ok,Op1} = Type:update({assign, Val, 10}, actor, Type:new()),
    #clocksi_payload{key = Key,
		     type = Type,
		     op_param = {merge, Op1},
		     snapshot_time = vectorclock:from_list(SnapshotTime),
		     commit_time = CommitTime,
		     txid = 1
                    }.

write_read_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    Key = myreg,
    DC1 = 1,
    Type = riak_dt_lwwreg,
    SnapshotTime = [{DC1, 5}],

    DownstreamOp1 = generate_payload(SnapshotTime, {DC1,6}, 1),

    true =  op_insert_gc(Key, DownstreamOp1, OpsCache),
    {ok, Snapshot} = internal_read(Key, Type, vectorclock:from_list([{DC1, 7}]), ignore, OpsCache),
    ?assertEqual(Type:value(Snapshot), 1).

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    Key = myreg,
    Type = riak_dt_lwwreg,
    DC1 = 1,
    DC2 = 2,

    %% Insert one increment in DC1
    
    DownstreamOp1 = generate_payload([{DC2,0}, {DC1,10}], {DC1,15}, 1),
    op_insert_gc(Key,DownstreamOp1, OpsCache),
    
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1,16},{DC2,0}]), ignore, OpsCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    DownstreamOp2 = generate_payload([{DC2,16}, {DC1,16}], {DC2,20}, 2), 
    op_insert_gc(Key, DownstreamOp2, OpsCache),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1,16}, {DC2,21}]), ignore, OpsCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1,15}, {DC2,15}]), ignore, OpsCache),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    Key = myreg,
    Type = riak_dt_lwwreg,
    DC1 = local,
    DC2 = remote,

    %% Insert one increment in DC1

    DownstreamOp1 = generate_payload([{DC1,0}, {DC2,0}], {DC2,2}, 1),
    op_insert_gc(Key,DownstreamOp1, OpsCache),

    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2,2}, {DC1,0}]), ignore, OpsCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    DownstreamOp2 = generate_payload([{DC1,0}, {DC2,0}], {DC1,1}, 2),
    op_insert_gc(Key,DownstreamOp2, OpsCache),

    %% Read different snapshots
    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1,1}, {DC2, 0}]), ignore, OpsCache),
    ?assertEqual(2, Type:value(ReadDC1)),
    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1,0},{DC2,2}]), ignore, OpsCache),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2,2}, {DC1,1}]), ignore, OpsCache),
    ?assertEqual(1, Type:value(Res2)),

    %% Read snapshot that doesnot including both increments
    {ok, Res3} = internal_read(Key, Type, vectorclock:from_list([{DC2,0}, {DC1,0}]), ignore, OpsCache),
    ?assertEqual(<<>>, Type:value(Res3)).

gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    Key = myreg,
    Type = riak_dt_lwwreg,
    DC1 = a,
    DC2 = b,
    
    true =  op_insert_gc(Key, generate_payload([{DC1,0}], {DC1,1}, 1), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,1}], {DC1,2}, 2), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,2}], {DC1,3}, 3), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,3}], {DC1,4}, 4), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,4}], {DC1,5}, 5), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,5}], {DC1,6}, 6), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,6}], {DC1,7}, 7), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,7}], {DC1,8}, 8), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,8}], {DC1,9}, 9), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,9}], {DC1,10}, 10), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,10}], {DC1,11}, 11), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC1,11}], {DC1,12}, 12), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,0}], {DC2,1}, 13), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,1}], {DC2,2}, 14), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,2}], {DC2,3}, 15), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,3}], {DC2,7}, 16), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,7}], {DC2,8}, 17), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,8}], {DC2,9}, 18), OpsCache),
    true =  op_insert_gc(Key, generate_payload([{DC2,9}], {DC2,10}, 19), OpsCache),
    
    [{Key, {_OpsLen, OpsDict}}] = ets:lookup(OpsCache,Key),
    %% Trigger GC
    {NewLen, NewDict} = gc_ops(OpsDict, 5, 10),
    ets:insert(OpsCache, {Key, {NewLen, NewDict}}),
    ?assertEqual(12, NewLen),
    
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2,5}, {DC1,10}]), ignore, OpsCache),
    ?assertEqual(10,Type:value(Res1)),
    
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2,10}, {DC1,3}]), ignore, OpsCache),
    ?assertEqual(19, Type:value(Res2)),
    
    {ok, Res3} = internal_read(Key, Type, vectorclock:from_list([{DC2,3}, {DC1,3}]), ignore, OpsCache),
    ?assertEqual(15, Type:value(Res3)). %% This is actually wrong. GC is naive and just remove entries from each list per DC.
    
    
    
    
    
-endif.
