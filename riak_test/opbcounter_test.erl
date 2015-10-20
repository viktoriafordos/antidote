%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(opbcounter_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

%% Test entry point.
confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, NumVNodes}]}
                             ]),
    [Nodes] = rt:build_clusters([3]),
    lager:info("Nodes: ~p", [Nodes]),
    new_bcounter_test(Nodes),
    increment_test(Nodes),
    decrement_test(Nodes),
    case ?CERT of
	true ->
	    conditional_write_test(Nodes);
	false ->
	    ok
    end,
    %rt:clean_cluster(Nodes),
    pass.

%% Tests creating a new `bcounter()'.
new_bcounter_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("new_bcounter_test started"),
    Type = crdt_bcounter,
    Key = bcounter1,
    %% Test reading a new key of type `crdt_bcounter' creates a new `bcounter()'.
    Result0 = rpc:call(FirstNode, antidote, read,
                       [Key, Type]),
    Counter0 = crdt_bcounter:new(),
    ?assertEqual({ok, Counter0}, Result0).

%% Tests incrementing a `bcounter()'.
increment_test(Nodes) ->
    FirstNode = hd(Nodes),
    SecondNode = hd(tl(Nodes)),
    lager:info("increment_test started"),
    Type = crdt_bcounter,
    Key = bcounter2,
    %% Test simple read and write operations.
    Result0 = rpc:call(FirstNode, antidote, append,
                       [Key, Type, {{increment, 10}, a}]),
    ?assertMatch({ok, _}, Result0),
    Result1 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    {ok, Counter1} = Result1,
    ?assertEqual(10, crdt_bcounter:permissions(Counter1)),

    %Cannot test local permissions on client-side, because it does not know the id of the server.
    %?assertEqual(10, crdt_bcounter:localPermissions(r1,Counter1)),
    %?assertEqual(0, crdt_bcounter:localPermissions(r2,Counter1)),

    %% Test bulk transaction with read and write operations.
    Result2 = rpc:call(SecondNode, antidote, clocksi_execute_tx,
                       [[{update, {Key, Type, {{increment, 7}, b}}}, {update, {Key, Type, {{increment, 5}, b}}}, {read, {Key, Type}}]]),
    ?assertMatch({ok, _}, Result2),
    {ok, {_, [Counter2], _}} = Result2,
    ?assertEqual(22, crdt_bcounter:permissions(Counter2)).

%% Tests decrementing a `bcounter()'.
decrement_test(Nodes) ->
    FirstNode = hd(Nodes),
    lager:info("decrement_test started"),
    Type = crdt_bcounter,
    Key = bcounter3,
    %% Test an allowed chain of operations.
    Result0 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
                       [[{update, {Key, Type, {{increment, 7}, a}}}, {update, {Key, Type, {{increment, 5}, a}}},
                         {update, {Key, Type, {{decrement, 3}, a}}}, {update, {Key, Type, {{decrement, 2}, a}}},
                         {read, {Key, Type}}]]),
    ?assertMatch({ok, _}, Result0),
    {ok, {_, [Counter0], _}} = Result0,
    ?assertEqual(7, crdt_bcounter:permissions(Counter0)),
    %% Test a forbidden chain of operations.
    Result1 = rpc:call(FirstNode, antidote, clocksi_execute_tx,
                       [[{update, {Key, Type, {{decrement, 8}, b}}}, {read, {Key, Type}}]]),
    ?assertMatch({error, {no_permissions, {_, 8}}}, Result1).

%% Tests the conditional write mechanism required for `generate_downstream()' and `update()' to be atomic.
%% Such atomic execution is required for the correctness of `bcounter()' CRDT.
conditional_write_test(Nodes) ->
    FirstNode = hd(Nodes),
    LastNode= lists:last(Nodes),
    lager:info("conditional_write_test started"),
    Type = crdt_bcounter,
    Key = bcounter5,
    rpc:call(FirstNode, antidote, append,
             [Key, Type, {{increment, 10}, a}]),
    %% Start a transaction on the first node and perform a read operation.
    {ok, TxId1} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    rpc:call(FirstNode, antidote, clocksi_iread, [TxId1, Key, Type]),
    %% Execute a transaction on the last node which performs a write operation.
    {ok, TxId2} = rpc:call(LastNode, antidote, clocksi_istart_tx, []),
    rpc:call(LastNode, antidote, clocksi_iupdate,
             [TxId2, Key, Type, {{decrement, 3}, a}]),
    CommitTime1=rpc:call(LastNode, antidote, clocksi_iprepare, [TxId2]),
    ?assertMatch({ok, _}, CommitTime1),
    End1=rpc:call(LastNode, antidote, clocksi_icommit, [TxId2]),
    ?assertMatch({ok, _}, End1),
    %% Resume the first transaction and check that it fails.
    Result0 = rpc:call(FirstNode, antidote, clocksi_iupdate,
                       [TxId1, Key, Type, {{decrement, 3}, a}]),
    ?assertEqual(ok, Result0),
    CommitTime2=rpc:call(FirstNode, antidote, clocksi_iprepare, [TxId1]),
    ?assertEqual({aborted, TxId1}, CommitTime2),
    %% Test that the failed transaction didn't affect the `bcounter()'.
    Result1 = rpc:call(FirstNode, antidote, read, [Key, Type]),
    {ok, Counter1} = Result1,
    ?assertEqual(7, crdt_bcounter:permissions(Counter1)).
