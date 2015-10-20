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
-module(opbcounter_manager_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

%% Test entry point.
confirm() ->
    % Must be a power of 2, minimum 8 and maximum 1024.
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, 8}]}
                             ]),
    [Cluster1, Cluster2] = rt:build_clusters([1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),

    rt:wait_until_registered(HeadCluster1, inter_dc_manager),
    rt:wait_until_registered(HeadCluster2, inter_dc_manager),

    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[8091]),
    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[8092]),
    lager:info("Receivers start results ~p and ~p", [DC1, DC2]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_dc, [DC2]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_dc, [DC1]),

    decrement_test(Cluster1, Cluster2, DC1, DC2),
    pass.

%% Tests decrementing a `bcounter()'.
decrement_test(Cluster1, Cluster2, DC1, DC2) ->
    {Id1, _} = DC1,
    {Id2, _} = DC2,
    [Node1 | _] = Cluster1,
    [Node2 | _] = Cluster2,
    lager:info("decrement_test started"),
    Type = crdt_bcounter,
    Key = bcounter,
    %% Test an allowed chain of operations.
    Result0 = rpc:call(Node1, antidote, clocksi_execute_tx, [[
                                                              {update, {Key, Type, {{increment, 7}, Id1}}},
                                                              {update, {Key, Type, {{decrement, 2}, Id1}}},
                                                              {read, {Key, Type}}
                                                             ]]),
    {ok,{_,_,VClock0}} = Result0,
    lager:info("Done 1st transaction with VClock ~p", [VClock0]),
    Result1 = rpc:call(Node2, antidote, clocksi_execute_tx, [VClock0, [
                                                                       {update, {Key, Type, {{increment, 5}, Id2}}},
                                                                       {update, {Key, Type, {{decrement, 3}, Id2}}},
                                                                       {read, {Key, Type}}
                                                                      ]]),
    {ok, {_, [Counter1], VClock1}} = Result1,
    lager:info("Done 2nd transaction with VClock ~p", [VClock1]),
    ?assertEqual(7, crdt_bcounter:permissions(Counter1)),
    ?assertEqual(5, crdt_bcounter:local_permissions(Id1,Counter1)),
    ?assertEqual(2, crdt_bcounter:local_permissions(Id2,Counter1)),
    %% Test a forbidden chain of operations.
    Result2 = rpc:call(Node2, antidote, clocksi_execute_tx, [ VClock1, [
                                                                        {update, {Key, Type, {{decrement, 3}, Id2}}},
                                                                        {read, {Key, Type}}
                                                                       ]]),
    lager:info("Done 3rd transaction", []),
    ?assertEqual({error, no_permissions}, Result2).
