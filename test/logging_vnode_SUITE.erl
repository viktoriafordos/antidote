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

-module(logging_vnode_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([read_pncounter_initial_value_not_in_ets_test/1,
    read_pncounter_old_value_not_in_ets_test/1,
    read_orset_old_value_not_in_ets_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    lager_common_test_backend:bounce(debug),
    test_utils:at_init_testsuite(),
    Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [read_pncounter_initial_value_not_in_ets_test,
    read_pncounter_old_value_not_in_ets_test,
    read_orset_old_value_not_in_ets_test].


%% First we remember the initial time of the counter (with value 0).
%% After 15 updates, the smallest snapshot in ets table is 10,
%% but if we read the initial value of the counter,
%% we get 0, because no operations have a smaller commit time in the
%% reconstruction of time 0 in the log. (this is a corner case)
read_pncounter_initial_value_not_in_ets_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("read_snapshot_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = antidote_crdt_counter,
    Key = initial_value_test,
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    increment_counter(FirstNode, Key, 15),
    %% old read value is 0
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    ?assertEqual(0, ReadResult1),
    %% most recent read value is 15
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode,
        antidote, clocksi_read, [Key, Type]),
    ?assertEqual(15, ReadResult2),
    lager:info("read_pncounter_initial_value_not_in_ets_test OK").

%% First increment a counter 5 times and remember the snapshot.
%% Then we increment the counter 15 times more so the old value is no
%% in the ets. Reading the old value returns 5, and the new value is
%% 20, as expected.
read_pncounter_old_value_not_in_ets_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("read_old_value_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = antidote_crdt_counter,
    Key = read_old_val,
    increment_counter(FirstNode, Key, 5),
    {ok, {_, [ReadResult], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    ?assertEqual(5, ReadResult),
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    increment_counter(FirstNode, Key, 15),
    %% old read value is 5
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    ?assertEqual(5, ReadResult1),
    %% most recent read value is 20
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    ?assertEqual(20, ReadResult2),
    lager:info("read_pncounter_old_value_not_in_ets_test OK").

%% Add number 1 to 5 to an orset and remember the snapshot.
%% Then we add numbers 6 to 20 and read the old value not in the ets.
read_orset_old_value_not_in_ets_test(Config) ->
    Nodes = proplists:get_value(nodes, Config),
    lager:info("read_pncounter_old_value_not_in_ets_test started"),
    FirstNode = hd(Nodes),
    Type = antidote_crdt_orset,
    Key = read_orset_old_val,
    add_elements_starting_from(FirstNode, Key, 5, 0),
    {ok, {_, [ReadResult], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    ?assertEqual([1, 2, 3, 4, 5], ReadResult),
    {ok, TxId} = rpc:call(FirstNode, antidote, clocksi_istart_tx, []),
    add_elements_starting_from(FirstNode, Key, 15, 5),
    {ok, ReadResult1} = rpc:call(FirstNode,
        antidote, clocksi_iread, [TxId, Key, Type]),
    %% old read value contains elements 1 to 5
    ?assertEqual([1, 2, 3, 4, 5], ReadResult1),
    {ok, {_, [ReadResult2], _}} = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{read, {Key, Type}}]]),
    %% most recent read vale contains elements 1 to 20
    ?assertEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 15, 16, 17, 18, 19, 20], ReadResult2),
    lager:info("read_pncounter_old_value_not_in_ets_test OK").

%% Auxiliary method to increment a counter N times.
increment_counter(_FirstNode, _Key, 0) ->
    ok;
increment_counter(FirstNode, Key, N) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, antidote_crdt_counter, increment}}]]),
    ?assertMatch({ok, _}, WriteResult),
    increment_counter(FirstNode, Key, N - 1).

%% Auxiliary method that adds numbers in the range
%% (Start, Start + N] to an orset.
add_elements_starting_from(_FirstNode, _key, 0, _start) ->
    ok;
add_elements_starting_from(FirstNode, Key, N, Start) ->
    WriteResult = rpc:call(FirstNode, antidote, clocksi_execute_tx,
        [[{update, {Key, antidote_crdt_orset, {add, Start + N}}}]]),
    ?assertMatch({ok, _}, WriteResult),
    add_elements_starting_from(FirstNode, Key, N - 1, Start).