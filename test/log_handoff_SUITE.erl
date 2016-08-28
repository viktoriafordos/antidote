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
-module(log_handoff_SUITE).

-compile({parse_transform, lager_transform}).

%% common_test callbacks
-export([
         init_per_suite/1,
         end_per_suite/1,
         init_per_testcase/2,
         end_per_testcase/2,
         all/0]).

%% tests
-export([log_test/1]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/inet.hrl").

init_per_suite(Config) ->
    lager_common_test_backend:bounce(debug),
    test_utils:at_init_testsuite(),
    %Clusters = test_utils:set_up_clusters_common(Config),
    Nodes = hd(Clusters),
    RootNode = hd(Nodes),
    NTestItems = 10,
  
    lager:info("Testing handoff (items ~p)", [NTestItems]),

    lager:info("Spinning up test nodes"),
    test_utils:wait_for_service(RootNode, logging),


    lager:info("Waiting until vnodes are started up"),
    rt:wait_until(RootNode,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),

    lager:info("Populating root node."),
    multiple_writes(RootNode, 1, NTestItems),

    %% Test handoff on each node:
    lager:info("Testing handoff for cluster."),
    lists:foreach(fun(TestNode) -> 
                          test_handoff(RootNode, TestNode, NTestItems)
                  end, TestNodes),


    [{nodes, Nodes}|Config].

end_per_suite(Config) ->
    %% Prepare for the next call to our test (we aren't polite about it,
    %% it's faster that way):
%    lager:info("Bringing down test nodes."),
%    lists:foreach(fun(N) -> rt:brutal_kill(N) end, TestNodes),

    %% The "root" node can't leave() since it's the only node left:
%    lager:info("Stopping root node."),
%    rt:brutal_kill(RootNode),

    Config.

init_per_testcase(_Case, Config) ->
    Config.

end_per_testcase(_, _) ->
    ok.

all() -> [handoff_test].

    
%% See if we get the same data back from our new nodes as we put into the root node:
handoff_test(RootNode, NewNode, NTestItems) ->

    lager:info("Waiting for service on new node."),
    test_utils:wait_for_service(NewNode, logging),

    lager:info("Joining new node with cluster."),
    test_utils:join(NewNode, RootNode),
    ?assertEqual(ok, test_utils:wait_until_nodes_ready([RootNode, NewNode])),
    test_utils:wait_until_no_pending_changes([RootNode, NewNode]),
    lager:info("Waiting until vnodes are started up"),
    test_utils:wait_until(RootNode,fun wait_init:check_ready/1),
    lager:info("Vnodes are started up"),



    %% See if we get the same data back from the joined node that we
    %% added to the root node.  Note: systest_read() returns
    %% /non-matching/ items, so getting nothing back is good:
    lager:info("Validating data after handoff:"),
    Results = multiple_reads(NewNode, 1, NTestItems),
    lager:info("The read data looks like: ~p", [Results]),
    ?assertEqual(0, length(Results)),
    lager:info("Data looks ok."),

    pass.

multiple_writes(Node, Start, End)->
    F = fun(N, Acc) ->
            case rpc:call(Node, antidote, append, [N, antidote_crdt_gset, {add, {N, actor}}]) of
                {ok, _} ->
                    Acc;
                Other ->
                    [{N, Other} | Acc]
            end
    end,
    Res = lists:foldl(F, [], lists:seq(Start, End)),
    ?assertEqual(0, length(Res)).

multiple_reads(Node, Start, End) ->
    F = fun(N, Acc) ->
            case rpc:call(Node, antidote, read, [N, antidote_crdt_gset]) of
                {error, _} ->
                    [{N, error} | Acc];
                {ok, [Value]} ->
                    case Value =:= N of
                        true ->
                            Acc;
                        false ->
                            [{N, {wrong_val, Value}} | Acc]
                    end
            end
    end,
    lists:foldl(F, [], lists:seq(Start, End)).