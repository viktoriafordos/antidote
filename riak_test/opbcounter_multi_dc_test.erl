-module(opbcounter_multi_dc_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).


confirm() ->

    %% This resets nodes, cleans up stale directories, etc.:
    lager:info("Cleaning up..."),
    rt:setup_harness(dummy, dummy),

    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
                              {riak_core, [{ring_creation_size, NumVNodes}]}
                             ]),
    [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
    HeadCluster1 = hd(Cluster1),
    HeadCluster2 = hd(Cluster2),
    HeadCluster3 = hd(Cluster3),

    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),
    rt:wait_until_ring_converged(Cluster3),

    rt:wait_until_registered(HeadCluster1, inter_dc_manager),
    rt:wait_until_registered(HeadCluster2, inter_dc_manager),
    rt:wait_until_registered(HeadCluster3, inter_dc_manager),

    {ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[8091]),
    {ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[8092]),
    {ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[8093]),
    lager:info("Receivers start results ~p, ~p and ~p", [DC1, DC2, DC3]),

    ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_dcs,[[DC2, DC3]]),
    ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_dcs,[[DC1, DC3]]),
    ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_dcs,[[DC1, DC2]]),

    rt:wait_until(hd(Cluster1),fun wait_init:check_ready/1),
    rt:wait_until(hd(Cluster2),fun wait_init:check_ready/1),
    rt:wait_until(hd(Cluster3),fun wait_init:check_ready/1),

    new_bcounter_test(key1, HeadCluster1),
    read_increment_at_dc_test(key2, HeadCluster1),
    transfer_test(key3, HeadCluster1, HeadCluster2),
    request_resources_test(key4, HeadCluster1, HeadCluster2),
    pass.


%% Tests creating a new `bcounter()'.
new_bcounter_test(Key, Node) ->
    lager:info("new_bcounter_test started"),
    Type = crdt_bcounter,
    %% Test reading a new key of type `crdt_bcounter' creates a new `bcounter()'.
    Result0 = rpc:call(Node, antidote, read, [Key, Type]),
    Counter0 = crdt_bcounter:new(),
    ?assertEqual({ok, Counter0}, Result0).

%% Tests reading a `bcounter()' and incrementing it.
read_increment_at_dc_test(Key, Node) ->
    lager:info("read_increment_at_dc_test started"),
    Type = crdt_bcounter,
    %% Test simple read and write operations.
    Read0 = rpc:call(Node, antidote, read, [Key, Type]),
    {ok, Counter0} = Read0,
    ReadValue = crdt_bcounter:permissions(Counter0),
    Result = rpc:call(Node, antidote, append,
                      [Key, Type, {{increment, 10}, a}]),
    ?assertMatch({ok, _}, Result),
    {ok,{_,_,CommitTime}} = Result,
    Read1 = rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, Type]),
    {ok, {_,[Counter1],_}} = Read1,
    ?assertEqual(ReadValue + 10, crdt_bcounter:permissions(Counter1)).

transfer_test(Key, Node1, Node2) ->
    lager:info("transfer_test started"),
    Type = crdt_bcounter,
    Result0 = rpc:call(Node2, antidote, append,
                       [Key, Type, {{increment, 10}, b}]),
    ?assertMatch({ok, _}, Result0),
    {ok, {_,_,_}} = Result0,

    Result1 = rpc:call(Node1, antidote, append,
                       [Key, Type, {{increment, 1}, a}]),
    ?assertMatch({ok, _}, Result1),
    {ok,{_,_,CommitTime1}} = Result1,

    Read0 = rpc:call(Node2, antidote, clocksi_read, [CommitTime1, Key, Type]),
    {ok, {_,[Counter0],_}} = Read0,

    Id1 = get_node_id_from_node_name(Node1, Counter0),

    Result = rpc:call(Node2, antidote, append,
                      [Key, Type, {{transfer, 2, Id1}, b}]),
    ?assertMatch({ok, _}, Result),
    {ok, {_, _, CommitTime2}} = Result,

    Read1 = rpc:call(Node1, antidote, clocksi_read, [CommitTime2, Key, Type]),
    {ok, {_,[Counter1],_}} = Read1,

    Id2 = get_node_id_from_node_name(Node2, Counter1),

    ?assertEqual(3,
                 crdt_bcounter:local_permissions(Id1, Counter1)),
    ?assertEqual(8,
                 crdt_bcounter:local_permissions(Id2, Counter1)).

request_resources_test(Key, Node1, Node2) ->
    lager:info("request_resources_test started"),
    Type = crdt_bcounter,

    Result0 = rpc:call(Node2, antidote, append,
                       [Key, Type, {{increment, 10}, b}]),
    ?assertMatch({ok, _}, Result0),
    {ok, {_,_,CommitTime0}} = Result0,

    Result1 = rpc:call(Node1, antidote, append,
                       [Key, Type, {{increment, 1}, a}]),
    ?assertMatch({ok, _}, Result1),
    {ok,{_,_,_CommitTime1}} = Result1,

    rpc:call(Node1, antidote, clocksi_read,
             [CommitTime0, Key, Type]),

    decrement_loop(Node1, Key, Type, 5, start),
    pass.


decrement_loop(_Node, _Key, _Type, _Value, {ok, _}=Res) -> Res;

decrement_loop(Node, Key, Type, Value, {error, no_permissions}) ->
    timer:sleep(1000),
    decrement_loop(Node, Key, Type, Value, retry);

decrement_loop(Node, Key, Type, Value, _) ->
    Result = rpc:call(Node, antidote, append,
                      [Key, Type, {{decrement, Value}, a}]),
    decrement_loop(Node, Key, Type, Value, Result).

get_node_id_from_node_name(SearchName, {_,D}) ->
    orddict:fold(
      fun({NodeName, _}=NodeId, _, In) ->
              case NodeName of
                  SearchName -> NodeId;
                  _ -> In
              end
      end, nil, D).

