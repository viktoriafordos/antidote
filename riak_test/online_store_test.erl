-module(online_store_test).

%% API
-export([confirm/0, main_test/3, dc1_txns/3, dc2_txns/3, dc3_txns/3, handle_event/1]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->

  rt:setup_harness(dummy, dummy),

  NumVNodes = rt_config:get(num_vnodes, 8),
  rt:update_app_config(all, [{riak_core, [{ring_creation_size, NumVNodes}]}]),

  _Clean = rt_config:get(clean_cluster, true),
  [Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
  rt:wait_until_ring_converged(Cluster1),
  rt:wait_until_ring_converged(Cluster2),
  rt:wait_until_ring_converged(Cluster3),

  ok = common:setup_dc_manager([Cluster1, Cluster2, Cluster3], first_run),

  main_test(Cluster1, Cluster2, Cluster3),
  pass.

main_test(Cluster1, Cluster2, Cluster3) ->
  Node1 = hd(Cluster1),
  Node2 = hd(Cluster2),
  Node3 = hd(Cluster3),
  Key = store_key,
  Store = {Key, riak_dt_pncounter, bucket},
  Pid = self(),

  {_Re, CC} = comm_test:event(?MODULE, [2, Node1, [Store, 5]]),

  [?assertEqual(get_val_store(Node, Store, CC), 5) || Node <- [Node1, Node2, Node3]],

  CT1 = dc1_txns(Node1, Store, Pid),
  CT2 = dc2_txns(Node2, Store, Pid),
  CT3 = dc3_txns(Node3, Store, Pid),

  Time = dict:merge(fun(_K, T1, T2) -> max(T1, T2) end,
                           CT1,
                           dict:merge(fun(_K, T1, T2) -> max(T1, T2) end, CT2, CT3)),

  Vals = [get_val_store(Node, Store, Time) || Node <- [Node1, Node2, Node3]],

  lager:info("Vals: ~p", [Vals]),

  Quiescence_val = lists:usort(Vals),
  ?assertMatch(Quiescence_val, [hd(Vals)]),

  %%lager:info("Cookie: ~p, Node: ~p", [erlang:get_cookie(), node()]),
  %%lager:info("Self: ~p~n riak_test: ~p", [self(), whereis(riak_test)]),

  pass.

get_val_store(Node, Store, Clock) ->
  {ok, Tx} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
  {ok, [Res]} = rpc:call(Node, antidote, read_objects, [[Store], Tx]),
  {ok, _CT1} = rpc:call(Node, antidote, commit_transaction, [Tx]),
  Res.

dc1_txns(Node, Store, ReplyTo) ->
  ?assertEqual(get_val_store(Node, Store, ignore), 5),
  par_txns(Node, Store, ReplyTo).

dc2_txns(Node, Store, ReplyTo) ->
  ?assertEqual(get_val_store(Node, Store, ignore), 5),
  par_txns(Node, Store, ReplyTo).

dc3_txns(Node, Store, ReplyTo) ->
  ?assertEqual(get_val_store(Node, Store, ignore), 5),
  par_txns(Node, Store, ReplyTo).

par_txns(Node, Store, _ReplyTo) ->
  N1 = 3,
  {Res1, _CT1} = comm_test:event(?MODULE, [1, Node, [Store, N1]]),

  ?assertMatch(Res1, 2),

  N2 = 2,
  {Res2, CT2} = comm_test:event(?MODULE, [2, Node, [Store, N2]]),

  ?assertMatch(Res2, 4),

  %%ReplyTo ! {ok, CT2}.
  CT2.

%%%====================================
%%% Callbacks
%%%====================================
handle_event([1, Node, Args]) ->
    [Store, N] = Args,
    {Res1, {_Tx1, CT1}} = online_store:remove_from_store(Node, Store, N),
    %%?assertMatch(Res1, 2),
    {Res1, CT1};

handle_event([2, Node, Args]) ->
    [Store, N] = Args,
    {Res2, {_Tx2, CT2}} = online_store:add_to_store(Node, Store, N),
    %%?assertMatch(Res2, 4),
    {Res2, CT2}.