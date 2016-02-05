-module(shopping_cart_test).

%% API
-export([confirm/0, parallel_test/3, dc1_txns/3, dc2_txns/3, dc3_txns/3]).

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

  parallel_test(Cluster1, Cluster2, Cluster3),
  pass.

parallel_test(Cluster1, Cluster2, Cluster3) ->
  Node1 = hd(Cluster1),
  Node2 = hd(Cluster2),
  Node3 = hd(Cluster3),
  Key = parkey,
  Cart = {Key, riak_dt_pncounter, bucket},
  Pid = self(),

  set_val_cart(Node1, Cart, 5),

  timer:sleep(5000),
  [?assertEqual(get_val_cart(Node, Cart, ignore), 5) || Node <- [Node1, Node2, Node3]],

  spawn(?MODULE, dc1_txns, [Node1, Cart, Pid]),
  spawn(?MODULE, dc2_txns, [Node2, Cart, Pid]),
  spawn(?MODULE, dc3_txns, [Node3, Cart, Pid]),

  lager:info("Processes for each DC have been spawned..."),

  Result = receive
             {ok, CT1} ->
               lager:info("Received first snapshot..."),
               receive
                 {ok, CT2} ->
                   lager:info("Received second snapshot..."),
                   receive
                     {ok, CT3} ->
                       lager:info("Received third snapshot..."),
                       Time = dict:merge(fun(_K, T1, T2) -> max(T1, T2) end,
                         CT1,
                         dict:merge(fun(_K, T1, T2) -> max(T1, T2) end, CT2, CT3)),
                       Vals = [get_val_cart(Node, Cart, Time) || Node <- [Node1, Node2, Node3]],
                       Quiescence_val = lists:usort(Vals),
                       ?assertMatch(Quiescence_val, [hd(Vals)])
                   end
               end,
               pass
           end,
  ?assertMatch(Result, pass),
  pass.

get_val_cart(Node, Cart, Clock) ->
  {ok, Tx} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
  {ok, [Res]} = rpc:call(Node, antidote, read_objects, [[Cart], Tx]),
  {ok, _CT1} = rpc:call(Node, antidote, commit_transaction, [Tx]),
  Res.

%% Set initial number of items in the Cart to 5
set_val_cart(Node, Cart, Val) ->
  {ok, Tx} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  ok = rpc:call(Node, antidote, update_objects, [[{Cart, increment, Val}], Tx]),
  {ok, _CT} = rpc:call(Node, antidote, commit_transaction, [Tx]).

dc1_txns(Node, Cart, ReplyTo) ->
  par_txns(Node, Cart, ReplyTo).

dc2_txns(Node, Cart, ReplyTo) ->
  par_txns(Node, Cart, ReplyTo).

dc3_txns(Node, Cart, ReplyTo) ->
  par_txns(Node, Cart, ReplyTo).

par_txns(Node, Cart, ReplyTo) ->
  %% Tx1
  {Res1, {_Tx1, _CT1}} = shopping_cart:remove_from_cart(Node, Cart, 3),

  ?assertMatch(Res1, 2),

  %% Tx2
  {Res2, {_Tx2, CT2}} = shopping_cart:add_to_cart(Node, Cart, 2),
  ?assertMatch(Res2, 4),

  ReplyTo ! {ok, CT2}.