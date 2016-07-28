%%%-------------------------------------------------------------------
%%% @author maryam
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. May 2016 4:16 PM
%%%-------------------------------------------------------------------
-module(ad_counter_test).
-author("maryam").

%% API
-export([confirm/0, main_test/3, dc1_txns/3, dc2_txns/3, dc3_txns/3, handle_event/1, handle_object_invariant/2]).
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
  io:format("~nDC setup is done for clusters.~n"),

  main_test(Cluster1, Cluster2, Cluster3),
  pass.

main_test(Cluster1, Cluster2, Cluster3) ->
  Node1 = hd(Cluster1),
  Node2 = hd(Cluster2),
  Node3 = hd(Cluster3),
%%  MaxView = 5,
  Key = ad_key,
  Ad = {Key, riak_dt_pncounter, bucket},
  Pid = self(),

  %%% Specify invariant objects
  comm_test:objects(?MODULE, [Ad]),

  CT1 = dc1_txns(Node1, Ad, Pid),
  CT2 = dc2_txns(Node2, Ad, Pid),
  CT3 = dc3_txns(Node3, Ad, Pid),

  Time = dict:merge(fun(_K, T1, T2) -> max(T1, T2) end,
                      CT1,
                      dict:merge(fun(_K, T1, T2) -> max(T1, T2) end, CT2, CT3)),

  Vals = [ad_counter:get_val(Node, Ad, Time) || Node <- [Node1, Node2, Node3]],

  lager:info("Vals: ~p", [Vals]),

  Quiescence_val = lists:usort(Vals),
  ?assertMatch(Quiescence_val, [hd(Vals)]),

  %%lager:info("Cookie: ~p, Node: ~p", [erlang:get_cookie(), node()]),
  %%lager:info("Self: ~p~n riak_test: ~p", [self(), whereis(riak_test)]),

  pass.

dc1_txns(Node, Ad, ReplyTo) ->
  par_txns(Node, Ad, ReplyTo).

dc2_txns(Node, Ad, ReplyTo) ->
  par_txns(Node, Ad, ReplyTo).

dc3_txns(Node, Ad, ReplyTo) ->
  par_txns(Node, Ad, ReplyTo).

par_txns(Node, Ad, _ReplyTo) ->
  {Res1, _CT1} = comm_test:event(?MODULE, [1, Node, [Ad]]),

  ?assertMatch(Res1, 1),

  {Res2, CT2} = comm_test:event(?MODULE, [1, Node, [Ad]]),

  ?assertMatch(Res2, 2),
  CT2.

%%%====================================
%%% Callbacks
%%%====================================
handle_event([1, Node, AppArgs]) ->
  [Ad] = AppArgs,
  {Res1, {_Tx1, CT1}} = ad_counter:view_ad(Node, Ad),
  {Res1, CT1}.

handle_object_invariant(Node, [Ad]) ->
  AdVal = ad_counter:get_val(Node, Ad, ignore),
  %%% if assert fails inform commander to provide a counter example
  io:format("~nAd value:~p~n", [AdVal]),
  ?assert(AdVal =< 5 ),
  true.