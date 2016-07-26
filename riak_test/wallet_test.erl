-module(wallet_test).

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
  Key = wallet_key,
  Wallet = {Key, riak_dt_pncounter, bucket},

  %%% Specify invariant objects
  comm_test:objects(?MODULE, [Wallet]),

  Pid = self(),

  {_Re, CC} = comm_test:event(?MODULE, [2, Node1, [Wallet, 500]]),

  [?assertEqual(get_val(Node, Wallet, CC), 500) || Node <- [Node1, Node2, Node3]],

  CT1 = dc1_txns(Node1, Wallet, Pid),
  CT2 = dc2_txns(Node2, Wallet, Pid),
  CT3 = dc3_txns(Node3, Wallet, Pid),

  Time = dict:merge(fun(_K, T1, T2) -> max(T1, T2) end,
                           CT1,
                           dict:merge(fun(_K, T1, T2) -> max(T1, T2) end, CT2, CT3)),

  Vals = [get_val(Node, Wallet, Time) || Node <- [Node1, Node2, Node3]],

  lager:info("Vals: ~w", [Vals]),
  lager:info("Val on node 1: ~p", [get_val(Node1, Wallet, Time)]),

  Quiescence_val = lists:usort(Vals),
  ?assertMatch(Quiescence_val, [hd(Vals)]),

  %%lager:info("Cookie: ~p, Node: ~p", [erlang:get_cookie(), node()]),
  %%lager:info("Self: ~p~n riak_test: ~p", [self(), whereis(riak_test)]),

  pass.

get_val(Node, Wallet, Clock) ->
  {ok, Tx} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
  {ok, [Res]} = rpc:call(Node, antidote, read_objects, [[Wallet], Tx]),
  {ok, _CT1} = rpc:call(Node, antidote, commit_transaction, [Tx]),
  Res.

dc1_txns(Node, Wallet, ReplyTo) ->
  ?assertEqual(get_val(Node, Wallet, ignore), 500),
  par_txns(Node, Wallet, ReplyTo).%,
%%  ToWallet = {wallet2, riak_dt_pncounter, bucket},
%%  CT = comm_test:event(?MODULE, [3, Node, [Wallet, ToWallet, 300]]),
%%  CT.

dc2_txns(Node, Wallet, ReplyTo) ->
  ?assertEqual(get_val(Node, Wallet, ignore), 500),
  par_txns(Node, Wallet, ReplyTo).

dc3_txns(Node, Wallet, ReplyTo) ->
  ?assertEqual(get_val(Node, Wallet, ignore), 500),
  par_txns(Node, Wallet, ReplyTo).

par_txns(Node, Wallet, _ReplyTo) ->
  Amount1 = 300,
  {Res1, _CT1} = comm_test:event(?MODULE, [1, Node, [Wallet, Amount1]]),

  ?assertMatch(Res1, 200),

  Amount2 = 200,
  {Res2, CT2} = comm_test:event(?MODULE, [2, Node, [Wallet, Amount2]]),

  ?assertMatch(Res2, 400),

  %%ReplyTo ! {ok, CT2}.
  CT2.

%%%====================================
%%% Callbacks
%%%====================================
handle_event([1, Node, AppArgs]) ->
    [Wallet, N] = AppArgs,
    {Res1, {_Tx1, CT1}} = wallet:debit(Node, Wallet, N),
    {Key, Type, bucket} = Wallet,
    {ok, Res} = rpc:call(Node, antidote, read, [Key, Type]),
    io:format("~n:::::::::::::::::: DEBIT EVENT (1) FOR ::::::::::::::::::::~n~p~nWITH RESULT:~p [~p]", [AppArgs, Res, Res1]),
    {Res1, CT1};

handle_event([2, Node, AppArgs]) ->
    [Wallet, N] = AppArgs,
    {Res2, {_Tx2, CT2}} = wallet:credit(Node, Wallet, N),
    {Key, Type, bucket} = Wallet,
    {ok, Res} = rpc:call(Node, antidote, read, [Key, Type]),
    io:format("~n:::::::::::::::::: CREDIT EVENT (2) FOR ::::::::::::::::::::~n~p~nWITH RESULT:~p [~p]", [AppArgs, Res, Res2]),
    {Res2, CT2};

handle_event([3, Node, AppArgs]) ->
    [Wallet1, Wallet2, N] = AppArgs,
    CT = wallet:transfer(Node, Wallet1, Wallet2, N),
    CT.

handle_object_invariant(Node, [Wallet]) ->
  WalletVal = get_val(Node, Wallet, ignore),
  %%% if assert fails inform commander to provide a counter example
  io:format("~nWallet value:~p~n", [WalletVal]),
  ?assert(WalletVal >= 0),
  true.
