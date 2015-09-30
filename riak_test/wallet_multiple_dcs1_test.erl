-module(wallet_multiple_dcs1_test).

-export([confirm/0, multiple_credits1/4, multiple_credits2/4]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).
	
confirm() ->
	%% TODO: get this from command line
	Phase = record, %% replay
	[Cluster1, Cluster2, Cluster3] = rt:build_clusters([1,1,1]),
	HeadCluster1 = hd(Cluster1),
	HeadCluster2 = hd(Cluster2),
	HeadCluster3 = hd(Cluster3),

	rt:wait_until_ring_converged(Cluster1),
	rt:wait_until_ring_converged(Cluster2),
	rt:wait_until_ring_converged(Cluster3),
	
	%% Wait for inter_dc_manager to be up
	rt:wait_until_registered(HeadCluster1, inter_dc_manager),
    rt:wait_until_registered(HeadCluster2, inter_dc_manager),
    rt:wait_until_registered(HeadCluster3, inter_dc_manager),
	
	{ok, DC1} = rpc:call(HeadCluster1, inter_dc_manager, start_receiver,[8091]),
	{ok, DC2} = rpc:call(HeadCluster2, inter_dc_manager, start_receiver,[8092]),
	{ok, DC3} = rpc:call(HeadCluster3, inter_dc_manager, start_receiver,[8093]),
	lager:info("Receivers start results ~p, ~p, and ~p", [DC1, DC2, DC3]),

	ok = rpc:call(HeadCluster1, inter_dc_manager, add_list_dcs,[[DC2, DC3]]),
	ok = rpc:call(HeadCluster2, inter_dc_manager, add_list_dcs,[[DC1, DC3]]),
	ok = rpc:call(HeadCluster3, inter_dc_manager, add_list_dcs,[[DC1, DC2]]),
     
    {ok, _} = rpc:call(HeadCluster1, commander, start_link, [Phase]),

    PPP = rpc:call(HeadCluster1, commander, get_phase, []),
    lager:info("~ncommander_srv started in phase ~p~n",[PPP]),
    
	parallel_credit_test(Cluster2, Cluster3),
	
	pass.
 
parallel_credit_test(Cluster2, Cluster3) ->
	Node2 = hd(Cluster2),
	Node3 = hd(Cluster3),
	Key = parkey,
	Pid = self(),
	Quiescent_Balance = 900,
	
	P = rpc:call('dev1@127.0.0.1', commander, get_phase, []),
	lager:info("00000000======00000000~n~p~n", [P]),
	
	CreditRes = my_walletapp1:credit(Node2, Key, 200, node2),
	?assertMatch({ok, _}, CreditRes),
	
	
	spawn(?MODULE, multiple_credits1, [Node2, Key, node2, Pid]),
	spawn(?MODULE, multiple_credits2, [Node3, Key, node3, Pid]),
	%%Wait until multiple_credits in all nodes executes and sends the commit time
	Result = receive
	 {ok, CT2} ->
		    receive
			    {ok, CT3} ->
				Time = dict:merge(fun(_K, T1, T2) ->
									max(T1, T2)
								  end,
								  CT3, CT2),
				ReadRes2 = my_walletapp1:getbalance(Node2, Key, Time),
				{ok, {_,[ReadVal2],_}} = ReadRes2,
				?assertEqual(Quiescent_Balance, ReadVal2),
				
				ReadRes3 = my_walletapp1:getbalance(Node3, Key, Time),
				{ok, {_,[ReadVal3],_}} = ReadRes3,
				?assertEqual(Quiescent_Balance, ReadVal3),
				lager:info("Parallel credits and debits passed!"),
				pass
			end
		%%end
	end,
	?assertEqual(Result, pass),
	pass.

multiple_credits1(Node, Key, Actor, ReplyTo) ->	
	CreditRes1 = my_walletapp1:credit(Node, Key, 600, Actor),
	?assertMatch({ok, _}, CreditRes1),

	DebitRes2 = my_walletapp1:debit(Node, Key, 200, Actor),
	?assertMatch({ok, _}, DebitRes2),
	
	{ok, {_,_,CommitTime}} = DebitRes2,
	ReplyTo ! {ok, CommitTime}.
	
multiple_credits2(Node, Key, Actor, ReplyTo) ->
	CreditRes1 = my_walletapp1:credit(Node, Key, 500, Actor),
	?assertMatch({ok, _}, CreditRes1),
	
	DebitRes1 = my_walletapp1:debit(Node, Key, 200, Actor),
	?assertMatch({ok, _}, DebitRes1),
	
	{ok, {_,_,CommitTime}} = DebitRes1,
	ReplyTo ! {ok, CommitTime}.
