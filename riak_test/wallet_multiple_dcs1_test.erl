-module(wallet_multiple_dcs1_test).

-compile({parse_transform, rt_intercept_pt}).

-export([confirm/0, multiple_credits/4]).
%%, parallel_credit_test/3]).

-include_lib("eunit/include/eunit.hrl").

-define(HARNESS, (rt_config:get(rt_harness))).

	%% TODO: update my_wallet functions to return an appropriate result,
	%% e.g., getbalance returns the value,
	%% e.g., credit and debit return {ok, commit_time()}
	
confirm() ->
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
   
    AllNodes = nodes(),
    lager:info("########Cluster Nodes: ~p#########~n", [AllNodes]),
    
    %% Add intercept for finish_updtae_dc
    Intercept = {inter_dc_repl_update, [{{finish_update_dc, 4}, finish_update_dc_intrcptd}]},
    [ok = rt_intercept:add(Node, Intercept) || Node <- AllNodes],
    
	parallel_credit_test(Cluster1, Cluster2, Cluster3),
	
	pass.
 
parallel_credit_test(Cluster1, Cluster2, Cluster3) ->
	Node1 = hd(Cluster1),
	Node2 = hd(Cluster2),
	Node3 = hd(Cluster3),
	Key = parkey,
	Pid = self(),
	Quiescent_Balance = 2550,
	
	spawn(?MODULE, multiple_credits, [Node1, Key, node1, Pid]),
	spawn(?MODULE, multiple_credits, [Node2, Key, node2, Pid]),
	spawn(?MODULE, multiple_credits, [Node3, Key, node3, Pid]),
	
	%%Wait until multiple_credits in all nodes executes and sends the commit time
	Result = receive
	    {ok, CT1} ->
		receive
		    {ok, CT2} ->
			receive
			    {ok, CT3} ->
				%%Get the maximum commit time, read values corresponding max CT 
				Time = dict:merge(fun(_K, T1, T2)->
				 			max(T1, T2)
						  end,
						  CT3, dict:merge(fun(_K, T1, T2) ->
									max(T1, T2)
								  end,
								  CT1, CT2)),
				ReadRes1 = my_walletapp1:getbalance(Node1, Key, Time),
				{ok, {_,[ReadVal1],_}} = ReadRes1,
				?assertEqual(Quiescent_Balance, ReadVal1),
				
				ReadRes2 = my_walletapp1:getbalance(Node2, Key, Time),
				{ok, {_,[ReadVal2],_}} = ReadRes2,
				?assertEqual(Quiescent_Balance, ReadVal2),
				
				ReadRes3 = my_walletapp1:getbalance(Node3, Key, Time),
				{ok, {_,[ReadVal3],_}} = ReadRes3,
				?assertEqual(Quiescent_Balance, ReadVal3),
				lager:info("Parallel credits and debits passed!"),
				pass
			end
		end
	end,
	?assertEqual(Result, pass),
	pass.
	
multiple_credits(Node, Key, Actor, ReplyTo) ->
	CreditRes1 = my_walletapp1:credit(Node, Key, 500, Actor),
	?assertMatch({ok, _}, CreditRes1),
		
	CreditRes2 = my_walletapp1:credit(Node, Key, 400, Actor),
	?assertMatch({ok, _}, CreditRes2),
	
	CreditRes3 = my_walletapp1:credit(Node, Key, 300, Actor),
	?assertMatch({ok, _}, CreditRes3),
	
	DebitRes4 = my_walletapp1:debit(Node, Key, 150, Actor),
	?assertMatch({ok, _}, DebitRes4),
	
	DebitRes5 = my_walletapp1:debit(Node, Key, 200, Actor),
	?assertMatch({ok, _}, DebitRes5),
	
	{ok, {_,_,CommitTime}} = DebitRes5,
	ReplyTo ! {ok, CommitTime}.
