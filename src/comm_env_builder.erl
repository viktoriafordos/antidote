-module(comm_env_builder).

-behaviour(gen_server).

-include_lib("eunit/include/eunit.hrl").
-include("antidote.hrl").
-include("inter_dc_repl.hrl").
-include("commander.hrl").

%% Public API
-export([start_link/0,
        stop/0,
        build_clusters/1,
        leave_cluster/1]).

%% callbacks
-export([init/1,
		 handle_cast/2,
		 handle_call/3,
		 handle_info/2,
		 code_change/3,
		 terminate/2
		 ]).

-define(SERVER, ?MODULE).
-record(state, {}).

%%%====================================
%%% Public API
%%%====================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Builds a group of data centers with the specified settings, and connect them together as the testing environment.
-spec build_clusters([pos_integer()]) -> [dc()].
build_clusters(Settings) ->
	gen_server:call(?SERVER, {build_clusters, {Settings}}).

%% @doc Disconnects the specified node from its DC
-spec leave_cluster(node()) -> ok.
leave_cluster(Node) ->
	gen_server:call(?SERVER, {leave_cluster, {Node}}).

stop() ->
    gen_server:cast(?SERVER, stop).

%%%====================================
%%% Callbacks
%%%====================================
init([]) ->
    lager:info("comm_env_builder started on node ~p", [node()]),
    {ok, #state{}, 0}.

handle_call({leave_cluster, {Node}}, _From, State) ->
	ok = leave(Node),
	{reply, ok, State};
	
handle_call({build_clusters, {Settings}}, _From, State) ->
	Clusters = establish_clusters(Settings),
	[begin
		join_cluster(Nodes),
		lager:info("Cluster built: ~p", [Nodes])
	end || Nodes <- Clusters],
	connect_clusters(Clusters),
    {reply, Clusters, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

%%%====================================
%%% Internal functions
%%%====================================
%% @doc Establishes and returns a list of nodes for each DC
-spec establish_clusters([pos_integer()]) -> [dc()].
establish_clusters(Settings) ->
    Available_NumNodes = comm_config:fetch(available_num_nodes),
    Requested_NumNodes = lists:foldl(fun(V, Sum) -> V+Sum end, 0, Settings),
    case (Requested_NumNodes =< Available_NumNodes) of
        false ->
            erlang:error("Requested number nodes are not available!");
        true ->
            Nodes = setup_nodes(Requested_NumNodes), %%['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1', 'dev4@127.0.0.1'],
            {Clusters_Nodes, _} = lists:foldl(
                fun(DC_NumNodes, {DCs, Rem_Nodes}) ->
                    {DC_Nodes, Rest} = lists:split(DC_NumNodes,Rem_Nodes),
                    {DCs ++ [DC_Nodes], Rest}
                end, {[], Nodes}, Settings),
            Clusters_Nodes
    end.

%% @doc Starts the specified number of nodes and ensures all nodes are pingable and singleton nodes.
-spec setup_nodes(pos_integer()) -> [node()].
setup_nodes(Requested_NumNodes) ->
    Node_Numbers = lists:seq(1, Requested_NumNodes),
    Nodes = [?DEV(N) || N <- Node_Numbers],
    [run_antidote_cmd(?NODES_PATH, N, "start") || N <- Node_Numbers],

    [ok = wait_until_pingable(Node) || Node <- Nodes],
    io:format("All nodes are pingable. ~n"),

    [ok = wait_until_registered(Node, riak_core_ring_manager) || Node <- Nodes],
    io:format("ring manager is registered on all nodes. ~n"),

    [ok = check_singleton_node(Node) || Node <- Nodes],
    io:format("All nodes are singleton. ~n"),
    Nodes.

-spec check_singleton_node(node()) -> ok.
check_singleton_node(Node) ->
    Ring = get_ring(Node),
    Owners = lists:usort([Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)]),
    ?assertEqual([Node], Owners),
    ok.

-spec wait_until_registered(node(), atom()) -> ok.
wait_until_registered(Node, Name) ->
    Fun = fun() ->
        Registered = rpc:call(Node, erlang, registered, []),
        lists:member(Name, Registered)
          end,
    ?assertEqual(ok, wait_until(Fun)),
    ok.

-spec wait_until_pingable(node()) -> ok.
wait_until_pingable(N) ->
    Fun = fun(N1) ->
        pong =:= net_adm:ping(N1)
          end,
    ?assertEqual(ok, wait_until(N, Fun)),
    ok.

wait_until(Node, Fun) ->
    wait_until(fun() -> Fun(Node) end).

wait_until(Fun) when is_function(Fun) ->
    WaitTime = comm_config:fetch(comm_wait_time),
    DelayTime = comm_config:fetch(comm_retry_delay),
    RetryNum = WaitTime div DelayTime,
    wait_until(Fun, RetryNum, DelayTime).

wait_until(Fun, RetryNum, DelayTime) ->
    Res = Fun(),
    case Res of
        true ->
            ok;
        _ when RetryNum == 1 ->
            {fail, Res};
        _ ->
            timer:sleep(DelayTime),
            wait_until(Fun, RetryNum-1, DelayTime)
    end.

%% @doc Runs an Antidote command from OS shell.
run_antidote_cmd(Path, N, Cmd) ->
    os:cmd(antidote_cmd(Path, N, Cmd)).

%% @doc Creates a shell command from the specified parameters: Path of devN, N, Antidote command
-spec antidote_cmd(string(), pos_integer(), string()) -> string().
antidote_cmd(Path, N, Cmd) ->
    Exe_Name = comm_config:fetch(executable_name),
    io_lib:format("~sdev~b/bin/~s ~s", [Path, N, Exe_Name, Cmd]).

%% @doc Connects the specified cluster DCs to build the testing environment
-spec connect_clusters([dc()]) -> ok.
connect_clusters(Clusters) ->
	  lager:info("Connecting clusters data centers ..."),
    lists:foreach(fun(Cluster) ->
                    Node1 = hd(Cluster),
                    lager:info("Waiting until required services start on node ~p", [Node1]),
                    wait_until_registered(Node1, inter_dc_pub),
                    wait_until_registered(Node1, inter_dc_log_reader_response),
                    wait_until_registered(Node1, inter_dc_log_reader_query),
                    wait_until_registered(Node1, inter_dc_sub),
                    rpc:call(Node1, inter_dc_manager, start_bg_processes, [stable])
                  end, Clusters),
    Descriptors = descriptors(Clusters),
    lists:foreach(fun(Cluster) ->
            Node = hd(Cluster),
            lager:info("Making node ~p observe other Clusters...", [Node]),
            rpc:call(Node, inter_dc_manager, observe_dcs_sync, [Descriptors])
          end, Clusters),
    lager:info("Clusters connected!"),
    ok.

%% @doc Gets the list of descriptors of cluster DCs.
-spec descriptors([dc()]) -> [#descriptor{}].
descriptors(Clusters) ->
    lists:map(fun(Cluster) ->
                {ok, Descriptor} = rpc:call(hd(Cluster), inter_dc_manager, get_descriptor, []),
                Descriptor
              end, Clusters).

%% @doc Builds a cluster DC by joining the specified nodes together.
-spec join_cluster([node()]) -> ok.
join_cluster(Nodes) ->
    [?assertEqual([Node], owners_according_to(Node)) || Node <- Nodes],

    [Node1|OtherNodes] = Nodes,
    case OtherNodes of
        [] ->
            ok;
        _ ->
            [staged_join(Node, Node1) || Node <- OtherNodes],
            plan_and_commit(Node1),
            try_nodes_ready(Nodes, 3, 500)
    end,

    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),

    wait_until_nodes_agree_about_ownership(Nodes),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)),
    ok.

%% @doc Gets the list of all owners nodes of the same ring which the specified node owns.
-spec owners_according_to(node()) -> list().
owners_according_to(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- riak_core_ring:all_owners(Ring)],
            lists:usort(Owners);
        {badrpc, _}=BadRpc ->
            BadRpc
    end.

staged_join(Node, PNode) ->
    %% `riak_core:staged_join/1' can now return an `{error,
    %% node_still_starting}' tuple which indicates retry.
    Fun = fun() -> rpc:call(Node, riak_core, staged_join, [PNode]) end,
    ?assertEqual(ok, join_with_retry(Fun)),
    ok.

plan_and_commit(Node) ->
    timer:sleep(500),
    lager:info("planning and commiting cluster join"),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            lager:info("plan: ring not ready"),
            timer:sleep(100),
            plan_and_commit(Node);
        {ok, _, _} ->
            lager:info("plan: done"),
            do_commit(Node)
    end.

do_commit(Node) ->
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            lager:info("commit: plan changed"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            plan_and_commit(Node);
        {error, ring_not_ready} ->
            lager:info("commit: ring not ready"),
            timer:sleep(100),
            maybe_wait_for_changes(Node),
            do_commit(Node);
        {error,nothing_planned} ->
            ok;
        ok ->
            ok
    end.

maybe_wait_for_changes(Node) ->
    Ring = get_ring(Node),
    Changes = riak_core_ring:pending_changes(Ring),
    Joining = riak_core_ring:members(Ring, [joining]),
    lager:info("maybe_wait_for_changes, changes: ~p joining: ~p",
               [Changes, Joining]),
    if Changes =:= [] ->
            ok;
       Joining =/= [] ->
            ok;
       true ->
            ok = wait_until_no_pending_changes([Node])
    end.

%% @doc Gets the raw ring for the specified node.
get_ring(Node) ->
    {ok, Ring} = rpc:call(Node, riak_core_ring_manager, get_raw_ring, []),
    Ring.
    
%% @doc Disconnects the specified node from the cluster DC.
leave(Node) ->
    R = rpc:call(Node, riak_core, leave, []),
    lager:info("[leave] ~p: ~p", [Node, R]),
    ?assertEqual(ok, R),
    ok.

try_nodes_ready([Node1 | _Nodes], 0, _SleepMs) ->
    lager:info("Nodes not ready after initial plan/commit, retrying"),
    plan_and_commit(Node1);
try_nodes_ready(Nodes, N, SleepMs) ->
    ReadyNodes = [Node || Node <- Nodes, is_ready(Node) =:= true],
    case ReadyNodes of
        Nodes ->
            ok;
        _ ->
            timer:sleep(SleepMs),
            try_nodes_ready(Nodes, N-1, SleepMs)
    end.
 
%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%      See {@link wait_until_ready/1} for definition of ready.
wait_until_nodes_ready(Nodes) ->
    lager:info("Wait until nodes are ready : ~p", [Nodes]),
    [?assertEqual(ok, wait_until(Node, fun is_ready/1)) || Node <- Nodes],
    ok.	 
 
%% @private
is_ready(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            case lists:member(Node, riak_core_ring:ready_members(Ring)) of
                true -> true;
                false -> {not_ready, Node}
            end;
        Other ->
            Other
    end.

wait_until_nodes_agree_about_ownership(Nodes) ->
    lager:info("Wait until nodes agree about ownership ~p", [Nodes]),
    Results = [ wait_until_owners_according_to(Node, Nodes) || Node <- Nodes ],
    ?assert(lists:all(fun(X) -> ok =:= X end, Results)).

wait_until_owners_according_to(Node, Nodes) ->
    SortedNodes = lists:usort(Nodes),
    F = fun(N) ->
        owners_according_to(N) =:= SortedNodes
    end,
    ?assertEqual(ok, wait_until(Node, F)),
    ok.

%% @doc Given a list of nodes, wait until all nodes believe there are no
%% on-going or pending ownership transfers.
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes(Nodes) ->
    lager:info("Wait until no pending changes on ~p", [Nodes]),
    F = fun() ->
                rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
                {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
                Changes = [ riak_core_ring:pending_changes(Ring) =:= [] || {ok, Ring} <- Rings ],
                BadNodes =:= [] andalso length(Changes) =:= length(Nodes) andalso lists:all(fun(T) -> T end, Changes)
        end,
    ?assertEqual(ok, wait_until(F)),
    ok.
 
%% Ideally we'd use `wait_until' for join retries, but it isn't
%% currently flexible enough to only retry on a specific error
%% tuple. Rather than rework all of the various arities, steal the
%% concept and worry about this later if the need arises.
join_with_retry(Fun) ->
    MaxTime = comm_config:fetch(comm_wait_time),
    Delay = comm_config:fetch(comm_retry_delay),
    Retry = MaxTime div Delay,
    join_retry(Fun(), Fun, Retry, Delay).

join_retry(ok, _Fun, _Retry, _Delay) ->
    ok;
join_retry({error, node_still_starting}, _Fun, 0, _Delay) ->
    lager:warning("Too many retries, join failed"),
    {error, too_many_retries};
join_retry({error, node_still_starting}, Fun, RetryCount, Delay) ->
    lager:warning("Join error because node is not yet ready, retrying after ~Bms", [Delay]),
    timer:sleep(Delay),
    join_retry(Fun(), Fun, RetryCount - 1, Delay);
join_retry(Error, _Fun, _Retry, _Delay) ->
    Error.
