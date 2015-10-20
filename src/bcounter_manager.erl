-module(bcounter_manager).
-behaviour(gen_server).

-export([start_link/0,
         request_permissions/6]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {requests}).

-define(LEASE_DELAY, 5000).
-define(REBALANCE_DELAY, 5000).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

request_permissions(Node, Key, Time, Counter, Id, Amount) ->
    Permissions = crdt_bcounter:value(Counter),
    if
        Permissions >= Amount ->
            gen_server:cast({?MODULE, Node}, {request_permissions,
                                              {Key, Time, Counter, Id, Amount}});
        true -> ok
    end.

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    lager:info("Bounded Counter manager started in ~p.",[node()]),
    {ok, #state{
            requests = ets:new(bcounter_requests, [])
           }}.

handle_call(_Info, _From, State) ->
    {stop,badmsg,State}.

handle_cast({request_permissions, {Key, Time, Counter, Id, Amount}},
            #state{requests=Requests} = State) ->
    lager:info("Permission Request: ~p ~p ~p ~p", [Time, Counter, Id, Amount]),
    OldRequest = ets:lookup(Requests, Key),
    NewRequest = inner_request_permissions(Key, Id, OldRequest, Time, Counter, Amount),
    if  NewRequest =/= OldRequest -> ets:insert(Requests, {Key, NewRequest});
        true -> ok
    end,
    {noreply, State};

handle_cast(_Info, State) ->
    {stop,badmsg,State}.

handle_info(_Info, State) ->
    {stop,badmsg,State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

inner_request_permissions(Key, Id, OldRequest, Time, Counter, Amount) ->
    case OldRequest of
        [{OldAmount, OldPermissions, OldTime}] ->
            {NewPermissions, NewTime} = if
                                            Time > OldTime ->
                                                {crdt_bcounter:permissions(Counter), Time};
                                            true -> {OldPermissions, Time}
                                        end,
            DeltaPermissions = Amount - OldAmount,
            NewAmount = if
                            DeltaPermissions > 0 ->
                                remote_request_permissions(Key, Id, Counter, DeltaPermissions),
                                Amount;
                            true -> OldAmount
                        end,
            {NewAmount, NewPermissions, NewTime};
        _ ->
            remote_request_permissions(Key, Id, Counter, Amount),
            Permissions = crdt_bcounter:permissions(Counter),
            {Amount, Permissions, Time}
    end.

remote_request_permissions(Key, Id, Counter, Amount) ->
    LocalPermissions = crdt_bcounter:local_permissions(Id, Counter),
    MissingPermissions = Amount - LocalPermissions,
    RequestList = get_request_list(Counter, Id, MissingPermissions),
    remote_request_permissions(Key, RequestList).

remote_request_permissions(Key, ReqList) ->
    lager:info("Request list for ~p: ~p", [Key, ReqList]).
%TODO: Issue remote permission request.

get_request_list(Counter, MyId, Amount) ->
    UnsortedPermissions = lists:filter(
                            fun({Id,Val}) ->
                                    case Id of
                                        MyId -> false;
                                        _ when Val == 0 -> false;
                                        _ -> true
                                    end
                            end,
                                    crdt_bcounter:permissions_per_owner(Counter)),
    %lager:info("Unsorted permissions ~p",[UnsortedPermissions]),
    Permissions = lists:sort(
                    fun({_Id1,P1}, {_Id2,P2}) -> P1 >= P2 end,
                    UnsortedPermissions),
    %lager:info("Sorted permissions ~p",[Permissions]),
    build_request_list(Permissions, Amount, [], 0).

get_top_list([{Id,P}], Length) ->
    {[{Id,0}], P, [], Length+1};
get_top_list([{Id1,P1}, {Id2,P1} | Rest], Length) ->
    {List, Step, Tail, NewLength} = get_top_list([{Id2,P1} | Rest], Length+1),
    {[{Id1,0} | List], Step, Tail, NewLength};
get_top_list([{Id1,P1}, {Id2,P2} | Rest], Length) ->
    {[{Id1,0}], P1 - P2, [{Id2,P2} | Rest], Length+1};
get_top_list([], _) -> {[], [], [], 0}.

build_request_list(Permissions, Amount, ReqList, Length) ->
    {TopList, Step, Rest, NewLength} = get_top_list(Permissions, Length),
    NewReqList = lists:append(ReqList, TopList),
    StepArea = Step*NewLength,
    Missing = Amount-StepArea,
    if  Missing =< 0 ->
            finish_req_list(NewReqList, Amount, NewLength);
        true ->
            NewList = update_req_list(NewReqList, Step),
            build_request_list(Rest, Missing, NewList, NewLength)
    end.

finish_req_list(ReqList, Amount, Length) ->
    Rem = Amount rem Length,
    Step = Amount div Length + 1,
    update_req_list(ReqList, Step, Rem).

update_req_list(ReqList, Step, 0) ->
    update_req_list(ReqList, Step-1);
update_req_list([{Id,P} | Rest], Step, Rem) ->
    ReqList = update_req_list(Rest, Step, Rem-1),
    [{Id, P+Step} | ReqList].

update_req_list(ReqList, Step) ->
    lists:map(
      fun({Id,P}) -> {Id,P+Step} end,
      ReqList).
