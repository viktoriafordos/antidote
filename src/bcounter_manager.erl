-module(bcounter_manager).
-behaviour(gen_server).

-export([start_link/0,
         request_permissions/6,
         transfer_permissions/3]).

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

% TODO: Support synch and asynch requests.
request_permissions(Node, Key, Time, Counter, Id, Amount) ->
    Permissions = crdt_bcounter:value(Counter),
    if
        Permissions >= Amount ->
            gen_server:cast({?MODULE, Node}, {request_permissions,
                                              {Key, Time, Counter, Id, Amount}});
        true -> ok
    end.

% TODO: ignore duplicate requests.
transfer_permissions(RemoteId, Key, Amount) ->
    gen_server:call({?MODULE, node()}, {transfer_permissions,
                                      {RemoteId, Key, Amount}}).

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    lager:info("Bounded Counter manager started in ~p.",[node()]),
    {ok, #state{
            requests = ets:new(bcounter_requests, [])
           }}.

handle_call({transfer_permissions, {RemoteId, Key, Amount}},
            _From, State) ->
    LocalId = dc_utilities:get_my_dc_id(),
    Result = transfer_txn(LocalId, RemoteId, Key, Amount),
    {reply, Result, State};

handle_call(_Info, _From, State) ->
    {stop,badmsg,State}.

handle_cast({request_permissions, {Key, Time, Counter, Id, Amount}},
            #state{requests=Requests} = State) ->
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
    remote_request_permissions(Id, Key, RequestList).

remote_request_permissions(Id, Key, ReqList) ->
    {ok, DCs} = inter_dc_manager:get_dcs(),
    DCReqPair = lists:foldl(
                   fun({DCId, V}, FilteredAcc) ->
                           case orddict:find(DCId, DCs) of
                               {ok, DCAddress} -> [{{DCId,DCAddress}, V} | FilteredAcc];
                               _ -> FilteredAcc
                           end
                   end, [], ReqList),

    lists:foreach(
      fun({DC, ReqAmount}) ->
              %TODO: Make new propagate method to request to multiple DCs in parallel.
              inter_dc_communication_sender:propagate_sync(
                {remote_transfer, {Id, Key, ReqAmount}}, [DC])
      end,
      DCReqPair
     ).

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
    Permissions = lists:sort(
                    fun({_Id1,P1}, {_Id2,P2}) -> P1 >= P2 end,
                    UnsortedPermissions),
    build_request_list(Permissions, Amount, [], 0).

get_top_list([{Id,P}], Length) ->
    {[{Id,0}], P, [], Length+1};
get_top_list([{Id1,P1}, {Id2,P1} | Rest], Length) ->
    {List, Step, Tail, NewLength} = get_top_list([{Id2,P1} | Rest], Length+1),
    {[{Id1,0} | List], Step, Tail, NewLength};
get_top_list([{Id1,P1}, {Id2,P2} | Rest], Length) ->
    {[{Id1,0}], P1 - P2, [{Id2,P2} | Rest], Length+1};
get_top_list([], _) -> {[], 0, [], 0}.

% This code is hard to follow nad does not seem to work very well.
build_request_list(Permissions, Amount, ReqList, Length) ->
    {TopList, Step, Rest, NewLength} = get_top_list(Permissions, Length),
    NewReqList = lists:append(ReqList, TopList),
    %TODO: Problem with step 0
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

% Will we support more data-types with the same interface?
% Need to abstract type in that case.
transfer_txn(LocalId, RemoteId, Key, Amount) ->
    Type = crdt_bcounter,
    {ok, TxId} = antidote:clocksi_istart_tx(),
    {ok, Counter} = antidote:clocksi_iread(TxId, Key, Type),
    LocalPermissions = Type:local_permissions(LocalId, Counter),
    Result = case LocalPermissions >= Amount of
                 true ->
                     antidote:clocksi_iupdate(TxId, Key, Type,
                                              {{transfer, Amount, RemoteId},node()}),
                     ok;
                 false when LocalPermissions - Amount > 0 ->
                     antidote:clocksi_iupdate(TxId, Key, Type,
                                              {{transfer, LocalPermissions, RemoteId},node()}),
                     partial;
                 false -> no_permissions
             end,
    antidote:clocksi_iprepare(TxId),
    {_, CommitTime} = antidote:clocksi_icommit(TxId),
    {Result, CommitTime}.


