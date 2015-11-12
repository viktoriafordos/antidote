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
    case Permissions >= Amount of
        true ->
            gen_server:cast({?MODULE, Node}, {request_permissions,
                                              {Key, Time, Counter, Id, Amount}});
        false -> ok
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
    case  NewRequest =/= OldRequest of
        true -> ets:insert(Requests, {Key, NewRequest});
        false -> ok
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
            {NewPermissions, NewTime} = case Time > OldTime of
                                            true ->
                                                {crdt_bcounter:permissions(Counter), Time};
                                            false -> {OldPermissions, Time}
                                        end,
            DeltaPermissions = Amount - OldAmount,
            NewAmount = case DeltaPermissions > 0 of
                            true -> remote_request_permissions(Key, Id, Counter, DeltaPermissions),
                                    Amount;
                            false -> OldAmount
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
    Permissions = crdt_bcounter:sorted_permission_per_owner(Counter),
    inner_get_request_list([], Permissions, MyId, Amount).

inner_get_request_list(Requests, _Permissions, _MyId, 0) -> Requests;

inner_get_request_list(Requests, [], _MyId, _AmountRem) -> Requests;

inner_get_request_list(Requests, [{MyId, _} | TailPer], MyId, AmountRem) ->
    inner_get_request_list(Requests, TailPer, MyId, AmountRem);

inner_get_request_list(Requests, [{DCId, Value} | TailPer], MyId, AmountRem) ->
    case AmountRem - Value > 0 of
        true ->
            inner_get_request_list([{DCId, Value} | Requests], TailPer, MyId, AmountRem - Value);
        false when Value > 0 ->
            inner_get_request_list([{DCId, AmountRem} | Requests], TailPer, MyId, 0);
        false ->
            inner_get_request_list(Requests, [], MyId, 0)
    end.

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


