-module(inter_dc_manager).
-behaviour(gen_server).

-export([start_link/0,
         start_receiver/1,
         get_dcs/0,
         add_dc/1,
         add_list_dcs/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {
        dcs,
        port
    }).

%% ===================================================================
%% Public API
%% ===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_receiver(Port) ->
    gen_server:call(?MODULE, {start_receiver, Port}, infinity).

get_dcs() ->
    gen_server:call(?MODULE, get_dcs, infinity).

add_dc(NewDC) ->
    gen_server:call(?MODULE, {add_dc, NewDC}, infinity).

add_list_dcs(DCs) ->
    gen_server:call(?MODULE, {add_list_dcs, DCs}, infinity).
   

%% ===================================================================
%% gen_server callbacks
%% ===================================================================

init([]) ->
    {ok, #state{dcs=[]}}.

handle_call({start_receiver, Port}, _From, State) ->
    {ok, _} = antidote_sup:start_rep(self(), Port),
    receive
        ready -> {reply, {ok, my_dc(Port)}, State#state{port=Port}}
    end;

handle_call(get_dcs, _From, #state{dcs=DCs} = State) ->
    {reply, {ok, DCs}, State};

handle_call({add_dc, OtherDC}, _From, #state{dcs=DCs} = State) ->
    NewDCs = add_dc(OtherDC, DCs),
    {reply, ok, State#state{dcs=NewDCs}};

handle_call({add_list_dcs, OtherDCs}, _From, #state{dcs=DCs} = State) ->
    NewDCs = add_dcs(OtherDCs, DCs),
    {reply, ok, State#state{dcs=NewDCs}}.

handle_cast(_Info, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, _State) ->
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

my_dc(DcPort) ->
    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    DcIp = inet_parse:ntoa(Ip),
    DcId = dc_utilities:get_my_dc_id(),
    {DcId, {DcIp, DcPort}}.

add_dc({DcId, DcAddress}, DCs) -> 
    orddict:store(DcId, DcAddress, DCs).

add_dcs(OtherDCs, DCs) ->
    lists:foldl(fun add_dc/2, DCs, OtherDCs).