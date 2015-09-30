-module(commander).

-behaviour(gen_server).
-include("commander.hrl").

%% API
-export([start_link/1,
		 send_request/2,
		 acknowledge/3,
		 process_request/2,
		 get_phase/0,
		 stop/0]).

%% Callbacks
-export([init/1,
		 handle_cast/2,
		 handle_call/3,
		 terminate/2,
		 handle_info/2,
		 code_change/3
		 ]).

-define(SERVER, commander_srv).
-define(TRACE_NAME, "trace.trc").

%%%======================================
%%% API
%%%======================================
start_link(Phase) -> 
	gen_server:start_link({local, ?SERVER}, ?MODULE, [Phase], []).

send_request(Req_type, Req_data) ->
	case Req_type of
		replay ->
			gen_server:call(?SERVER, {send_request, {replay, Req_data}})
	end.

acknowledge(replayed, EndOfTx, Locality) ->
	gen_server:call(?SERVER, {acknowledge, {replayed, EndOfTx, Locality}}).

process_request(Request, Msg) ->
	case Request of
		record ->
			gen_server:call(?SERVER, {process_request, {record, Msg}});
		replay ->
			gen_server:call(?SERVER, {process_request, {replay, Msg}})
	end.

get_phase() ->
	gen_server:call(?SERVER, get_phase).

stop() ->
	gen_server:cast(?SERVER, stop).


%%%======================================
%%% callbacks
%%%======================================
init([Phase]) ->
	case Phase of
		record ->
			Rec_device = recorder:init(),
			{ok, #state{phase = record, device = Rec_device}, 0};
		replay ->
			Replay_device = replayer:get_device(?TRACE_NAME),
			[_ | Events] =  replayer:read_next_events(Replay_device),
			NewState = #state{phase = replay, device = Replay_device, events = Events, waiting_requests = [], tx_mapping = dict:new()},
			gen_server:cast(?SERVER, schedule_next_event),
			{ok, NewState, 0}
	end.

handle_call(get_phase, _From, State=#state{phase=Phase}) ->
	{reply, Phase, State};

handle_call({process_request, {record, Msg}}, _From, State=#state{device=Rec_device}) ->
	ok = recorder:record(Rec_device, Msg), 
	{reply, ok, State}.

handle_cast(stop, State) ->
	{stop, normal, State}.

handle_info(_Info, State) ->
	{noreply, State}.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

terminate(_Reason, _State) ->
	ok.

%%%======================================
%%% Internal functions
%%%======================================	
