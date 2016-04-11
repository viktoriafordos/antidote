%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(logging_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
         asyn_read/2,
	 get_stable_time/1,
         read/2,
         asyn_append/3,
         append/3,
         append_commit/3,
         append_group/4,
         asyn_append_group/4,
         asyn_read_from/3,
         read_from/3,
         get/2]).

-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
         handle_exit/3]).

-ignore_xref([start_vnode/1]).

-record(state, {partition :: partition_id(),
		logs_map :: dict(),
		log,
		clock :: non_neg_integer(),
		senders_awaiting_ack :: dict(),
		last_read :: term()}).

%% API
-spec start_vnode(integer()) -> any().
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a `threshold read' asynchronous command to the Logs in
%%      `Preflist' From is the operation id form which the caller wants to
%%      retrieve the operations.  The operations are retrieved in inserted
%%      order and the From operation is also included.
-spec asyn_read_from(preflist(), key(), op_id()) -> ok.
asyn_read_from(Preflist, Log, From) ->
    riak_core_vnode_master:command(Preflist,
                                   {read_from, Log, From},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc synchronous read_from operation
-spec read_from({partition(), node()}, log_id(), op_id()) -> {ok, [term()]} | {error, term()}.
read_from(Node, LogId, From) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read_from, LogId, From},
					?LOGGING_MASTER).

%% @doc Sends a `read' asynchronous command to the Logs in `Preflist'
-spec asyn_read(preflist(), key()) -> ok.
asyn_read(Preflist, Log) ->
    riak_core_vnode_master:command(Preflist,
                                   {read, Log},
                                   {fsm, undefined, self()},
                                   ?LOGGING_MASTER).

%% @doc Sends a `get_stable_time' synchronous command to `Node'
-spec get_stable_time({partition(), node()}) -> ok.
get_stable_time(Node) ->
    riak_core_vnode_master:command(Node,
				   {get_stable_time},
				   ?LOGGING_MASTER).

%% @doc Sends a `read' synchronous command to the Logs in `Node'
-spec read({partition(), node()}, key()) -> {error, term()} | {ok, [term()]}.
read(Node, Log) ->
    riak_core_vnode_master:sync_command(Node,
                                        {read, Log},
                                        ?LOGGING_MASTER).

%% @doc Sends an `append' asyncrhonous command to the Logs in `Preflist'
-spec asyn_append(preflist(), key(), term()) -> ok.
asyn_append(Preflist, Log, Payload) ->
    riak_core_vnode_master:command(Preflist,
                                   {append, Log, Payload},
                                   {fsm, undefined, self(), ?SYNC_LOG},
                                   ?LOGGING_MASTER).

%% @doc synchronous append operation
-spec append(index_node(), key(), term()) -> {ok, op_id()} | {error, term()}.
append(IndexNode, LogId, Payload) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append, LogId, Payload, false},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc synchronous append operation
%% If enabled in antidote.hrl will ensure item is written to disk
-spec append_commit(index_node(), key(), term()) -> {ok, op_id()} | {error, term()}.
append_commit(IndexNode, LogId, Payload) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append, LogId, Payload, ?SYNC_LOG},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc synchronous append list of operations
%% The IsLocal flag indicates if the operations in the transaction were handled by the local or remote DC.
-spec append_group(index_node(), key(), [term()], boolean()) -> {ok, op_id()} | {error, term()}.
append_group(IndexNode, LogId, PayloadList, IsLocal) ->
    riak_core_vnode_master:sync_command(IndexNode,
                                        {append_group, LogId, PayloadList, IsLocal},
                                        ?LOGGING_MASTER,
                                        infinity).

%% @doc asynchronous append list of operations
-spec asyn_append_group(index_node(), key(), [term()], boolean()) -> ok.
asyn_append_group(IndexNode, LogId, PayloadList, IsLocal) ->
    riak_core_vnode_master:command(IndexNode,
				   {append_group, LogId, PayloadList, IsLocal},
				   ?LOGGING_MASTER,
				   infinity).

%% @doc given the MinSnapshotTime and the type, this method fetchs from the log the
%% desired operations so a new snapshot can be created.
-spec get(index_node(), {get, key(), vectorclock(), term(), term()}) ->
    {number(), list(), snapshot(), vectorclock(), false} | {error, term()}.
get(IndexNode, Command) ->
    riak_core_vnode_master:sync_command(IndexNode,
        Command,
        ?LOGGING_MASTER,
        infinity).

%% @doc Opens the persistent copy of the Log.
%%      The name of the Log in disk is a combination of the the word
%%      `log' and the partition identifier.
init([Partition]) ->
    LogFile = integer_to_list(Partition),
    lager:info("Opening logs for partition ~w", [Partition]),
    case open_log(LogFile, Partition) of
        {error, Reason} ->
	    lager:error("ERROR: opening logs for partition ~w, reason ~w", [Partition, Reason]),
            {error, Reason};
        {ok,Log} ->
	    lager:info("Done opening logs for partition ~w", [Partition]),
            {ok, #state{partition=Partition,
			log = Log,
                        clock=0,
                        senders_awaiting_ack=dict:new(),
                        last_read=start}}
    end.

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

%% @doc Read command: Returns the phyiscal time of the 
%%      clocksi vnode for which no transactions will commit with smaller time
%%      Output: {ok, Time}
handle_command({send_min_prepared, Time}, _Sender,
               #state{partition=Partition}=State) ->
    ok = inter_dc_log_sender_vnode:send_stable_time(Partition, Time),
    {noreply, State};

%% @doc Read command: Returns the operations logged for Key
%%          Input: The id of the log to be read
%%      Output: {ok, {vnode_id, Operations}} | {error, Reason}
handle_command({read, _LogId}, _Sender,
               #state{partition=_Partition, log=Log}=State) ->
    {Continuation, Ops} = 
	case disk_log:chunk(Log, start) of
	    {C, O} -> {C,O};
	    {C, O, _} -> {C,O};
	    eof -> {eof, []}
	end,
    case Continuation of
	error -> {reply, {error, Ops}, State};
	eof -> {reply, {ok, Ops}, State#state{last_read=start}};
	_ -> {reply, {ok, Ops}, State#state{last_read=Continuation}}
    end;

%% @doc Threshold read command: Returns the operations logged for Key
%%      from a specified op_id-based threshold.
%%
%%      Input:  From: the oldest op_id to return
%%              LogId: Identifies the log to be read
%%      Output: {vnode_id, Operations} | {error, Reason}
%%
handle_command({read_from, _LogId, _From}, _Sender,
               #state{partition=_Partition, log=Log, last_read=Lastread}=State) ->
            ok = disk_log:sync(Log),
    {Continuation, Ops} = 
	case disk_log:chunk(Log, Lastread) of
	    {error, Reason} -> {error, Reason};
	    {C, O} -> {C,O};
	    {C, O, _} -> {C,O};
	    eof -> {eof, []}
	end,
    case Continuation of
	error -> {reply, {error, Ops}, State};
	eof -> {reply, {ok, Ops}, State};
	_ -> {reply, {ok, Ops}, State#state{last_read=Continuation}}
    end;

%% @doc Append command: Appends a new op to the Log of Key
%%      Input:  LogId: Indetifies which log the operation has to be
%%              appended to.
%%              Payload of the operation
%%              OpId: Unique operation id
%%      Output: {ok, {vnode_id, op_id}} | {error, Reason}
%%
handle_command({append, LogId, Payload, Sync}, _Sender,
               #state{log=Log,
                      clock=Clock,
                      partition=Partition}=State) ->
    OpId = generate_op_id(Clock),
    {NewClock, _Node} = OpId,
    Operation = #operation{op_number = OpId, payload = Payload},
    case insert_operation(Log, LogId, Operation) of
	{ok, OpId} ->
	    inter_dc_log_sender_vnode:send(Partition, Operation),
	    case Sync of
		true ->
		    case disk_log:sync(Log) of
			ok ->
			    {reply, {ok, OpId}, State#state{clock=NewClock}};
			{error, Reason} ->
			    {reply, {error, Reason}, State}
		    end;
		false ->
		    {reply, {ok, OpId}, State#state{clock=NewClock}}
	    end;
	{error, Reason} ->
	    {reply, {error, Reason}, State}
    end;


handle_command({append_group, LogId, PayloadList, IsLocal}, _Sender,
               #state{log=Log,
                      clock=Clock,
                      partition=Partition}=State) ->
    {ErrorList, SuccList, _NNC} = lists:foldl(fun(Payload, {AccErr, AccSucc,NewClock}) ->
						      OpId = generate_op_id(NewClock),
						      {NewNewClock, _Node} = OpId,
						      Operation = #operation{op_number = OpId, payload = Payload},
						      case insert_operation(Log, LogId, Operation) of
							  {ok, OpId} ->
							      case IsLocal of
								  true -> inter_dc_log_sender_vnode:send(Partition, Operation);
								  false -> ok
							      end,
							      {AccErr, AccSucc ++ [OpId], NewNewClock};
							  {error, Reason} ->
							      {AccErr ++ [{reply, {error, Reason}, State}], AccSucc,NewNewClock}
						      end
					      end, {[],[],Clock}, PayloadList),
    case ErrorList of
	[] ->
	    [SuccId|_T] = SuccList,
	    {NewC, _Node} = lists:last(SuccList),
	    {reply, {ok, SuccId}, State#state{clock=NewC}};
	[Error|_T] ->
	    %%Error
	    {reply, Error, State}
    end;

handle_command({get, _LogId, MinSnapshotTime, Type, Key}, _Sender,
	       #state{logs_map = _Map, clock = _Clock, partition = _Partition, log = Log} = State) ->
    case get_ops_from_log(Log, {key, Key}, start, MinSnapshotTime, dict:new(), dict:new()) of
	{error, Reason} ->
	    {reply, {error, Reason}, State};
	CommittedOpsForKeyDict ->
	    CommittedOpsForKey =
		case dict:find(Key, CommittedOpsForKeyDict) of
		    {ok, Val} ->
			Val;
		    error ->
			[]
		end,
	    {reply, {length(CommittedOpsForKey), CommittedOpsForKey, {0,clocksi_materializer:new(Type)},
		     vectorclock:new(), false}, State}
    end;

%% This will reply with all downstream operations that have
%% been stored in the log given by LogId
%% The resut is a dict, with a list of ops per key
handle_command({get_all, _LogId}, _Sender,
	       #state{logs_map = _Map, clock = _Clock, partition = _Partition, log = Log} = State) ->
    case get_ops_from_log(Log, undefined, start, undefined, dict:new(), dict:new()) of
	{error, Reason} ->
	    {reply, {error, Reason}, State};
	CommittedOpsForKeyDict ->
	    {reply, CommittedOpsForKeyDict, State}
    end;

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

reverse_and_add_op_id([],_Id,Acc) ->
    Acc;
reverse_and_add_op_id([Next|Rest],Id,Acc) ->
    reverse_and_add_op_id(Rest,Id+1,[{Id,Next}|Acc]).

%% @doc This method successively calls disk_log:chunk so all the log is read.
%% With each valid chunk, filter_terms_for_key is called.
get_ops_from_log(Log, Key, Continuation, MinSnapshotTime, Ops, CommittedOpsDict) ->
    case disk_log:chunk(Log, Continuation) of
        eof ->
	    dict:fold(fun(Key1, CommittedOps, Acc) ->
			      dict:store(Key1, reverse_and_add_op_id(CommittedOps,0,[]), Acc)
		      end, dict:new(), CommittedOpsDict);
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewTerms} ->
            {NewOps, NewCommittedOps} = filter_terms_for_key(NewTerms, Key, MinSnapshotTime, Ops, CommittedOpsDict),
            get_ops_from_log(Log, Key, NewContinuation, MinSnapshotTime, NewOps, NewCommittedOps);
        {NewContinuation, NewTerms, BadBytes} ->
            case BadBytes > 0 of
                true -> {error, bad_bytes};
                false -> {NewOps, NewCommittedOps} = filter_terms_for_key(NewTerms, Key, MinSnapshotTime, Ops, CommittedOpsDict),
                    get_ops_from_log(Log, Key, NewContinuation, MinSnapshotTime, NewOps, NewCommittedOps)
            end
    end.

%% @doc Given a list of log_records, this method filters the ones corresponding to Key.
%% If key is undefined then is returns all records for all keys
%% It returns a dict corresponding to all the ops matching Key and
%% a list of the commited operations for that key which have a smaller commit time than MinSnapshotTime.
filter_terms_for_key([], _Key, _MinSnapshotTime, Ops, CommittedOpsDict) ->
    {Ops, CommittedOpsDict};
filter_terms_for_key([H|T], Key, MinSnapshotTime, Ops, CommittedOpsDict) ->
    {_, {operation, _, #log_record{tx_id = TxId, op_type = OpType, op_payload = OpPayload}}} = H,
    case OpType of
        update ->
            handle_update(TxId, OpPayload, T, Key, MinSnapshotTime, Ops, CommittedOpsDict);
        commit ->
            handle_commit(TxId, OpPayload, T, Key, MinSnapshotTime, Ops, CommittedOpsDict);
        _ ->
            filter_terms_for_key(T, Key, MinSnapshotTime, Ops, CommittedOpsDict)
    end.

handle_update(TxId, OpPayload,  T, Key, MinSnapshotTime, Ops, CommittedOpsDict) ->
    {Key1, _, _} = OpPayload,
    case (Key == {key, Key1}) or (Key == undefined) of
        true ->
            filter_terms_for_key(T, Key, MinSnapshotTime,
                dict:append(TxId, OpPayload, Ops), CommittedOpsDict);
        false ->
            filter_terms_for_key(T, Key, MinSnapshotTime, Ops, CommittedOpsDict)
    end.

handle_commit(TxId, OpPayload, T, Key, MinSnapshotTime, Ops, CommittedOpsDict) ->
    {{DcId, TxCommitTime}, SnapshotTime} = OpPayload,
    case dict:find(TxId, Ops) of
        {ok, OpsList} ->
	    NewCommittedOpsDict = 
		lists:foldl(fun({KeyInternal, Type, Op}, Acc) ->
				    case ((MinSnapshotTime == undefined) orelse
									   (not vectorclock:gt(SnapshotTime, MinSnapshotTime))) of
					true ->
					    CommittedDownstreamOp =
						#clocksi_payload{
						   key = KeyInternal,
						   type = Type,
						   op_param = Op,
						   snapshot_time = SnapshotTime,
						   commit_time = {DcId, TxCommitTime},
						   txid = TxId},
					    dict:append(KeyInternal, CommittedDownstreamOp, Acc);
					false ->
					    Acc
				    end
			    end, CommittedOpsDict, OpsList),
	    filter_terms_for_key(T, Key, MinSnapshotTime, Ops,
				 NewCommittedOpsDict);
	false ->
	    filter_terms_for_key(T, Key, MinSnapshotTime, Ops, CommittedOpsDict)
    end.

handle_handoff_command({get_all, _logId}, _Sender, State) ->
    {reply, {error, not_ready}, State};

handle_handoff_command(_Msg, _Sender,
                       State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_info({sync, Log, LogId},
            #state{senders_awaiting_ack=SendersAwaitingAck0}=State) ->
    case dict:find(LogId, SendersAwaitingAck0) of
        {ok, Senders} ->
            _ = case dets:sync(Log) of
                ok ->
                    [riak_core_vnode:reply(Sender, {ok, OpId}) || {Sender, OpId} <- Senders];
                {error, Reason} ->
                    [riak_core_vnode:reply(Sender, {error, Reason}) || {Sender, _OpId} <- Senders]
            end,
            ok;
        _ ->
            ok
    end,
    SendersAwaitingAck = dict:erase(LogId, SendersAwaitingAck0),
    {ok, State#state{senders_awaiting_ack=SendersAwaitingAck}}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State = #state{log=Log}) ->
    try
	lager:info("closing log ~p", [Log]),
	disk_log:close(Log)
    catch
	_:Err->
	    lager:info("error closing log ~p", [Err])
    end,
    ok.

%%====================%%
%% Internal Functions %%
%%====================%%



open_log(LogFile, Partition)->
    LogId = LogFile ++ "--" ++ integer_to_list(Partition) ++ atom_to_list(node()),
    LogPath = filename:join(
                app_helper:get_env(riak_core, platform_data_dir), LogId),
    case disk_log:open([{name, LogPath}]) of
        {ok, Log} ->
	    {ok, Log};
        {repaired, Log, _, _} ->
	    {ok, Log};
        {error, Reason} ->
            {error, Reason}
    end.



%% @doc insert_operation: Inserts an operation into the log only if the
%%      OpId is not already in the log
%%      Input:
%%          Log: The identifier log the log where the operation will be
%%               inserted
%%          LogId: Log identifier to which the operation belongs.
%%          OpId: Id of the operation to insert
%%          Payload: The payload of the operation to insert
%%      Return: {ok, OpId} | {error, Reason}
%%
-spec insert_operation(log(), log_id(), operation()) -> {ok, op_id()} | {error, reason()}.
insert_operation(Log, LogId, Operation) ->
    Result = disk_log:log(Log, {LogId, Operation}),
    case Result of
        ok ->
            {ok, Operation#operation.op_number};
        {error, Reason} ->
            {error, Reason}
    end.

generate_op_id(Current) ->
    {Current + 1, node()}.

