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
-module(antidote_db_wrapper).

-include("antidote.hrl").

-export([get_snapshot/3,
    put_snapshot/4,
    get_ops/4,
    put_op/4]).

%% Gets the most suitable snapshot for Key that has been committed
%% before CommitTime. If its nothing is found, returns {error, not_found}
-spec get_snapshot(antidote_db:antidote_db(), key(),
    snapshot_time()) -> {ok, snapshot(), snapshot_time()} | {error, not_found}.
get_snapshot(AntidoteDB, Key, CommitTime) ->
    try
        antidote_db:fold_keys(AntidoteDB,
            fun(K, AccIn) ->
                {Key1, VC, SNAP} = binary_to_term(K),
                case (Key1 == Key) of %% check same key
                    true ->
                        %% check its a snapshot and its time is less than the one required
                        case (SNAP == snap) and
                            vectorclock:lt(vectorclock:from_list(VC), CommitTime) of
                            true ->
                                Snapshot = antidote_db:get(AntidoteDB, K),
                                throw({break, Snapshot, VC});
                            _ ->
                                AccIn
                        end;
                    false ->
                        throw({break})
                end
            end,
            [],
            [{first_key, term_to_binary({Key})}]),
        {error, not_found}
    catch
        {break, SNAP, VC} ->
            {ok, SNAP, VC};
        _ ->
            {error, not_found}
    end.

%% Saves the snapshot into AntidoteDB
-spec put_snapshot(antidote_db:antidote_db(), key(), snapshot_time(),
    snapshot()) -> ok | error.
put_snapshot(AntidoteDB, Key, SnapshotTime, Snapshot) ->
    SnapshotTimeList = vectorclock_to_list(SnapshotTime),
    antidote_db:put(AntidoteDB, {Key, SnapshotTimeList, snap}, Snapshot).

%% Returns a list of operations that have commit time in the range [VCFrom, VCTo]
-spec get_ops(antidote_db:antidote_db(), key(), vectorclock(), vectorclock()) -> list().
get_ops(AntidoteDB, Key, VCFrom, VCTo) ->
    VCFromDict = vectorclock_to_dict(VCFrom),
    VCToDict = vectorclock_to_dict(VCTo),
    try
        antidote_db:fold(AntidoteDB,
            fun({K, V}, AccIn) ->
                {Key1, VC1, OP} = binary_to_term(K),
                VC1Dict = vectorclock:from_list(VC1),
                case Key == Key1 of %% check same key
                    true ->
                        %% if its greater, continue
                        case vectorclock:gt(VC1Dict, VCToDict) of
                            true ->
                                AccIn;
                            false ->
                                %% check its an op and its commit time is in the required range
                                case not vectorclock:lt(VC1Dict, VCFromDict) of
                                    true ->
                                        case (OP == op) of
                                            true ->
                                                AccIn ++ [binary_to_term(V)];
                                            false ->
                                                AccIn
                                        end;
                                    false ->
                                        throw({break, AccIn})
                                end
                        end;
                    false ->
                        throw({break, AccIn})
                end
            end,
            [],
            [{first_key, term_to_binary({Key})}])
    catch
        {break, OPS} ->
            OPS;
        _ ->
            []
    end.

%% Saves the operation into AntidoteDB
-spec put_op(antidote_db:antidote_db(), key(), vectorclock(), operation()) -> ok | error.
put_op(AntidoteDB, Key, VC, Op) ->
    VCList = vectorclock_to_list(VC),
    antidote_db:put(AntidoteDB, {Key, VCList, op}, Op).

vectorclock_to_dict(VC) ->
    case is_list(VC) of
        true -> vectorclock:from_list(VC);
        false -> VC
    end.

vectorclock_to_list(VC) ->
    case is_list(VC) of
        true -> VC;
        false -> vectorclock:to_list(VC)
    end.

