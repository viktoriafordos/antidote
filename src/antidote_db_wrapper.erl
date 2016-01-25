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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

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
                            vectorclock:le(vectorclock:from_list(VC), CommitTime) of
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
    antidote_db:put(AntidoteDB, {binary_to_atom(Key), SnapshotTimeList, snap}, Snapshot).

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
    antidote_db:put(AntidoteDB, {binary_to_atom(Key), VCList, op}, Op).

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

%% Workaround for basho bench
%% TODO find a better solution to this
binary_to_atom(Key) ->
    case is_binary(Key) of
        true -> list_to_atom(integer_to_list(binary_to_integer(Key)));
        false -> Key
    end.

-ifdef(TEST).

get_snapshot_test() ->
    eleveldb:destroy("get_snapshot_test", []),
    {ok, AntidoteDB} = antidote_db:new("get_snapshot_test"),

    Key = key,
    put_n_snapshots(AntidoteDB, Key, 10),

    %% no snapshot with time 0 in both DCs
    NotFound = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 0}, {remote, 0}])),
    ?assertEqual({error, not_found}, NotFound),

    %% get some of the snapshots inserted (matches VC)
    S1 = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 1}, {remote, 1}])),
    S2 = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 4}, {remote, 4}])),
    S3 = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 8}, {remote, 8}])),
    ?assertEqual({ok, 1, [{local, 1}, {remote, 1}]}, S1),
    ?assertEqual({ok, 4, [{local, 4}, {remote, 4}]}, S2),
    ?assertEqual({ok, 8, [{local, 8}, {remote, 8}]}, S3),

    %% get snapshots with different times in their DCs
    S4 = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 1}, {remote, 0}])),
    S5 = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 5}, {remote, 4}])),
    S6 = get_snapshot(AntidoteDB, Key, vectorclock:from_list([{local, 8}, {remote, 9}])),
    ?assertEqual({error, not_found}, S4),
    ?assertEqual({ok, 4, [{local, 4}, {remote, 4}]}, S5),
    ?assertEqual({ok, 8, [{local, 8}, {remote, 8}]}, S6),

    antidote_db:close_and_destroy(AntidoteDB, "get_snapshot_test").


get_operations_test() ->
    eleveldb:destroy("get_operations_test", []),
    {ok, AntidoteDB} = antidote_db:new("get_operations_test"),

    Key = key,
    put_n_operations(AntidoteDB, Key, 10),

    O1 = get_ops(AntidoteDB, Key, [{local, 2}, {remote, 2}], [{local, 8}, {remote, 9}]),
    O2 = get_ops(AntidoteDB, Key, [{local, 4}, {remote, 5}], [{local, 7}, {remote, 7}]),
    ?assertEqual([9,8,7,6,5,4,3,2], O1),
    ?assertEqual([7,6,5,4], O2),

    antidote_db:close_and_destroy(AntidoteDB, "get_operations_test").

put_n_snapshots(_AntidoteDB, _Key, 0) ->
    ok;
put_n_snapshots(AntidoteDB, Key, N) ->
    put_snapshot(AntidoteDB, Key, [{local, N}, {remote, N}], N),
    put_n_snapshots(AntidoteDB, Key, N - 1).

put_n_operations(_AntidoteDB, _Key, 0) ->
    ok;
put_n_operations(AntidoteDB, Key, N) ->
    put_op(AntidoteDB, Key, [{local, N}, {remote, N}], N),
    put_n_operations(AntidoteDB, Key, N - 1).

-endif.