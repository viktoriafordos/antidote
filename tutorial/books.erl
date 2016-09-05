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
%% @doc This is small code collection we use for the EUC 2016 tutorial.
%%

-module(books).

-export([test_crdts/0, add_user/0, add_user2/0, add_owned_book/0, remove_book/0, borrow_book/0,borrow_book_causal/0]).

test_crdts() ->
   CounterObj = {my_counter, antidote_crdt_counter, my_bucket},
   {ok, _} = antidote:update_objects(ignore, [], [{CounterObj, increment, 1}]),
   {ok, _CounterVal, _} = antidote:read_objects(ignore, [], [CounterObj]),

   SetObj = {my_set, antidote_crdt_orset, my_bucket},
   {ok, Result, _} = antidote:read_objects(ignore, [], [CounterObj, SetObj]),
   Result.

add_user() ->
   User1 = {michel, antidote_crdt_mvreg, user_bucket},
   {ok, _} = antidote:update_objects(ignore, [], [{User1, assign, {["Michel", "michel@blub.org"], client1}}]),
   {ok, Result, _} = antidote:read_objects(ignore, [], [User1]),
   Result.
   
add_user2() ->
   User2 = {alex, antidote_crdt_mvreg, user_bucket},
   {ok, _} = antidote:update_objects(ignore, [], [{User2, assign, {["Alex", "alex@wow.org"], client1}}]),
   ok.

add_owned_book() ->
   Owned = {michel, antidote_crdt_orset, owned_bucket},
   {ok, _} = antidote:update_objects(ignore, [], [{Owned, add, "Algorithms"}]),
   {ok, _} = antidote:update_objects(ignore, [], [{Owned, add_all, ["Erlang", "Java"]}]),   
   {ok, Result, _} = antidote:read_objects(ignore, [], [Owned]),
   Result.

remove_book() ->   
   Owned = {michel, antidote_crdt_orset, owned_bucket},
   {ok, _} = antidote:update_objects(ignore, [], [{Owned, remove, "Algorithms"}]),
   {ok, Result, _} = antidote:read_objects(ignore, [], [Owned]), 
   Result.

borrow_book() ->
   {ok, TxId} = antidote:start_transaction(ignore, []),
   Owned = {michel, antidote_crdt_orset, owned_bucket},
   ok = antidote:update_objects([{Owned, remove, "Erlang"}], TxId),
   Borrowed = {alex, antidote_crdt_orset, borrowed_bucket},
   ok = antidote:update_objects([{Borrowed, add, "Erlang"}], TxId),
   {ok, TS} = antidote:commit_transaction(TxId),

   {ok, Result, _} = antidote:read_objects(TS, [], [Borrowed, Owned]), 
   
   Result.
   
borrow_book_causal() ->
   {ok, TxId} = antidote:start_transaction(ignore, []),
   Owned = {michel, antidote_crdt_orset, owned_bucket},
   ok = antidote:update_objects([{Owned, remove, "Java"}], TxId),
   Borrowed = {alex, antidote_crdt_orset, borrowed_bucket},
   ok = antidote:update_objects([{Borrowed, add, "Java"}], TxId),
   {ok, TS} = antidote:commit_transaction(TxId),

   {ok, TxId2} = antidote:start_transaction(TS, []),
   ok = antidote:update_objects([{Borrowed, remove, "Java"}], TxId2),
   Borrowed2 = {hugo, antidote_crdt_orset, borrowed_bucket},
   ok = antidote:update_objects([{Borrowed2, add, "Java"}], TxId2),
   {ok, TS2} = antidote:commit_transaction(TxId2),


   {ok, Result, _} = antidote:read_objects(TS2, [], [Owned, Borrowed, Borrowed2]), 
   
   [Result].




