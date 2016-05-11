-module(online_store).

%% API
-export([add_to_store/3, remove_from_store/3, transfer/4]).

-include_lib("eunit/include/eunit.hrl").

remove_from_store(Node, Store, Num) ->
  lager:info("Txn1 is starting on Node: ~p", [Node]),
  {ok, Tx1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn1 with ID: ~p, started on Node: ~p", [Tx1, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{Store, decrement, Num}], Tx1]),
  lager:info("Txn1 with ID: ~p, updated Store on Node: ~p", [Tx1, Node]),
  {ok, [Res1]} = rpc:call(Node, antidote, read_objects, [[Store], Tx1]),
  lager:info("Txn1 read Store val: ~b", [Res1]),
  {ok, CT1} = rpc:call(Node, antidote, commit_transaction, [Tx1]),
  lager:info("Txn1 with ID: ~p committed on Node: ~p", [Tx1, Node]),
  {Res1, {Tx1, CT1}}.

add_to_store(Node, Store, Num) ->
  lager:info("Txn2 is starting on Node: ~p", [Node]),
  {ok, Tx2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn2 with ID: ~p, started on Node: ~p", [Tx2, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{Store, increment, Num}], Tx2]),
  lager:info("Txn2 with ID: ~p, updated Store on Node: ~p", [Tx2, Node]),
  {ok, [Res2]} = rpc:call(Node, antidote, read_objects, [[Store], Tx2]),
  lager:info("Txn2 read Store val: ~b", [Res2]),
  {ok, CT2} = rpc:call(Node, antidote, commit_transaction, [Tx2]),
  lager:info("Txn2 with ID: ~p committed on Node: ~p", [Tx2, Node]),
  {Res2, {Tx2, CT2}}.

transfer(Node, StoreSrc, StoreDes, Num) ->
  {ok, TxTrnsfr} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn with ID: ~p, started on Node: ~p", [TxTrnsfr, Node]),
  %% ok = rpc:call(Node, antidote, update_objects, [[{{store_3,riak_dt_pncounter, bucket}, increment, 3}], TxTrnsfr]),
  ok = rpc:call(Node, antidote, update_objects, [[{StoreSrc, decrement, Num}, {StoreDes, increment, Num}], TxTrnsfr]),
  lager:info("Txn with ID: ~p, updated Stores on Node: ~p", [TxTrnsfr, Node]),
  {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxTrnsfr]),
  lager:info("Txn with ID: ~p committed on Node: ~p", [TxTrnsfr, Node]),
  CT.