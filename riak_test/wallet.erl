-module(wallet).

%% API
-export([credit/3, debit/3, transfer/4]).

-include_lib("eunit/include/eunit.hrl").

debit(Node, Wallet, Amount) ->
  lager:info("Txn1 is starting on Node: ~p", [Node]),
  {ok, Tx1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn1 with ID: ~p, started on Node: ~p", [Tx1, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{Wallet, decrement, Amount}], Tx1]),
  lager:info("Txn1 with ID: ~p, updated Wallet on Node: ~p", [Tx1, Node]),
  {ok, [Res1]} = rpc:call(Node, antidote, read_objects, [[Wallet], Tx1]),
  lager:info("Txn1 read wallet val: ~b", [Res1]),
  {ok, CT1} = rpc:call(Node, antidote, commit_transaction, [Tx1]),
  lager:info("Txn1 with ID: ~p committed on Node: ~p", [Tx1, Node]),
  {Res1, {Tx1, CT1}}.

credit(Node, Wallet, Amount) ->
  lager:info("Txn2 is starting on Node: ~p", [Node]),
  {ok, Tx2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn2 with ID: ~p, started on Node: ~p", [Tx2, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{Wallet, increment, Amount}], Tx2]),
  lager:info("Txn2 with ID: ~p, updated wallet on Node: ~p", [Tx2, Node]),
  {ok, [Res2]} = rpc:call(Node, antidote, read_objects, [[Wallet], Tx2]),
  lager:info("Txn2 read wallet val: ~b", [Res2]),
  {ok, CT2} = rpc:call(Node, antidote, commit_transaction, [Tx2]),
  lager:info("Txn2 with ID: ~p committed on Node: ~p, CT: ~p", [Tx2, Node, CT2]),
  {Res2, {Tx2, CT2}}.

transfer(Node, FromWallet, ToWallet, Amount) ->
  {ok, TxTrnsfr} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn with ID: ~p, started on Node: ~p", [TxTrnsfr, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{FromWallet, decrement, Amount}, {ToWallet, increment, Amount}], TxTrnsfr]),
  lager:info("Txn with ID: ~p, updated wallets on Node: ~p", [TxTrnsfr, Node]),
  {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxTrnsfr]),
  lager:info("Txn with ID: ~p committed on Node: ~p", [TxTrnsfr, Node]),
  CT.