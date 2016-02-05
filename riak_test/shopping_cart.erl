-module(shopping_cart).

%% API
-export([add_to_cart/3, remove_from_cart/3]).

-include_lib("eunit/include/eunit.hrl").

remove_from_cart(Node, Cart, Num) ->
  lager:info("Txn1 is starting on Node: ~p", [Node]),
  {ok, Tx1} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn1 with ID: ~p, started on Node: ~p", [Tx1, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{Cart, decrement, Num}], Tx1]),
  lager:info("Txn1 with ID: ~p, updated Cart on Node: ~p", [Tx1, Node]),
  {ok, [Res1]} = rpc:call(Node, antidote, read_objects, [[Cart], Tx1]),
  lager:info("Txn1 read Cart val: ~b", [Res1]),
  {ok, CT1} = rpc:call(Node, antidote, commit_transaction, [Tx1]),
  lager:info("Txn1 with ID: ~p committed on Node: ~p", [Tx1, Node]),
  {Res1, {Tx1, CT1}}.

add_to_cart(Node, Cart, Num) ->
  lager:info("Txn2 is starting on Node: ~p", [Node]),
  {ok, Tx2} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lager:info("Txn2 with ID: ~p, started on Node: ~p", [Tx2, Node]),
  ok = rpc:call(Node, antidote, update_objects, [[{Cart, increment, Num}], Tx2]),
  lager:info("Txn2 with ID: ~p, updated Cart on Node: ~p", [Tx2, Node]),
  {ok, [Res2]} = rpc:call(Node, antidote, read_objects, [[Cart], Tx2]),
  lager:info("Txn2 read Cart val: ~b", [Res2]),
  {ok, CT2} = rpc:call(Node, antidote, commit_transaction, [Tx2]),
  lager:info("Txn2 with ID: ~p committed on Node: ~p", [Tx2, Node]),
  {Res2, {Tx2, CT2}}.