%%%-------------------------------------------------------------------
%%% @author maryam
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 27. May 2016 3:29 PM
%%%-------------------------------------------------------------------
-module(ad_counter).
-author("maryam").

%% API
-export([view_ad/2, get_val/3]).

-include_lib("eunit/include/eunit.hrl").

view_ad(Node, Ad) ->
  {ok, TxId} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  {ok, [Res1]} = rpc:call(Node, antidote, read_objects, [[Ad], TxId]),
  if
    Res1<5 ->
      ok = rpc:call(Node, antidote, update_objects, [[{Ad, increment, 1}], TxId]);
    true ->
      skip
  end,
  {ok, [Res]} = rpc:call(Node, antidote, read_objects, [[Ad], TxId]),
  {ok, CT} = rpc:call(Node, antidote, commit_transaction, [TxId]),
  {Res, {TxId, CT}}.

get_val(Node, Ad, Clock) ->
  {ok, Tx} = rpc:call(Node, antidote, start_transaction, [Clock, []]),
  {ok, [Res]} = rpc:call(Node, antidote, read_objects, [[Ad], Tx]),
  {ok, _CT1} = rpc:call(Node, antidote, commit_transaction, [Tx]),
  Res.