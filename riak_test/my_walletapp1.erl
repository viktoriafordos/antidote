-module(my_walletapp1).

-export([credit/4,
         debit/4,
         getbalance/2,
		 getbalance/3]).

%% @doc Increases the available credit for a customer.
%%-spec credit(Node::node() , Key::term(), Amount::non_neg_integer(), Actor::term()) -> {ok, _} | {error, reason()}.
credit(Node, Key, Amount, Actor) ->
    rpc:call(Node, antidote, append, [Key, riak_dt_pncounter, {{increment, Amount}, Actor}]).

%% @doc Decreases the available credit for a customer.
%%-spec debit(Node::node() , Key::term(), Amount::non_neg_integer(), Actor::term()) -> {ok, _} | {error, reason()}.
debit(Node, Key, Amount, Actor) ->
	rpc:call(Node, antidote,  append, [Key, riak_dt_pncounter, {{decrement,Amount}, Actor}]).

%% @doc Returns the current balance for a customer.
%%-spec getbalance(Node::node() , Key::term()) -> {error, reason()} | {ok, number()}.
getbalance(Node, Key) ->
    rpc:call(Node, antidote, read, [Key, riak_dt_pncounter]).

%% @doc Returns the current balance for a customer.
%%-spec getbalance(Node::node() , Key::term(), CommitTime::term()) -> {error, error_in_read} | {ok, {_,[number()],_} }.
getbalance(Node, Key, CommitTime) ->
	rpc:call(Node, antidote, clocksi_read, [CommitTime, Key, riak_dt_pncounter]).
