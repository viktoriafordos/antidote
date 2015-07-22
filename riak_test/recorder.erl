-module(recorder).

-export([start/0]).

start() ->
	record().

record() ->
	lager:info("########Recorder started!########"),
	receive
		found -> 	
			test ! received,
			lager:info("*****recorder is found!*****"),
			record();
		finish -> 
			lager:info("*****terminating!*****"),
			true
	end.

