-module(inter_dc_repl_update_intercepts).
-compile(export_all).

-include("intercept.hrl").

-define(M, inter_dc_repl_update_orig).

finish_update_dc_intrcptd(Dc, DcQ, Cts, State=#recvr_state{lastCommitted=LastCTS, recQ=RecQ}) ->
	%%receive
		%%wait -> loop
	%%end,
	recorder ! found,
	?M:finish_update_dc(Dc, DcQ, Cts, State).
