-module(inter_dc_repl_update_intercepts).
-compile(export_all).

-include("intercept.hrl").

-define(M, inter_dc_repl_update_orig).

finish_update_dc_intrcptd(Dc, DcQ, Cts, State=#recvr_state{lastCommitted=LastCTS, recQ=RecQ}) ->
	?I_INFO("#############The finish_update_dc is intercepted###############"),
	%global:send(rrecorder, found),
	receive
		continue -> ?M:finish_update_dc(Dc, DcQ, Cts, State)
	end.
	
	
