-module(inter_dc_repl_update_intercepts).
-compile(export_all).

-include("intercept.hrl").

-define(M, inter_dc_repl_update_orig).

finish_update_dc_intrcptd(Dc, DcQ, Cts, State) ->
	%% This should arise an execption (Statee is mis-spelled)
	?M:finish_update_dc(Dc, DcQ, Cts, State).
	
