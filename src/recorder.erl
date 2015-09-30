-module(recorder).

-include_lib("kernel/include/file.hrl").

-define(TRACE_FILE, "/home/antidote/commander_log/trace.trc").
-define(ERROR_LOG, "/home/antidote/commander_log/error.log").
-define(EVENT, {local_dc, node, transaction, locality}).

-export([init/0, record/2]).

init() ->
	ok = filelib:ensure_dir("/home/antidote/commander_log/"),
	case file:open(?TRACE_FILE, [write]) of
		{ok, IoDevice} ->
			file:truncate(IoDevice),
			file:write(IoDevice, io_lib:format("~w~n", [?EVENT])),
			IoDevice;
		{error, Reason} -> 
			case file:open(?ERROR_LOG, [append]) of
				{ok, LogIoDevice} ->
					file:write(LogIoDevice, io_lib:format("~n~w", [Reason])),
					file:close(LogIoDevice)
			end,
			{error, Reason}
	end.

record(Device, Msg) ->
	file:write(Device, io_lib:format("~w~n", [Msg])),
	ok.

