-module(comm_config).

-export([fetch/1]).

%%% TODO: Use Erlang config
fetch(Key) ->
	case Key of
		available_num_nodes -> 4;
		root_path -> "$HOME/antidote/";
		cluster_path -> "$HOME/antidote/dev/";
		executable_name -> "antidote";
		executable_admin_name -> "antidote-admin";
		comm_wait_time -> 600000;
		comm_retry_delay -> 1000
	end.
