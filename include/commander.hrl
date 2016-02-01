-type dc() :: [node()].

-define(DEV(N), list_to_atom(lists:concat(["dev", N, "@127.0.0.1"]))).
-define(NODES_PATH, (comm_config:fetch(cluster_path))).