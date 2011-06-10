
-module(lockstep_test).
-author("Orion Henry <orion@heroku.com>").

-export([test/0 ]).

-record(ps, { id, ip, port }).

test() ->
  inets:start(),
  {ok, Pid}  = lockstep:start("http://0.0.0.0:4567/servers/", { id, ip, port }, "servers.dets"),
  Table = lockstep:ets(Pid),
  io:format("Have ets table: ~p~n",[Table]),
  io:format("Dump -> ~p~n",[ets:tab2list(Table)]),
  ok.
