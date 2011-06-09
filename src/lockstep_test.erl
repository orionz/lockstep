
-module(lockstep_test).
-author("Orion Henry <orion@heroku.com>").

-export([test/0, digest/1 ]).

-record(ps, { id, ip, port }).

test() ->
  inets:start(),
%  {ok, Pid}  = lockstep:start("http://0.0.0.0:9999/pids/", fun digest/1),
  {ok, Pid}  = lockstep:start("http://0.0.0.0:4567/pids/", fun digest/1, "lockstep.dets"),
  Ets = lockstep:ets(Pid),
  io:format("Have ets table: ~p~n",[Ets]),
  ok.

digest(Props) -> digest(Props, #ps{}).

digest([], Ps) ->
  { Ps#ps.id, Ps#ps.ip, Ps#ps.port };
digest([ Prop | Props ], Ps) ->
  case Prop of
    { <<"id">>, Id }     -> digest(Props, Ps#ps{id=Id} );
    { <<"ip">>, Ip }     -> digest(Props, Ps#ps{ip=Ip} );
    { <<"port">>, Port } -> digest(Props, Ps#ps{port=Port} );
    _                    -> digest(Props, Ps)
  end.
