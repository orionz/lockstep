-module(lockstep_parse_callback).

-compile(export_all).

handle_msg(Message, {TestName, Ets}=State) ->
    ets:insert(Ets, {TestName, Message}),
    {noreply, State};
handle_msg(_Message, undefined) ->
    {noreply, undefined}.

handle_event(Message, {TestName, Ets}) ->
    ets:insert(Ets, {TestName, Message}),
    ok.

