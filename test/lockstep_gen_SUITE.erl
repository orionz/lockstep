%%%-------------------------------------------------------------------
%% @copyright Heroku, 2013
%% @author Omar Yasin <omarkj@heroku.com>
%% @doc CommonTest test suite for gen_lockstep
%% @end
%%%-------------------------------------------------------------------

-module(lockstep_gen_SUITE).

-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [
     connect_content_length,
     connect_and_chunked
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(connect_content_length, Config) ->
    Tid = ets:new(connect_content_length, [public]),
    {Server, Url} = get_server(fun(Req) ->
                                       connect_content_length_loop(Req, Tid)
                               end),
    [{url, Url},
     {server, Server},
     {tid, Tid}|Config];
init_per_testcase(connect_and_chunked, Config) ->
    Tid = ets:new(connect_and_chunked, [public, bag]),
    {Server, Url} = get_server(fun(Req) ->
                                       connect_and_chunked_loop(Req, Tid)
                               end),
    [{url, Url},
     {server, Server},
     {tid, Tid}|Config];
init_per_testcase(_CaseName, Config) ->
    Config.

end_per_testcase(connect_content_length, Config) ->
    ets:delete(?config(tid, Config)),
    mochiweb_http:stop(?config(server, Config)),
    Config;
end_per_testcase(connect_and_chunked, Config) ->
    ets:delete(?config(tid, Config)),
    loop ! stop, % Kill the loop
    mochiweb_http:stop(?config(server, Config)),
    Config;
end_per_testcase(_CaseName, Config) ->
    Config.

%% Tests
connect_content_length(Config) ->
    Tid = ?config(tid, Config),
    {ok, Pid}  = gen_lockstep:start_link(lockstep_gen_callback, 
                                         ?config(url, Config), [Tid]),
    true = is_pid(Pid) and is_process_alive(Pid),
    bye = gen_lockstep:call(Pid, stop_test, 1000),
    false = is_pid(Pid) and is_process_alive(Pid),
    [{get, Values}] = wait_for_value(get, Tid, future(1)),
    'GET' = proplists:get_value(method, Values),
    "/" = proplists:get_value(path, Values),
    [{"since", "0"}] = proplists:get_value(qs, Values),
    Config.

connect_and_chunked(Config) ->
    register(connect_and_chunked, self()),
    Tid = ?config(tid, Config),
    {ok, Pid} = gen_lockstep:start_link(lockstep_gen_callback, 
                                        ?config(url, Config), [Tid]),
    ok = wait_for_messages(Tid, 2),
    bye = gen_lockstep:call(Pid, stop_test, 1000),
    false = is_pid(Pid) and is_process_alive(Pid),
    [{get, Values}] = wait_for_value(get, Tid, future(1)),
    'GET' = proplists:get_value(method, Values),
    "/" = proplists:get_value(path, Values),
    [{"since", "0"}] = proplists:get_value(qs, Values),
    Config.

%% Internal
wait_for_messages(Tid, TotalMessages) ->
    receive
        sent_first ->
            [{msg, _X}] = wait_for_value(msg, Tid, future(2)),
            loop ! next_please,
            wait_for_messages(Tid, TotalMessages);
        sent_second ->
            timer:sleep(1000),
            [{msg, _}, {msg, _}] = wait_for_value(msg, Tid, future(2), 2),
            loop ! next_please,
            wait_for_messages(Tid, TotalMessages);
        closed_chunk ->
            ok
    end.

get_server(CallbackFun) ->
    ServerOpts = [{ip, "127.0.0.1"}, {port, 0}, {loop, CallbackFun}],
    {ok, Server} = mochiweb_http:start(ServerOpts),
    Port = mochiweb_socket_server:get(Server, port),
    Url = "http://127.0.0.1:" ++ integer_to_list(Port),
    {Server, Url}.

wait_for_value(Key, Tid, Timeout) ->
    wait_for_value(Key, Tid, Timeout, 1).

wait_for_value(Key, Tid, Timeout, Count) ->
    Now =  calendar:datetime_to_gregorian_seconds(
                 calendar:now_to_datetime(os:timestamp())
                ),
    case Now < Timeout of
        true ->
            case ets:lookup(Tid, Key) of
                [] ->
                    timer:sleep(10),
                    wait_for_value(Key, Tid, Timeout);
                Values when length(Values) == Count ->
                    Values;
                Values when length(Values) < Count ->
                    too_many_values;
                Values when length(Values) > Count ->
                    timer:sleep(10),
                    wait_for_value(Key, Tid, Timeout, Count)
            end;
        false ->
            timeout
    end.

future(Seconds) ->
    calendar:datetime_to_gregorian_seconds(
      calendar:now_to_datetime(os:timestamp())
     ) + Seconds.

connect_content_length_loop(Req, Tid) ->
    Method = Req:get(method),
    Path = Req:get(path),
    Query = Req:parse_qs(),
    ets:insert(Tid, {get, [{method, Method},
                           {path, Path},
                           {qs, Query}]}),
    % We're going to send a single message to the server, and get it
    % back via the test callback module. Since it's Content-Length
    % we're closing the connection after we send it.
    Message = create_message(get_message()) ++ "\n",
    Req:respond({200, [{"Content-Type", "application/json"}],
                 Message}).

connect_and_chunked_loop(Req, Tid) ->
    register(loop, self()),
    Method = Req:get(method),
    Path = Req:get(path),
    Query = Req:parse_qs(),
    ets:insert(Tid, {get, [{method, Method},
                           {path, Path},
                           {qs, Query}]}),
    % We are going to send a single message to the test, then message
    % the test to make sure it arrived. After that we send another one
    % and close the connection.
    Message1 = create_message(get_message()),
    Message2 = create_message(get_message()),
    Message3 = "",
    whereis(connect_and_chunked) ! sent_first,
    Resp = Req:start_response({200, [{"Content-Type", "application/json"},
                                     {"Transfer-Encoding", "chunked"}]}),
    ok = mochiweb_response:write_chunk(Message1, Resp),
    connect_and_chunked ! sent_first,
    receive
        next_please ->
            ok = mochiweb_response:write_chunk(Message2, Resp),
            connect_and_chunked ! sent_second
    end,
    receive
        next_please ->
            ok = mochiweb_response:write_chunk(list_to_binary(Message3), Resp),
            connect_and_chunked ! closed_chunk
    end,
    receive
        stop ->
            unregister(loop),
            ok
    end.

get_message() ->
    {Mega, Secs, _} = now(),
    UnixTimestamp = Mega*1000000 + Secs,
    [{txid, random:uniform(10000*10000)}, % pos_int
     {since, random:uniform(10000*10000)}, % pos_int
     {active, true}, % boolean
     {id, <<"506dacf3-d0e3-4ef6-a3e1-7c18799a0485">>}, % uuid
     {service_id, <<"691a0caf-c168-4cb7-93f6-84ad1877f7fe">>}, % uuid
     {app_id, random:uniform(10000*10000)}, % pos int
     {ps, <<"web.1">>}, % string
     {state, <<"starting">>}, % LOOKUP
     {release_id, 3}, % pos_int
     {route_id, <<"1163429_07296a0_bf21">>}, % string
     {ip, null}, % ip_addr
     {port, null}, % port
     {created_at, UnixTimestamp}, % unix timestamp
     {emitted_at, UnixTimestamp}, % unix timestamp
     {deleted_at, null} % unix timestamp
    ].

create_message(List) ->
    mochijson2:encode(List).
