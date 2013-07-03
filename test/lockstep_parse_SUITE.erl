%%%-------------------------------------------------------------------
%% @copyright Heroku, 2013
%% @author Omar Yasin <omarkj@heroku.com>
%% @doc CommonTest test suite for lockstep parser
%% @end
%%%-------------------------------------------------------------------

-module(lockstep_parse_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() ->
    [chunked_parser,
     chunked_partial_parse
     ,chunked_heartbeat
     ,chunked_eof
     ,malformed_chunk
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(chunked_parser, Config) ->
    Tid = ets:new(chunked_parser, [public]),
    [{tid, Tid}|Config];
init_per_testcase(chunked_heartbeat, Config) ->
    Tid = ets:new(chunked_heartbeat, [public]),
    [{tid, Tid}|Config];
init_per_testcase(_CaseName, Config) ->
    Config.

end_per_testcase(_CaseName, Config) ->
    Config.

chunked_parser(Config) ->
    Table = ?config(tid, Config),
    Data = get_message(),
    Message = create_message(Data),
    Message0 = chunkify(Message),
    Message1 = close_chunk(Message0),
    Message2 = to_bin(Message1),
    {ok, end_of_stream} = chunked_parser:parse_msgs(Message2, lockstep_parse_callback, {chunked_parser, Table}),
    [{chunked_parser, Result}] = ets:lookup(Table, chunked_parser),
    ok = compare(Data, Result),
    Config.

chunked_partial_parse(Config) ->
    Message1 = chunkify(create_message(get_message())),
    Message2 = chunkify(create_message(get_message())),
    Message3 = close_chunk(Message1++Message2),
    {PartM1, PartM2} = lists:split(random:uniform(length(Message3)), Message3),
    {ok, undefined, Rest} = chunked_parser:parse_msgs(to_bin(PartM1), lockstep_parse_callback, 
                                                      undefined),
    {ok, end_of_stream} = chunked_parser:parse_msgs(<<Rest/binary, (to_bin(PartM2))/binary>>, 
                                                    lockstep_parse_callback, undefined),
    Config.

chunked_heartbeat(Config) ->
    Table = ?config(tid, Config),
    Beat = close_chunk(chunkify("\r\n")),
    {ok, end_of_stream} = chunked_parser:parse_msgs(to_bin(Beat), lockstep_parse_callback,
                                                    {chunked_heartbeat, Table}),
    [{chunked_heartbeat, heartbeat}] = ets:lookup(Table, chunked_heartbeat),
    Config.

chunked_eof(Config) ->
    Garbage = to_bin(random_string(50)),
    {ok, undefined, Garbage} = chunked_parser:parse_msgs(Garbage, lockstep_parse_callback,
                                                         undefined),
    Config.

malformed_chunk(Config) ->
    Message1 = lists:reverse(tl(lists:reverse(chunkify(create_message(get_message()))))) ++ "f",
    {error, malformed_chunk} = chunked_parser:parse_msgs(to_bin(Message1), lockstep_parse_callback,
                                                         undefined),
    Config.

% Internal
compare([], _) ->
    ok;
compare([{Key, Val}|Rest], FromParser) when is_atom(Key) ->
    Val = proplists:get_value(atom_to_binary(Key, latin1), FromParser),
    compare(Rest, FromParser).

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

chunkify(Data) when is_list(Data) ->
    Length = iolist_size(Data),
    io_lib:format("~.16b\r\n", [Length]) ++ Data ++ "\r\n".    

close_chunk(Bin) ->
    Bin ++ "0\r\n\r\n".

to_bin(Message) when is_list(Message) ->
    list_to_binary(Message).

random_string(0) ->
    [];
random_string(Length) ->
    [random_char() | random_string(Length-1)].

random_char() ->
    random:uniform(95) + 31.
