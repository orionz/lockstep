
-module(lockstep).
-author("Orion Henry <orion@heroku.com>").
-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([start/2, start/3, start_link/2, start_link/3, ets/1 ]).

-record(state, { socket, ets, dets, host, port, path, digest, transfer }).

start(Url, Digest) -> start(Url, Digest, null).

start(Url, Digest, Dets) -> gen_server:start(?MODULE, [Url, Digest, Dets], []).

start_link(Url, Digest) -> start_link(Url, Digest, null).

start_link(Url, Digest, Dets) -> gen_server:start_link(?MODULE, [Url, Digest, Dets], []).

ets(Pid) -> gen_server:call(Pid,ets).

%% handle URL path that does not end in '/'
%% handle non-chunked responses
%% handle multiline chunks

init([Uri, Digest, DetsFile]) when is_list(Uri) and is_function(Digest) ->
  {http,[],Host, Port, Path, []} = http_uri:parse(Uri),
  { Ets, Dets } = setup_tables(DetsFile),
  erlang:send(self(), connect),
  { ok, #state{ets=Ets, dets=Dets, host=Host, port=Port, path=Path, digest=Digest } }.

handle_call(ets, _From, State) ->
    {reply, State#state.ets, State};
handle_call(_Message, _From, State) ->
    {reply, error, State}.

handle_cast(_Message, State) ->
  {noreply, State}.

handle_info({http, Sock, Http}, State) ->
  State2 = case Http of
    { http_response, {1,1}, 200, <<"OK">> } ->
      State;
    { http_header, 'Transfer-Encoding',  <<"chunked">> } ->
      State#state{ transfer=chunked };
    { http_header, _, _Key, _, _Value } -> 
      State;
    http_eoh ->
      inet:setopts(Sock, [{packet, line}]),
      State
  end,
  inet:setopts(Sock, [{active, once}]),
  {noreply, State2};
handle_info({tcp, Sock, Line}, #state{transfer=chunked}=State) ->
  io:format("Chunk ~p~n", Line),
  Size = read_chunk_size(Line),
  Data = read_chunk(Sock, Size),
  process_chunk(Data, State),
  {noreply, State};
handle_info({tcp, Sock, Line}, State) ->
  io:format("Normal Line ~p~n", Line),
  process_chunk(Line, State),
  inet:setopts(Sock, [{active, once}]),
  {noreply, State};
handle_info({tcp_closed, _Sock}, State) ->
  {noreply, connect(State)};
handle_info(connect, State) ->
  {noreply, connect(State)};
handle_info(Message, State) ->
  io:format("UNHANDLED INFO ~p~n", Message),
  {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVersion, State, _Extra) -> {ok, State}.

read_chunk_size(Line) ->
  { ok, [Bytes], [] } = io_lib:fread("~16u\r\n", binary_to_list(Line)),
  Bytes.

read_chunk(Socket, Bytes) ->
  inet:setopts(Socket, [{active, false}, { packet, raw }]),
  { ok, <<Data:Bytes/binary,"\r\n">> } = gen_tcp:recv(Socket, Bytes + 2),
  inet:setopts(Socket, [{active, once}, { packet, line }]),
  Data.

process_chunk(Chunk, State) ->
  case mochijson2:decode(Chunk) of
    { struct, Props } -> analyze(Props, State)
  end.

analyze(Props, State) ->
  { ok, Action, Time } = analyze(Props, set, 0),
  case Action of
    set -> set(Time, (State#state.digest)(Props), State);
    delete -> delete(Time, (State#state.digest)(Props), State)
  end.

analyze([{<<"deleted_at">>, Time} | Props], _Action, Time) when is_integer(Time) ->
  analyze(Props, delete, Time);
analyze([{<<"updated_at">>, Time} | Props], Action, _Time) when is_integer(Time) ->
  analyze(Props, Action, Time);
analyze([ _ | Props], Action, Time) ->
  analyze(Props, Action, Time);
analyze([], Action, Time) ->
  { ok, Action, Time }.

head(State) ->
  case ets:lookup(State#state.ets, lockstep_head) of
    [{ lockstep_head, Time }] -> Time;
    _ -> 0
  end.

set(Time, Record, State) ->
%  io:format("SET ~p ~p~n",[Time, Record]),
  ets:insert(State#state.ets, Record),
  ets:insert(State#state.ets, { lockstep_head, Time }),
  set_dets(Time, Record, State#state.dets).

set_dets(_Time, _Record, null) -> ok;
set_dets(Time, Record, Dets) ->
  dets:insert(Dets, Record),
  dets:insert(Dets, { lockstep_head, Time }).

delete(Time, Record, State) ->
%  io:format("DEL ~p ~p~n",[Time, Record]),
  ets:delete(State#state.ets, Record),
  ets:insert(State#state.ets, { lockstep_head, Time }),
  delete_dets(Time, Record, State#state.dets).

delete_dets(_Time, _Record, null) -> ok;
delete_dets(Time, Record, Dets) ->
  dets:delete(Dets, Record),
  dets:insert(Dets, { lockstep_head, Time }).

setup_tables(null) ->
  { ets:new(?MODULE, [ set, protected, { read_concurrency, true } ]), null };
setup_tables(Filename) ->
  Ets = ets:new(?MODULE, [ set, protected, { read_concurrency, true } ]),
  {ok, Dets} = dets:open_file(Filename, []),
  Ets = dets:to_ets(Dets, Ets),
  { Ets, Dets }.

connect(State) ->
  Opts = [binary, {packet, http_bin}, {packet_size, 1024 * 1024}, {recbuf, 1024 * 1024}, {active, once}],
  io:format("Connecting to http://~s:~p~s~p...",[ State#state.host, State#state.port, State#state.path, head(State) ]),
  case gen_tcp:connect( State#state.host, State#state.port, Opts, 10000) of
    {ok, Sock} ->
      io:format("ok~n"),
      gen_tcp:send(Sock, [ <<"GET ">> , State#state.path, integer_to_list(head(State)), <<" HTTP/1.1\r\nHost: ">>, State#state.host ,<<"\r\n\r\n">> ]),
      State#state{socket=Sock};
    {error, Error } ->
      io:format("error/~p~n",[Error]),
      erlang:send_after(3000, self(), connect),
      State#state{socket=null}
  end.

