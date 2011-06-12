%% Copyright (c) 2011
%% Orion Henry <orion@heroku.com>
%% Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%
%% Permission is hereby granted, free of charge, to any person
%% obtaining a copy of this software and associated documentation
%% files (the "Software"), to deal in the Software without
%% restriction, including without limitation the rights to use,
%% copy, modify, merge, publish, distribute, sublicense, and/or sell
%% copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following
%% conditions:
%%
%% The above copyright notice and this permission notice shall be
%% included in all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
%% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
%% OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
%% NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
%% HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
%% WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
%% OTHER DEALINGS IN THE SOFTWARE.
-module(lockstep).
-author("Orion Henry <orion@heroku.com>").
-behaviour(gen_server).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
         handle_info/2, terminate/2, code_change/3]).

%% API
-export([start_link/2, start_link/3]).

-record(state, {socket,
                tid=?MODULE,
                disk=false,
                host,
                port,
                path,
                schema,
                transfer,
                contentlength,
                writes=0}).

-define(RECONNECT_TIME, 3000).
-define(STAT_FREQ, 10).

start_link(Uri, Schema) ->
  start_link(Uri, Schema, []).

start_link(Uri, Schema, Opts) when is_list(Uri),
                                   is_tuple(Schema),
                                   is_list(Opts) ->
  gen_server:start_link(?MODULE, [Uri, Schema, Opts], []).

%% TODO: handle URL path that does not end in '/'
%% TODO: handle redirects

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Uri, Schema, Opts]) ->
  State = init_state([{uri, Uri}, {schema, Schema} | Opts]),
  setup_tables(State),
  erlang:send_after(?STAT_FREQ * 1000, self(), stats),
  {ok, State, 0}.

handle_call(_Message, _From, State) ->
  {reply, error, State}.

handle_cast(_Message, State) ->
  {noreply, State}.

handle_info({http, Sock, Http}, State) ->
  State2 = case Http of
    { http_response, {1,1}, 200, <<"OK">> } ->
      State;
    { http_header, _, _Key = 'Content-Length', _,  Size } ->
      io:format("Header: ~p: ~p~n",[_Key, Size]),
      State#state{ contentlength=list_to_integer(binary_to_list(Size)) };
    { http_header, _, _Key = 'Transfer-Encoding', _,  _Value = <<"chunked">> } ->
      io:format("Header: ~p: ~p~n",[_Key, _Value]),
      State#state{ transfer=chunked };
    { http_header, _, _Key, _, _Value } ->
      io:format("Header: ~p: ~p~n",[_Key, _Value]),
      State;
    http_eoh ->
      inet:setopts(Sock, [{packet, line}]),
      State
  end,
  inet:setopts(Sock, [{active, once}]),
  {noreply, State2};
handle_info({tcp, _Sock, <<"0\r\n">> }, #state{transfer=chunked}=State) ->
  {noreply, disconnect(State), ?RECONNECT_TIME};
handle_info({tcp, Sock, Line}, #state{writes=Writes, transfer=chunked}=State) ->
  Size = read_chunk_size(Line),
  case read_chunk(Sock, Size, State) of
    State2 when is_record(State2, state) ->
      {noreply, State2#state{writes=Writes+1}};
    {error, closed} ->
      {noreply, disconnect(State), ?RECONNECT_TIME}
  end;
handle_info({tcp, Sock, Line}, #state{writes=Writes}=State) ->
  Size = size(Line),
  Remaining = State#state.contentlength - Size,
  State2 = process_chunk(Line, State),
  case Remaining < 1 of
    true ->
      {noreply, disconnect(State2#state{writes=Writes+1}), ?RECONNECT_TIME};
    false ->
      inet:setopts(Sock, [{active, once}, { packet, line }]),
      {noreply, State2#state{contentlength=Remaining, writes=Writes+1}}
  end;
handle_info({tcp_closed, _Sock}, State) ->
  {noreply, State#state{socket=undefined}, 0};
handle_info({tcp_error, _Sock, Err}, State) ->
  io:format("tcp_error/~p~n",[Err]),
  {noreply, State#state{socket=undefined}, 0};
handle_info(timeout, #state{socket=undefined}=State) ->
  case connect(State) of
    {ok, Sock} ->
      {noreply, State#state{socket=Sock, transfer=undefined}};
    Err ->
      io:format("error/~p~n",[Err]),
      {noreply, State#state{socket=undefined}, ?RECONNECT_TIME}
  end;
handle_info(stats, State) ->
  io:format("Stats: ~p writes/sec~n",[State#state.writes/?STAT_FREQ]),
  erlang:send_after(?STAT_FREQ * 1000, self(), stats),
  {noreply, State#state{writes=0}, 0};
handle_info(Message, State) ->
  io:format("unhandled info: ~p~n", [Message]),
  {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVersion, State, _Extra) -> {ok, State}.

%% Internal functions
init_state(Opts) ->
  init_state(Opts, #state{}).

init_state([], State) ->
  State;
init_state([{table, TabName}|Tail], State) when is_atom(TabName) ->
  init_state(Tail, State#state{tid=TabName});
init_state([{disk, Disk}|Tail], State) when is_boolean(Disk) ->
  init_state(Tail, State#state{disk=Disk});
init_state([{uri, Uri}|Tail], State) ->
  {http, [], Host, Port, Path, []} = http_uri:parse(Uri),
  init_state(Tail, State#state{host=Host, port=Port, path=Path});
init_state([{schema, Schema}|Tail], State) ->
  init_state(Tail, State#state{schema=digest_schema(Schema)});
init_state([_|Tail], State) ->
  init_state(Tail, State).

read_chunk_size(Line) ->
  { ok, [Bytes], [] } = io_lib:fread("~16u\r\n", binary_to_list(Line)),
  Bytes.

read_chunk(Socket, Bytes, State) ->
  inet:setopts(Socket, [{active, false}, { packet, raw }]),
  case gen_tcp:recv(Socket, Bytes + 2) of
    { ok, <<Data:Bytes/binary,"\r\n">> } ->
      inet:setopts(Socket, [{active, once}, { packet, line }]),
      process_chunk(Data, State);
    _ ->
      {error, closed}
  end.

process_chunk(Chunk, State) ->
  case mochijson2:decode(Chunk) of
    { struct, Props } -> process_proplist(Props, State)
  end,
  State.

process_proplist(Props, #state{schema=Schema}=State) ->
  { ok, Meta, Record } = analyze(Props, Schema),
  perform_update(Meta, Record, State).

analyze(Props, Schema) ->
  BlankRecord = erlang:make_tuple( length(Schema), undefined),
  BlankMeta = { set, 0 },
  analyze(Props, Schema, BlankRecord, BlankMeta).

analyze([ P | Props], Schema, Record, Meta) ->
  NewMeta = build_meta(P, Meta),
  NewRecord = build_record(P, Schema, Record, 1),
  analyze(Props, Schema, NewRecord, NewMeta);
analyze([], _, Record, Meta) ->
  { ok, Meta, Record }.

head(State) ->
  case ets:lookup(State#state.tid, lockstep_head) of
    [{ lockstep_head, Time }] -> Time - 1; %% 1 second before
    _ -> 0
  end.

build_meta({<<"deleted_at">>, DeletedAt}, { _, UpdatedAt }) when is_integer(DeletedAt) ->
  { delete, UpdatedAt };
build_meta({<<"updated_at">>, UpdatedAt}, { Action, _ }) ->
  { Action, UpdatedAt};
build_meta(_, Meta) ->
  Meta.

build_record({ Key, Value}, [ Key | _ ], Tuple, Index ) ->
  erlang: setelement( Index, Tuple, Value );
build_record({ Key, Value}, [ _ | Tail ], Tuple, Index ) ->
  build_record({ Key, Value}, Tail, Tuple, Index + 1 );
build_record(_, [], Tuple, _ ) ->
  Tuple.

perform_update( { set, Time }, Record, State ) ->
  set(Time, Record, State);
perform_update( { delete, Time }, Record, State ) ->
  delete(Time, Record, State).

set(Time, Record, #state{tid=Tid, disk=Disk}) ->
  io:format("SET ~p ~p~n",[Time, Record]),
  ets:insert(Tid, Record),
  ets:insert(Tid, {lockstep_head, Time}),
  Disk andalso set_dets(Time, Record, Tid).

set_dets(Time, Record, Tid) ->
  dets:insert(Tid, Record),
  dets:insert(Tid, {lockstep_head, Time}).

delete(Time, Record, #state{tid=Tid, disk=Disk}) ->
  io:format("DEL ~p ~p~n",[Time, Record]),
  ets:delete_object(Tid, Record),
  ets:insert(Tid, {lockstep_head, Time}),
  Disk andalso delete_dets(Time, Record, Tid).

delete_dets(Time, Record, Tid) ->
  dets:delete_object(Tid, Record),
  dets:insert(Tid, {lockstep_head, Time}).

setup_tables(#state{tid=TabName, disk=Disk}) when is_atom(TabName) ->
  TabName = ets:new(TabName, [named_table, set, protected, {read_concurrency, true}]),
  Disk == true andalso setup_dets(TabName).

setup_dets(TabName) ->
  {ok, TabName} = dets:open_file(TabName, [{file, atom_to_list(TabName) ++ ".dets"}]),
  TabName = dets:to_ets(TabName, TabName).

disconnect(State) ->
  io:format("closing connection~n"),
  gen_tcp:close(State#state.socket),
  State#state{socket=undefined}.

digest_schema(Schema) ->
  Schema2 = tuple_to_list(Schema),
  Schema3 = lists:map(fun erlang:atom_to_list/1, Schema2),
  lists:map(fun erlang:list_to_binary/1, Schema3).

connect(State) ->
  Opts = [binary, {packet, http_bin}, {packet_size, 1024 * 1024}, {recbuf, 1024 * 1024}, {active, once}],
  io:format("Connecting to http://~s:~p~s~p...~n",[ State#state.host, State#state.port, State#state.path, head(State) ]),
  case gen_tcp:connect( State#state.host, State#state.port, Opts, 10000) of
    {ok, Sock} ->
      io:format("ok~n"),
      ok = gen_tcp:send(Sock, [ <<"GET ">> , State#state.path, integer_to_list(head(State)), <<" HTTP/1.1\r\nHost: ">>, State#state.host ,<<"\r\n\r\n">> ]),
      {ok, Sock};
    {error, Error} ->
      {error, Error}
  end.
