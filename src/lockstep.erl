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
-export([start_link/1, start_link/2]).

-record(state, {socket,
                mod,
                fsm,
                tid=?MODULE,
                disk=false,
                ets_opts=[],
                order_by = <<"updated_at">>,
                uri,
                callback,
                writes=0}).

-define(RECONNECT_TIME, 3000).
-define(STAT_FREQ, 10).

start_link(Uri) ->
  start_link(Uri, []).

start_link(Uri, Opts) when is_list(Uri), is_list(Opts) ->
  gen_server:start_link(?MODULE, [Uri, Opts], []).

%% TODO: handle URL path that does not end in '/'
%% TODO: handle redirects

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Uri, Opts]) ->
  State = init_state([{uri, Uri} | Opts]),
  {ok, Fsm} = lockstep_fsm:start_link(),
  setup_tables(State),
  %spawn(fun() -> timer:sleep(?STAT_FREQ * 1000), gen_server:cast(Self, stats) end),
  {ok, State#state{fsm=Fsm}, 0}.

handle_call(_Message, _From, State) ->
  {reply, error, State}.

handle_cast(stats, State) ->
  io:format("Stats: ~p writes/sec~n",[State#state.writes/?STAT_FREQ]),
  Self = self(),
  spawn(fun() -> timer:sleep(?STAT_FREQ * 1000), gen_server:cast(Self, stats) end),
  {noreply, State#state{writes=0}};

handle_cast(_Message, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  (catch gen_tcp:close(State#state.socket)),
  case connect(State) of
    {ok, Mod, Sock} ->
      {noreply, State#state{socket=Sock, mod=Mod}};
    Err ->
      io:format("error/~p~n",[Err]),
      {noreply, State#state{socket=undefined}, ?RECONNECT_TIME}
  end;

handle_info({Closed, _Sock}, State) when Closed == tcp_closed; Closed == ssl_closed ->
  {noreply, State#state{socket=undefined}, 0};

handle_info({Err, _Sock, Err}, State) when Err == tcp_error; Err == ssl_error ->
  io:format("error/~p~n", [Err]),
  {noreply, State#state{socket=undefined}, 0};

handle_info(Packet, #state{socket=Sock, mod=Mod, fsm=Fsm, writes=Writes}=State) ->
  Packet1 =
    case Packet of
      {ssl, Sock, Bin} when is_binary(Bin) -> Bin;
      {tcp, Bin} when is_binary(Bin) -> Bin;
      {ssl, Sock, P} -> P;
      P -> P
    end,
  case lockstep_fsm:parse(Fsm, Packet1) of
    ok ->
      Mod:setopts(Sock, [{active, once}]),
      {noreply, State};
    eoh ->
      Mod:setopts(Sock, [{active, once}, {packet, line}]),
      {noreply, State};
    {ok, Msg} ->
      process_msg(Msg, State),
      Mod:setopts(Sock, [{active, once}, {packet, line}]),
      {noreply, State#state{writes=Writes+1}};
    closed ->
      {noreply, disconnect(State), 0}
  end.

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
init_state([{ets_opts, Opts}|Tail], State) when is_list(Opts) ->
  init_state(Tail, State#state{ets_opts=Opts});
init_state([{order_by, OrderBy}|Tail], State) when is_atom(OrderBy) ->
  init_state(Tail, State#state{order_by=list_to_binary(atom_to_list(OrderBy))});
init_state([{uri, Uri}|Tail], State) ->
  case http_uri:parse(Uri) of
    {error, Err} ->
      exit({error, Err});
    ParsedUri ->
      init_state(Tail, State#state{uri=ParsedUri})
  end;
init_state([{callback, Callback}|Tail], State) when is_tuple(Callback) ->
  init_state(Tail, State#state{callback=Callback});
init_state([_|Tail], State) ->
  init_state(Tail, State).

process_msg(Msg, #state{callback=Callback, order_by=OrderBy}=State) ->
  {struct, Props} = mochijson2:decode(Msg),
  Action =
    lists:foldl(
        fun({K, V}, Acc) ->
          case K of
            <<"deleted_at">> when is_integer(V) ->
              {delete, V};
            OrderBy when is_integer(V), Acc == undefined ->
              {update, V};
            _ ->
              Acc
          end
        end, undefined, Props),
  Record =
    case Callback of
      {M,F,A} -> apply(M, F, A ++ [Props]);
      _ -> list_to_tuple([V || {_K, V} <- Props])
    end,
  perform_update(Action, Record, State).

head(State) ->
  case ets:lookup(State#state.tid, lockstep_head) of
    [{ lockstep_head, Time }] -> Time - 1; %% 1 second before
    _ -> 0
  end.

perform_update({update, Time}, Record, State) ->
  set(Time, Record, State);

perform_update({delete, Time}, Record, State) ->
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

setup_tables(#state{tid=TabName, disk=Disk, ets_opts=Opts}) when is_atom(TabName) ->
  Opts1 = [named_table, set, protected, {read_concurrency, true} | Opts],
  TabName = ets:new(TabName, Opts1),
  Disk == true andalso setup_dets(TabName).

setup_dets(TabName) ->
  {ok, TabName} = dets:open_file(TabName, [{file, atom_to_list(TabName) ++ ".dets"}]),
  TabName = dets:to_ets(TabName, TabName).

disconnect(State) ->
  io:format("closing connection~n"),
  gen_tcp:close(State#state.socket),
  State#state{socket=undefined}.

connect(#state{uri={Proto, Pass, Host, Port, Path, _}}=State) ->
  Opts = [binary, {packet, http_bin}, {packet_size, 1024 * 1024}, {recbuf, 1024 * 1024}, {active, once}],
  io:format("Connecting to ~s:~w~n",[Host, Port]),
  case gen_tcp:connect(Host, Port, Opts, 10000) of
    {ok, Sock} ->
      {ok, Mod, Sock1} = ssl_upgrade(Proto, Sock),
      Req = req(Pass, Host, Path, integer_to_list(head(State))),
      io:format("Sending ~p~n", [Req]),
      ok = Mod:send(Sock1, Req),
      {ok, Mod, Sock1};
    {error, Error} ->
      {error, Error}
  end.

ssl_upgrade(https, Sock) ->
  case ssl:connect(Sock, []) of
    {ok, SslSock} -> {ok, ssl, SslSock};
    Err -> Err
  end;

ssl_upgrade(http, Sock) ->
    {ok, gen_tcp, Sock}.

req(Pass, Host, Path, Head) ->
  iolist_to_binary([
    <<"GET ">>, Path, qs(Head), <<" HTTP/1.1\r\n">>,
    authorization(Pass),
    <<"Host: ">>, Host ,<<"\r\n\r\n">>
  ]).

qs(0) -> "";

qs(Head) ->
  [<<"?update=true&since=">>, Head].

authorization([]) -> [];

authorization(UserPass) ->
  Auth =
    case string:tokens(UserPass, ":") of
      [User] -> base64:encode(User ++ ":");
      [User, Pass] -> base64:encode(User ++ ":" ++ Pass)
    end,
  [<<"Authorization: Basic ">>, Auth, <<"\r\n">>].
