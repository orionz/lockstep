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
-module(lockstep_fsm).
-behaviour(gen_fsm).

-export([init/1, handle_event/3, handle_info/3,
         handle_sync_event/4, code_change/4, terminate/3]).

-export([start_link/0, parse/2, request_line/3,
         header/3, chunk_size/3, chunk_line/3, body/3]).

-record(state, {transfer, content_len}).

start_link() ->
  gen_fsm:start_link(?MODULE, [], []).

parse(Pid, Packet) ->
  gen_fsm:sync_send_event(Pid, Packet).

init(_) ->
  {ok, request_line, #state{}}.

handle_event(Event, _StateName, State) ->
  {stop, {unexpected_event, Event}, State}.

handle_info(Info, _StateName, State) ->
  {stop, {unexpected_info, Info}, State}.

handle_sync_event(Event, _From, _StateName, State) ->
  {stop, {unexpected_sync_event, Event}, State}.

code_change(_OldVsn, StateName, State, _Extra) ->
  {ok, StateName, State}.

terminate(_Reason, _StateName, _State) ->
  ok.

request_line({http_response, {1,1}, Success, _}, _From, State) when Success >= 200, Success < 300 ->
  {reply, ok, header, State};

request_line({http_response, _, _, _}=Resp, _From, State) ->
  error_logger:error_report([?MODULE, request_line, Resp]),
  {stop, {error, unexpected_response}, State}.

header({http_header, _, 'Content-Length', _, Size}, _From, State) ->
  {reply, ok, header, State#state{content_len=list_to_integer(binary_to_list(Size))}};

header({http_header, _, 'Transfer-Encoding', _,  <<"chunked">>}, _From, State) ->
  {reply, ok, header, State#state{transfer=chunked}};

header({http_header, _, _Key, _, _Value}, _From, State) ->
  {reply, ok, header, State};

header(http_eoh, _From, #state{transfer=chunked}=State) ->
  {reply, eoh, chunk_size, State};

header(http_eoh, _From, State) ->
  {reply, eoh, body, State}.

chunk_size(NL, _From, State) when NL == <<"\n">>; NL == <<"\r\n">>;
                                  NL == <<"0\r\n">>; NL == <<"2\r\n">> ->
  {reply, ok, chunk_size, State};

chunk_size(Line, _From, State) ->
  {ok, Size} = parse_chunk_size(Line),
  {reply, ok, chunk_line, State#state{content_len=Size}}.

chunk_line(Line, _From, #state{content_len=Size}=State) ->
  case Line of
    <<Body:Size/binary, _/binary>> ->
      {reply, {ok, Body}, chunk_size, State#state{content_len=undefined}};
    Other ->
      io:format("recv'd poorly formatted chunk body size=~w body=~p~n", [Size, Other]),
      {reply, closed, request_line, #state{}}
  end.

body(NL, _From, State) when NL == <<"\n">>; NL == <<"\r\n">> ->
  {reply, ok, body, State};

body(Packet, _From, State) ->
  {reply, {ok, Packet}, body, State}.

parse_chunk_size(Line) ->
  Size0 = parse_chunk_size(Line, []),
  {ok, [Size], []} = io_lib:fread("~16u", Size0),
  {ok, Size}.

parse_chunk_size(<<"\r\n">>, Acc) ->
  lists:reverse(Acc);

parse_chunk_size(<<$;, _/binary>>, Acc) ->
  lists:reverse(Acc);

parse_chunk_size(<<C, Rest/binary>>, Acc) ->
  parse_chunk_size(Rest, [C|Acc]).

