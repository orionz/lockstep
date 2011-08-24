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
-module(gen_lockstep).
-behaviour(gen_server).

%% API
-export([start_link/3,
         start_link/4,
         call/3,
         cast/2,
         suspend/1,
         resume/2]).

%% Behavior callbacks
-export([behaviour_info/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% @hidden
behaviour_info(callbacks) ->
    [{init, 1},
     {handle_call, 3},
     {handle_cast, 2},
     {handle_msg, 2},
     {terminate, 2}];

behaviour_info(_) ->
    undefined.

-record(state, {uri,
                cb_state,
                cb_mod,
                api_opts,
                seq_num,
                sock,
                sock_mod,
                encoding,
                buffer,
                suspended=false}).

-define(IDLE_TIMEOUT, 60000).

%%====================================================================
%% API functions
%%====================================================================
-spec start_link(atom(), list(), [any()]) -> ok | ignore | {error, any()}.
start_link(CallbackModule, LockstepUrl, InitParams) ->
    gen_server:start_link(?MODULE, [CallbackModule, LockstepUrl, InitParams], [{fullsweep_after, 0}]).

-spec start_link(atom(), atom(), list(), [any()]) -> ok | ignore | {error, any()}.
start_link(RegisterName, CallbackModule, LockstepUrl, InitParams) ->
    gen_server:start_link({local, RegisterName}, ?MODULE, [CallbackModule, LockstepUrl, InitParams], [{fullsweep_after, 0}]).

call(Pid, Msg, Timeout) when is_integer(Timeout) ->
    gen_server:call(Pid, {call, Msg}, Timeout).

cast(Pid, Msg) ->
    gen_server:cast(Pid, Msg).

suspend(Pid) ->
    gen_server:call(Pid, suspend, 5000).

resume(Pid, SeqNum) when is_integer(SeqNum) ->
    gen_server:call(Pid, {resume, SeqNum}, 5000).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Callback, LockstepUrl, InitParams]) ->
    process_flag(trap_exit, true),
    case http_uri:parse(LockstepUrl) of
        {error, Err} ->
            {stop, {error, Err}, undefined};
        Uri ->
            case catch Callback:init(InitParams) of
                {ok, SeqNum, Opts, CbState} ->
                    {ok, #state{
                        cb_state = CbState,
                        cb_mod = Callback,
                        api_opts = Opts,
                        seq_num = SeqNum,
                        uri = Uri,
                        buffer = <<>>
                    }, 0};
                {stop, Err} ->
                    {stop, Err};
                {'EXIT', Err} ->
                    {stop, Err}
            end
    end.

handle_call({call, Msg}, From, #state{cb_mod=Callback, cb_state=CbState}=State) ->
    case catch Callback:handle_call(Msg, From, CbState) of
        {reply, Reply, CbState1} ->
            {reply, Reply, State#state{cb_state=CbState1}};
        {stop, Reason, CbState1} ->
            catch Callback:terminate(Reason, CbState1),
            {stop, Reason, State#state{cb_state=CbState1}};
        {stop, Reason, Reply, CbState1} ->
            catch Callback:terminate(Reason, CbState1),
            {stop, Reason, Reply, State#state{cb_state=CbState1}};
        {'EXIT', Err} ->
            catch Callback:terminate(Err, CbState),
            {stop, Err, State}
    end;

handle_call(suspend, _From, #state{sock=Sock, sock_mod=Mod}=State) ->
    Mod:close(Sock),
    {reply, ok, State#state{suspended=true}};

handle_call({resume, SeqNum}, _From, State) ->
    {reply, ok, State#state{suspended=false, seq_num=SeqNum}, 0};

handle_call(_Message, _From, State) ->
    {reply, error, State}.

handle_cast({cast, Msg}, #state{cb_mod=Callback, cb_state=CbState}=State) ->
    case catch Callback:handle_cast(Msg, CbState) of
        {noreply, CbState1} ->
            {noreply, State#state{cb_state=CbState1}};
        {stop, Reason, CbState1} ->
            catch Callback:terminate(Reason, CbState1),
            {stop, Reason, State#state{cb_state=CbState1}};
        {'EXIT', Err} ->
            catch Callback:terminate(Err, CbState),
            {stop, Err, State}
    end;

handle_cast(_Message, State) ->
    {noreply, State}.

handle_info({Proto, Sock, {http_response, _Vsn, Status, _}}, #state{sock_mod=Mod, cb_mod=Callback}=State) when Proto == tcp; Proto == ssl ->
    case Status >= 200 andalso Status < 300 of
        true ->
            Mod:setopts(Sock, [{active, once}]),
            {noreply, State};
        false ->
            catch Callback:terminate({http_status, Status}, State#state.cb_state),
            {stop, {http_status, Status}, State}
    end;

handle_info({Proto, Sock, {http_header, _, Key, _, Val}}, #state{sock_mod=Mod}=State) when Proto == tcp; Proto == ssl ->
    Mod:setopts(Sock, [{active, once}]),
    case [Key, Val] of
        ['Transfer-Encoding', <<"chunked">>] ->
            {noreply, State#state{encoding=chunked}};
        _ ->
            {noreply, State}
    end;

handle_info({Proto, Sock, http_eoh}, #state{cb_mod=Callback, sock=Sock, sock_mod=Mod, encoding=Enc}=State) when Proto == tcp; Proto == ssl ->
    case Enc of
        chunked ->
            Mod:setopts(Sock, [{active, once}, {packet, raw}]),
            {noreply, State};
        _ ->
            catch Callback:terminate({error, expected_chunked_encoding}, State#state.cb_state),
            {stop, {error, expected_chunked_encoding}, State}
    end;

handle_info({Proto, Sock, Data}, #state{cb_mod=Callback, sock_mod=Mod, buffer=Buffer}=State) when Proto == tcp; Proto == ssl ->
    case parse_msgs(<<Buffer/binary, Data/binary>>, Callback, State#state.cb_state) of
        {ok, CbState, Rest} -> 
            Mod:setopts(Sock, [{active, once}]),
            {noreply, State#state{cb_state=CbState, buffer=Rest}, ?IDLE_TIMEOUT};
        {Err, CbState} ->
            catch Callback:terminate(Err, CbState),
            {stop, Err, State}
    end;

handle_info(ClosedTuple, #state{cb_mod=Callback, cb_state=CbState, suspended=false}=State)
when is_tuple(ClosedTuple) andalso
    (element(1, ClosedTuple) == tcp_closed orelse
     element(1, ClosedTuple) == ssl_closed orelse
     element(1, ClosedTuple) == tcp_error orelse
     element(1, ClosedTuple) == ssl_error) ->
    case catch Callback:handle_msg({error, closed}, CbState) of
        {noreply, CbState1} ->
            timer:sleep(1000),
            {noreply, State#state{cb_state=CbState1}, 0};
        {stop, Reason, CbState1} ->
            catch Callback:terminate(Reason, CbState1),
            {stop, Reason, State};
        {'EXIT', Err} ->
            catch Callback:terminate(Err, CbState),
            {stop, Err, State}
    end;

handle_info(timeout, #state{sock_mod=OldSockMod, sock=OldSock, uri=Uri, cb_mod=Callback, cb_state=CbState}=State) ->
    catch OldSockMod:close(OldSock),
    case connect(Uri) of
        {ok, Mod, Sock} ->
            case send_req(Sock, Mod, Uri, State, Callback, CbState) of
                {ok, CbState1} ->
                    {noreply, State#state{sock=Sock, sock_mod=Mod, cb_state=CbState1}};
                {error, Err, CbState1} ->
                    case notify_callback(Err, Callback, CbState1) of
                        {ok, CbState2} ->
                            {noreply, State#state{cb_state=CbState2}, 0};  
                        {Err, CbState2} ->
                            catch Callback:terminate(Err, CbState2),
                            {stop, Err, State}
                    end;
                {Err, CbState1} ->
                    catch Callback:terminate(Err, CbState1),
                    {stop, Err, State}
            end;
        Err ->
            case notify_callback(Err, Callback, CbState) of
                {ok, CbState1} ->
                    {noreply, State#state{cb_state=CbState1}, 0};  
                {Err, CbState1} ->
                    catch Callback:terminate(Err, CbState1),
                    {stop, Err, State}
            end
    end;

handle_info(_Message, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Internal functions
connect({Proto, _Pass, Host, Port, _Path, _}) ->
    Opts = [binary, {packet, http_bin}, {packet_size, 1024 * 1024}, {recbuf, 1024 * 1024}, {active, once}],
    case gen_tcp:connect(Host, Port, Opts, 5000) of
        {ok, Sock} ->
            case ssl_upgrade(Proto, Sock) of
                {ok, Mod, Sock1} ->
                    {ok, Mod, Sock1};
                Err ->
                    gen_tcp:close(Sock),
                    Err
            end;
        Err ->
            Err
    end.

notify_callback(Err, Callback, CbState) ->
    case catch Callback:handle_msg(Err, CbState) of
        {noreply, CbState1} ->
            {ok, CbState1};
        {stop, Reason, CbState1} ->
            {Reason, CbState1};
        {'EXIT', Err} ->
            {Err, CbState}
    end.

ssl_upgrade(https, Sock) ->
    case ssl:connect(Sock, []) of
        {ok, SslSock} ->
            {ok, ssl, SslSock};
        Err ->
            Err
    end;

ssl_upgrade(http, Sock) ->
    {ok, gen_tcp, Sock}.

req(Pass, Host, Path, State) ->
    iolist_to_binary([
        <<"GET ">>, Path, qs(State), <<" HTTP/1.1\r\n">>,
        authorization(Pass),
        <<"Host: ">>, Host ,<<"\r\n\r\n">>
    ]).

qs(State) ->
    [<<"?since=">>, integer_to_list(State#state.seq_num)] ++
    [[<<"&">>, to_binary(Key), <<"=">>, to_binary(Val)] || {Key, Val} <- State#state.api_opts, Val =/= undefined].

authorization([]) -> [];

authorization(UserPass) ->
    Auth =
        case string:tokens(UserPass, ":") of
            [User] -> base64:encode(User ++ ":");
            [User, Pass] -> base64:encode(User ++ ":" ++ Pass)
        end,
    [<<"Authorization: Basic ">>, Auth, <<"\r\n">>].

send_req(Sock, Mod, {_Proto, Pass, Host, _Port, Path, _}, State, Callback, CbState) ->
    Req = req(Pass, Host, Path, State),
    case catch Callback:handle_msg({connect, Req}, CbState) of
        {noreply, CbState1} ->
            case Mod:send(Sock, Req) of
                ok ->
                    {ok, CbState1};
                Err ->
                    Mod:close(Sock),
                    {error, Err, CbState1}
            end;
        {stop, Reason, CbState1} ->
            {Reason, CbState1};
        {'EXIT', Err} ->
            {Err, CbState}
    end.

parse_msgs(<<>>, _Callback, CbState) ->
    {ok, CbState, <<>>};

parse_msgs(Data, Callback, CbState) ->
    case read_size(Data) of
        {ok, 0, _Rest} ->
            case catch Callback:handle_msg(disconnect, CbState) of
                {noreply, CbState1} ->
                    {normal, CbState1};
                {stop, Reason, CbState1} ->
                    {Reason, CbState1};
                {'EXIT', Err} ->
                    {Err, CbState}
            end;
        {ok, Size, Rest} ->
            case read_chunk(Rest, Size) of
                {ok, <<"\r\n">>, Rest1} ->
                    parse_msgs(Rest1, Callback, CbState);
                {ok, Chunk, Rest1} ->
                    case (catch mochijson2:decode(Chunk)) of
                        {struct, Props} ->
                            case catch Callback:handle_msg({msg, Props}, CbState) of
                                {noreply, CbState1} ->
                                    parse_msgs(Rest1, Callback, CbState1);
                                {stop, Reason, CbState1} ->
                                    {Reason, CbState1};
                                {'EXIT', Err} ->
                                    {Err, CbState}
                            end;
                        {'EXIT', Err} ->
                            {Err, CbState};
                        Err ->
                            {Err, CbState}
                    end;
                eof ->
                    {ok, CbState, Data};
                Err ->
                    {Err, CbState}
            end;
        eof ->
            {ok, CbState, Data};
        Err ->
            {Err, CbState}
    end.

read_size(Data) ->
    case read_size(Data, [], true) of
        {ok, Line, Rest} ->
            case io_lib:fread("~16u", Line) of
                {ok, [Size], []} ->
                    {ok, Size, Rest};
                _ ->
                    {error, {poorly_formatted_size, Line}} 
            end;
        Err ->
            Err
    end.

read_size(<<>>, _, _) ->
    eof;

read_size(<<"\r\n", Rest/binary>>, Acc, _) ->
    {ok, lists:reverse(Acc), Rest};

read_size(<<$;, Rest/binary>>, Acc, _) ->
    read_size(Rest, Acc, false);

read_size(<<C, Rest/binary>>, Acc, AddToAcc) ->
    case AddToAcc of
        true ->
            read_size(Rest, [C|Acc], AddToAcc);
        false ->
            read_size(Rest, Acc, AddToAcc)
    end.

read_chunk(Data, Size) ->
    case Data of
        <<Chunk:Size/binary, "\r\n", Rest/binary>> ->
            {ok, Chunk, Rest};
        _ ->
            eof
    end.

to_binary(Bin) when is_binary(Bin) ->
    Bin;

to_binary(List) when is_list(List) ->
    list_to_binary(List);

to_binary(Int) when is_integer(Int) ->
    to_binary(integer_to_list(Int));

to_binary(Atom) when is_atom(Atom) ->
    to_binary(atom_to_list(Atom)).
