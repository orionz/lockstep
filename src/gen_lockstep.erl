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
         cast/2]).

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
     {handle_msg, 2},
     {handle_event, 2},
     {current_seq_no, 1},
     {current_opts, 1},
     {terminate, 2}];

behaviour_info(_) ->
    undefined.

-record(state, {uri,
                snapshot_uri,
                cb_state,
                cb_mod,
                sock,
                sock_mod,
                encoding,
                content_length,
                parser_mod,
                buffer}).

-define(IDLE_TIMEOUT, get_env(idle_timeout)).

%%====================================================================
%% API functions
%%====================================================================
-spec start_link(atom(), list(), [any()]) -> ok | ignore | {error, any()}.
start_link(CallbackModule, LockstepUrl, InitParams) ->
    gen_server:start_link(?MODULE, [CallbackModule, LockstepUrl, InitParams],
                          [{spawn_opt, [{fullsweep_after, 0}]}]).

-spec start_link(atom(), atom(), list(), [any()]) -> ok | ignore | {error, any()}.
start_link(RegisterName, CallbackModule, LockstepUrl, InitParams) ->
    gen_server:start_link({local, RegisterName}, ?MODULE,
                          [CallbackModule, LockstepUrl, InitParams],
                          [{spawn_opt, [{fullsweep_after, 0}]}]).

call(Pid, Msg, Timeout) when is_integer(Timeout) ->
    gen_server:call(Pid, {call, Msg}, Timeout).

cast(Pid, Msg) ->
    gen_server:cast(Pid, Msg).

%%====================================================================
%% gen_server callbacks
%%====================================================================
init([Callback, LockstepUrl, InitParams]) ->
    process_flag(trap_exit, true),
    case parse_uri(LockstepUrl) of
        {error, Err} ->
            {stop, {error, Err}, undefined};
        Uri ->
            case catch Callback:init(InitParams) of
                {ok, CbState} ->
                    {ok, #state{
                        cb_state = CbState,
                        cb_mod = Callback,
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
            {stop, Reason, anonymize(State#state{cb_state=CbState1})};
        {stop, Reason, Reply, CbState1} ->
            catch Callback:terminate(Reason, CbState1),
            {stop, Reason, Reply, anonymize(State#state{cb_state=CbState1})};
        {'EXIT', Err} ->
            catch Callback:terminate(Err, CbState),
            {stop, Err, anonymize(State)}
    end;

handle_call(_Message, _From, State) ->
    {reply, error, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

handle_info({Proto, Sock, {http_response, _Vsn, Status, _}}, #state{sock_mod=Mod, cb_mod=Callback}=State) when Proto == http; Proto == ssl ->
    case lockstep_response(Status) of
        success ->
            setopts(Mod, Sock, [{active, once}]),
            {noreply, State};
        fail ->
            catch Callback:terminate({http_status, Status}, State#state.cb_state),
            {stop, {http_status, Status}, anonymize(State)}
    end;

handle_info({Proto, _Sock, {http_header, _, 'Location', _, Location}}, State) when Proto == http; Proto == ssl ->
    Uri = parse_uri(binary_to_list(Location)),
    {noreply, State#state{snapshot_uri=Uri}, 0};
handle_info({Proto, Sock, {http_header, _, Key, _, Val}}, #state{sock_mod=Mod, cb_mod=Callback, cb_state=CbState}=State) when Proto == http; Proto == ssl ->
    setopts(Mod, Sock, [{active, once}]),
    case [Key, Val] of
        [<<"Instance-Name">>, InstanceName] ->
            case notify_callback({instance_name, InstanceName}, Callback, CbState) of
                {ok, CbState1} ->
                    {noreply, State#state{cb_state=CbState1}};
                {Err, CbState1} ->
                    {stop, Err, anonymize(State#state{cb_state=CbState1})}
            end;
        ['Transfer-Encoding', <<"chunked">>] ->
            {noreply, State#state{encoding=chunked}};
        ['Content-Length', ContentLength] ->
            {noreply, State#state{content_length=list_to_integer(binary_to_list(ContentLength))}};
        _ ->
            {noreply, State}
    end;

handle_info({Proto, Sock, http_eoh}, #state{cb_mod=Callback, sock=Sock, sock_mod=Mod, encoding=Enc, content_length=Len}=State) when Proto == http; Proto == ssl ->
    case [Enc, Len] of
        [chunked, _] ->
            setopts(Mod, Sock, [{active, once}, {packet, raw}]),
            {noreply, State#state{parser_mod=chunked_parser}};
        [_, Len] when is_integer(Len) ->
            setopts(Mod, Sock, [{active, once}, {packet, raw}]),
            {noreply, State#state{parser_mod=content_len_parser}};
        _ ->
            catch Callback:terminate({error, unrecognized_encoding}, State#state.cb_state),
            {stop, {error, unrecognized_encoding}, anonymize(State)}
    end;

handle_info({Proto, Sock, Data}, #state{cb_mod=Callback,
                                        cb_state=CbState0,
                                        sock_mod=Mod,
                                        parser_mod=chunked_parser,
                                        buffer=Buffer}=State)
  when Proto == tcp; Proto == ssl ->
    case chunked_parser:parse_msgs(<<Buffer/binary, Data/binary>>, Callback, CbState0) of
        {ok, CbState1, Rest} ->
            setopts(Mod, Sock, [{active, once}]),
            {noreply, State#state{cb_state=CbState1, buffer=Rest}, ?IDLE_TIMEOUT};
        {ok, end_of_stream} ->
            Mod:close(Sock),
            disconnect(State);
        {Err, CbState1} ->
            catch Callback:terminate(Err, CbState1),
            {stop, Err, anonymize(State)}
    end;

handle_info({Proto, Sock, Data}, #state{cb_mod=Callback,
                                        cb_state=CbState0,
                                        sock_mod=Mod,
                                        parser_mod=content_len_parser,
                                        content_length=ContentLen,
                                        buffer=Buffer}=State)
  when Proto == tcp; Proto == ssl ->
    case content_len_parser:parse_msgs(<<Buffer/binary, Data/binary>>, ContentLen, Callback, CbState0) of
        {ok, CbState1, ContentLen1, Rest} ->
            setopts(Mod, Sock, [{active, once}]),
            {noreply, State#state{cb_state=CbState1, content_length=ContentLen1, buffer=Rest}, ?IDLE_TIMEOUT};
        {ok, end_of_body} ->
            Mod:close(Sock),
            disconnect(State);
        {Err, CbState1} ->
            catch Callback:terminate(Err, CbState1),
            {stop, Err, anonymize(State)}
    end;

handle_info(ClosedTuple, State)
when is_tuple(ClosedTuple) andalso
    (element(1, ClosedTuple) == tcp_closed orelse
     element(1, ClosedTuple) == ssl_closed orelse
     element(1, ClosedTuple) == tcp_error orelse
     element(1, ClosedTuple) == ssl_error) ->
    close(State);

handle_info(timeout, #state{sock_mod=OldSockMod, sock=OldSock, uri=DefaultUri, snapshot_uri=SnapshotUri, cb_mod=Callback, cb_state=CbState}=State) ->
    catch OldSockMod:close(OldSock),
    {Opts, CbState} = Callback:current_opts(CbState),
    Uri =
        case is_snapshot_redirect(SnapshotUri, Opts) of
              true ->
                IsRedirect = true,
                SnapshotUri;
              false ->
                IsRedirect = false,
                DefaultUri
          end,
    case connect(Uri) of
        {ok, Mod, Sock} ->
            case send_req(IsRedirect, Sock, Mod, Uri, Callback, CbState) of
                {ok, CbState1} ->
                    {noreply, State#state{sock=Sock, sock_mod=Mod, cb_state=CbState1, buffer = <<>>}};
                {error, Err, CbState1} ->
                    case notify_callback(Err, Callback, CbState1) of
                        {ok, CbState2} ->
                            {noreply, State#state{cb_state=CbState2, buffer = <<>>}, 0};
                        {Err, CbState2} ->
                            catch Callback:terminate(Err, CbState2),
                            {stop, Err, anonymize(State)}
                    end;
                {Err, CbState1} ->
                    catch Callback:terminate(Err, CbState1),
                    {stop, Err, anonymize(State)}
            end;
        Err ->
            case notify_callback(Err, Callback, CbState) of
                {ok, CbState1} ->
                    {noreply, State#state{cb_state=CbState1, buffer = <<>>}, 0};
                {Err, CbState1} ->
                    catch Callback:terminate(Err, CbState1),
                    {stop, Err, anonymize(State)}
            end
    end;

handle_info(_Message, State) ->
    {noreply, State}.

terminate(Reason, #state{sock_mod=Mod, sock=Sock, cb_mod=Callback, cb_state=CbState}) ->
    catch Callback:terminate(Reason, CbState),
    if is_atom(Mod), Mod =/= undefined, Sock =/= undefined -> Mod:close(Sock)
     ; true -> ok
    end.

code_change(_OldVersion, State, _Extra) ->
    {ok, State}.

%% Internal functions
close(State) ->
    handle_close_or_disconnect(close, State).

disconnect(State) ->
    handle_close_or_disconnect(disconnect, State).

handle_close_or_disconnect(Event, #state{cb_mod=Callback, cb_state=CbState}=State)
  when Event == close; Event == disconnect ->
    case catch Callback:handle_event(Event, CbState) of
        {noreply, CbState1} ->
            {noreply, State#state{cb_state=CbState1, buffer = <<>>}, 0};
        {stop, Reason, CbState1} ->
            catch Callback:terminate(Reason, CbState1),
            {stop, Reason, anonymize(State)};
        {'EXIT', Err} ->
            catch Callback:terminate(Err, CbState),
            {stop, Err, anonymize(State)}
    end.

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
    case catch Callback:handle_event(Err, CbState) of
        {noreply, CbState1} ->
            {ok, CbState1};
        {stop, Reason, CbState1} ->
            {Reason, CbState1};
        {'EXIT', Err} ->
            {Err, CbState}
    end.

parse_uri(Url) ->
    case http_uri:parse(Url) of
        {ok, Uri} -> Uri;
        Uri -> Uri
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

req(Pass, Host, Path, QS) ->
    iolist_to_binary([
        <<"GET ">>, Path, QS, <<" HTTP/1.1\r\n">>,
        authorization(Pass),
        <<"Host: ">>, Host ,<<"\r\n\r\n">>
    ]).

qs(SeqNo, Opts) when is_integer(SeqNo)->
    [<<"?since=">>, integer_to_list(SeqNo)] ++
    [[<<"&">>, to_binary(Key), <<"=">>, to_binary(Val)] || {Key, Val} <- Opts, Val =/= undefined].

authorization([]) -> [];

authorization(UserPass) ->
    Auth =
        case string:tokens(UserPass, ":") of
            [User] -> base64:encode(User ++ ":");
            [User, Pass] -> base64:encode(User ++ ":" ++ Pass)
        end,
    [<<"Authorization: Basic ">>, Auth, <<"\r\n">>].

send_req(IsRedirect, Sock, Mod, {_Proto, Pass, Host, _Port, Path, QS}, Callback, CbState) ->
    case catch Callback:handle_event(connect, CbState) of
        {noreply, CbState1} ->
            Req = case IsRedirect of
                      true ->
                          CbState3 = CbState1,
                          req(Pass, Host, Path, QS);
                      _ ->
                          {NewSeqNo, CbState2} = Callback:current_seq_no(CbState1),
                          {NewOpts, CbState3} = Callback:current_opts(CbState2),
                          req(Pass, Host, Path, qs(NewSeqNo, NewOpts))
                  end,
            case Mod:send(Sock, Req) of
                ok ->
                    {ok, CbState3};
                Err ->
                    Mod:close(Sock),
                    {error, Err, CbState3}
            end;
        {stop, Reason, CbState1} ->
            {Reason, CbState1};
        {'EXIT', Err} ->
            {Err, CbState}
    end.

setopts(gen_tcp, Sock, Opts) ->
    inet:setopts(Sock, Opts);

setopts(ssl, Sock, Opts) ->
    ssl:setopts(Sock, Opts).

to_binary(Bin) when is_binary(Bin) ->
    Bin;

to_binary(List) when is_list(List) ->
    list_to_binary(List);

to_binary(Int) when is_integer(Int) ->
    to_binary(integer_to_list(Int));

to_binary(Atom) when is_atom(Atom) ->
    to_binary(atom_to_list(Atom)).

get_env(EnvKey) ->
    {ok, Val} = application:get_env(lockstep, EnvKey),
    Val.

%% Check that there is a snapshot uri set and we are not in update mode
is_snapshot_redirect(undefined, _Opts) ->
    false;
is_snapshot_redirect(_SnapshotUri, Opts) ->
    not proplists:get_value(update, Opts, false).

lockstep_response(307) ->
    success;
lockstep_response(Status) when Status >= 200 andalso Status < 300 ->
    success;
lockstep_response(_) ->
    fail.

%% Remove credentials in the URL if any
%%
anonymize(State=#state{uri=Uri, snapshot_uri=SnapUri}) ->
    State#state{uri=hide_pass(Uri), snapshot_uri=hide_pass(SnapUri)}.

hide_pass(undefined) -> undefined;
hide_pass({Scheme, UserInfo, Host, Port, Path, Query}) ->
    UInfo = case string:tokens(UserInfo, ":") of
        [User,_Pass] -> User ++ ":***********";
        [_Term] -> "***********";
        "" -> ""
    end,
    {Scheme, UInfo, Host, Port, Path, Query}.
