-module(lockstep_gen_callback).
-behaviour(gen_lockstep).

-export([init/1
         ,handle_call/3
         ,handle_event/2
         ,handle_msg/2
         ,current_seq_no/1
         ,current_opts/1
         ,terminate/2
        ]).
-type message() :: term().
-type from() :: gen_server:from().
-type handler_state() :: term().
-type reply() :: term().
-type stop_reason() :: term().
-type event() :: term(). %% CHECK
-type lockstep_message() :: [term()]. %% Proplist
-type seq_no() :: pos_integer().
-type opts() :: [term()]. %% Proplists. What can I put here?

-record(hstate, {seq_no = 0,
                 tid}).

-spec init(list()) -> {ok, handler_state()}|
                      {stop, stop_reason()}.
init([Tid]) ->
    {ok, #hstate{tid=Tid}}.

-spec handle_call(message(), from(), handler_state()) ->
                         {reply, reply(), handler_state()}|
                         {stop, stop_reason(), handler_state()}|
                         {stop, stop_reason(), reply(), handler_state()}.
handle_call(stop_test, _From, HandlerState) ->
    {stop, normal, bye, HandlerState};
handle_call(_Msg, _From, HandlerState) ->
    {reply, ok, HandlerState}.

-spec handle_event(event(), handler_state()) ->
                          {noreply, handler_state()}|
                          {stop, stop_reason(), handler_state()}.
handle_event(_, HandlerState) ->
    {noreply, HandlerState}.

-spec handle_msg(lockstep_message(), handler_state()) ->
                        {noreply, handler_state()}|
                        {stop, stop_reason(), handler_state()}.
handle_msg(Msg, #hstate{tid=Tid}=HandlerState) ->
    true = ets:insert(Tid, {msg, Msg}),
    {noreply, HandlerState}.

-spec current_seq_no(handler_state()) ->
                            {seq_no(), handler_state()}.
current_seq_no(#hstate{seq_no=SeqNo}=HandlerState) ->
    {SeqNo, HandlerState}.

-spec current_opts(handler_state()) ->
                          {opts(), handler_state()}.
current_opts(HandlerState) ->
    {[], HandlerState}.

-spec terminate(stop_reason(), handler_state()) -> any().
terminate(_Reason, _HandlerState) ->
    ok.
