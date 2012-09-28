%% Copyright (c) 2012
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
-module(chunked_parser).
-export([parse_msgs/3]).

parse_msgs(<<>>, _Callback, CbState) ->
    {ok, CbState, <<>>};

parse_msgs(Data, Callback, CbState) ->
    case read_size(Data) of
        {ok, 0, _Rest} ->
            {ok, end_of_stream};
        {ok, Size, Rest} ->
            case read_chunk(Rest, Size) of
                {ok, <<"\r\n">>, Rest1} ->
                    Callback:handle_event(heartbeat, CbState),
                    parse_msgs(Rest1, Callback, CbState);
                {ok, Chunk, Rest1} ->
                    case (catch mochijson2:decode(Chunk)) of
                        {struct, Props} ->
                            case catch Callback:handle_msg(Props, CbState) of
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
                {error, malformed_chunk} ->
                    error_logger:info_report([{error, malformed_chunk}, {size, Size}, {data, Data}]),
                    {error, malformed_chunk};
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
        <<_Chunk:Size/binary, _Rest/binary>> when size(_Rest) >= 2 ->
            {error, malformed_chunk};
        _ ->
            eof
    end.
