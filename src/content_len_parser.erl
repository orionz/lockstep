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
-module(content_len_parser).
-export([parse_msgs/4]).

parse_msgs(<<>>, 0, _Callback, _CbState) ->
    {ok, end_of_body};

parse_msgs(<<>>, ContentLen, _Callback, CbState) ->
    {ok, CbState, ContentLen, <<>>};

parse_msgs(Data, ContentLen, Callback, CbState) ->
    case read_line(Data) of
        {ok, <<"\r\n">>, Rest} ->
            parse_msgs(Rest, ContentLen - (size(Data)-size(Rest)), Callback, CbState);
        {ok, Line, Rest} ->
            case (catch mochijson2:decode(Line)) of
                {struct, Props} ->
                    case catch Callback:handle_msg(Props, CbState) of
                        {noreply, CbState1} ->
                            parse_msgs(Rest, ContentLen - (size(Data)-size(Rest)), Callback, CbState1);
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
            {ok, CbState, ContentLen, Data}
    end.

read_line(Data) ->
    read_line(Data, <<>>).

read_line(<<"\n", Rest/binary>>, Acc) ->
    {ok, <<Acc/binary, "\n">>, Rest};

read_line(<<A, Rest/binary>>, Acc) ->
    read_line(Rest, <<Acc/binary, A>>);

read_line(<<>>, _Acc) ->
    eof.
