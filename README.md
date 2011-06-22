## Overview

lockstep is a client for the lockstep protocol for keeping up to date on a set of changing data over HTTP.

## lockstep protocol overview

Given an http endpoint, pull down records as a list of tuples expressed in json, one per line. 

    $ curl http://0.0.0.0:4567/servers/998
    {"id":123, "updated_at":999, "deleted_at":null, "ip":"0.0.0.0", "port":1234}
    {"id":124, "updated_at":1000, "deleted_at":1000, "ip":"0.0.0.0", "port":1234}

The response can be finite, or be a chunked response that continues to feed updates to the receiver.  The final element of the path endpoint is the most recent updated_at time seen and only updates equal to or after that time should be received.

All records sent must have an updated_at attribute to identify when they were last changed.  Records should always be sent sorted by updated_at.

All records sent must have a deleted_at attribute which should be null normally, or the time at which the tuple was deleted.  If the endpoint requests time '0' then deleted elements may be skipped.

## Make

    $ make get-deps
    $ make

## API

    start_link(Uri) -> Result
    start_link(Uri, Opts) -> Result
      Uri = list() %% http endpoint to pull data from
      Opts = [Opt]
      Opt = {callback, Callback} | {table, TabName} | {ets_opts, EtsOpts} | {disk, SyncToDisk} | {order_by, OrderField}
      Callback = {Module, Function, Args}
      Module = atom()
      Function = atom()
      Args = list()
      TabName = atom() %% the name of the ets (and optionally dets) table to which lockstep data is written
      EtsOpts = list() %% arguments to pass to ets:new
      SyncToDisk = boolean() %% if the options list contains {disk, true}, lockstep will
                             %% sync its ets table to disk and pick up the stream from where
                             %% the dets table left off rather that resyncing the whole data set.
      OrderField = atom() %% specify the field to extract from json to use as the numeric sequence id
      Result = {ok, pid()} | {error, term()}

## Run

    $ node server.js
    Listening on port 4567

    $ erl -pa ebin deps/*/ebin
    1> {ok, Pid}  = lockstep:start_link("http://0.0.0.0:4567/servers/", [{table, servers}, {disk, true}]).
    {ok, <0.33.0>
    2> ets:tab2list(servers).
    [...]
