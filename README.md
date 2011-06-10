## Overview

lockstep is a client for the lockstep protocol for keeping up to date on a set of changing data over HTTP.

## lockstep protocol overview

Given an http endpoint, pull down records as a list of tuples expressed in json, one per line. 

    $ curl http://0.0.0.0:4567/psmgr/998
    {"id":123,"updated_at":999,"deleted_at":null,"ip":"0.0.0.0","port":1234}
    {"id":124,"updated_at":1000,"deleted_at":1000,"ip":"0.0.0.0","port":1234}
    $

The response can be finite, or be a chunked response that continues to feed
updates to the receiver.  The final element of the path endpoint is the most
recent updated_at time seen and should this only receive records updated since
that time.

All records sent must have an updated_at attribute to identify when they were
last changed.  Records should always be sent sorted by updated_at.

All records sent must have a deleted_at attribute which should be null
normally, or the time at which the tuple was deleted.  If the endpont is
request at time '0' deleted elements may be skipped.

## client overview

Call lockstep:start or lockstep:start_link.  The first argument is the http
endpoint to pull data from.  The second argument is a funtion to transform the
received proplists into tuples.  The third (optional) firld is the name of the
dets table to save data to.  If used, lockstep will pick up the stream from
where the dets table left off rather that resyncing the whole data set.

    {ok, Pid}  = lockstep:start("http://0.0.0.0:4567/psmgr/", fun digest/1, "psmgr.dets"),

Lockstep will track the lockstep endpoint and keep data in an ets table.  The
ets table can be accessed like this.

    Table = lockstep:ets(Pid),
    ets:lookup(Table, Key),



