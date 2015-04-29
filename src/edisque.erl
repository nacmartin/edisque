-module(edisque).

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start_link/0, q/2, q/3, add_job/4, add_job/5, get_job/2, get_job/3, ack_job/2, fast_ack_job/2]).

start_link() ->
    start_link([{"127.0.0.1", 7711}]).
start_link(Hosts) ->
    edisque_client:start_link(Hosts).

q(Client, Command) ->
    q(Client, Command, ?TIMEOUT).

q(Client, Command, Timeout) ->
    gen_server:call(Client, {request, Command, Timeout}).

%% ADDJOB syntax is:
%%
%% ADDJOB queue_name job <ms-timeout>
%%   [REPLICATE <count>]
%%   [DELAY <sec>]
%%   [RETRY <sec>]
%%   [TTL <sec>]
%%   [MAXLEN <count>]
%%   [ASYNC]
%%
add_job(Client, Queue, Job, Timeout) ->
    add_job(Client, Queue, Job, Timeout, []).

add_job(Client, Queue, Job, Timeout, Options) ->
    q(Client, [<<"ADDJOB">>, Queue, Job, Timeout] ++ Options).


%%  GETJOB syntax is:
%%
%%  GETJOB [TIMEOUT <ms-timeout>]
%%         [COUNT <count>]
%%         FROM queue1 queue2 ... queueN
%%
get_job(Client, Queues) ->
    gen_server:call(Client, {get_job, [], Queues}).

get_job(Client, Queues, Options) ->
    gen_server:call(Client, {get_job, Options, Queues}).

ack_job(Client, Jobs) ->
    q(Client, [<<"ACKJOB">>] ++ Jobs).

fast_ack_job(Client, Jobs) ->
    q(Client, [<<"ACKJOB">>] ++ Jobs).
