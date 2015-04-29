-module(edisque).

%% Default timeout for calls to the client gen_server
%% Specified in http://www.erlang.org/doc/man/gen_server.html#call-3
-define(TIMEOUT, 5000).

-export([start_link/0, q/2, q/3, add_job/3, get_job/2, ack_job/2, fast_ack_job/2]).

start_link() ->
    eredis:start_link("127.0.0.1", 7711).

q(Client, Command) ->
    q(Client, Command, ?TIMEOUT).

q(Client, Command, Timeout) ->
    eredis:q(Client, Command, Timeout).

% ADDJOB syntax is:
%
% ADDJOB queue_name job <ms-timeout>
%   [REPLICATE <count>]
%   [DELAY <sec>]
%   [RETRY <sec>]
%   [TTL <sec>]
%   [MAXLEN <count>]
%   [ASYNC]
add_job(Client, Queue, Job) ->
    q(Client, [<<"ADDJOB">>, Queue, Job, 0]).

% GETJOB syntax is:
%
% GETJOB [TIMEOUT <ms-timeout>] [COUNT <count>] FROM queue1 queue2 ... queueN
get_job(Client, Queues) ->
    q(Client, [<<"GETJOB">>, <<"FROM">>] ++ Queues).

ack_job(Client, Jobs) ->
    q(Client, [<<"ACKJOB">>] ++ Jobs).

fast_ack_job(Client, Jobs) ->
    q(Client, [<<"ACKJOB">>] ++ Jobs).
