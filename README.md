# edisque

Erlang Disque client. Disque is an in-memory, distributed job queue.

## Example

Example:

    git clone git://github.com/nacmartin/edisque.git
    cd edisque
    make
    erl -pa ebin/ deps/**/ebin/
    {ok, C} = edisque:start_link().
    edisque:add_job(C, <<"queue">>, <<"body">>, 0).
    {ok, Resp} = edisque:get_job(C, [<<"queue">>]).
    JobId = lists:nth(2, lists:nth(1, Resp)).
    edisque:ack_job(C, [JobId]).

## Host cycling

Every time edisque runs `GETJOB`, edisque update statistics of the Disque nodes that are producing the jobs that edisque is consuming. When a number of jobs is consumed (default is 1000), it will check the statistics to see if it can connect to a better node, and will do so if possible.

For more information about this behaviour, check the [Disque](https://github.com/antirez/disque) documentation.

When edisque is started with `edisque:start_link(Hosts, Cycle)`, where Hosts is a list of tuples of the form `{IP, Port}` as in `{"127.0.0.1", 7711}`, `Cycle` is the number of jobs to be retrieved before reconnecting to the best host, that is, the host node from which we are retrieving jobs more frequently.

If `Cycle` is 0, this feature is disabled.

## Running tests

To run the tests run `make eunit`.

In order to test cycling, it is expected to have two edisque nodes running in a cluster listening to `127.0.01:7711` and `127.0.0.1:7712`.

## Dependencies

Edisque uses [eredis](https://github.com/wooga/eredis) as base Redis client.
