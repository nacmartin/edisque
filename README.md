# edisque

Disque client. Disque is an in-memory, distributed job queue.

## Example

Example:

    git clone git://github.com/nacmartin/edisque.git
    cd edisque
    make
    erl -pa ebin/ deps/**/ebin/
    {ok, C} = edisque:start_link().
    edisque:add_job(Client, <<"queue">>, <<"body">>).
    edisque:get_job(C, [<<"queue">>]).

## Dependencies

Edisque uses [eredis](https://github.com/wooga/eredis) as base Redis client.
