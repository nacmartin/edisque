-module(edisque_client).
-behaviour(gen_server).

%% API
-export([start_link/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
          hosts :: list() | undefined,
          eredis_client :: pid() | undefined,
          stats :: dict:dict() | undefined
         }).

start_link(Hosts) ->
    gen_server:start_link(?MODULE, [Hosts], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%
init([Hosts]) ->
    case connect(Hosts) of
        {ok, NewState} ->
            {ok, NewState};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
    {reply, unknown_request, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(stop, State) ->
    {stop, shutdown, State};

handle_info(_Info, State) ->
    {stop, {unhandled_message, _Info}, State}.

terminate(_Reason, State) ->
    case State#state.eredis_client of
        undefined -> ok;
        Client    -> eredis:stop(Client)
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

shuffle(List) ->
    io:format("list is ~p~n", [List]),
    [X||{_,X} <- lists:sort([ {random:uniform(), N} || N <- List])].

clear_stats(Client) ->
    _Stats = dict:new(),
    io:format("~p~n", [eredis:q(Client, [<<"HELLO">>])]).

connect(Hosts) ->
    RHosts = shuffle(Hosts),
    case try_connect_by_order(RHosts) of
        {ok, Client} -> 
            Stats = clear_stats(Client),
            {ok, #state{hosts = Hosts,
                   eredis_client = Client,
                   stats = Stats}};
        undefined ->
            {error, <<"Disque client failed to connect to any supplied host">>}
    end.

try_connect_by_order([]) ->
    undefined;
try_connect_by_order([{Host, Port}|Hosts]) ->
    case eredis:start_link(Host, Port) of
        {ok, Client} -> {ok, Client};
        {error, _Reason} -> try_connect_by_order(Hosts)
    end.


