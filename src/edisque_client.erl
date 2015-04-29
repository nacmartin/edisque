-module(edisque_client).
-behaviour(gen_server).

-define(DEFAULT_CYCLE, 1000).

%% API
-export([start_link/1, start_link/2, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% exported only for testing
-export([parse_hello_hosts/1]).

-record(state, {
          hosts :: list() | undefined,
          cycle :: integer() | undefined,
          cycle_count :: integer() | undefined,
          eredis_client :: pid() | undefined,
          stats :: dict:dict() | undefined
         }).

start_link(Hosts) ->
    gen_server:start_link(?MODULE, [Hosts, ?DEFAULT_CYCLE], []).
start_link(Hosts, Cycle) ->
    gen_server:start_link(?MODULE, [Hosts, Cycle], []).

stop(Pid) ->
    gen_server:call(Pid, stop).

%%====================================================================
%% gen_server callbacks
%%====================================================================
%%
init([Hosts, Cycle]) ->
    case connect(Hosts) of
        {ok, NewState} ->
            {ok, NewState#state{cycle = Cycle, cycle_count = 0}};
        {error, Reason} ->
            {stop, {connection_error, Reason}}
    end.

handle_call({request, Command, Timeout}, _From, State = #state{eredis_client = Client}) ->
    Resp = eredis:q(Client, Command, Timeout),
    {reply, Resp, State};
handle_call({get_job, Options, Queues}, _From, State = #state{eredis_client = Client}) ->
    Resp = eredis:q(Client, [<<"GETJOB">>] ++ Options ++ [<<"FROM">>] ++ Queues),
    {reply, Resp, State};

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
    [X||{_,X} <- lists:sort([ {random:uniform(), N} || N <- List])].

clear_stats(Client) ->
    Stats = dict:new(),
    {ok, RawHello} = eredis:q(Client, [<<"HELLO">>]),
    Hosts = parse_hello_hosts(RawHello),
    %io:format("~p~n", [eredis:q(Client, [<<"HELLO">>])]),
    Stats.

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

parse_long_host(LongHost) ->
    {binary:part(lists:nth(1, LongHost), 0, 8), {lists:nth(2, LongHost), lists:nth(3, LongHost)}}.

parse_hello_hosts(Raw) ->
    LongHosts = lists:dropwhile(fun(X) -> not is_list(X) end, Raw),
    lists:map(fun parse_long_host/1, LongHosts).
