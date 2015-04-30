-module(edisque_client).
-behaviour(gen_server).

-define(DEFAULT_CYCLE, 1000).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([start_link/1, start_link/2, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% exported only for testing
-export([parse_hello_hosts/1, clear_stats/1, get_host_short_ids/1, parse_current_host/1]).

-record(state, {
          hosts :: list() | undefined,
          current_host :: binary() | undefined,
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
handle_call({get_job, Options, Queues}, _From, State = #state{cycle = Cycle, eredis_client = Client, stats = Stats}) ->
    {ok, Resp} = eredis:q(Client, [<<"GETJOB">>] ++ Options ++ [<<"FROM">>] ++ Queues),
    case Cycle of
        0 ->
            {reply, {ok, Resp}, State};
        _ ->
            HostsFrom = get_host_short_ids(Resp),
            Stats1 = update_stats(HostsFrom, Stats),
            NewState = maybe_pick_client(Stats1, State),
            {reply, {ok, Resp}, NewState}
    end;

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


get_hosts(Client) ->
    {ok, RawHello} = eredis:q(Client, [<<"HELLO">>]),
    {parse_current_host(RawHello), parse_hello_hosts(RawHello)}.

clear_stats(Hosts) ->
    [{Host, 0}||{Host,_} <- Hosts].

connect(Hosts) ->
    RHosts = shuffle(Hosts),
    case try_connect_by_order(RHosts) of
        {ok, Client} ->
            {CurrentHost, Hosts1} = get_hosts(Client),
            Stats = dict:from_list(clear_stats(Hosts1)),
            {ok, #state{hosts = dict:from_list(Hosts1),
                        current_host = CurrentHost,
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
    {binary:part(lists:nth(1, LongHost), 0, 8), {binary_to_list(lists:nth(2, LongHost)), list_to_integer(binary_to_list(lists:nth(3, LongHost)))}}.

parse_hello_hosts(Raw) ->
    LongHosts = lists:dropwhile(fun(X) -> not is_list(X) end, Raw),
    lists:map(fun parse_long_host/1, LongHosts).

parse_current_host(HelloSpec) ->
    binary:part(lists:nth(2, HelloSpec), 0, 8).

get_host_short_ids(JobSpecs) ->
    lists:map(fun(Spec) -> binary:part(lists:nth(2, Spec), 2, 8) end, JobSpecs).

update_stats([], Stats) ->
    Stats;
update_stats([Host|Hosts], Stats) ->
    Stats1 = dict:update_counter(Host, 1, Stats),
    update_stats(Hosts, Stats1).

maybe_pick_client(Stats, State = #state{eredis_client = Client, current_host = CurrentHost, hosts = Hosts}) when State#state.cycle_count >= State#state.cycle ->

    {BestHash, SortedHosts} = sort_hosts_from_stats(Hosts, Stats),
    case BestHash of
        CurrentHost ->
            % We are connected to the best host
            State#state{cycle_count = 0, stats = dict:from_list(clear_stats(dict:to_list(Hosts)))};
        _ ->
            % There are best hosts, try to connect to them
            {ok, NewClient} = try_connect_by_order(SortedHosts),
            eredis:stop(Client),
            State#state{eredis_client = NewClient, cycle_count = 0, stats = dict:from_list(clear_stats(dict:to_list(Hosts)))}
    end;
maybe_pick_client(Stats, State = #state{cycle_count = CycleCount}) ->
    State#state{cycle_count = CycleCount + 1, stats = Stats}.

sort_hosts_from_stats(Hosts, Stats) ->
    HostHashes = [Hash || {Hash, _Count} <- lists:reverse(lists:keysort(2, dict:to_list(Stats)))],
    SortedHosts = lists:map(fun(Hash) ->
                                    {ok, Host} = dict:find(Hash, Hosts),
                                    Host
                            end, HostHashes),
    {lists:nth(1, HostHashes), SortedHosts}.
