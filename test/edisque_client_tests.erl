-module(edisque_client_tests).

-include_lib("eunit/include/eunit.hrl").

parse_hosts_test() ->
    Res = edisque_client:parse_hello_hosts([<<"1">>,<<"851a7a1416b0cf0474146d6a6d3555cb1ebeee50">>,
                                [<<"e890eef86300471a6033488f7b6af3f2be42f468">>,<<"127.0.0.1">>,
                                 <<"7712">>,<<"1">>],
                                [<<"851a7a1416b0cf0474146d6a6d3555cb1ebeee50">>,<<"127.0.0.1">>,
                                 <<"7711">>,<<"1">>]]),
    ?assertEqual([{<<"e890eef8">>, {<<"127.0.0.1">>,<<"7712">>}},
                  {<<"851a7a14">>, {<<"127.0.0.1">>,<<"7711">>}}]
                 , Res).

clear_stats_test() ->
    Res = edisque_client:clear_stats([{<<"e890eef8">>, {<<"127.0.0.1">>,<<"7712">>}},
                                      {<<"851a7a14">>, {<<"127.0.0.1">>,<<"7711">>}}]),
    ?assertEqual([{<<"e890eef8">>, 0},
                  {<<"851a7a14">>, 0}]
                 , Res).



add_and_get_job_test() ->
    C = c(),
    ?assertMatch({ok, _}, edisque:add_job(C, "queue", "body", 0)),
    ?assertMatch({ok, _}, edisque:get_job(C, ["queue"])).


c() ->
    Res = edisque:start_link(),
    ?assertMatch({ok, _}, Res),
    {ok, C} = Res,
    C.
