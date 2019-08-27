-module(dpi_eunit).
-include_lib("eunit/include/eunit.hrl").

%load_test() -> 
%    ?assertEqual(ok, dpi:load_unsafe()),
%    c:c(dpi),
%
%    ?debugMsg("triggering upgrade callback"),
%    ?assertEqual(ok, dpi:load_unsafe()),
%    % at this point, both old and current dpi code might be "bad"
%
%    % delete the old code
%    ?debugMsg("triggering unload callback"),
%    code:purge(dpi),
%
%    % make the new code old
%    code:delete(dpi),
%
%    % delete that old code, too. Now all the code is gone
%    ?debugMsg("triggering unload callback"),
%    code:purge(dpi).

slave_reuse_test() ->
    ?assertEqual(ok, dpi:load_unsafe()),
    ?debugFmt("procs in NIF ~p", [dpi:pids_get()]),

    RxTO = 1000, % 5 seconds

    Self = self(),
    Pid1 = spawn(fun() -> slave_client_proc(Self) end),
    Pid2 = spawn(fun() -> slave_client_proc(Self) end),
    Pid3 = spawn(fun() -> slave_client_proc(Self) end),
    Pid4 = spawn(fun() -> slave_client_proc(Self) end),

    ?assertEqual([self()], dpi:pids_get()),

    Pid1 ! load,
    Pid2 ! load,
    Pid3 ! load,
    Pid4 ! load,

    ?assertEqual(ok, receive {Pid1, loaded} -> ok after RxTO -> timeout end),
    ?assertEqual(ok, receive {Pid2, loaded} -> ok after RxTO -> timeout end),
    ?assertEqual(ok, receive {Pid3, loaded} -> ok after RxTO -> timeout end),
    ?assertEqual(ok, receive {Pid4, loaded} -> ok after RxTO -> timeout end),

    ?debugHere,

    ?assertEqual(ok, dpi:unload(node())),
    ?assertEqual(
        lists:usort([Pid1, Pid2, Pid3, Pid4]),
        lists:usort(dpi:pids_get())
    ),

    ?debugHere,

    Pid1 ! unload,
    Pid2 ! exit,
    Pid3 ! unload,
    Pid4 ! exit,

    ?debugHere,

    ?assertEqual(ok, receive {Pid1, unloaded} -> ok after RxTO -> timeout end),
    ?assertEqual(ok, receive {Pid2, exited} -> ok after RxTO -> timeout end),
    ?assertEqual(ok, receive {Pid3, unloaded} -> ok after RxTO -> timeout end),
    ?assertEqual(ok, receive {Pid4, exited} -> ok after RxTO -> timeout end),

    ?debugHere,

    ?assertEqual(false, is_process_alive(Pid1)),
    ?assertEqual(false, is_process_alive(Pid2)),
    ?assertEqual(false, is_process_alive(Pid3)),
    ?assertEqual(false, is_process_alive(Pid4)),

    ?debugHere,

    ?assertEqual(
        lists:usort([self(), Pid2, Pid3]),
        lists:usort(dpi:pids_get())
    ).
    
slave_client_proc(TestPid) ->
    Node = node(),
    receive
        load ->
            Node = dpi:load(testnode),
            TestPid ! {self(), loaded},
            ?debugFmt("~p has ~p", [self(), dpi:safe(Node, dpi, pids_get, [])]),
            slave_client_proc(TestPid);
        unload ->
            ok = dpi:unload(Node),
            TestPid ! {self(), unloaded};
        exit ->
            TestPid ! {self(), exited}
    end.
