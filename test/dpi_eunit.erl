-module(dpi_eunit).
-include_lib("eunit/include/eunit.hrl").

load_test() -> 
    ?assertEqual(ok, dpi:load_unsafe()),
    c:c(dpi),

    %load dpi again, triggering the upgrade when the NIF is loaded
    ?assertEqual(ok, dpi:load_unsafe()),
    % at this point, both old and current dpi code might be "bad"

    % delete the old code
    ?debugMsg("triggering unload callback"),
    code:purge(dpi),

    % make the new code old
    code:delete(dpi),

    %delete that old code, too. Now all the code is gone
    code:purge(dpi).
