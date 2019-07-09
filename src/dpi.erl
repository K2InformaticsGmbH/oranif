-module(dpi).
-compile({parse_transform, dpi_transform}).

-export([load/1, unload/1, register_process/1, pids_get/0, flush_process/0]).

-export([load_unsafe/0, load_unsafe/1]).
-export([safe/2, safe/3, safe/4]).

-include("dpiContext.hrl").
-include("dpiConn.hrl").
-include("dpiStmt.hrl").
-include("dpiQueryInfo.hrl").
-include("dpiData.hrl").
-include("dpiVar.hrl").

%===============================================================================
%   Slave Node APIs
%===============================================================================

-spec load(atom()) -> node().
load(SlaveNodeName) when is_atom(SlaveNodeName) ->
    case is_alive() of
        false -> {error, not_distributed};
        true ->
            case start_slave(SlaveNodeName) of
                {ok, SlaveNode} ->
                    case slave_call(SlaveNode, code, add_paths, [code:get_path()]) of
                        ok ->
                            case slave_call(SlaveNode, dpi, load_unsafe, []) of
                                ok -> SlaveNode;
                                Error -> Error
                            end;
                        Error -> Error
                    end;
                {error, {already_running, SlaveNode}} ->
                    %% TODO: Revisit if this is required. 
                    %  case catch rpc_call(SlaveNode, erlang, monotonic_time, []) of
                    %      Time when is_integer(Time) -> ok;
                    %      _ ->
                    %          catch unload(),
                    %          load(SlaveNodeName)
                    %  end
                    SlaveNode;
                Error -> Error
            end
    end.

-spec unload(atom()) -> ok.
unload(SlaveNode) ->
    slave_call(SlaveNode, dpi, flush_process, []),
    Self = self(),
    case catch rpc_call(SlaveNode, dpi, pids_get, []) of
        [] -> slave:stop(SlaveNode);
        [Self] -> slave:stop(SlaveNode);
        Refs -> io:format("~p still referencing ~p~n", [Refs, SlaveNode])
    end.

%===============================================================================
%   NIF test / debug interface (DO NOT use in production)
%===============================================================================

load_unsafe() -> load_unsafe(self()).
load_unsafe(RemotePid) ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path -> Path
    end,
    case erlang:load_nif(filename:join(PrivDir, "dpi_nif"), 0) of
        ok -> register_process(RemotePid);
        {error, {reload, _}} -> ok;
        {error, Error} -> {error, Error}
    end.

-spec(pids_get() -> [pid()]).
pids_get() ->
    exit({nif_library_not_loaded, dpi, pids_get}).

-spec(pids_set([pid()]) -> ok).
pids_set(Pids) when is_list(Pids)  ->
    exit({nif_library_not_loaded, dpi, pids_set}).

register_process(Pid) ->
    ExistingPids = pids_get(),
    NewPids = lists:filter(
            fun(P) ->
                true == rpc_call(
                    node(P), erlang, is_process_alive, [P]
                )
            end,
            [Pid | ExistingPids]
        ),
    io:format(
        user, "~p: Adding ~p to ~p now ~p~n",
        [{?MODULE, ?FUNCTION_NAME, ?LINE}, Pid, ExistingPids, NewPids]
    ),
    pids_set(NewPids).

flush_process() ->
    ExistingPids = pids_get(),
    NewPids = lists:filter(
            fun(P) ->
                true == rpc_call(
                    node(P), erlang, is_process_alive, [P]
                )
            end,
            ExistingPids
        ),
    io:format(
        user, "~p: flushing ~p to ~p~n",
        [{?MODULE, ?FUNCTION_NAME, ?LINE}, ExistingPids, NewPids]
    ),
    pids_set(NewPids).

%===============================================================================
%   local helper functions
%===============================================================================

start_slave(SlaveNodeName) when is_atom(SlaveNodeName) ->
    [_,SlaveHost] = string:tokens(atom_to_list(node()), "@"),
    ExtraArgs =
        case {init:get_argument(pa), init:get_argument(boot)} of
            {error, error} -> {error, bad_config};
            {error, {ok, [[Boot]]}} ->
                [_ | T] = lists:reverse(filename:split(Boot)),
                StartClean = filename:join(lists:reverse(["start_clean" | T])),
                case filelib:is_regular(StartClean ++ ".boot") of
                    true -> " -boot \"" ++ StartClean ++ "\"";
                    false ->
                    % {error, "Start clean boot not found"}
                        io:format(user, "[ERROR] REVISIT ~p:~p boot file not found!!~n", [?MODULE, ?LINE]),
                        []
                end;
            {{ok, _}, _} -> []
        end,
    case ExtraArgs of
        {error, _} = Error -> Error;
        ExtraArgs ->
            slave:start(
                SlaveHost, SlaveNodeName,
                lists:concat([
                    " -hidden ",
                    "-setcookie ", erlang:get_cookie(),
                    ExtraArgs
                ])
            )
    end.

slave_call(SlaveNode, Mod, Fun, Args) ->
    try rpc_call(SlaveNode, Mod, Fun, Args) of
        Result -> Result
    catch
        _Class:Error ->
            {error, Error}
    end.

rpc_call(Node, Mod, Fun, Args) ->
    case (catch rpc:call(Node, Mod, Fun, Args)) of
        {badrpc, {'EXIT', {Error, _}}} -> error(Error);
        {badrpc, nodedown} -> error(slave_down);
        Result -> Result
    end.

-spec safe(atom(), atom(), atom(), list()) -> term().
safe(SlaveNode, Module, Fun, Args) when is_atom(Module), is_atom(Fun), is_list(Args) ->
    slave_call(SlaveNode, Module, Fun, Args).

-spec safe(atom(), function(), list()) -> term().
safe(SlaveNode, Fun, Args) when is_function(Fun), is_list(Args) ->
    slave_call(SlaveNode, erlang, apply, [Fun, Args]).

-spec safe(atom(), function()) -> term().
safe(SlaveNode, Fun) when is_function(Fun)->
    slave_call(SlaveNode, erlang, apply, [Fun, []]).
