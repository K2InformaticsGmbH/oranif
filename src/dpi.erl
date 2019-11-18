-module(dpi).
-compile({parse_transform, dpi_transform}).

-export([load/1, unload/1]).

-export([load_unsafe/0]).
-export([safe/2, safe/3, safe/4]).

-export([resource_count/0]).

-include("dpiContext.hrl").
-include("dpiConn.hrl").
-include("dpiStmt.hrl").
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
                    case slave_call(
                        SlaveNode, code, add_paths, [code:get_path()]
                    ) of
                        ok ->
                            case slave_call(SlaveNode, dpi, load_unsafe, []) of
                                ok ->
                                    case reg(SlaveNode) of
                                        SlaveNode -> SlaveNode;
                                        Error ->
                                            slave:stop(SlaveNode),
                                            Error
                                    end;
                                Error -> Error
                            end;
                        Error -> Error
                    end;
                {error, {already_running, SlaveNode}} ->
                    case get_reg_name(SlaveNode) of
                        none ->
                            reg(SlaveNode);
                        Name ->
                            case global:whereis_name(Name) of
                                Pid when Pid == self() ->
                                    SlaveNode;
                                _ ->
                                    reg(SlaveNode)
                            end
                    end;
                Error -> Error
            end
    end.

-spec unload(atom()) -> ok | unloaded.
unload(SlaveNode) when is_atom(SlaveNode) ->
    UnloadingPid = self(),
    case get_reg_name(SlaveNode) of
        none ->
            ok;
        Name ->
            Pid = global:whereis_name(Name),
            case
                is_pid(Pid) andalso
                Pid /= UnloadingPid andalso
                rpc:call(node(Pid), erlang, is_process_alive, [Pid])
            of
                true ->
                    ok;
                _ ->
                    ok = global:unregister_name(Name),
                    slave:stop(SlaveNode),
                    unloaded
            end
    end.

%===============================================================================
%   NIF test / debug interface (DO NOT use in production)
%===============================================================================

load_unsafe() ->
    PrivDir = case code:priv_dir(?MODULE) of
        {error, _} ->
            io:format(
                user, "{~p,~p,~p} priv not found~n",
                [?MODULE, ?FUNCTION_NAME, ?LINE]
            ),
            EbinDir = filename:dirname(code:which(?MODULE)),
            AppPath = filename:dirname(EbinDir),
            filename:join(AppPath, "priv");
        Path ->
            io:format(
                user, "{~p,~p,~p} priv found ~p~n",
                [?MODULE, ?FUNCTION_NAME, ?LINE, Path]
            ),
            Path
    end,
    io:format(
        user, "{~p,~p,~p} PrivDir ~p~n",
        [?MODULE, ?FUNCTION_NAME, ?LINE, PrivDir]
    ),
    case erlang:load_nif(filename:join(PrivDir, "dpi_nif"), 0) of
        ok -> ok;
        {error, {reload, _}} -> ok;
        {error, Error} -> {error, Error}
    end.

%===============================================================================
%   local helper functions
%===============================================================================

reg(SlaveNode) ->
    Name = {?MODULE, SlaveNode, node(), make_ref()},
    case global:register_name(Name, self()) of
        yes -> SlaveNode;
        no -> {error, "failed to register process globally"}
    end.

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
    case rpc:call(SlaveNode, Mod, Fun, Args) of
        {badrpc, nodedown} -> {error, slave_down};
        {badrpc, {'EXIT', {Error, _}}} -> Error;
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

-spec get_reg_name(atom()) -> none | {atom, node(), node(), reference()}.
get_reg_name(SlaveNode) ->
    get_reg_name(SlaveNode, global:registered_names()).

-spec get_reg_name(atom(), list()) -> none | {atom, node(), node(), reference()}.
get_reg_name(_SlaveNode, []) ->
    none;
get_reg_name(SlaveN, [{?MODULE, SN, N, _} = Name | _]) when SN == SlaveN, N == node() ->
    Name;
get_reg_name(SlaveNode, [_ | Rest]) ->
    get_reg_name(SlaveNode, Rest).

resource_count() -> ?NIF_NOT_LOADED.
