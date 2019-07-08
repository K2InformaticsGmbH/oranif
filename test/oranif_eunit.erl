-module(oranif_eunit).
-include_lib("eunit/include/eunit.hrl").

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).

-define(EXEC_STMT(_Conn, _Sql),
    (fun() ->
        __Stmt = dpiCall(TestCtx, conn_prepareStmt, [_Conn, false, _Sql, <<"">>]),
        R = (catch dpiCall(TestCtx, stmt_execute, [__Stmt, []])),
        catch dpiCall(TestCtx, stmt_close, [__Stmt, <<>>]),
        R
    end)()
).

%-------------------------------------------------------------------------------
% Context tests
%-------------------------------------------------------------------------------

contextCreate(TestCtx) ->
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?assert(is_reference(Context)),
    dpiCall(TestCtx, context_destroy, [Context]).

contextCreate_NegativeMajType(TestCtx) ->
    ?assertException(
        error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, context_create, [foobar, ?DPI_MINOR_VERSION])
    ).

contextCreate_NegativeMinType(TestCtx) ->
    ?assertException(
        error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, context_create, [?DPI_MAJOR_VERSION, foobar])
    ).

% fails due to nonsense major version
contextCreate_NegativeFailCall(TestCtx) ->
    ?assertException(error, {error, _},
        dpiCall(TestCtx, context_create, [1337, ?DPI_MINOR_VERSION])),
    ok.

contextDestroy(TestCtx) ->
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, context_destroy, [Context])),
    ok.

contextDestroy_BadContext(TestCtx) ->
   ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, context_destroy, [foobar])),
    ok.

contextDestroy_NegativeContextState(TestCtx) ->
    Context = dpiCall(
        TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
    ),
    % destroy the context
    ?assertEqual(ok, dpiCall(TestCtx, context_destroy, [Context])),
    % try to destroy it again
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, context_destroy, [Context])),
    ok.

contextGetClientVersion(TestCtx) -> 
    Context = dpiCall(TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),

    #{
        releaseNum := CRNum, versionNum := CVNum, fullVersionNum := CFNum
    } = dpiCall(TestCtx, context_getClientVersion, [Context]),

    ?assert(is_integer(CRNum)),
    ?assert(is_integer(CVNum)),
    ?assert(is_integer(CFNum)).

contextGetClientVersion_NegativeContextType(TestCtx) -> 
    Context = dpiCall(TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, context_getClientVersion, [foobar])),
    dpiCall(TestCtx, context_destroy, [Context]),
    ok.

%% fails due to invalid context
contextGetClientVersion_NegativeFailCall(TestCtx) -> 
    Context = dpiCall(TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]),
    ?assertEqual(ok, dpiCall(TestCtx, context_destroy, [Context])), %% context is now invalid
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, context_getClientVersion, [foobar])), %% try to get client version of invalid context
    ok.


%%
%% CONN APIS
%%

connCreate(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    ?assert(is_reference(Conn)),
    dpiCall(TestCtx, conn_close, [Conn, [], <<>>]).

connCreate_BadContext(TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(
        error, {error, _File, _Line, _Exception},
        dpiCall(
            TestCtx, conn_create, [
                make_ref(), User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
            ]
        )
    ).

connCreate_NegativeUsernameType(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := _User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, foobar, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativePassType(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := _Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, User, foobat, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeTNSType(#{context := Context} = TestCtx) ->
    #{tns := _Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, User, Password, foobar,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeParamsType(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            foobar, #{}])),
    ok.

connCreate_NegativeEncodingType(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            #{encoding =>foobar, nencoding => "AL32UTF8"}, #{}])),
    ok.

connCreate_NegativeNencodingType(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => foobar}, #{}])),
    ok.

%% fails due to invalid user/pass combination
connCreate_NegativeFailCall(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_create, [Context, <<"Chuck">>, <<"Norris">>, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}])),
    ok.

connPrepareStmt(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, <<"foo">>]),
    ?assert(is_reference(Stmt)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

connPrepareStmt_emptyTag(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, <<"">>]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

connPrepareStmt_NegativeConnType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_prepareStmt, [foobar, false, <<"miau">>, <<"">>])),
    ok.

connPrepareStmt_NegativeScrollableType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_prepareStmt, [Conn, "foobar", <<"miau">>, <<>>])),
    ok.

connPrepareStmt_NegativeSQLType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, foobar, <<"">>])),
    ok.

connPrepareStmt_NegativeTagType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"miau">>, foobar])),
    ok.

%% fails due to both SQL and Tag being empty
connPrepareStmt_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"">>, <<"">>])),
    ok.
connNewVar(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    ?assert(is_reference(Var)),
    ?assert(is_list(Data)),
    [FirstData | _] = Data,
    ?assert(is_reference(FirstData)),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

connNewVar_NegativeConnType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [foobar, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null])),
    ok.

connNewVar_NegativeOraTypeType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, "foobar", 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null])),
    ok.

connNewVar_NegativeDpiTypeType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', "foobar", 100, 0, false, false, null])),
    ok.

connNewVar_NegativeArraySizeType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', foobar, 0, false, false, null])),
    ok.

connNewVar_NegativeSizeType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, foobar, false, false, null])),
    ok.

connNewVar_NegativeSizeIsBytesType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, "foobar", false, null])),
    ok.

connNewVar_NegativeIsArrayType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, "foobar", null])),
    ok.

connNewVar_NegativeObjTypeType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, "foobar"])),
    ok.

%% fails due to array size being 0
connNewVar_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 0, 0, false, false, null])),
    ok.

connCommit(#{session := Conn} = TestCtx) ->
    Result = dpiCall(TestCtx, conn_commit, [Conn]),
    ?assertEqual(ok, Result),
    ok.
  
connCommit_NegativeConnType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_commit, [foobar])),
    ok.

%% fails due to the reference being wrong
connCommit_NegativeFailCall(#{context := Context} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_commit, [Context])),
    ok.

connRollback(#{session := Conn} = TestCtx) ->
    Result = dpiCall(TestCtx, conn_rollback, [Conn]),
    ?assertEqual(ok, Result),
    ok.
  
connRollback_NegativeConnType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_rollback, [foobar])),
    ok.

%% fails due to the reference being wrong
connRollback_NegativeFailCall(#{context := Context} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_rollback, [Context])),
    ok.

connPing(#{session := Conn} = TestCtx) ->
    Result = dpiCall(TestCtx, conn_ping, [Conn]),
    ?assertEqual(ok, Result),
    ok.
  
connPing_NegativeConnType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_ping, [foobar])),
    ok.

%% fails due to the reference being wrong
connPing_NegativeFailCall(#{context := Context} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_ping, [Context])),
    ok.

connClose(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    Result = dpiCall(TestCtx, conn_close, [Conn, [], <<"">>]),
    ?assertEqual(ok, Result),
    ok.

connCloseWithModes(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    Result = dpiCall(TestCtx, conn_close, [Conn, ['DPI_MODE_CONN_CLOSE_DEFAULT'], <<"">>]), %% the other two don't work without a session pool
    ?assertEqual(ok, Result),
    ok.
  
connClose_NegativeConnType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_close, [foobar, [], <<"">>])),
    ok.

connClose_NegativeModesType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_close, [Conn, foobar, <<"">>])),
    ok.

connClose_NegativeModeInsideType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_close, [Conn, ["not an atom"], <<"">>])),
    ok.

connClose_NegativeInvalidMode(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_close, [Conn, [foobar], <<"">>])),
    ok.

connClose_NegativeTagType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_close, [Conn, [], foobar])),
    ok.

%% fails due to the reference being wrong
connClose_NegativeFailCall(#{context := Context} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_close, [Context, [], <<"">>])),
    ok.

connGetServerVersion(#{session := Conn} = TestCtx) ->
    #{
        releaseNum := ReleaseNum, versionNum := VersionNum, fullVersionNum := FullVersionNum,
        portReleaseNum := PortReleaseNum, portUpdateNum := PortUpdateNum,
        releaseString := ReleaseString
    } = dpiCall(TestCtx, conn_getServerVersion, [Conn]),
    ?assert(is_integer(ReleaseNum)),
    ?assert(is_integer(VersionNum)),
    ?assert(is_integer(FullVersionNum)),
    ?assert(is_integer(PortReleaseNum)),
    ?assert(is_integer(PortUpdateNum)),
    ?assert(is_list(ReleaseString)),
    ok.
  
connGetServerVersion_NegativeConnType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_getServerVersion, [foobar])),
    ok.

%% fails due to the reference being completely wrong (apparently passing a released connection isn't bad enough)
connGetServerVersion_NegativeFailCall(#{context := Context} = TestCtx) ->
    #{tns := Tns, user := User, password := Password} = getConfig(),
    Conn = dpiCall(TestCtx, conn_create, [Context, User, Password, Tns,
            #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}]),
    dpiCall(TestCtx, conn_close, [Conn, [], <<>>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, conn_getServerVersion, [Context])),
    ok.


%%%
%%% STMT APIS
%%%

stmtExecute(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryCols = dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(1, QueryCols),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtExecuteWithModes(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryCols = dpiCall(TestCtx, stmt_execute, [Stmt, ['DPI_MODE_EXEC_DEFAULT']]),
    ?assertEqual(1, QueryCols),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtExecute_NegativeStmtType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_execute, [foobar, []])),
    ok.

stmtExecute_NegativeModesType(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_execute, [Stmt, foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtExecute_NegativeModeInsideType(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_execute, [Stmt, ["not an atom"]])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the SQL being invalid
stmtExecute_NegativeFailCall(#{session := Conn, context := Context} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"all your base are belong to us">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_execute, [Stmt, []])),
    ok.

stmtFetch(#{session := Conn} = TestCtx) ->
    SQL = <<"select 1337 from dual">>,
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, SQL, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    #{found := Found, bufferRowIndex := BufferRowIndex} = dpiCall(TestCtx, stmt_fetch, [Stmt]),
    ?assert(is_atom(Found)),
    ?assert(is_integer(BufferRowIndex)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtFetch_NegativeStmtType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_fetch, [foobar])),
    ok.

%% fails due to the reference being of the wrong type
stmtFetch_NegativeFailCall(#{session := Conn, context := Context} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi (a) values (1337)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_fetch, [Conn])),
    ok.

stmtGetQueryValue(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    dpiCall(TestCtx, stmt_fetch, [Stmt]),
    #{nativeTypeNum := Type, data := Result} =
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, 1]),
    ?assert(is_atom(Type)),
    ?assert(is_reference(Result)),
    dpiCall(TestCtx, data_release, [Result]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetQueryValue_NegativeStmtType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getQueryValue, [foobar, 1])),
    ok.

stmtGetQueryValue_NegativePosType(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the fetch not being done
stmtGetQueryValue_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getQueryValue, [Stmt, 1])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetQueryInfo(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    Info = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    ?assert(is_reference(Info)),
    dpiCall(TestCtx, queryInfo_delete, [Info]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetQueryInfo_NegativeStmtType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getQueryInfo, [foobar, 1])),
    ok.

stmtGetQueryInfo_NegativePosType(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the SQL being bad
stmtGetQueryInfo_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"bibidi babidi boo">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetNumQueryColumns(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1337 from dual">>, <<"">>]),
    Count = dpiCall(TestCtx, stmt_getNumQueryColumns, [Stmt]),
    ?assert(is_integer(Count)),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtGetNumQueryColumns_NegativeStmtType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getNumQueryColumns, [foobar])),
    ok.


%% fails due to the statement being released too early
stmtGetNumQueryColumns_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"it is showtime">>, <<"">>]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_getNumQueryColumns, [Stmt])),
    ok.

stmtBindValueByPos(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindValueByPos_NegativeStmtType(#{session := Conn} = TestCtx) -> 
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
    dpiCall(TestCtx, stmt_bindValueByPos, [foobar, 1, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    ok.

stmtBindValueByPos_NegativePosType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, foobar, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByPos_NegativeTypeType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, 1, "foobar", BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByPos_NegativeDataType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, 1, 'DPI_NATIVE_TYPE_INT64', foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the position being invalid
stmtBindValueByPos_NegativeFailCall(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByPos, [Stmt, -1, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.


stmtBindValueByName(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindValueByName, [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindValueByName_NegativeStmtType(#{session := Conn} = TestCtx) -> 
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
    dpiCall(TestCtx, stmt_bindValueByName, [foobar, <<"A">>, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    ok.

stmtBindValueByName_NegativePosType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByName, [Stmt, foobar, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByName_NegativeTypeType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByName, [Stmt, <<"A">>, "foobar", BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtBindValueByName_NegativeDataType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByName, [Stmt, <<"A">>, 'DPI_NATIVE_TYPE_INT64', foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the name being invalid
stmtBindValueByName_NegativeFailCall(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    BindData = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindValueByName, [Stmt, <<"B">>, 'DPI_NATIVE_TYPE_INT64', BindData])),
    dpiCall(TestCtx, data_release, [BindData]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>),
    %% also freeing the stmt here causes it to abort
    ok.

stmtBindByPos(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindByPos, [Stmt, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByPos_NegativeStmtType(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByPos, [foobar, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByPos_NegativePosType(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, foobar, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByPos_NegativeVarType(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, 1, foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

%% fails due to the position being invalid
stmtBindByPos_NegativeFailCall(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByPos, [Stmt, -1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"A">>, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_NegativeStmtType(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByName, [foobar, <<"A">>, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_NegativePosType(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByName, [Stmt, foobar, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtBindByName_NegativeVarType(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"A">>, foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

%% fails due to the position being invalid
stmtBindByName_NegativeFailCall(#{session := Conn} = TestCtx) -> 
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ?EXEC_STMT(Conn, <<"create table test_dpi (a integer)">>), 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"insert into test_dpi values (:A)">>, <<"">>]),
    #{var := Var, data := Data} = 
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"B">>, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ?EXEC_STMT(Conn, <<"drop table test_dpi">>), 
    ok.

stmtDefine(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    #{var := Var, data := Data} =
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_define, [Stmt, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefine_NegativeStmtType(#{session := Conn} = TestCtx) -> 
    #{var := Var, data := Data} =
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_define, [foobar, 1, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

stmtDefine_NegativePosType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    #{var := Var, data := Data} =
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_define, [Stmt, foobar, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefine_NegativeVarType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
    dpiCall(TestCtx, stmt_define, [Stmt, 1, foobar])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to the pos being invalid
stmtDefine_NegativeFailCall(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    #{var := Var, data := Data} =
        dpiCall(TestCtx, conn_newVar, [Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 100, 0, false, false, null]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_define, [Stmt, 12345, Var])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefineValue(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertEqual(ok, dpiCall(TestCtx, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

stmtDefineValue_NegativeStmtType(#{session := Conn} = TestCtx) -> 
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [foobar, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    ok.

stmtDefineValue_NegativePosType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [Stmt, foobar, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeOraTypeType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [Stmt, 1, "foobar", 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeNativeTypeType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', "foobar", 0, false, null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeSizeType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', foobar, false, null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.


stmtDefineValue_NegativeSizeInBytesType(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [Stmt, 1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, "foobar", null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%% fails due to invalid position
stmtDefineValue_NegativeFailCall(#{session := Conn} = TestCtx) -> 
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, stmt_defineValue, [Stmt, -1, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 0, false, null])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

%%%
%%% Var APIS
%%%

varSetNumElementsInArray(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setNumElementsInArray, [Var, 100])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

varSetNumElementsInArray_NegativeVarType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setNumElementsInArray, [foobar, 100])),
    ok.

varSetNumElementsInArray_NegativeNumElementsType(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setNumElementsInArray, [Var, foobar])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

%% fails due to invalid array size
varSetNumElementsInArray_NegativeFailCall(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setNumElementsInArray, [Var, -1])),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

varSetFromBytes(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setFromBytes, [Var, 0, <<"abc">>])),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

varSetFromBytes_NegativeVarType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setFromBytes, [foobar, 0, <<"abc">>])),
    ok.

varSetFromBytes_NegativePosType(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setFromBytes, [Var, foobar, <<"abc">>])),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

varSetFromBytes_NegativeBinaryType(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setFromBytes, [Var, 0, foobar])),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

%% fails due to position being invalid
varSetFromBytes_NegativeFailCall(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_setFromBytes, [Var, -1, <<"abc">>])),
    
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    dpiCall(TestCtx, var_release, [Var]),
    ok.

varRelease(#{session := Conn} = TestCtx) ->
    #{var := Var, data := Data} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 100, 100,
            true, true, null
        ]
    ),
    [dpiCall(TestCtx, data_release, [X]) || X <- Data],
    ?assertEqual(ok, dpiCall(TestCtx, var_release, [Var])),
    ok.

varRelease_NegativeVarType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_release, [foobar])),
    ok.

%% fails due to the reference being wrong
varRelease_NegativeFailCall(#{session := Conn, context := Context} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, var_release, [Context])),
    ok.

%%%
%%% QuryInfo APIS
%%%

queryInfoGet(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    #{name := Name, nullOk := NullOk,
        typeInfo := #{clientSizeInBytes := ClientSizeInBytes, dbSizeInBytes := DbSizeInBytes,
            defaultNativeTypeNum := DefaultNativeTypeNum, fsPrecision := FsPrecision,
            objectType := ObjectType, ociTypeCode := OciTypeCode,
            oracleTypeNum := OracleTypeNum , precision := Precision,
            scale := Scale, sizeInChars := SizeInChars}} = dpiCall(TestCtx, queryInfo_get, [QueryInfoRef]),

    ?assert(is_list(Name)),
    ?assert(is_atom(NullOk)),
    ?assert(is_integer(ClientSizeInBytes)),
    ?assert(is_integer(DbSizeInBytes)),
    ?assert(is_atom(DefaultNativeTypeNum)),
    ?assert(is_integer(FsPrecision)),
    ?assert(is_atom(ObjectType)),
    ?assert(is_integer(OciTypeCode)),
    ?assert(is_atom(OracleTypeNum)),
    ?assert(is_integer(Precision)),
    ?assert(is_integer(Scale)),
    ?assert(is_integer(SizeInChars)),
    
    dpiCall(TestCtx, queryInfo_delete, [QueryInfoRef]),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

queryInfoGet_NegativeQueryInfoType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, queryInfo_get, [foobar])),
    ok.

%% fails due to getting a completely wrong reference
queryInfoGet_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, queryInfo_get, [Conn])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

queryInfoDelete(#{session := Conn} = TestCtx) ->
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [Conn, false, <<"select 1 from dual">>, <<"">>]),
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, 1]),
    ?assertEqual(ok, dpiCall(TestCtx, queryInfo_delete, [QueryInfoRef])),
    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    ok.

queryInfoDelete_NegativeQueryInfoType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, queryInfo_delete, [foobar])),
    ok.

%% fails due to getting a completely wrong reference
queryInfoDelete_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, queryInfo_delete, [Conn])),
    ok.

%%%
%%% Data APIS
%%%

dataSetTimestamp(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [foobar, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    ok.

dataSetTimestamp_NegativeYearType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, foobar, 2, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeMonthType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, foobar, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeDayType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, foobar, 4, 5, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeHourType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, foobar, 5, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeMinuteType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, foobar, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeSecondType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, foobar, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeFSecondType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, foobar, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeTZHourOffsetType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, foobar, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_NegativeTZMinuteOffsetType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, foobar])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
%% (it doesn't seem to mind the nonsense parameters. Year -1234567? Sure. Timezone of -22398 hours and 3239 minutes? No problem)
dataSetTimestamp_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setTimestamp, [Conn, -1234567, 2, 3, 4, 5, 6, 7, -22398, 3239])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetTimestamp_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setTimestamp, [Data, 1, 2, 3, 4, 5, 6, 7, 8, 9])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataSetIntervalDS(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [foobar, 1, 2, 3, 4, 5])),
    ok.

dataSetIntervalDS_NegativeDayType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [Data, foobar, 2, 3, 4, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeHoursType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, foobar, 3, 4, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeMinutesType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, foobar, 4, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeSecondsType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, foobar, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalDS_NegativeFSecondsType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, foobar])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetIntervalDS_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalDS, [Conn, 1, 2, 3, 4, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalDS_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalDS, [Data, 1, 2, 3, 4, 5])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.


dataSetIntervalYM(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, 2])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalYM_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalYM, [foobar, 1, 2])),
    ok.

dataSetIntervalYM_NegativeYearType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalYM, [Data, foobar, 2])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalYM_NegativeMonthType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, foobar])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetIntervalYM_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIntervalYM, [Conn, 1, 2])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIntervalYM_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIntervalYM, [Data, 1, 2])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.


dataSetInt64(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setInt64, [Data, 1])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetInt64_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setInt64, [foobar, 1])),
    ok.

dataSetInt64_NegativeAmountType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setInt64, [Data, foobar])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetInt64_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setInt64, [Conn, 1])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetInt64_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setInt64, [Data, 1])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataSetBytes(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setBytes, [Data, <<"my string">>])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetBytes_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setBytes, [foobar, <<"my string">>])),
    ok.

dataSetBytes_NegativeBinaryType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setBytes, [Data, foobar])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetBytes_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setBytes, [Conn, <<"my string">>])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIsNullTrue(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIsNull, [Data, true])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIsNullFalse(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIsNull, [Data, false])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIsNull_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIsNull, [foobar, 1])),
    ok.

dataSetIsNull_NegativeIsNullType(TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIsNull, [Data, "not an atom"])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

%% fails due to the Data ref passed being completely wrong
dataSetIsNull_NegativeFailCall(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_setIsNull, [Conn, 1])),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataSetIsNull_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        dpiCall(TestCtx, data_setIsNull, [Data, true])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetNull(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, true]),
    ?assertEqual(null, dpiCall(TestCtx, data_get, [Data])),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetTInt64(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetUint64(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_UINT', 'DPI_NATIVE_TYPE_UINT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetFloat(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_FLOAT', 'DPI_NATIVE_TYPE_FLOAT', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetDouble(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_DOUBLE', 'DPI_NATIVE_TYPE_DOUBLE', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_float(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetBinary(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 100,
            true, true, null
        ]
    ),
    ?assertEqual(ok, dpiCall(TestCtx, var_setFromBytes, [Var, 0, <<"my string">>])),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(TestCtx, data_get, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetTimestamp(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_TIMESTAMP_TZ', 'DPI_NATIVE_TYPE_TIMESTAMP', 1, 100,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    #{year := Year, month := Month, day := Day,
        hour := Hour, minute := Minute, second := Second, 
        fsecond := Fsecond, tzHourOffset := TzHourOffset, tzMinuteOffset := TzMinuteOffset} =
        dpiCall(TestCtx, data_get, [Data]),
    ?assert(is_integer(Year)),
    ?assert(is_integer(Month)),
    ?assert(is_integer(Day)),
    ?assert(is_integer(Hour)),
    ?assert(is_integer(Minute)),
    ?assert(is_integer(Second)),
    ?assert(is_integer(Fsecond)),
    ?assert(is_integer(TzHourOffset)),
    ?assert(is_integer(TzMinuteOffset)),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetIntervalDS(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_DS', 'DPI_NATIVE_TYPE_INTERVAL_DS', 1, 100,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    #{days := Days, hours := Hours, minutes := Minutes, 
        seconds := Seconds, fseconds := Fseconds} =
        dpiCall(TestCtx, data_get, [Data]),
    ?assert(is_integer(Days)),
    ?assert(is_integer(Hours)),
    ?assert(is_integer(Minutes)),
    ?assert(is_integer(Seconds)),
    ?assert(is_integer(Fseconds)),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.


dataGetIntervalYM(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_INTERVAL_YM', 'DPI_NATIVE_TYPE_INTERVAL_YM', 1, 100,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    #{years := Years, months := Months} =
        dpiCall(TestCtx, data_get, [Data]),
    ?assert(is_integer(Years)),
    ?assert(is_integer(Months)),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetStmt(#{session := Conn} = TestCtx) ->
    CreateStmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure ORANIF_TEST_1
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select 1 from dual;
            end ORANIF_TEST_1;">>,
        <<"">>
    ]),
    dpiCall(TestCtx, stmt_execute, [CreateStmt, []]),
    dpiCall(TestCtx, stmt_close, [CreateStmt, <<>>]),

    #{var := VarStmt, data := [DataStmt]} = dpiCall(TestCtx, conn_newVar, [
        Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0,
        false, false, null
    ]),
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false, <<"begin ORANIF_TEST_1(:cursor); end;">>, <<"">>
    ]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"cursor">>, VarStmt]),

    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assert(is_reference( dpiCall(TestCtx, data_get, [DataStmt]))), %% first-time get
    ?assert(is_reference( dpiCall(TestCtx, data_get, [DataStmt]))), %% cached re-get
    dpiCall(TestCtx, data_release, [DataStmt]),
    dpiCall(TestCtx, var_release, [VarStmt]),
    ok.

dataGetStmtChange(#{session := Conn} = TestCtx) ->
    CreateStmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure ORANIF_TEST_1
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select 1 from dual;
            end ORANIF_TEST_1;">>,
        <<"">>
    ]),
    dpiCall(TestCtx, stmt_execute, [CreateStmt, []]),
    dpiCall(TestCtx, stmt_close, [CreateStmt, <<>>]),

    CreateStmt2 = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false,
        <<"create or replace procedure ORANIF_TEST_2
            (p_cur out sys_refcursor)
                is
                begin
                    open p_cur for select 2 from dual;
            end ORANIF_TEST_2;">>,
        <<"">>
    ]),
    dpiCall(TestCtx, stmt_execute, [CreateStmt2, []]),
    dpiCall(TestCtx, stmt_close, [CreateStmt2, <<>>]),

    #{var := VarStmt, data := [DataStmt]} = dpiCall(TestCtx, conn_newVar, [
        Conn, 'DPI_ORACLE_TYPE_STMT', 'DPI_NATIVE_TYPE_STMT', 1, 0,
        false, false, null
    ]),
    Stmt = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false, <<"begin ORANIF_TEST_1(:cursor); end;">>, <<"">>
    ]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt, <<"cursor">>, VarStmt]),
    dpiCall(TestCtx, stmt_execute, [Stmt, []]),
    ?assert(is_reference( dpiCall(TestCtx, data_get, [DataStmt]))), %% first-time get

    Stmt2 = dpiCall(TestCtx, conn_prepareStmt, [
        Conn, false, <<"begin ORANIF_TEST_2(:cursor); end;">>, <<"">>
    ]),
    ok = dpiCall(TestCtx, stmt_bindByName, [Stmt2, <<"cursor">>, VarStmt]),
    dpiCall(TestCtx, stmt_execute, [Stmt2, []]),
    ?assert(is_reference( dpiCall(TestCtx, data_get, [DataStmt]))), %% "ref cursor changed"
    dpiCall(TestCtx, data_release, [DataStmt]),
    dpiCall(TestCtx, var_release, [VarStmt]),

    dpiCall(TestCtx, stmt_close, [Stmt, <<>>]),
    dpiCall(TestCtx, stmt_close, [Stmt2, <<>>]),
    ok.

dataGet_NegativeDataType(TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_get, [foobar])),
    ok.

%% fails due to completely wrong reference
dataGet_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        dpiCall(TestCtx, data_get, [Conn])),
    ok.


dataGetInt64(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_getInt64, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    ok.

dataGetInt64_NegativeDataType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(TestCtx, data_getInt64, [foobar]))),
    ok.

%% fails due to completely wrong reference
dataGetInt64_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(TestCtx, data_getInt64, [Conn]))),
    ok.

dataGetInt64_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_integer(dpiCall(TestCtx, data_getInt64, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

%% no non-pointer test for this one
dataGetBytes(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 1, 1,
            true, true, null
        ]
    ),
    dpiCall(TestCtx, data_setIsNull, [Data, false]),
    ?assert(is_binary(dpiCall(TestCtx, data_getBytes, [Data]))),
    dpiCall(TestCtx, data_release, [Data]),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

dataGetBytes_NegativeDataType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(TestCtx, data_getBytes, [foobar]))),
    ok.

%% fails due to completely wrong reference
dataGetBytes_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(TestCtx, data_getBytes, [Conn]))),
    ok.

dataRelease(#{session := Conn} = TestCtx) ->
    Data = dpiCall(TestCtx, data_ctor, []),
    ?assertEqual(ok,
        (dpiCall(TestCtx, data_release, [Data]))),
    ok.

dataRelease_NegativeDataType(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(TestCtx, data_release, [foobar]))),
    ok.

%% fails due to completely wrong reference
dataRelease_NegativeFailCall(#{session := Conn} = TestCtx) ->
    ?assertException(error, {error, _File, _Line, _Exception},
        (dpiCall(TestCtx, data_release, [Conn]))),
    ok.

dataRelease_viaPointer(#{session := Conn} = TestCtx) ->
    #{var := Var, data := [Data]} = dpiCall(
        TestCtx, conn_newVar, [
            Conn, 'DPI_ORACLE_TYPE_NATIVE_INT', 'DPI_NATIVE_TYPE_INT64', 1, 1,
            true, true, null
        ]
    ),
    ?assertEqual(ok,
        (dpiCall(TestCtx, data_release, [Data]))),
    dpiCall(TestCtx, var_release, [Var]),
    ok.

%-------------------------------------------------------------------------------
% eunit infrastructure callbacks
%-------------------------------------------------------------------------------
-define(SLAVE, oranif_slave).

setup(#{safe := false}) ->
    ok = dpi:load_unsafe(),
    #{safe => false};
setup(#{safe := true}) ->
    SlaveNode = dpi:load(?SLAVE),
    pong = net_adm:ping(SlaveNode),
    #{safe => true, node => SlaveNode}.

setup_context(TestCtx) ->
    SlaveCtx = setup(TestCtx),
    SlaveCtx#{
        context => dpiCall(
            TestCtx, context_create, [?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION]
        )
    }.

setup_connecion(TestCtx) ->
    ContextCtx = #{context := Context} = setup_context(TestCtx),
    #{tns := Tns, user := User, password := Password} = getConfig(),
    ContextCtx#{
        session => dpiCall(
            TestCtx, conn_create, [
                Context, User, Password, Tns,
                #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
            ]
        )
    }.

cleanup(#{safe := Safe, session := Connnnection} = Ctx) ->
    dpiCall(Ctx, conn_close, [Connnnection, [], <<>>]),
    cleanup(maps:without([session], Ctx));
cleanup(#{safe := Safe, context := Context} = Ctx) ->
    dpiCall(Ctx, context_destroy, [Context]),
    cleanup(maps:without([context], Ctx));
cleanup(#{safe := true, node := SlaveNode}) ->
    dpiCall(true, unload, [SlaveNode]);
cleanup(_) -> ok.

testDescriptorMatch(List) ->
    fun(Setup) ->
    [?_assertNot(error =:= X(Setup)) || X <- List] end.

-define(NO_CONTEXT_TESTS, [
    fun contextCreate/1,
    fun contextCreate_NegativeMajType/1,
    fun contextCreate_NegativeMinType/1,
    fun contextCreate_NegativeFailCall/1,
    fun contextDestroy/1,
    fun contextDestroy_BadContext/1,
    fun contextDestroy_NegativeContextState/1,
    fun contextGetClientVersion/1,
    fun contextGetClientVersion_NegativeContextType/1,
    fun contextGetClientVersion_NegativeFailCall/1
]).

-define(AFTER_CONTEXT_TESTS, [
    fun connCreate/1,
    fun connCreate_BadContext/1,
    fun connCreate_NegativeUsernameType/1,
    fun connCreate_NegativePassType/1,
    fun connCreate_NegativeTNSType/1,
    fun connCreate_NegativeParamsType/1,
    fun connCreate_NegativeEncodingType/1,
    fun connCreate_NegativeNencodingType/1,
    fun connCreate_NegativeFailCall/1
]).

-define(AFTER_CONNECTION_TESTS, [
    fun connPrepareStmt/1,
    fun connPrepareStmt_emptyTag/1,
    fun connPrepareStmt_NegativeConnType/1,
    fun connPrepareStmt_NegativeScrollableType/1,
    fun connPrepareStmt_NegativeSQLType/1,
    fun connPrepareStmt_NegativeTagType/1,
    fun connPrepareStmt_NegativeFailCall/1,
    fun connNewVar/1,
    fun connNewVar_NegativeConnType/1,
    fun connNewVar_NegativeOraTypeType/1,
    fun connNewVar_NegativeDpiTypeType/1,
    fun connNewVar_NegativeArraySizeType/1,
    fun connNewVar_NegativeSizeType/1,
    fun connNewVar_NegativeSizeIsBytesType/1,
    fun connNewVar_NegativeIsArrayType/1,
    fun connNewVar_NegativeObjTypeType/1,
    fun connNewVar_NegativeFailCall/1,
    fun connCommit/1,
    fun connCommit_NegativeConnType/1,
    fun connCommit_NegativeFailCall/1,
    fun connRollback/1,
    fun connRollback_NegativeConnType/1,
    fun connRollback_NegativeFailCall/1,
    fun connPing/1,
    fun connPing_NegativeConnType/1,
    fun connPing_NegativeFailCall/1,
    fun connClose/1,
    fun connCloseWithModes/1,
    fun connClose_NegativeConnType/1,
    fun connClose_NegativeModesType/1,
    fun connClose_NegativeModeInsideType/1,
    fun connClose_NegativeInvalidMode/1,
    fun connClose_NegativeTagType/1,
    fun connClose_NegativeFailCall/1,
    fun connGetServerVersion/1,
    fun connGetServerVersion_NegativeConnType/1,
    fun connGetServerVersion_NegativeFailCall/1,
    fun stmtExecute/1,
    fun stmtExecuteWithModes/1,
    fun stmtExecute_NegativeStmtType/1,
    fun stmtExecute_NegativeModesType/1,
    fun stmtExecute_NegativeModeInsideType/1,
    fun stmtExecute_NegativeFailCall/1,
    fun stmtFetch/1,
    fun stmtFetch_NegativeStmtType/1,
    fun stmtFetch_NegativeFailCall/1,
    fun stmtGetQueryValue/1,
    fun stmtGetQueryValue_NegativeStmtType/1,
    fun stmtGetQueryValue_NegativePosType/1,
    fun stmtGetQueryValue_NegativeFailCall/1,
    fun stmtGetQueryInfo/1,
    fun stmtGetQueryInfo_NegativeStmtType/1,
    fun stmtGetQueryInfo_NegativePosType/1,
    fun stmtGetQueryInfo_NegativeFailCall/1,
    fun stmtGetNumQueryColumns/1,
    fun stmtGetNumQueryColumns_NegativeStmtType/1,
    fun stmtGetNumQueryColumns_NegativeFailCall/1,
    fun stmtBindValueByPos/1,
    fun stmtBindValueByPos_NegativeStmtType/1,
    fun stmtBindValueByPos_NegativePosType/1,
    fun stmtBindValueByPos_NegativeTypeType/1,
    fun stmtBindValueByPos_NegativeDataType/1,
    fun stmtBindValueByPos_NegativeFailCall/1,
    fun stmtBindValueByName/1,
    fun stmtBindValueByName_NegativeStmtType/1,
    fun stmtBindValueByName_NegativePosType/1,
    fun stmtBindValueByName_NegativeTypeType/1,
    fun stmtBindValueByName_NegativeDataType/1,
    fun stmtBindValueByName_NegativeFailCall/1,
    fun stmtBindByPos/1,
    fun stmtBindByPos_NegativeStmtType/1,
    fun stmtBindByPos_NegativePosType/1,
    fun stmtBindByPos_NegativeVarType/1,
    fun stmtBindByPos_NegativeFailCall/1,
    fun stmtBindByName/1,
    fun stmtBindByName_NegativeStmtType/1,
    fun stmtBindByName_NegativePosType/1,
    fun stmtBindByName_NegativeVarType/1,
    fun stmtBindByName_NegativeFailCall/1,
    fun stmtDefine/1,
    fun stmtDefine_NegativeStmtType/1,
    fun stmtDefine_NegativePosType/1,
    fun stmtDefine_NegativeVarType/1,
    fun stmtDefine_NegativeFailCall/1,
    fun stmtDefineValue/1,
    fun stmtDefineValue_NegativeStmtType/1,
    fun stmtDefineValue_NegativePosType/1,
    fun stmtDefineValue_NegativeOraTypeType/1,
    fun stmtDefineValue_NegativeNativeTypeType/1,
    fun stmtDefineValue_NegativeSizeType/1,
    fun stmtDefineValue_NegativeSizeInBytesType/1,
    fun stmtDefineValue_NegativeFailCall/1,
    fun varSetNumElementsInArray/1,
    fun varSetNumElementsInArray_NegativeVarType/1,
    fun varSetNumElementsInArray_NegativeNumElementsType/1,
    fun varSetNumElementsInArray_NegativeFailCall/1,
    fun varSetFromBytes/1,
    fun varSetFromBytes_NegativeVarType/1,
    fun varSetFromBytes_NegativePosType/1,
    fun varSetFromBytes_NegativeBinaryType/1,
    fun varSetFromBytes_NegativeFailCall/1,
    fun varRelease/1,
    fun varRelease_NegativeVarType/1,
    fun varRelease_NegativeFailCall/1,
    fun queryInfoGet/1,
    fun queryInfoGet_NegativeQueryInfoType/1,
    fun queryInfoGet_NegativeFailCall/1,
    fun queryInfoDelete/1,
    fun queryInfoDelete_NegativeQueryInfoType/1,
    fun queryInfoDelete_NegativeFailCall/1,
    fun dataSetTimestamp/1,
    fun dataSetTimestamp_NegativeDataType/1,
    fun dataSetTimestamp_NegativeYearType/1,
    fun dataSetTimestamp_NegativeMonthType/1,
    fun dataSetTimestamp_NegativeDayType/1,
    fun dataSetTimestamp_NegativeHourType/1,
    fun dataSetTimestamp_NegativeMinuteType/1,
    fun dataSetTimestamp_NegativeSecondType/1,
    fun dataSetTimestamp_NegativeFSecondType/1,
    fun dataSetTimestamp_NegativeTZHourOffsetType/1,
    fun dataSetTimestamp_NegativeTZMinuteOffsetType/1,
    fun dataSetTimestamp_NegativeFailCall/1,
    fun dataSetTimestamp_viaPointer/1,
    fun dataSetIntervalDS/1,
    fun dataSetIntervalDS_NegativeDataType/1,
    fun dataSetIntervalDS_NegativeDayType/1,
    fun dataSetIntervalDS_NegativeHoursType/1,
    fun dataSetIntervalDS_NegativeMinutesType/1,
    fun dataSetIntervalDS_NegativeSecondsType/1,
    fun dataSetIntervalDS_NegativeFSecondsType/1,
    fun dataSetIntervalDS_NegativeFailCall/1,
    fun dataSetIntervalDS_viaPointer/1,
    fun dataSetIntervalYM/1,
    fun dataSetIntervalYM_NegativeDataType/1,
    fun dataSetIntervalYM_NegativeYearType/1,
    fun dataSetIntervalYM_NegativeMonthType/1,
    fun dataSetIntervalYM_NegativeFailCall/1,
    fun dataSetIntervalYM_viaPointer/1,
    fun dataSetInt64/1,
    fun dataSetInt64_NegativeDataType/1,
    fun dataSetInt64_NegativeAmountType/1,
    fun dataSetInt64_NegativeFailCall/1,
    fun dataSetInt64_viaPointer/1,
    fun dataSetBytes/1,
    fun dataSetBytes_NegativeDataType/1,
    fun dataSetBytes_NegativeBinaryType/1,
    fun dataSetBytes_NegativeFailCall/1,
    fun dataSetIsNullTrue/1,
    fun dataSetIsNullFalse/1,
    fun dataSetIsNull_NegativeDataType/1,
    fun dataSetIsNull_NegativeIsNullType/1,
    fun dataSetIsNull_NegativeFailCall/1,
    fun dataSetIsNull_viaPointer/1,
    fun dataGetNull/1,
    fun dataGetTInt64/1,
    fun dataGetUint64/1,
    fun dataGetFloat/1,
    fun dataGetDouble/1,
    fun dataGetBinary/1,
    fun dataGetTimestamp/1,
    fun dataGetIntervalDS/1,
    fun dataGetIntervalYM/1,
    fun dataGetStmt/1,
    fun dataGetStmtChange/1,
    fun dataGet_NegativeDataType/1,
    fun dataGet_NegativeFailCall/1,
    fun dataGetInt64/1,
    fun dataGetInt64_NegativeDataType/1,
    fun dataGetInt64_NegativeFailCall/1,
    fun dataGetInt64_viaPointer/1,
    fun dataGetBytes/1,
    fun dataGetBytes_NegativeDataType/1,
    fun dataGetBytes_NegativeFailCall/1,
    fun dataRelease/1,
    fun dataRelease_NegativeDataType/1,
    fun dataRelease_NegativeFailCall/1,
    fun dataRelease_viaPointer/1
]).

unsafe_no_context_test_() ->
    {
        setup,
        fun() -> setup(#{safe => false}) end,
        fun cleanup/1,
        testDescriptorMatch(?NO_CONTEXT_TESTS)
    }.

unsafe_context_test_() ->
    {
        setup,
        fun() -> setup_context(#{safe => false}) end,
        fun cleanup/1,
        testDescriptorMatch(?AFTER_CONTEXT_TESTS)
    }.

unsafe_session_test_() ->
    {
        setup,
        fun() -> setup_connecion(#{safe => false}) end,
        fun cleanup/1,
        testDescriptorMatch(?AFTER_CONNECTION_TESTS)
    }.


no_context_test_() ->
    {
        setup,
        fun() -> setup(#{safe => true}) end,
        fun cleanup/1,
        testDescriptorMatch(?NO_CONTEXT_TESTS)
    }.

context_test_() ->
    {
        setup,
        fun() -> setup_context(#{safe => true}) end,
        fun cleanup/1,
        testDescriptorMatch(?AFTER_CONTEXT_TESTS)
    }.

session_test_() ->
    {
        setup,
        fun() -> setup_connecion(#{safe => true}) end,
        fun cleanup/1,
        testDescriptorMatch(?AFTER_CONNECTION_TESTS)
    }.

%-------------------------------------------------------------------------------
% Internal functions
%-------------------------------------------------------------------------------

dpiCall(#{safe := true}, F, A) -> dpi:safe(dpi, F, A);
dpiCall(#{safe := false}, F, A) -> apply(dpi, F, A).

getConfig() ->
    case file:get_cwd() of
        {ok, Cwd} ->
            ConnectConfigFile = filename:join(
                lists:reverse(
                    ["connect.config", "test"
                        | lists:reverse(filename:split(Cwd))]
                )
            ),
            case file:consult(ConnectConfigFile) of
                {ok, [Params]} when is_map(Params) -> Params;
                {ok, Params} ->
                    ?debugFmt("bad config (expected map) ~p", [Params]),
                    error(badconfig);
                {error, Reason} ->
                    ?debugFmt("~p", [Reason]),
                    error(Reason)
            end;
        {error, Reason} ->
            ?debugFmt("~p", [Reason]),
            error(Reason)
    end.

get_column_values(_Safe, _Stmt, ColIdx, Limit) when ColIdx > Limit -> [];
get_column_values(TestCtx, Stmt, ColIdx, Limit) ->
    #{data := Data} = dpiCall(TestCtx, stmt_getQueryValue, [Stmt, ColIdx]),
    [dpiCall(TestCtx, data_get, [Data])
     | get_column_values(TestCtx, Stmt, ColIdx + 1, Limit)].

%% gets a value out of a fetched set, compares it using an assertation,
%% then cleans is up again
assert_getQueryValue(TestCtx, Stmt, Index, Value) ->
    #{data := QueryValueRef} = dpiCall(TestCtx, stmt_getQueryValue, [Stmt, Index]),
    ?assertEqual(Value, dpiCall(TestCtx, data_get, [QueryValueRef])),
	dpiCall(TestCtx, data_release, [QueryValueRef]),
    ok.


assert_getQueryInfo(TestCtx, Stmt, Index, Value, Atom) ->
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, Index]),
    ?assertEqual(Value, maps:get(Atom, dpiCall(TestCtx, queryInfo_get, [QueryInfoRef]))),
	dpiCall(TestCtx, queryInfo_delete, [QueryInfoRef]),
    ok.

extract_getQueryValue(TestCtx, Stmt, Index) ->
    #{data := QueryValueRef} = dpiCall(TestCtx, stmt_getQueryValue, [Stmt, Index]),
    Result = dpiCall(TestCtx, data_get, [QueryValueRef]),
	dpiCall(TestCtx, data_release, [QueryValueRef]),
    Result.

extract_getQueryInfo(TestCtx, Stmt, Index, Atom) ->
    QueryInfoRef = dpiCall(TestCtx, stmt_getQueryInfo, [Stmt, Index]),
    Result = maps:get(Atom, dpiCall(TestCtx, queryInfo_get, [QueryInfoRef])),
	dpiCall(TestCtx, queryInfo_delete, [QueryInfoRef]),
    Result.
