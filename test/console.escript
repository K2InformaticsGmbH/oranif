#!/user/bin/escript
%% -*- erlang -*-
%%! -name console@127.0.0.1 -setcookie console -pa _build/default/lib/oranif/ebin

-define(DPI_MAJOR_VERSION, 3).
-define(DPI_MINOR_VERSION, 0).
-define(TNS,
	<<"(DESCRIPTION=(ADDRESS_LIST=(ADDRESS="
	"(PROTOCOL=tcp)(HOST=127.0.0.1)(PORT=1521)))"
	"(CONNECT_DATA=(SERVICE_NAME=XE)))">>
).
main([]) ->
	dpi:load_unsafe(),
	Context = dpi:context_create(?DPI_MAJOR_VERSION, ?DPI_MINOR_VERSION),
    Conn = dpi:conn_create(
		Context, <<"scott">>, <<"tiger">>, ?TNS,
        #{encoding => "AL32UTF8", nencoding => "AL32UTF8"}, #{}
	),
	Sql = <<"insert into test (col2) values (:col2) returning rowid into :rid">>,
	Stmt = dpi:conn_prepareStmt(Conn, false, Sql, <<>>),
	io:format("statement : ~p~n", [Stmt]),
 	#{var := VarCol} = dpi:conn_newVar(
		Conn, 'DPI_ORACLE_TYPE_VARCHAR', 'DPI_NATIVE_TYPE_BYTES', 10,
        10, true, false, null
    ),
 	#{var := VarRowId, data := TDatas} = dpi:conn_newVar(
		Conn, 'DPI_ORACLE_TYPE_ROWID', 'DPI_NATIVE_TYPE_ROWID',
		10, 0, false, false, null
    ),
	ok = dpi:stmt_bindByName(Stmt, <<"col2">>, VarCol),
	ok = dpi:stmt_bindByName(Stmt, <<"rid">>, VarRowId),
	Data = lists:seq($0, $z),
    DataLen = length(Data),
	Indices = lists:seq(0, 9),
    [ok = dpi:var_setFromBytes(
		VarCol, Idx,
        << <<(lists:nth(rand:uniform(DataLen), Data))>>
                || _ <- lists:seq(1, 10) >>        
    ) || Idx <- Indices],
    ok = dpi:stmt_executeMany(Stmt, [], 10),
	[ok = dpi:data_release(TData) || TData <- TDatas],
	[begin
		#{numElements := 1, data  := [D]} = dpi:var_getReturnedData(VarRowId, Idx),
		io:format("[~p] data~p ~s~n", [?LINE, Idx, dpi:data_get(D)]),
		ok = dpi:data_release(D)
	end || Idx <- Indices],
	ok = dpi:var_release(VarCol),
	ok = dpi:var_release(VarRowId),
	ok = dpi:stmt_close(Stmt, <<>>),
	ok = dpi:conn_close(Conn, [], <<>>),
	ok = dpi:context_destroy(Context),
	io:format("DONE~n"),
	halt(1).
