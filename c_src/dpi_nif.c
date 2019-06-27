#ifdef EMBED
#include "odpi/embed/dpi.c"
#endif

#include "dpi_nif.h"
#include "dpiContext_nif.h"
#include "dpiConn_nif.h"
#include "dpiStmt_nif.h"
#include "dpiQueryInfo_nif.h"
#include "dpiData_nif.h"
#include "dpiVar_nif.h"

ERL_NIF_TERM ATOM_OK;
ERL_NIF_TERM ATOM_NULL;
ERL_NIF_TERM ATOM_TRUE;
ERL_NIF_TERM ATOM_FALSE;
ERL_NIF_TERM ATOM_ERROR;
ERL_NIF_TERM ATOM_ENOMEM;

static ERL_NIF_TERM processes(ErlNifEnv *, int, const ERL_NIF_TERM[]);

static ErlNifFunc nif_funcs[] = {
    DPICONTEXT_NIFS,
    DPICONN_NIFS,
    DPISTMT_NIFS,
    DPIQUERYINFO_NIFS,
    DPIDATA_NIFS,
    DPIVAR_NIFS,
    {"pids_get", 0, processes},
    {"pids_set", 1, processes}};

typedef struct
{
    int test;
    dpiContext *context;
} oranif_priv;

/*******************************************************************************
 * Helper internal functions
 ******************************************************************************/

ERL_NIF_TERM dpiErrorInfoMap(ErlNifEnv *env, dpiErrorInfo e)
{
    CALL_TRACE;

    ERL_NIF_TERM map = enif_make_new_map(env);

    enif_make_map_put(
        env, map,
        enif_make_atom(env, "code"), enif_make_int(env, e.code), &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "offset"), enif_make_uint(env, e.offset), &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "message"),
        enif_make_string_len(env, e.message, e.messageLength, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "encoding"),
        enif_make_string(env, e.encoding, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "fnName"),
        enif_make_string(env, e.fnName, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "action"),
        enif_make_string(env, e.action, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "sqlState"),
        enif_make_string(env, e.sqlState, ERL_NIF_LATIN1),
        &map);
    enif_make_map_put(
        env, map,
        enif_make_atom(env, "isRecoverable"),
        (e.isRecoverable == 0 ? ATOM_FALSE : ATOM_TRUE), &map);

    /* #{ code => integer(), offset => integer(), message => string(),
          encoding => string(), fnName => string(), action => string(),
          sqlState => string, isRecoverable => true | false } */
    RETURNED_TRACE;
    return map;
}

/*******************************************************************************
 * NIF Interface
 ******************************************************************************/

typedef struct proc_t
{
    ErlNifEnv *env;
    ERL_NIF_TERM pids;
} proc;

static ERL_NIF_TERM processes(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    CALL_TRACE;

    proc *p = (proc *)enif_priv_data(env);
    D("proc %p, proc->env %p\r\n", p, p->env);
    if (argc == 0) // pids_get
    {
        RETURNED_TRACE;
        return p->pids;
    }
    if (argc == 1) // pids_set([pid()])
    {
        if (!enif_is_list(env, argv[0]))
            BADARG_EXCEPTION(1, "list of pids");

        p->pids = enif_make_copy(p->env, argv[0]);

        RETURNED_TRACE;
        return ATOM_OK;
    }
    else
    {
        RAISE_STR_EXCEPTION("Wrong number of arguments. Required 0 or 1");
    }
}

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    CALL_TRACE;

    DEF_RES(dpiContext);
    DEF_RES(dpiConn);
    DEF_RES(dpiStmt);
    DEF_RES(dpiQueryInfo);
    DEF_RES(dpiData);
    DEF_RES(dpiDataPtr);
    DEF_RES(dpiVar);

    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NULL = enif_make_atom(env, "null");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_ENOMEM = enif_make_atom(env, "enomem");

    proc *p = enif_alloc(sizeof(proc));
    p->env = enif_alloc_env();
    p->pids = enif_make_list(p->env, 0);
    *priv_data = p;

    D("proc %p, proc->env %p\r\n", p, p->env);

    RETURNED_TRACE;
    return 0;
}

static int upgrade(
    ErlNifEnv *env, void **priv_data, void **old_priv_data,
    ERL_NIF_TERM load_info)
{
    CALL_TRACE;
    RETURNED_TRACE;
    return 0;
}

static void unload(ErlNifEnv *env, void *priv_data)
{
    CALL_TRACE;

    enif_free_env(((proc *)priv_data)->env);

    RETURNED_TRACE;
}

ERL_NIF_INIT(dpi, nif_funcs, load, NULL, upgrade, unload)
