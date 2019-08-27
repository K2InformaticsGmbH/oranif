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
DPI_NIF_FUN(resource_count);

static ErlNifFunc nif_funcs[] = {
    DPICONTEXT_NIFS,
    DPICONN_NIFS,
    DPISTMT_NIFS,
    DPIDATA_NIFS,
    DPIVAR_NIFS,
    {"pids_get", 0, processes},
    {"pids_set", 1, processes},
    {"resource_count", 0, resource_count}};

/*******************************************************************************
 * Helper internal functions
 ******************************************************************************/
DPI_NIF_FUN(resource_count)
{
    CHECK_ARGCOUNT(0);

    oranif_st *st = (oranif_st *)enif_priv_data(env);

    ERL_NIF_TERM ret = enif_make_new_map(env);
    ret = enif_make_new_map(env);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "context"),
        enif_make_ulong(env, st->dpiContext_count), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "connection"),
        enif_make_ulong(env, st->dpiConn_count), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "statement"),
        enif_make_ulong(env, st->dpiStmt_count), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "variable"),
        enif_make_ulong(env, st->dpiVar_count), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "data"),
        enif_make_ulong(env, st->dpiData_count), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "datapointer"),
        enif_make_ulong(env, st->dpiDataPtr_count), &ret);

    RETURNED_TRACE;
    return ret;
}

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

static ERL_NIF_TERM processes(
    ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    CALL_TRACE;

    ERL_NIF_TERM pids;
    oranif_st *st = (oranif_st *)enif_priv_data(env);
    unsigned len;
    // pids_get
    if (argc == 0)
    {
        D("pids_get\r\n");

        if (!enif_get_list_length(st->env, st->pids, &len))
            BADARG_EXCEPTION(0, "list length of p->pids");
        D("pids_get p->pids has %u\r\n", len);

        enif_mutex_lock(st->lock);
        pids = enif_make_copy(env, st->pids);
        enif_mutex_unlock(st->lock);

        RETURNED_TRACE;
        return pids;
    }
    // pids_set([pid()])
    else if (argc == 1)
    {
        D("pids_set\r\n");

        if (!enif_is_list(env, argv[0]))
            BADARG_EXCEPTION(0, "list of pids");

        if (!enif_get_list_length(env, argv[0], &len))
            BADARG_EXCEPTION(0, "list length of pids");
        D("pids_set argv[0] has %u\r\n", len);

        enif_mutex_lock(st->lock);
        st->pids = enif_make_copy(st->env, argv[0]);
        enif_mutex_unlock(st->lock);

        if (!enif_get_list_length(st->env, st->pids, &len))
            BADARG_EXCEPTION(0, "list length of p->pids");
        D("pids_set p->pids has %u\r\n", len);

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

    oranif_st *st = enif_alloc(sizeof(oranif_st));
    if (st == NULL)
    {
        E("failed allocate private structure of %d bytes", sizeof(oranif_st));
        return 1;
    }

    st->lock = enif_mutex_create("oranif");
    if (st->lock == NULL)
    {
        E("failed to create oranif mutex");
        return 1;
    }

    st->env = enif_alloc_env();
    st->dpiVar_count = 0;
    st->dpiData_count = 0;
    st->dpiStmt_count = 0;
    st->dpiConn_count = 0;
    st->dpiContext_count = 0;
    st->dpiDataPtr_count = 0;
    st->pids = enif_make_list(st->env, 0);

    DEF_RES(dpiContext);
    DEF_RES(dpiConn);
    DEF_RES(dpiStmt);
    DEF_RES(dpiData);
    DEF_RES(dpiDataPtr);
    DEF_RES(dpiVar);

    ATOM_OK = enif_make_atom(env, "ok");
    ATOM_NULL = enif_make_atom(env, "null");
    ATOM_TRUE = enif_make_atom(env, "true");
    ATOM_FALSE = enif_make_atom(env, "false");
    ATOM_ERROR = enif_make_atom(env, "error");
    ATOM_ENOMEM = enif_make_atom(env, "enomem");

    *priv_data = (void *)st;

    RETURNED_TRACE;
    return 0;
}

static int upgrade(
    ErlNifEnv *env, void **priv_data, void **old_priv_data,
    ERL_NIF_TERM load_info)
{
    CALL_TRACE;

    oranif_st *st = enif_alloc(sizeof(oranif_st));
    if (st == NULL)
    {
        E("failed allocate private structure of %d bytes", sizeof(oranif_st));
        return 1;
    }

    st->lock = enif_mutex_create("oranif");
    if (st->lock == NULL)
    {
        E("failed to create oranif mutex");
        return 1;
    }

    oranif_st *old_st = (oranif_st *)*old_priv_data;
    st->dpiVar_count = old_st->dpiVar_count;
    st->dpiData_count = old_st->dpiData_count;
    st->dpiStmt_count = old_st->dpiStmt_count;
    st->dpiConn_count = old_st->dpiConn_count;
    st->dpiContext_count = old_st->dpiContext_count;
    st->dpiDataPtr_count = old_st->dpiDataPtr_count;

    *priv_data = (void *)st;

    RETURNED_TRACE;
    return 0;
}

static void unload(ErlNifEnv *env, void *priv_data)
{
    CALL_TRACE;

    oranif_st *st = (oranif_st *)priv_data;
    enif_mutex_destroy(st->lock);
    enif_free_env(st->env);
    enif_free(priv_data);

    RETURNED_TRACE;
}

ERL_NIF_INIT(dpi, nif_funcs, load, NULL, upgrade, unload)
