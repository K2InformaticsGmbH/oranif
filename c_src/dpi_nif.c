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

DPI_NIF_FUN(resource_count);

static ErlNifFunc nif_funcs[] = {
    DPICONTEXT_NIFS,
    DPICONN_NIFS,
    DPISTMT_NIFS,
    DPIDATA_NIFS,
    DPIVAR_NIFS,
    {"resource_count", 0, resource_count}};

typedef struct
{
    int test;
    dpiContext *context;
} oranif_priv;

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
        enif_make_ulong(env, st->res_count.context), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "connection"),
        enif_make_ulong(env, st->res_count.connection), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "statement"),
        enif_make_ulong(env, st->res_count.statement), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "variable"),
        enif_make_ulong(env, st->res_count.variable), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "data"),
        enif_make_ulong(env, st->res_count.data), &ret);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "datapointer"),
        enif_make_ulong(env, st->res_count.datapointer), &ret);

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

static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    CALL_TRACE;

    oranif_st *st = enif_alloc(sizeof(oranif_st));
    if (st == NULL)
    {
        return 1;
    }

    st->res_count.data = 0;
    st->res_count.context = 0;
    st->res_count.variable = 0;
    st->res_count.statement = 0;
    st->res_count.connection = 0;
    st->res_count.datapointer = 0;

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

static int upgrade(ErlNifEnv *env, void **priv_data,
                   void **old_priv_data, ERL_NIF_TERM load_info)
{
    CALL_TRACE;
    RETURNED_TRACE;
    return 0;
}

static void unload(ErlNifEnv *env, void *priv_data)
{
    CALL_TRACE;

    enif_free(priv_data);

    RETURNED_TRACE;
}

ERL_NIF_INIT(dpi, nif_funcs, load, NULL, upgrade, unload)
