#include "dpiVar_nif.h"
#include "dpiData_nif.h"

ErlNifResourceType *dpiVar_type;

void dpiVar_res_dtor(ErlNifEnv *env, void *resource)
{
    CALL_TRACE;
    RETURNED_TRACE;
}

DPI_NIF_FUN(var_setNumElementsInArray)
{
    CHECK_ARGCOUNT(2);

    dpiVar_res *vRes = NULL;
    uint32_t numElements;

    if ((!enif_get_resource(env, argv[0], dpiVar_type, (void **)&vRes)))
        BADARG_EXCEPTION(0, "resource var");
    if (!enif_get_uint(env, argv[1], &numElements))
        BADARG_EXCEPTION(1, "uint numElements");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        vRes->context,
        dpiVar_setNumElementsInArray(vRes->var, numElements), NULL);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(var_setFromBytes)
{
    CHECK_ARGCOUNT(3);

    dpiVar_res *vRes = NULL;
    ErlNifBinary value;
    uint32_t pos;

    if ((!enif_get_resource(env, argv[0], dpiVar_type, (void **)&vRes)))
        BADARG_EXCEPTION(0, "resource var");
    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(1, "uint pos");

    if (!enif_inspect_binary(env, argv[2], &value))
        BADARG_EXCEPTION(2, "binary/string value");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        vRes->context,
        dpiVar_setFromBytes(
            vRes->var, pos, (const char *)value.data, value.size),
        NULL);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(var_release)
{
    CHECK_ARGCOUNT(1);

    dpiVar_res *vRes = NULL;

    if ((!enif_get_resource(env, argv[0], dpiVar_type, (void **)&vRes)))
        BADARG_EXCEPTION(0, "resource var");

    RAISE_EXCEPTION_ON_DPI_ERROR(
        vRes->context, dpiVar_release(vRes->var), NULL);

    dpiDataPtr_res *t_itr;
    for (dpiDataPtr_res *itr = vRes->head; itr != NULL;)
    {
        t_itr = itr;
        itr = itr->next;
        enif_release_resource(t_itr);
    }

    enif_release_resource(vRes);

    RETURNED_TRACE;
    return ATOM_OK;
}

DPI_NIF_FUN(var_getReturnedData)
{
    CHECK_ARGCOUNT(2);

    dpiVar_res *varRes = NULL;

    if ((!enif_get_resource(env, argv[0], dpiVar_type, (void **)&varRes)))
        BADARG_EXCEPTION(0, "resource var");

    uint32_t pos;
    if (!enif_get_uint(env, argv[1], &pos))
        BADARG_EXCEPTION(3, "uint pos");

    uint32_t numElements;
    dpiData *data;
    RAISE_EXCEPTION_ON_DPI_ERROR(
        varRes->context,
        dpiVar_getReturnedData(varRes->var, pos, &numElements, &data),
        NULL);

    ERL_NIF_TERM dataList = enif_make_list(env, 0);

    dpiDataPtr_res *dataRes;
    dpiNativeTypeNum nativeTypeNum = 0;
    if (varRes->head)
        nativeTypeNum = ((dpiDataPtr_res *)varRes->head)->type;

    varRes->head = NULL;
    for (int i = numElements - 1; i >= 0; i--)
    {
        dataRes = enif_alloc_resource(dpiDataPtr_type, sizeof(dpiDataPtr_res));
        dataRes->stmtRes = NULL;
        dataRes->next = NULL;
        dataRes->isQueryValue = 0;
        dataRes->context = varRes->context;
        if (varRes->head == NULL)
        {
            varRes->head = dataRes;
        }
        else
        {
            dataRes->next = varRes->head;
            varRes->head = dataRes;
        }
        dataRes->dpiDataPtr = data + i;
        dataRes->type = nativeTypeNum;
        ERL_NIF_TERM dataResTerm = enif_make_resource(env, dataRes);
        dataList = enif_make_list_cell(env, dataResTerm, dataList);
    }
    ERL_NIF_TERM ret = enif_make_new_map(env);
    ret = enif_make_new_map(env);
    enif_make_map_put(
        env, ret, enif_make_atom(env, "numElements"),
        enif_make_uint(env, numElements), &ret);
    enif_make_map_put(env, ret, enif_make_atom(env, "data"), dataList, &ret);

    RETURNED_TRACE;
    return ret;
}
