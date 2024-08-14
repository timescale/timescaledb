
#define AGG_NAME MIN
#define PG_TYPE INT4
#define CTYPE int32
#define DATUM_TO_CTYPE DatumGetInt32
#define CTYPE_TO_DATUM Int32GetDatum
#define PG_AGG_OID(AGG_NAME, ARGUMENT_TYPE) F_##AGG_NAME##_##ARGUMENT_TYPE
#define PREDICATE(CURRENT, NEW) (NEW) < (CURRENT)
#include "minmax_impl.c"
