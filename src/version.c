#include "postgres.h"
#include <string.h>

#include "fmgr.h"
#include "compat.h"
#include "gitcommit.h"

#define STR_EXPAND(x) #x
#define STR(x) STR_EXPAND(x)

const char *git_commit = STR(EXT_GIT_COMMIT);

TS_FUNCTION_INFO_V1(get_git_commit);

Datum
get_git_commit(PG_FUNCTION_ARGS)
{
	size_t		var_size = VARHDRSZ + strlen(git_commit);
	text	   *version_text = (text *) palloc(var_size);

	SET_VARSIZE(version_text, var_size);

	memcpy((void *) VARDATA(version_text),
		   (void *) git_commit,
		   var_size - VARHDRSZ);

	PG_RETURN_TEXT_P(version_text);
}
