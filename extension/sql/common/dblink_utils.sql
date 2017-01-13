--Registers a dblink connection to be commited on local pre-commit or aborted on local abort.
CREATE OR REPLACE FUNCTION _sysinternal.register_dblink_precommit_connection(text) RETURNS VOID
	AS '$libdir/iobeamdb', 'register_dblink_precommit_connection' LANGUAGE C VOLATILE STRICT;


