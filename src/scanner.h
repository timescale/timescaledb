#ifndef IOBEAMDB_SCANNER_H
#define IOBEAMDB_SCANNER_H

#include <postgres.h>
#include <access/relscan.h>

typedef enum ScannerType {
	ScannerTypeHeap,
	ScannerTypeIndex,
} ScannerType;

typedef struct ScannerCtx {
	Oid table;
	Oid index;
	ScannerType scantype;
	ScanKey scankey;
	int nkeys, norderbys;
	LOCKMODE lockmode;
	ScanDirection scandirection;
	void *data; /* User-provided data passed on to filter() and tuple_found() */

	/* Optional function to filter tuples. Should return true for tuples that
	 * should be passed on to tuple_found, or false otherwise. */
	bool (*filter)(HeapTuple tuple, TupleDesc desc, void *data);

	/* Callback for found tuples. Should return true to continue the scan or
	 * false to abort. */
    bool (*tuple_found)(HeapTuple tuple, TupleDesc desc, void *data);
} ScannerCtx;

/* Performs an index scan or heap scan and returns the number of matching
 * tuples. */
int scanner_scan(ScannerCtx *ctx);

#endif /* IOBEAMDB_SCANNER_H */
