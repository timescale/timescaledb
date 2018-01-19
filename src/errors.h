/* Defines error codes used
-- PREFIX IO
*/

/*
-- IO000 - GROUP: query errors
-- IO001 - hypertable does not exist
-- IO002 - column does not exist
*/
#define ERRCODE_IO_QUERY_ERRORS MAKE_SQLSTATE('I','O','0','0','0')
#define ERRCODE_IO_HYPERTABLE_NOT_EXIST MAKE_SQLSTATE('I','O','0','0','1')
#define ERRCODE_IO_DIMENSION_NOT_EXIST MAKE_SQLSTATE('I','O','0','0','2')

/*
--IO100 - GROUP: DDL errors
--IO101 - operation not supported
--IO102 - bad hypertable definition
--IO103 - bad hypertable index definition
--IO110 - hypertable already exists
--I0120 - node already exists
--I0130 - user already exists
*/
#define ERRCODE_IO_DDL_ERRORS MAKE_SQLSTATE('I','O','1','0','0')
#define ERRCODE_IO_OPERATION_NOT_SUPPORTED MAKE_SQLSTATE('I','O','1','0','1')
#define ERRCODE_IO_BAD_HYPERTABLE_DEFINITION MAKE_SQLSTATE('I','O','1','0','2')
#define ERRCODE_IO_BAD_HYPERTABLE_INDEX_DEFINITION MAKE_SQLSTATE('I','O','1','0','3')
#define ERRCODE_IO_HYPERTABLE_EXISTS MAKE_SQLSTATE('I','O','1','1','0')
#define ERRCODE_IO_NODE_EXISTS MAKE_SQLSTATE('I','O','1','2','0')
#define ERRCODE_IO_USER_EXISTS MAKE_SQLSTATE('I','O','1','3','0')
#define ERRCODE_IO_TABLESPACE_ALREADY_ATTACHED MAKE_SQLSTATE('I','O','1','4','0')
#define ERRCODE_IO_TABLESPACE_NOT_ATTACHED MAKE_SQLSTATE('I','O','1','5','0')
#define ERRCODE_IO_DUPLICATE_DIMENSION MAKE_SQLSTATE('I','O','1','6','0')

/*
--IO500 - GROUP: internal error
--IO501 - unexpected state/event
--IO502 - communication/remote error
*/
#define ERRCODE_IO_INTERNAL_ERROR MAKE_SQLSTATE('I','O','5','0','0')
#define ERRCODE_IO_UNEXPECTED MAKE_SQLSTATE('I','O','5','0','1')
#define ERRCODE_IO_COMMUNICATION_ERROR MAKE_SQLSTATE('I','O','5','0','2')
