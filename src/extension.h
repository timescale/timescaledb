#ifndef TIMESCALEDB_EXTENSION_H
#define TIMESCALEDB_EXTENSION_H

bool		extension_is_being_dropped(Oid relid);
void		extension_reset(void);
bool		extension_is_loaded(void);

#endif   /* TIMESCALEDB_EXTENSION_H */
