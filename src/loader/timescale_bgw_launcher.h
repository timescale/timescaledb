/*
 *
 * Interface for the whole cluster bgw launcher, which simply looks through the dbs and
 * launches a db specific launcher in each db then dies. The db launchers will look to see
 * Timescale is installed and die if it is not. 
 *
 */

void register_timescale_bgw_launcher(void);
void timescale_bgw_launcher_main (void) ; 