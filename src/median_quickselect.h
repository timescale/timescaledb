#ifndef TIMESCALE_MEDIAN_QUICKSELECT
#define TIMESCALE_MEDIAN_QUICKSELECT

/*
 * This median function implements the 'quickselect' algorithm, aka Hoare's
 * "Algorithm 65: Find".
 *
 * https://en.wikipedia.org/wiki/Quickselect
 *
 */

#include <postgres.h>
#include <utils/numeric.h>

extern Numeric median_numeric_quickselect(Numeric * arr, size_t arr_size);

#endif
