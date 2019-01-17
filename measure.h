
#ifndef _LIBOS_MEASURE_H_
#define _LIOOS_MEASURE_H_

#include <stdint.h>
#include <stdio.h>

/**
 * Note: to make sure this measurement could be used in .c files (e.g., rte_XXX), no cpp things used here
 **/

/* redis/src/server.c */
#define _LIBOS_MEASURE_REDIS_SERVER_ID_BASE_ 0

/* redis/src/networking.c */
#define _LIBOS_MEASURE_REDIS_NETWORKING_ID_BASE_     10
//#define _LIBOS_MEASURE_REDIS_NETWORKING_READ_ID_     ((_LIBOS_MEASURE_REDIS_NETWORKING_ID_BASE_) + 0)
//#define _LIBOS_MEASURE_REDIS_NETWORKING_WRITE_ID_    ((_LIBOS_MEASURE_REDIS_NETWORKING_ID_BASE_) + 1)
//#define _LIBOS_MEASURE_REDIS_NETWORKING_PUSH_ID_    ((_LIBOS_MEASURE_REDIS_NETWORKING_ID_BASE_) + 2)
//#define _LIBOS_MEASURE_REDIS_NETWORKING_POP_ID_    ((_LIBOS_MEASURE_REDIS_NETWORKING_ID_BASE_) + 3)
//#define _LIBOS_MEASURE_REDIS_ALL_OVERHEAD_

/* redis/src/ae.c */
//#define _LIBOS_MEASURE_REDIS_AEEPOLL_ID_BASE_        20
//#define _LIBOS_MEASURE_REDIS_AEEPOLL_EPOLLWAIT_ID_   ((_LIBOS_MEASURE_REDIS_AEEPOLL_ID_BASE_) + 0)

//#define _LIBOS_MEASURE_REDIS_APP_LOGIC_

#endif
// _LIBOS_MEASURE_H_


/*************************************************************************/
// JUST FOR ILLUSTRATION
/**
 * Example rdtsc function
 *
 **/

#if 0
static inline uint64_t rdtsc(void)
{
    uint64_t eax, edx;
    __asm volatile ("rdtsc" : "=a" (eax), "=d" (edx));
    return (edx << 32) | eax;
}

// example usage: unit64_t start = rdtsc();
#endif
