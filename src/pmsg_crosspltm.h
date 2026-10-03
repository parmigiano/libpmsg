#ifndef PMSG_CROSSPLTM_H
#define PMSG_CROSSPLTM_H

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

typedef int pmsg_socket_t;

#define PMSG_SOCKET_INVALID (-1)
#define pmsg_socket_close(s) close(s)

typedef pthread_mutex_t pmsg_mutex_t;
typedef pthread_cond_t pmsg_cond_t;
typedef pthread_t pmsg_thread_t;

static inline void pmsg_mutex_init(pmsg_mutex_t *mu)
{
    pthread_mutex_init(mu, NULL);
}

static inline void pmsg_mutex_destroy(pmsg_mutex_t *mu)
{
    pthread_mutex_destroy(mu);
}

static inline void pmsg_mutex_lock(pmsg_mutex_t *mu)
{
    pthread_mutex_lock(mu);
}

static inline void pmsg_mutex_unlock(pmsg_mutex_t *mu)
{
    pthread_mutex_unlock(mu);
}

static inline void pmsg_cond_init(pmsg_cond_t *cv)
{
    pthread_cond_init(cv, NULL);
}

static inline void pmsg_cond_destroy(pmsg_cond_t *cv)
{
    pthread_cond_destroy(cv);
}

static inline void pmsg_cond_signal(pmsg_cond_t *cv)
{
    pthread_cond_signal(cv);
}

static inline void pmsg_cond_broadcast(pmsg_cond_t *cv)
{
    pthread_cond_broadcast(cv);
}

static inline void pmsg_cond_wait(pmsg_cond_t *cv, pmsg_mutex_t *mu)
{
    pthread_cond_wait(cv, mu);
}

static inline int pmsg_cond_timedwait(pmsg_cond_t *cv, pmsg_mutex_t *mu, uint32_t timeout_ms)
{
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);

    ts.tv_sec += (time_t)(timeout_ms / 1000u);
    ts.tv_nsec += (long)(timeout_ms % 1000u) * 1000000L;

    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += 1;
        ts.tv_nsec -= 1000000000L;
    }

    if (pthread_cond_timedwait(cv, mu, &ts) == ETIMEDOUT) {
        return 1;
    }

    return 0;
}

static inline uint64_t pmsg_now_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);

    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static inline int pmsg_last_socket_error(void)
{
    return errno;
}

#endif
