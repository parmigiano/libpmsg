#include "pmsg_crosspltm.h"
#include "pmsg_frame.h"
#include "pmsg_server.h"

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>

#define PMSG_INQ_CAP 65536u

typedef struct pmsg_conn pmsg_conn_t;

typedef struct pmsg_user_slot {
    uint64_t key;
    pmsg_conn_t **items;
    size_t count;
    size_t cap;
    uint8_t state;
} pmsg_user_slot_t;

typedef struct pmsg_id_slot {
    uint64_t key;
    pmsg_conn_t *conn;
    uint8_t state;
} pmsg_id_slot_t;

typedef struct pmsg_queue {
    pmsg_event_t *slots;
    size_t cap;
    size_t head;
    size_t count;
    pmsg_mutex_t mu;
    pmsg_cond_t cv;
    int closed;
} pmsg_queue_t;

struct pmsg_conn {
    pmsg_socket_t fd;
    uint64_t id;
    uint64_t user_id;
    int has_user;
    size_t index;
    atomic_int refs;
    atomic_int alive;
    pmsg_decoder_t dec;
    pmsg_outq_t out;
    pmsg_mutex_t mu;
    uint64_t last_ms;
    uint64_t partial_since;
    uint64_t send_progress_ms;
    int tracked;
    struct pmsg_server *serv;
    uint8_t *recv_buf;
    uint8_t *send_buf;
    int shard;
    int in_kick;
    pmsg_conn_t *kick_next;
};

typedef struct pmsg_shard {
    struct pmsg_server *serv;
    pmsg_thread_t thread;
    int ep;
    int wake;
    pmsg_mutex_t mu;
    pmsg_conn_t *kick_head;
} pmsg_shard_t;

struct pmsg_server {
    char *name;
    pmsg_config_t cfg;
    char *bind_host;
    uint16_t port;
    pmsg_socket_t listen_fd;
    atomic_int running;
    atomic_uint next_id;
    pmsg_mutex_t mu;
    pmsg_mutex_t send_mu;
    pmsg_conn_t **conns;
    size_t conn_n;
    size_t conn_cap;
    pmsg_user_slot_t *users;
    size_t user_cap;
    size_t user_live;
    pmsg_id_slot_t *ids;
    size_t id_cap;
    size_t id_live;
    pmsg_queue_t inq;
    pmsg_protobuf_t pb;
    uint32_t thread_n;
    int io_started;
    pmsg_shard_t *shards;
};

static void pmsg_conn_unref(pmsg_conn_t *conn);
static void pmsg_conn_close(pmsg_conn_t *conn);

static uint64_t pmsg_mix(uint64_t x)
{
    x ^= x >> 30;

    x *= 0xbf58476d1ce4e5b9ULL;

    x ^= x >> 27;

    x *= 0x94d049bb133111ebULL;

    x ^= x >> 31;

    return x;
}

static int pmsg_queue_init(pmsg_queue_t *q, size_t cap)
{
    memset(q, 0, sizeof(*q));

    q->slots = (pmsg_event_t *)calloc(cap, sizeof(*q->slots));

    if (!q->slots) {
        return PMSG_ERR_NOMEM;
    }

    q->cap = cap;

    pmsg_mutex_init(&q->mu);

    pmsg_cond_init(&q->cv);

    return PMSG_OK;
}

static void pmsg_queue_close(pmsg_queue_t *q)
{
    size_t i;

    if (!q->slots) {
        return;
    }

    pmsg_mutex_lock(&q->mu);

    q->closed = 1;

    for (i = 0; i < q->count; i++) {
        pmsg_event_free(&q->slots[(q->head + i) % q->cap]);
    }

    q->count = 0;

    pmsg_cond_broadcast(&q->cv);

    pmsg_mutex_unlock(&q->mu);
}

static void pmsg_queue_destroy(pmsg_queue_t *q)
{
    if (!q->slots) {
        return;
    }

    pmsg_queue_close(q);

    pmsg_cond_destroy(&q->cv);

    pmsg_mutex_destroy(&q->mu);

    free(q->slots);

    q->slots = NULL;
}

static int pmsg_queue_push(pmsg_queue_t *q, pmsg_event_t *ev)
{
    pmsg_mutex_lock(&q->mu);

    while (q->count == q->cap && !q->closed) {
        pmsg_cond_wait(&q->cv, &q->mu);
    }

    if (q->closed) {
        pmsg_mutex_unlock(&q->mu);

        return PMSG_ERR_CLOSED;
    }

    q->slots[(q->head + q->count) % q->cap] = *ev;

    q->count++;

    pmsg_cond_signal(&q->cv);

    pmsg_mutex_unlock(&q->mu);

    memset(ev, 0, sizeof(*ev));

    return PMSG_OK;
}

static int pmsg_queue_pop(pmsg_queue_t *q, pmsg_event_t *ev, uint32_t timeout_ms)
{
    pmsg_mutex_lock(&q->mu);

    if (q->count == 0 && !q->closed && timeout_ms > 0) {
        pmsg_cond_timedwait(&q->cv, &q->mu, timeout_ms);
    }

    if (q->count == 0) {
        pmsg_mutex_unlock(&q->mu);

        return q->closed ? PMSG_ERR_CLOSED : PMSG_AGAIN;
    }

    *ev = q->slots[q->head];

    memset(&q->slots[q->head], 0, sizeof(*ev));

    q->head = (q->head + 1u) % q->cap;

    q->count--;

    pmsg_cond_signal(&q->cv);

    pmsg_mutex_unlock(&q->mu);

    return PMSG_OK;
}

static int pmsg_users_grow(struct pmsg_server *serv)
{
    size_t cap = serv->user_cap ? serv->user_cap * 2u : 16u;

    pmsg_user_slot_t *next = (pmsg_user_slot_t *)calloc(cap, sizeof(*next));

    size_t i;

    if (!next) {
        return PMSG_ERR_NOMEM;
    }

    for (i = 0; i < serv->user_cap; i++) {
        if (serv->users[i].state == 1) {
            size_t at = (size_t)(pmsg_mix(serv->users[i].key) & (cap - 1u));

            while (next[at].state == 1) {
                at = (at + 1u) & (cap - 1u);
            }

            next[at] = serv->users[i];
        }
    }

    free(serv->users);

    serv->users = next;

    serv->user_cap = cap;

    return PMSG_OK;
}

static int pmsg_ids_grow(struct pmsg_server *serv)
{
    size_t cap = serv->id_cap ? serv->id_cap * 2u : 16u;

    pmsg_id_slot_t *next = (pmsg_id_slot_t *)calloc(cap, sizeof(*next));

    size_t i;

    if (!next) {
        return PMSG_ERR_NOMEM;
    }

    for (i = 0; i < serv->id_cap; i++) {
        if (serv->ids[i].state == 1) {
            size_t at = (size_t)(pmsg_mix(serv->ids[i].key) & (cap - 1u));

            while (next[at].state == 1) {
                at = (at + 1u) & (cap - 1u);
            }

            next[at] = serv->ids[i];
        }
    }

    free(serv->ids);

    serv->ids = next;

    serv->id_cap = cap;

    return PMSG_OK;
}

static pmsg_user_slot_t *pmsg_user_find(struct pmsg_server *serv, uint64_t user_id, int create)
{
    size_t at;

    size_t steps = 0;

    if (serv->user_cap == 0) {
        return NULL;
    }

    at = (size_t)(pmsg_mix(user_id) & (serv->user_cap - 1u));

    while (steps < serv->user_cap) {
        pmsg_user_slot_t *slot = &serv->users[at];

        if (slot->state == 0) {
            if (!create) {
                return NULL;
            }

            slot->state = 1;

            slot->key = user_id;

            serv->user_live++;

            return slot;
        }

        if (slot->state == 1 && slot->key == user_id) {
            return slot;
        }

        at = (at + 1u) & (serv->user_cap - 1u);

        steps++;
    }

    return NULL;
}

static void pmsg_id_put(struct pmsg_server *serv, pmsg_conn_t *conn)
{
    size_t at;

    if (serv->id_cap == 0 || (serv->id_live + 1u) * 10u > serv->id_cap * 7u) {
        if (pmsg_ids_grow(serv) != PMSG_OK) {
            return;
        }
    }

    at = (size_t)(pmsg_mix(conn->id) & (serv->id_cap - 1u));

    while (serv->ids[at].state == 1) {
        at = (at + 1u) & (serv->id_cap - 1u);
    }

    serv->ids[at].state = 1;

    serv->ids[at].key = conn->id;

    serv->ids[at].conn = conn;

    serv->id_live++;
}

static void pmsg_id_del(struct pmsg_server *serv, uint64_t id)
{
    size_t at;

    size_t steps = 0;

    if (serv->id_cap == 0) {
        return;
    }

    at = (size_t)(pmsg_mix(id) & (serv->id_cap - 1u));

    while (steps < serv->id_cap) {
        if (serv->ids[at].state == 0) {
            return;
        }

        if (serv->ids[at].state == 1 && serv->ids[at].key == id) {
            serv->ids[at].state = 2;

            serv->ids[at].conn = NULL;

            if (serv->id_live > 0) {
                serv->id_live--;
            }

            return;
        }

        at = (at + 1u) & (serv->id_cap - 1u);

        steps++;
    }
}

static pmsg_conn_t *pmsg_id_get(struct pmsg_server *serv, uint64_t id)
{
    size_t at;

    size_t steps = 0;

    if (serv->id_cap == 0) {
        return NULL;
    }

    at = (size_t)(pmsg_mix(id) & (serv->id_cap - 1u));

    while (steps < serv->id_cap) {
        if (serv->ids[at].state == 0) {
            return NULL;
        }

        if (serv->ids[at].state == 1 && serv->ids[at].key == id) {
            return serv->ids[at].conn;
        }

        at = (at + 1u) & (serv->id_cap - 1u);

        steps++;
    }

    return NULL;
}

static int pmsg_user_attach(struct pmsg_server *serv, pmsg_conn_t *conn, uint64_t user_id)
{
    pmsg_user_slot_t *slot;

    size_t i;

    if (conn->has_user && conn->user_id == user_id) {
        return PMSG_OK;
    }

    if (serv->user_cap == 0 || (serv->user_live + 1u) * 10u > serv->user_cap * 7u) {
        if (pmsg_users_grow(serv) != PMSG_OK) {
            return PMSG_ERR_NOMEM;
        }
    }

    if (conn->has_user) {
        pmsg_user_slot_t *old = pmsg_user_find(serv, conn->user_id, 0);

        if (old) {
            for (i = 0; i < old->count; i++) {
                if (old->items[i] == conn) {
                    old->items[i] = old->items[old->count - 1u];

                    old->count--;

                    break;
                }
            }
        }
    }

    slot = pmsg_user_find(serv, user_id, 1);

    if (!slot) {
        return PMSG_ERR_NOMEM;
    }

    for (i = 0; i < slot->count; i++) {
        if (slot->items[i] == conn) {
            conn->user_id = user_id;

            conn->has_user = 1;

            return PMSG_OK;
        }
    }

    if (slot->count == slot->cap) {
        size_t cap = slot->cap ? slot->cap * 2u : 2u;

        pmsg_conn_t **grown = (pmsg_conn_t **)realloc(slot->items, cap * sizeof(*grown));

        if (!grown) {
            return PMSG_ERR_NOMEM;
        }

        slot->items = grown;

        slot->cap = cap;
    }

    slot->items[slot->count++] = conn;

    conn->user_id = user_id;

    conn->has_user = 1;

    return PMSG_OK;
}

static void pmsg_user_detach(struct pmsg_server *serv, pmsg_conn_t *conn)
{
    size_t i;

    pmsg_user_slot_t *slot;

    if (!conn->has_user) {
        return;
    }

    slot = pmsg_user_find(serv, conn->user_id, 0);

    if (slot) {
        for (i = 0; i < slot->count; i++) {
            if (slot->items[i] == conn) {
                slot->items[i] = slot->items[slot->count - 1u];

                slot->count--;

                break;
            }
        }
    }

    conn->has_user = 0;
}

static void pmsg_conn_destroy(pmsg_conn_t *conn)
{
    pmsg_decoder_free(&conn->dec);

    pmsg_outq_free(&conn->out);

    pmsg_mutex_destroy(&conn->mu);

    free(conn->recv_buf);

    free(conn->send_buf);

    free(conn);
}

static void pmsg_conn_ref(pmsg_conn_t *conn)
{
    atomic_fetch_add(&conn->refs, 1);
}

static void pmsg_conn_unref(pmsg_conn_t *conn)
{
    if (conn && atomic_fetch_sub(&conn->refs, 1) == 1) {
        pmsg_conn_destroy(conn);
    }
}

static int pmsg_conn_ingest(pmsg_conn_t *conn, const uint8_t *data, size_t len)
{
    int st = pmsg_decoder_push(&conn->dec, data, len);

    conn->last_ms = pmsg_now_ms();

    if (st != PMSG_OK) {
        return st;
    }

    for (;;) {
        uint8_t *payload = NULL;

        size_t payload_len = 0;

        void *message;

        uint64_t user_id;

        pmsg_event_t ev;

        st = pmsg_decoder_pop(&conn->dec, &payload, &payload_len);

        if (st == PMSG_AGAIN) {
            conn->partial_since = pmsg_decoder_pending(&conn->dec) ? (conn->partial_since ? conn->partial_since : pmsg_now_ms()) : 0;

            return PMSG_OK;
        }

        if (st != PMSG_OK) {
            return st;
        }

        message = pmsg_pb_unpack(&conn->serv->pb, 0, payload, payload_len);

        free(payload);

        if (!message || !conn->serv->pb.client_user_id) {
            if (message && conn->serv->pb.client_free) {
                conn->serv->pb.client_free(message, NULL);
            }

            return PMSG_ERR_PROTO;
        }

        user_id = conn->serv->pb.client_user_id(message);

        pmsg_mutex_lock(&conn->serv->mu);

        st = pmsg_user_attach(conn->serv, conn, user_id);

        pmsg_mutex_unlock(&conn->serv->mu);

        if (st != PMSG_OK) {
            if (conn->serv->pb.client_free) {
                conn->serv->pb.client_free(message, NULL);
            }

            return st;
        }

        memset(&ev, 0, sizeof(ev));

        ev.kind = PMSG_EV_PACKET;

        ev.conn_id = conn->id;

        ev.user_id = user_id;

        ev.message = message;

        ev._free_message = conn->serv->pb.client_free;

        if (pmsg_queue_push(&conn->serv->inq, &ev) != PMSG_OK) {
            pmsg_event_free(&ev);

            return PMSG_ERR_CLOSED;
        }
    }
}

static pmsg_conn_t *pmsg_conn_new(struct pmsg_server *serv, pmsg_socket_t fd)
{
    pmsg_conn_t *conn = (pmsg_conn_t *)calloc(1, sizeof(*conn));

    uint32_t recv_buf = serv->cfg.recv_buf ? serv->cfg.recv_buf : 16384u;

    if (!conn) {
        return NULL;
    }

    conn->fd = fd;

    conn->serv = serv;

    conn->id = atomic_fetch_add(&serv->next_id, 1u);

    atomic_store(&conn->refs, 1);

    atomic_store(&conn->alive, 1);

    conn->last_ms = pmsg_now_ms();

    conn->send_progress_ms = conn->last_ms;

    pmsg_mutex_init(&conn->mu);

    pmsg_outq_init(&conn->out);

    if (pmsg_decoder_init(&conn->dec, serv->cfg.max_frame) != PMSG_OK) {
        pmsg_conn_destroy(conn);

        return NULL;
    }

    conn->recv_buf = (uint8_t *)malloc(recv_buf);

    conn->send_buf = (uint8_t *)malloc(65536u);

    if (!conn->recv_buf || !conn->send_buf) {
        pmsg_conn_destroy(conn);

        return NULL;
    }

    return conn;
}

static int pmsg_track_conn(struct pmsg_server *serv, pmsg_conn_t *conn)
{
    pmsg_mutex_lock(&serv->mu);

    if (serv->conn_n >= serv->cfg.max_connections) {
        pmsg_mutex_unlock(&serv->mu);

        return PMSG_ERR_BUSY;
    }

    if (serv->conn_n == serv->conn_cap) {
        size_t cap = serv->conn_cap ? serv->conn_cap * 2u : 16u;

        pmsg_conn_t **grown = (pmsg_conn_t **)realloc(serv->conns, cap * sizeof(*grown));

        if (!grown) {
            pmsg_mutex_unlock(&serv->mu);

            return PMSG_ERR_NOMEM;
        }

        serv->conns = grown;

        serv->conn_cap = cap;
    }

    conn->index = serv->conn_n;

    serv->conns[serv->conn_n++] = conn;

    conn->tracked = 1;

    pmsg_id_put(serv, conn);

    pmsg_mutex_unlock(&serv->mu);

    return PMSG_OK;
}

static void pmsg_conn_close(pmsg_conn_t *conn)
{
    struct pmsg_server *serv;

    pmsg_event_t ev;

    if (!conn || !atomic_exchange(&conn->alive, 0)) {
        return;
    }

    serv = conn->serv;

    pmsg_mutex_lock(&serv->mu);

    pmsg_user_detach(serv, conn);

    pmsg_id_del(serv, conn->id);

    if (serv->conn_n > 0 && conn->index < serv->conn_n && serv->conns[conn->index] == conn) {
        pmsg_conn_t *last = serv->conns[serv->conn_n - 1u];

        serv->conns[conn->index] = last;

        last->index = conn->index;

        serv->conn_n--;
    }

    pmsg_mutex_unlock(&serv->mu);

    if (conn->fd != PMSG_SOCKET_INVALID) {
        pmsg_socket_close(conn->fd);

        conn->fd = PMSG_SOCKET_INVALID;
    }

    if (conn->tracked) {
        memset(&ev, 0, sizeof(ev));

        ev.kind = PMSG_EV_CLOSE;

        ev.conn_id = conn->id;

        ev.user_id = conn->user_id;

        if (pmsg_queue_push(&serv->inq, &ev) != PMSG_OK) {
            pmsg_event_free(&ev);
        }
    }

    pmsg_conn_unref(conn);
}

static void pmsg_tune_socket(pmsg_socket_t fd, uint32_t recv_buf, uint32_t send_buf)
{
    int yes = 1;

    int rcv = recv_buf ? (int)recv_buf : 16 * 1024;

    int snd = send_buf ? (int)send_buf : 16 * 1024;

    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (const char *)&yes, sizeof(yes));

    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, (const char *)&rcv, sizeof(rcv));

    setsockopt(fd, SOL_SOCKET, SO_SNDBUF, (const char *)&snd, sizeof(snd));
}

static int pmsg_nonblock(pmsg_socket_t fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    if (flags < 0) {
        return -1;
    }

    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void pmsg_posix_arm(pmsg_conn_t *conn, uint32_t events)
{
    struct epoll_event ev;

    memset(&ev, 0, sizeof(ev));

    ev.events = events;

    ev.data.ptr = conn;

    epoll_ctl(conn->serv->shards[0].ep, EPOLL_CTL_MOD, conn->fd, &ev);
}

static void pmsg_posix_flush(pmsg_conn_t *conn)
{
    for (;;) {
        const uint8_t *data;

        size_t len = 0;

        size_t take;

        ssize_t n;

        pmsg_mutex_lock(&conn->mu);

        data = pmsg_outq_data(&conn->out, &len);

        if (!atomic_load(&conn->alive) || !data || len == 0) {
            pmsg_mutex_unlock(&conn->mu);

            if (atomic_load(&conn->alive) && conn->fd != PMSG_SOCKET_INVALID) {
                pmsg_posix_arm(conn, EPOLLIN);
            }

            return;
        }

        take = len > 65536u ? 65536u : len;

        memcpy(conn->send_buf, data, take);

        pmsg_mutex_unlock(&conn->mu);

        n = send(conn->fd, conn->send_buf, take, MSG_NOSIGNAL);

        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            pmsg_posix_arm(conn, EPOLLIN | EPOLLOUT);

            return;
        }

        if (n <= 0) {
            pmsg_conn_close(conn);

            return;
        }

        pmsg_mutex_lock(&conn->mu);

        pmsg_outq_consume(&conn->out, (size_t)n);

        conn->send_progress_ms = pmsg_now_ms();

        conn->last_ms = conn->send_progress_ms;

        pmsg_mutex_unlock(&conn->mu);
    }
}

static void pmsg_posix_kick(pmsg_conn_t *conn)
{
    pmsg_shard_t *shard = &conn->serv->shards[0];

    uint64_t one = 1;

    pmsg_mutex_lock(&shard->mu);

    if (!conn->in_kick && atomic_load(&conn->alive)) {
        conn->in_kick = 1;

        conn->kick_next = shard->kick_head;

        shard->kick_head = conn;

        pmsg_conn_ref(conn);
    }

    pmsg_mutex_unlock(&shard->mu);

    (void)write(shard->wake, &one, sizeof(one));
}

static void pmsg_posix_accept(struct pmsg_server *serv)
{
    for (;;) {
        pmsg_socket_t fd = accept(serv->listen_fd, NULL, NULL);

        pmsg_conn_t *conn;

        struct epoll_event ev;

        if (fd == PMSG_SOCKET_INVALID) {
            return;
        }

        if (pmsg_nonblock(fd) != 0) {
            pmsg_socket_close(fd);

            continue;
        }

        pmsg_tune_socket(fd, serv->cfg.recv_buf, serv->cfg.send_buf);

        conn = pmsg_conn_new(serv, fd);

        if (!conn || pmsg_track_conn(serv, conn) != PMSG_OK) {
            if (conn) {
                pmsg_conn_close(conn);
            } else {
                pmsg_socket_close(fd);
            }

            continue;
        }

        memset(&ev, 0, sizeof(ev));

        ev.events = EPOLLIN;

        ev.data.ptr = conn;

        if (epoll_ctl(serv->shards[0].ep, EPOLL_CTL_ADD, fd, &ev) != 0) {
            pmsg_conn_close(conn);
        }
    }
}

static void pmsg_posix_read(pmsg_conn_t *conn)
{
    for (;;) {
        int n;

        int st;

        if (!atomic_load(&conn->alive)) {
            return;
        }

        n = (int)recv(conn->fd, (char *)conn->recv_buf, conn->serv->cfg.recv_buf, 0);

        if (n > 0) {
            st = pmsg_conn_ingest(conn, conn->recv_buf, (size_t)n);

            if (st != PMSG_OK) {
                pmsg_conn_close(conn);

                return;
            }

            continue;
        }

        if (n == 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) {
            pmsg_conn_close(conn);
        }

        return;
    }
}

static void *pmsg_epoll_thread(void *arg)
{
    struct pmsg_server *serv = (struct pmsg_server *)arg;

    while (atomic_load(&serv->running)) {
        struct epoll_event evs[64];

        int n = epoll_wait(serv->shards[0].ep, evs, 64, 200);

        int i;

        for (i = 0; i < n; i++) {
            void *ptr = evs[i].data.ptr;

            if (ptr == NULL) {
                pmsg_posix_accept(serv);
            } else if (ptr == (void *)1) {
                pmsg_conn_t *head;

                pmsg_shard_t *shard = &serv->shards[0];

                uint64_t junk;

                (void)read(shard->wake, &junk, sizeof(junk));

                pmsg_mutex_lock(&shard->mu);

                head = shard->kick_head;

                shard->kick_head = NULL;
                {
                    pmsg_conn_t *it = head;

                    while (it) {
                        it->in_kick = 0;

                        it = it->kick_next;
                    }
                }

                pmsg_mutex_unlock(&shard->mu);

                while (head) {
                    pmsg_conn_t *next = head->kick_next;

                    if (atomic_load(&head->alive)) {
                        pmsg_posix_flush(head);
                    }

                    pmsg_conn_unref(head);

                    head = next;
                }
            } else {
                pmsg_conn_t *conn = (pmsg_conn_t *)ptr;

                if (evs[i].events & (EPOLLERR | EPOLLHUP)) {
                    pmsg_conn_close(conn);

                    continue;
                }

                if (evs[i].events & EPOLLIN) {
                    pmsg_posix_read(conn);
                }

                if (atomic_load(&conn->alive) && (evs[i].events & EPOLLOUT)) {
                    pmsg_posix_flush(conn);
                }
            }
        }
    }

    return NULL;
}

static int pmsg_enqueue_conns(struct pmsg_server *serv, pmsg_conn_t **conns, size_t count, const uint8_t *frame, size_t frame_len)
{
    size_t i;

    (void)serv;

    int alive = 0;

    if (count == 0) {
        return PMSG_ERR_OFFLINE;
    }

    for (i = 0; i < count; i++) {
        pmsg_mutex_lock(&conns[i]->mu);

        if (atomic_load(&conns[i]->alive)) {
            alive++;

            if (conns[i]->out.nframes >= PMSG_MAX_QUEUED_FRAMES) {
                pmsg_mutex_unlock(&conns[i]->mu);

                return PMSG_ERR_BUSY;
            }
        }

        pmsg_mutex_unlock(&conns[i]->mu);
    }

    if (!alive) {
        return PMSG_ERR_OFFLINE;
    }

    for (i = 0; i < count; i++) {
        int kick = 0;

        pmsg_mutex_lock(&conns[i]->mu);

        if (atomic_load(&conns[i]->alive) && pmsg_outq_push(&conns[i]->out, frame, frame_len) == PMSG_OK) {
            kick = 1;
        }

        pmsg_mutex_unlock(&conns[i]->mu);

        if (kick) {
            pmsg_posix_kick(conns[i]);
        }
    }

    return PMSG_OK;
}

static int pmsg_send_frame(struct pmsg_server *serv, int by_user, uint64_t key, const void *msg)
{
    uint8_t *payload = NULL;

    uint8_t *frame = NULL;

    size_t payload_len = 0;

    size_t frame_len = 0;

    pmsg_conn_t **list = NULL;

    size_t count = 0;

    size_t i;

    int st;

    if (!serv || !msg || !atomic_load(&serv->running)) {
        return PMSG_ERR_STATE;
    }

    st = pmsg_pb_pack(&serv->pb, 1, msg, serv->cfg.max_frame, &payload, &payload_len);

    if (st != PMSG_OK) {
        return st;
    }

    st = pmsg_frame_encode(payload, payload_len, &frame, &frame_len);

    free(payload);

    if (st != PMSG_OK) {
        return st;
    }

    pmsg_mutex_lock(&serv->send_mu);

    pmsg_mutex_lock(&serv->mu);

    if (by_user) {
        pmsg_user_slot_t *slot = pmsg_user_find(serv, key, 0);

        if (slot && slot->count) {
            list = (pmsg_conn_t **)malloc(slot->count * sizeof(*list));

            if (list) {
                for (i = 0; i < slot->count; i++) {
                    list[i] = slot->items[i];

                    pmsg_conn_ref(list[i]);
                }

                count = slot->count;
            }
        }
    } else {
        pmsg_conn_t *conn = pmsg_id_get(serv, key);

        if (conn) {
            list = (pmsg_conn_t **)malloc(sizeof(*list));

            if (list) {
                list[0] = conn;

                pmsg_conn_ref(conn);

                count = 1;
            }
        }
    }

    pmsg_mutex_unlock(&serv->mu);

    if (!list && count == 0) {
        st = PMSG_ERR_OFFLINE;
    } else if (!list) {
        st = PMSG_ERR_NOMEM;
    } else {
        st = pmsg_enqueue_conns(serv, list, count, frame, frame_len);
    }

    for (i = 0; i < count; i++) {
        pmsg_conn_unref(list[i]);
    }

    free(list);

    free(frame);

    pmsg_mutex_unlock(&serv->send_mu);

    return st;
}

void pmsg_event_free(pmsg_event_t *event)
{
    if (!event) {
        return;
    }

    if (event->message && event->_free_message) {
        event->_free_message(event->message, NULL);
    }

    memset(event, 0, sizeof(*event));
}

pmsg_result_t pmsg_poll(pmsg_server_t *server, pmsg_event_t *event, uint32_t timeout_ms)
{
    if (!server || !event) {
        return PMSG_ERR_ARG;
    }

    memset(event, 0, sizeof(*event));

    return pmsg_queue_pop(&server->inq, event, timeout_ms);
}

pmsg_result_t pmsg_send_user(pmsg_server_t *server, uint64_t user_id, const void *msg)
{
    return pmsg_send_frame(server, 1, user_id, msg);
}

pmsg_result_t pmsg_send_conn(pmsg_server_t *server, uint64_t conn_id, const void *msg)
{
    return pmsg_send_frame(server, 0, conn_id, msg);
}

uint16_t pmsg_server_port(const pmsg_server_t *server)
{
    return server ? server->port : 0;
}

static uint32_t pmsg_cpu_count(void)
{
    long n = sysconf(_SC_NPROCESSORS_ONLN);

    return n > 0 ? (uint32_t)n : 1u;
}

pmsg_server_t *pmsg_server_create(const char *name, const pmsg_config_t *config)
{
    struct pmsg_server *serv = (struct pmsg_server *)calloc(1, sizeof(*serv));

    pmsg_config_t defaults = pmsg_default_config();

    const pmsg_config_t *cfg = config ? config : &defaults;

    if (!serv) {
        return NULL;
    }

    serv->name = strdup(name);

    serv->bind_host = strdup(cfg->bind_host && cfg->bind_host[0] ? cfg->bind_host : "0.0.0.0");

    if (!serv->name || !serv->bind_host) {
        free(serv->name);

        free(serv->bind_host);

        free(serv);

        return NULL;
    }

    serv->cfg = *cfg;

    serv->cfg.bind_host = serv->bind_host;

    if (cfg->protobuf) {
        serv->pb = *cfg->protobuf;
    }

    if (serv->cfg.max_connections == 0) {
        serv->cfg.max_connections = defaults.max_connections;
    }

    if (serv->cfg.max_frame == 0) {
        serv->cfg.max_frame = defaults.max_frame;
    }

    if (serv->cfg.recv_buf == 0) {
        serv->cfg.recv_buf = defaults.recv_buf;
    }

    if (serv->cfg.send_buf == 0) {
        serv->cfg.send_buf = defaults.send_buf;
    }

    if (serv->cfg.backlog <= 0) {
        serv->cfg.backlog = defaults.backlog;
    }

    serv->thread_n = serv->cfg.io_threads ? serv->cfg.io_threads : pmsg_cpu_count();

    if (serv->thread_n == 0) {
        serv->thread_n = 1;
    }

    if (serv->thread_n > 32u) {
        serv->thread_n = 32u;
    }

    atomic_store(&serv->next_id, 1u);

    serv->listen_fd = PMSG_SOCKET_INVALID;

    pmsg_mutex_init(&serv->mu);

    pmsg_mutex_init(&serv->send_mu);

    if (pmsg_queue_init(&serv->inq, PMSG_INQ_CAP) != PMSG_OK) {
        pmsg_server_shutdown(serv);

        return NULL;
    }

    return serv;
}

static int pmsg_listen_open(struct pmsg_server *serv)
{
    struct sockaddr_in addr;

    struct sockaddr_in bound;

    socklen_t bound_len = sizeof(bound);

    int yes = 1;

    serv->listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (serv->listen_fd == PMSG_SOCKET_INVALID) {
        return PMSG_ERR_SOCKET;
    }

    setsockopt(serv->listen_fd, SOL_SOCKET, SO_REUSEADDR, (const char *)&yes, sizeof(yes));

    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;

    addr.sin_port = htons(serv->cfg.port);

    if (inet_pton(AF_INET, serv->bind_host, &addr.sin_addr) != 1) {
        return PMSG_ERR_SOCKET;
    }

    if (bind(serv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        return PMSG_ERR_SOCKET;
    }

    if (listen(serv->listen_fd, serv->cfg.backlog) != 0) {
        return PMSG_ERR_SOCKET;
    }

    if (getsockname(serv->listen_fd, (struct sockaddr *)&bound, &bound_len) == 0) {
        serv->port = ntohs(bound.sin_port);
    }

    return PMSG_OK;
}

int pmsg_server_start(pmsg_server_t *server)
{
    uint32_t i;

    int st;

    if (!server || atomic_load(&server->running)) {
        return PMSG_ERR_STATE;
    }

    st = pmsg_listen_open(server);

    if (st != PMSG_OK) {
        return st;
    }

    atomic_store(&server->running, 1);

    server->shards = (pmsg_shard_t *)calloc(server->thread_n, sizeof(*server->shards));

    if (!server->shards) {
        atomic_store(&server->running, 0);

        return PMSG_ERR_NOMEM;
    }

    for (i = 0; i < server->thread_n; i++) {
        server->shards[i].ep = -1;

        server->shards[i].wake = -1;
    }
    {
        struct epoll_event ev;

        if (pmsg_nonblock(server->listen_fd) != 0) {
            atomic_store(&server->running, 0);

            return PMSG_ERR_SOCKET;
        }

        server->shards[0].serv = server;

        server->shards[0].ep = epoll_create1(0);

        server->shards[0].wake = eventfd(0, EFD_NONBLOCK);

        pmsg_mutex_init(&server->shards[0].mu);

        if (server->shards[0].ep < 0 || server->shards[0].wake < 0) {
            atomic_store(&server->running, 0);

            return PMSG_ERR_SOCKET;
        }

        memset(&ev, 0, sizeof(ev));

        ev.events = EPOLLIN;

        ev.data.ptr = NULL;

        if (epoll_ctl(server->shards[0].ep, EPOLL_CTL_ADD, server->listen_fd, &ev) != 0) {
            atomic_store(&server->running, 0);

            return PMSG_ERR_SOCKET;
        }

        ev.data.ptr = (void *)1;

        if (epoll_ctl(server->shards[0].ep, EPOLL_CTL_ADD, server->shards[0].wake, &ev) != 0) {
            atomic_store(&server->running, 0);

            return PMSG_ERR_SOCKET;
        }

        if (pthread_create(&server->shards[0].thread, NULL, pmsg_epoll_thread, server) != 0) {
            atomic_store(&server->running, 0);

            return PMSG_ERR_SOCKET;
        }

        server->io_started = 1;
    }

    return PMSG_OK;
}

void pmsg_server_shutdown(pmsg_server_t *server)
{
    size_t n;

    pmsg_conn_t **left = NULL;

    size_t left_n = 0;

    if (!server) {
        return;
    }

    atomic_store(&server->running, 0);

    if (server->inq.slots) {
        pmsg_queue_close(&server->inq);
    }

    if (server->shards && server->shards[0].wake >= 0) {
        uint64_t one = 1;

        (void)write(server->shards[0].wake, &one, sizeof(one));
    }

    if (server->listen_fd != PMSG_SOCKET_INVALID) {
        pmsg_socket_close(server->listen_fd);

        server->listen_fd = PMSG_SOCKET_INVALID;
    }

    if (server->io_started && server->shards) {
        pthread_join(server->shards[0].thread, NULL);
    }

    if (server->shards && server->shards[0].ep >= 0) {
        close(server->shards[0].ep);
    }

    if (server->shards && server->shards[0].wake >= 0) {
        close(server->shards[0].wake);

        pmsg_mutex_destroy(&server->shards[0].mu);
    }

    free(server->shards);

    server->shards = NULL;

    pmsg_mutex_lock(&server->mu);

    left_n = server->conn_n;

    if (left_n) {
        left = (pmsg_conn_t **)malloc(left_n * sizeof(*left));

        if (left) {
            memcpy(left, server->conns, left_n * sizeof(*left));

            server->conn_n = 0;
        }
    }

    pmsg_mutex_unlock(&server->mu);

    for (n = 0; n < left_n && left; n++) {
        if (left[n]->fd != PMSG_SOCKET_INVALID) {
            pmsg_socket_close(left[n]->fd);

            left[n]->fd = PMSG_SOCKET_INVALID;
        }

        pmsg_conn_unref(left[n]);
    }

    free(left);

    free(server->conns);

    if (server->users) {
        for (n = 0; n < server->user_cap; n++) {
            free(server->users[n].items);
        }
    }

    free(server->users);

    free(server->ids);

    pmsg_queue_destroy(&server->inq);

    pmsg_mutex_destroy(&server->mu);

    pmsg_mutex_destroy(&server->send_mu);

    free(server->name);

    free(server->bind_host);

    free(server);
}
