#include "pmsg_crosspltm.h"
#include "pmsg_frame.h"
#include "pmsg_server.h"

#include <stdio.h>
#include <stdlib.h>

#define PMSG_CLIENT_INBOX 32

struct pmsg_client {
    pmsg_client_config_t cfg;
    char *host;
    pmsg_socket_t fd;
    pmsg_stream_t stream;
    pmsg_protobuf_t pb;
    pmsg_mutex_t send_mu;
    int send_mu_ready;
    int stream_ready;
    int started;
    int stop;
    int user_stop;
    int alive;
    int net_held;
    int thread_started;
    pmsg_thread_t thread;
    void *inbox[PMSG_CLIENT_INBOX];
    size_t inbox_head;
    size_t inbox_len;
    pmsg_mutex_t inbox_mu;
    pmsg_cond_t inbox_cv;
    int inbox_ready;
};

static int pmsg_client_on_reader(const pmsg_client_t *client)
{
    if (!client->thread_started) {
        return 0;
    }

    return pthread_equal(pthread_self(), client->thread);
}

static void pmsg_client_shutdown_fd(pmsg_client_t *client)
{
    if (client->fd == PMSG_SOCKET_INVALID) {
        return;
    }

    shutdown(client->fd, SHUT_RDWR);
}

static pmsg_result_t pmsg_send_all(pmsg_socket_t fd, const uint8_t *data, size_t len)
{
    size_t off = 0;

    while (off < len) {
        int n;
        n = (int)send(fd, data + off, len - off, MSG_NOSIGNAL);

        if (n <= 0) {
            return PMSG_ERR_SEND;
        }

        off += (size_t)n;
    }

    return PMSG_OK;
}

static void pmsg_client_inbox_wake(pmsg_client_t *client)
{
    if (!client->inbox_ready) {
        return;
    }

    pmsg_mutex_lock(&client->inbox_mu);
    pmsg_cond_broadcast(&client->inbox_cv);
    pmsg_mutex_unlock(&client->inbox_mu);
}

static void pmsg_client_inbox_push(pmsg_client_t *client, void *message)
{
    pmsg_mutex_lock(&client->inbox_mu);

    while (client->inbox_len == PMSG_CLIENT_INBOX && !client->stop) {
        pmsg_cond_wait(&client->inbox_cv, &client->inbox_mu);
    }

    if (client->stop) {
        pmsg_mutex_unlock(&client->inbox_mu);

        if (client->pb.server_free) {
            client->pb.server_free(message, NULL);
        }

        return;
    }

    client->inbox[(client->inbox_head + client->inbox_len) % PMSG_CLIENT_INBOX] = message;
    client->inbox_len++;

    pmsg_cond_signal(&client->inbox_cv);
    pmsg_mutex_unlock(&client->inbox_mu);
}

static void pmsg_client_inbox_clear(pmsg_client_t *client)
{
    size_t i;

    if (!client->inbox_ready) {
        return;
    }

    pmsg_mutex_lock(&client->inbox_mu);

    for (i = 0; i < client->inbox_len; i++) {
        void *message = client->inbox[(client->inbox_head + i) % PMSG_CLIENT_INBOX];

        if (client->pb.server_free) {
            client->pb.server_free(message, NULL);
        }
    }

    client->inbox_len = 0;

    pmsg_mutex_unlock(&client->inbox_mu);
}

static void pmsg_client_loop(pmsg_client_t *client)
{
    uint8_t *buf = (uint8_t *)malloc(16u * 1024u);

    if (!buf) {
        client->stop = 1;
        return;
    }

    while (!client->stop) {
        int n = recv(client->fd, (char *)buf, 16 * 1024, 0);
        int st;

        if (n == 0) {
            break;
        }

        if (n < 0) {
            if (!client->stop) {
                client->stop = 1;
            }
            break;
        }

        st = pmsg_decoder_push(&client->stream, buf, (size_t)n);

        if (st != PMSG_OK) {
            client->stop = 1;
            break;
        }

        for (;;) {
            uint8_t *payload = NULL;
            size_t payload_len = 0;
            void *message;

            st = pmsg_decoder_pop(&client->stream, &payload, &payload_len);

            if (st == PMSG_AGAIN) {
                break;
            }

            if (st != PMSG_OK) {
                client->stop = 1;
                break;
            }

            message = pmsg_pb_unpack(&client->pb, 1, payload, payload_len);

            free(payload);

            if (!message) {
                client->stop = 1;
                break;
            }

            if (client->cfg.on_server_message) {
                client->cfg.on_server_message(client, message, client->cfg.userdata);

                if (client->pb.server_free) {
                    client->pb.server_free(message, NULL);
                }
            } else {
                pmsg_client_inbox_push(client, message);
            }

            if (client->stop) {
                break;
            }
        }
    }

    free(buf);

    client->alive = 0;

    pmsg_client_inbox_wake(client);

    if (!client->user_stop && client->cfg.on_server_message) {
        client->cfg.on_server_message(client, NULL, client->cfg.userdata);
    }
}

static void *pmsg_client_thread(void *arg)
{
    pmsg_client_loop((pmsg_client_t *)arg);
    return NULL;
}

static pmsg_result_t pmsg_client_connect(pmsg_client_t *client)
{
    struct addrinfo hints;
    struct addrinfo *res = NULL;
    struct addrinfo *it;

    char service[8];
    int gai;

    int rcv = 16 * 1024;
    int snd = 16 * 1024;
    int nodelay = 1;

    snprintf(service, sizeof(service), "%u", (unsigned)client->cfg.port);
    memset(&hints, 0, sizeof(hints));

    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    gai = getaddrinfo(client->host, service, &hints, &res);

    if (gai != 0) {
        return PMSG_ERR_SOCKET;
    }

    client->fd = PMSG_SOCKET_INVALID;

    for (it = res; it; it = it->ai_next) {
        pmsg_socket_t fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);

        if (fd == PMSG_SOCKET_INVALID) {
            continue;
        }

        if (connect(fd, it->ai_addr, it->ai_addrlen) == 0) {
            client->fd = fd;
            break;
        }

        pmsg_socket_close(fd);
    }

    freeaddrinfo(res);

    if (client->fd == PMSG_SOCKET_INVALID) {
        return PMSG_ERR_SOCKET;
    }

    setsockopt(client->fd, SOL_SOCKET, SO_RCVBUF, (const char *)&rcv, sizeof(rcv));
    setsockopt(client->fd, SOL_SOCKET, SO_SNDBUF, (const char *)&snd, sizeof(snd));
    setsockopt(client->fd, IPPROTO_TCP, TCP_NODELAY, (const char *)&nodelay, sizeof(nodelay));

    return PMSG_OK;
}

pmsg_result_t pmsg_client_create(pmsg_client_t **out, const pmsg_client_config_t *config)
{
    pmsg_client_t *client;

    if (!out || !config || !config->protobuf || !config->host || !config->host[0] || config->port == 0) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    client = (pmsg_client_t *)calloc(1, sizeof(*client));

    if (!client) {
        return PMSG_ERR_NO_MEMORY;
    }

    client->cfg = *config;
    client->pb = *config->protobuf;
    client->cfg.protobuf = &client->pb;
    client->host = (char *)malloc(strlen(config->host) + 1u);

    if (!client->host) {
        free(client);
        return PMSG_ERR_NO_MEMORY;
    }

    memcpy(client->host, config->host, strlen(config->host) + 1u);

    client->cfg.host = client->host;
    client->fd = PMSG_SOCKET_INVALID;
    *out = client;

    return PMSG_OK;
}

pmsg_result_t pmsg_client_start(pmsg_client_t *client)
{
    pmsg_result_t st;

    if (!client || client->started) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    st = (pmsg_result_t)pmsg_net_acquire();

    if (st != PMSG_OK) {
        return st;
    }

    client->net_held = 1;

    st = pmsg_client_connect(client);

    if (st != PMSG_OK) {
        pmsg_net_release();
        client->net_held = 0;
        return st;
    }

    if (pmsg_decoder_init(&client->stream, 2u * 1024u * 1024u) != PMSG_OK) {
        pmsg_socket_close(client->fd);

        client->fd = PMSG_SOCKET_INVALID;

        pmsg_net_release();

        client->net_held = 0;

        return PMSG_ERR_NO_MEMORY;
    }

    client->stream_ready = 1;

    pmsg_mutex_init(&client->send_mu);

    client->send_mu_ready = 1;

    pmsg_mutex_init(&client->inbox_mu);
    pmsg_cond_init(&client->inbox_cv);

    client->inbox_ready = 1;
    client->alive = 1;
    client->started = 1;

    if (pthread_create(&client->thread, NULL, pmsg_client_thread, client) != 0) {
        client->alive = 0;
        client->started = 0;

        pmsg_socket_close(client->fd);

        client->fd = PMSG_SOCKET_INVALID;

        pmsg_decoder_free(&client->stream);

        client->stream_ready = 0;

        pmsg_mutex_destroy(&client->send_mu);

        client->send_mu_ready = 0;

        pmsg_cond_destroy(&client->inbox_cv);

        pmsg_mutex_destroy(&client->inbox_mu);

        client->inbox_ready = 0;

        pmsg_net_release();

        client->net_held = 0;

        return PMSG_ERR_SOCKET;
    }

    client->thread_started = 1;

    return PMSG_OK;
}

pmsg_result_t pmsg_client_send(pmsg_client_t *client, const void *message)
{
    uint8_t *payload = NULL;
    uint8_t *frame = NULL;
    size_t payload_len = 0;
    size_t frame_len = 0;

    pmsg_result_t st;

    if (!client || !message || !client->send_mu_ready) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    st = pmsg_pb_pack(&client->pb, 0, message, 2u * 1024u * 1024u, &payload, &payload_len);

    if (st != PMSG_OK) {
        return st;
    }

    st = pmsg_frame_encode(payload, payload_len, &frame, &frame_len);

    free(payload);

    if (st != PMSG_OK) {
        return st;
    }

    pmsg_mutex_lock(&client->send_mu);

    if (!client->alive || client->fd == PMSG_SOCKET_INVALID) {
        pmsg_mutex_unlock(&client->send_mu);
        free(frame);
        return PMSG_DISCONNECTED;
    }

    st = pmsg_send_all(client->fd, frame, frame_len);

    if (st != PMSG_OK) {
        client->alive = 0;
    }

    pmsg_mutex_unlock(&client->send_mu);
    free(frame);
    return st;
}

pmsg_result_t pmsg_client_recv(pmsg_client_t *client, void **message)
{
    if (!client || !message || !client->inbox_ready) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    if (pmsg_client_on_reader(client)) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    *message = NULL;

    pmsg_mutex_lock(&client->inbox_mu);

    while (client->inbox_len == 0 && client->alive && !client->stop) {
        pmsg_cond_wait(&client->inbox_cv, &client->inbox_mu);
    }

    if (client->inbox_len == 0) {
        pmsg_mutex_unlock(&client->inbox_mu);
        return PMSG_DISCONNECTED;
    }

    *message = client->inbox[client->inbox_head];
    
    client->inbox_head = (client->inbox_head + 1u) % PMSG_CLIENT_INBOX;
    client->inbox_len--;

    pmsg_cond_signal(&client->inbox_cv);
    pmsg_mutex_unlock(&client->inbox_mu);

    return PMSG_OK;
}

void pmsg_client_message_free(pmsg_client_t *client, void *message)
{
    if (!client || !message || !client->pb.server_free) {
        return;
    }

    client->pb.server_free(message, NULL);
}

void pmsg_client_stop(pmsg_client_t *client)
{
    int same;

    if (!client) {
        return;
    }

    client->user_stop = 1;
    client->stop = 1;
    client->alive = 0;

    pmsg_client_inbox_wake(client);
    pmsg_client_shutdown_fd(client);

    if (!client->thread_started) {
        if (client->fd != PMSG_SOCKET_INVALID) {
            pmsg_socket_close(client->fd);

            client->fd = PMSG_SOCKET_INVALID;
        }

        return;
    }

    same = pmsg_client_on_reader(client);

    if (!same) {
        pthread_join(client->thread, NULL);

        client->thread_started = 0;

        if (client->fd != PMSG_SOCKET_INVALID) {
            pmsg_socket_close(client->fd);

            client->fd = PMSG_SOCKET_INVALID;
        }
    }
}

void pmsg_client_destroy(pmsg_client_t *client)
{
    if (!client) {
        return;
    }

    pmsg_client_stop(client);

    if (client->thread_started && !pmsg_client_on_reader(client)) {
        pthread_join(client->thread, NULL);

        client->thread_started = 0;
    }

    if (client->fd != PMSG_SOCKET_INVALID) {
        pmsg_socket_close(client->fd);
    }

    if (client->stream_ready) {
        pmsg_decoder_free(&client->stream);
    }

    pmsg_client_inbox_clear(client);

    if (client->inbox_ready) {
        pmsg_cond_destroy(&client->inbox_cv);
        pmsg_mutex_destroy(&client->inbox_mu);

        client->inbox_ready = 0;
    }

    if (client->send_mu_ready) {
        pmsg_mutex_destroy(&client->send_mu);
    }

    if (client->net_held) {
        pmsg_net_release();
    }

    free(client->host);
    free(client);
}
