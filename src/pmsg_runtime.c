#include "pmsg_crosspltm.h"
#include "pmsg_server.h"

#include <stdlib.h>
#include <string.h>

static pmsg_mutex_t pmsg_net_mu;
static int pmsg_net_ready;
static int pmsg_net_refs;

static void pmsg_net_once(void)
{
    if (pmsg_net_ready) {
        return;
    }

    pmsg_mutex_init(&pmsg_net_mu);

    pmsg_net_ready = 1;
}

int pmsg_net_acquire(void)
{
    int st = PMSG_OK;

    pmsg_net_once();

    pmsg_mutex_lock(&pmsg_net_mu);

    if (st == PMSG_OK) {
        pmsg_net_refs++;
    }

    pmsg_mutex_unlock(&pmsg_net_mu);

    return st;
}

void pmsg_net_release(void)
{
    pmsg_net_once();

    pmsg_mutex_lock(&pmsg_net_mu);

    if (pmsg_net_refs > 0) {
        pmsg_net_refs--;
    }

    pmsg_mutex_unlock(&pmsg_net_mu);
}

pmsg_config_t pmsg_default_config(void)
{
    pmsg_config_t config;

    memset(&config, 0, sizeof(config));

    config.bind_host = "0.0.0.0";
    config.port = 9000;
    config.max_connections = 100000;
    config.max_frame = 2u * 1024u * 1024u;
    config.recv_buf = 16u * 1024u;
    config.send_buf = 16u * 1024u;
    config.io_threads = 0;
    config.idle_timeout_ms = 60000;
    config.recv_timeout_ms = 30000;
    config.send_timeout_ms = 30000;
    config.backlog = 4096;

    return config;
}

pmsg_result_t pmsg_runtime_init(pmsg_runtime_t *runtime)
{
    if (!runtime) {
        return PMSG_ERR_ARG;
    }

    memset(runtime, 0, sizeof(*runtime));

    if (pmsg_net_acquire() != PMSG_OK) {
        return PMSG_ERR_SOCKET;
    }

    runtime->_network_initialized = true;
    runtime->_initialized = true;

    return PMSG_OK;
}

pmsg_server_t *pmsg_runtime_server(pmsg_runtime_t *runtime, const char *name, const pmsg_config_t *config)
{
    pmsg_server_t *server;
    pmsg_server_t **grown;

    if (!runtime || !runtime->_initialized || runtime->_started || !name || !name[0]) {
        return NULL;
    }

    server = pmsg_server_create(name, config);

    if (!server) {
        return NULL;
    }

    if (runtime->_servers_count == runtime->_servers_capacity) {
        size_t cap = runtime->_servers_capacity ? runtime->_servers_capacity * 2u : 2u;

        grown = (pmsg_server_t **)realloc(runtime->_servers, cap * sizeof(*grown));

        if (!grown) {
            pmsg_server_shutdown(server);
            return NULL;
        }

        runtime->_servers = grown;
        runtime->_servers_capacity = cap;
    }

    runtime->_servers[runtime->_servers_count++] = server;
    return server;
}

pmsg_result_t pmsg_runtime_start(pmsg_runtime_t *runtime)
{
    size_t i;

    if (!runtime || !runtime->_initialized) {
        return PMSG_ERR_STATE;
    }

    if (runtime->_started) {
        return PMSG_ERR_STATE;
    }

    for (i = 0; i < runtime->_servers_count; i++) {
        int st = pmsg_server_start(runtime->_servers[i]);

        if (st != PMSG_OK) {
            size_t j;

            for (j = 0; j < i; j++) {
                pmsg_server_shutdown(runtime->_servers[j]);
            }

            return st;
        }
    }

    runtime->_started = true;

    return PMSG_OK;
}

pmsg_result_t pmsg_runtime_wait(pmsg_runtime_t *runtime)
{
    if (!runtime || !runtime->_started) {
        return PMSG_ERR_STATE;
    }

    while (runtime->_started) {
        struct timespec ts;

        ts.tv_sec = 0;
        ts.tv_nsec = 200000000L;

        nanosleep(&ts, NULL);
    }

    return PMSG_OK;
}

pmsg_result_t pmsg_runtime_run(pmsg_runtime_t *runtime)
{
    int st = pmsg_runtime_start(runtime);

    if (st != PMSG_OK) {
        return st;
    }

    return pmsg_runtime_wait(runtime);
}

void pmsg_runtime_shutdown(pmsg_runtime_t *runtime)
{
    size_t i;

    if (!runtime || !runtime->_initialized) {
        return;
    }

    for (i = 0; i < runtime->_servers_count; i++) {
        pmsg_server_shutdown(runtime->_servers[i]);
        runtime->_servers[i] = NULL;
    }

    free(runtime->_servers);

    runtime->_servers = NULL;
    runtime->_servers_count = 0;
    runtime->_started = false;
    runtime->_initialized = false;

    if (runtime->_network_initialized) {
        pmsg_net_release();
        runtime->_network_initialized = false;
    }
}
