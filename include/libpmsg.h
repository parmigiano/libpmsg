/**
 * libpmsg — Parmigiano TCP packet node.
 *
 * Wire format: uint32 big-endian length + one protobuf payload.
 * This library does not define proto messages. The application generates them
 * from buf.build/parmigiano/protocol and passes pack/unpack functions.
 */
#ifndef LIBPMSG_H
#define LIBPMSG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct pmsg_server pmsg_server_t;

typedef struct pmsg_client pmsg_client_t;

typedef enum {
    PMSG_OK = 0,
    PMSG_AGAIN,
    PMSG_DISCONNECTED,

    PMSG_ERR_INVALID_ARGUMENT,
    PMSG_ERR_NO_MEMORY,
    PMSG_ERR_FRAME,
    PMSG_ERR_PROTOBUF,
    PMSG_ERR_SOCKET,
    PMSG_ERR_SEND,
    PMSG_ERR_RECV
} pmsg_result_t;

typedef enum pmsg_event_kind {
    PMSG_EV_NONE = 0,
    PMSG_EV_PACKET = 1,
    PMSG_EV_CLOSE = 2
} pmsg_event_kind_t;

/**
 * Functions from the caller's generated protobuf code.
 * Signatures match protobuf-c pack/unpack/free_unpacked.
 * allocator is passed as NULL.
 * client_user_id reads ClientMessage.user_id so the TCP server can route.
 */
typedef struct pmsg_protobuf {
    size_t (*client_packed_size)(const void *message);
    size_t (*client_pack)(const void *message, uint8_t *out);
    void *(*client_unpack)(void *allocator, size_t len, const uint8_t *data);
    void (*client_free)(void *message, void *allocator);
    uint64_t (*client_user_id)(const void *message);

    size_t (*server_packed_size)(const void *message);
    size_t (*server_pack)(const void *message, uint8_t *out);
    void *(*server_unpack)(void *allocator, size_t len, const uint8_t *data);
    void (*server_free)(void *message, void *allocator);
} pmsg_protobuf_t;

typedef struct pmsg_event {
    pmsg_event_kind_t kind;
    uint64_t conn_id;
    uint64_t user_id;
    void *message;
    void (*_free_message)(void *message, void *allocator);
} pmsg_event_t;

typedef struct pmsg_config {
    const char *bind_host;
    uint16_t port;

    uint32_t max_connections;
    uint32_t max_frame;
    uint32_t recv_buf;
    uint32_t send_buf;
    uint32_t io_threads;

    uint32_t idle_timeout_ms;
    uint32_t recv_timeout_ms;
    uint32_t send_timeout_ms;

    int backlog;
    const pmsg_protobuf_t *protobuf;
} pmsg_config_t;

typedef struct pmsg_runtime {
    pmsg_server_t **_servers;
    size_t _servers_count;
    size_t _servers_capacity;

    bool _initialized;
    bool _started;
    bool _network_initialized;
} pmsg_runtime_t;

typedef struct pmsg_stream {
    uint8_t *data;
    size_t len;
    size_t cap;
    uint32_t max_frame;
    pmsg_protobuf_t protobuf;
    bool has_protobuf;
} pmsg_stream_t;

typedef struct pmsg_client_config {
    const char *host;
    uint16_t port;
    const pmsg_protobuf_t *protobuf;

    /**
     * Optional. message is valid only until this callback returns.
     * NULL means the TCP connection closed.
     * Leave NULL and call pmsg_client_recv from a handler instead.
     */
    void (*on_server_message)(pmsg_client_t *client, void *message, void *userdata);

    void *userdata;
} pmsg_client_config_t;

typedef pmsg_result_t pmsg_status_t;

#define PMSG_ERR_RANGE PMSG_ERR_FRAME
#define PMSG_ERR_PROTO PMSG_ERR_PROTOBUF
#define PMSG_ERR_NOMEM PMSG_ERR_NO_MEMORY
#define PMSG_ERR_ARG PMSG_ERR_INVALID_ARGUMENT
#define PMSG_ERR_OFFLINE PMSG_DISCONNECTED
#define PMSG_ERR_BUSY PMSG_ERR_SEND
#define PMSG_ERR_CLOSED PMSG_DISCONNECTED
#define PMSG_ERR_STATE PMSG_ERR_INVALID_ARGUMENT

pmsg_config_t pmsg_default_config(void);

pmsg_result_t pmsg_runtime_init(pmsg_runtime_t *runtime);

pmsg_server_t *pmsg_runtime_server(pmsg_runtime_t *runtime, const char *name, const pmsg_config_t *config);

pmsg_result_t pmsg_runtime_start(pmsg_runtime_t *runtime);

pmsg_result_t pmsg_runtime_wait(pmsg_runtime_t *runtime);

pmsg_result_t pmsg_runtime_run(pmsg_runtime_t *runtime);

void pmsg_runtime_shutdown(pmsg_runtime_t *runtime);

uint16_t pmsg_server_port(const pmsg_server_t *server);

pmsg_result_t pmsg_poll(pmsg_server_t *server, pmsg_event_t *event, uint32_t timeout_ms);

void pmsg_event_free(pmsg_event_t *event);

pmsg_result_t pmsg_send_user(pmsg_server_t *server, uint64_t user_id, const void *message);

pmsg_result_t pmsg_send_conn(pmsg_server_t *server, uint64_t conn_id, const void *message);

pmsg_result_t pmsg_frame_encode(const uint8_t *payload, size_t payload_len, uint8_t **out, size_t *out_len);

pmsg_result_t pmsg_stream_init(pmsg_stream_t *stream, const pmsg_protobuf_t *protobuf);

pmsg_result_t pmsg_stream_feed(pmsg_stream_t *stream, const void *data, size_t size);

pmsg_result_t pmsg_stream_next_client(pmsg_stream_t *stream, void **message);

void pmsg_stream_client_free(pmsg_stream_t *stream, void *message);

void pmsg_stream_free(pmsg_stream_t *stream);

/** TCP frame: uint32 big-endian length + bytes from protobuf.server_pack. */
pmsg_result_t pmsg_server_message_encode(
    const pmsg_protobuf_t *protobuf,
    const void *message,
    void **data,
    size_t *size);

pmsg_result_t pmsg_client_create(pmsg_client_t **client, const pmsg_client_config_t *config);

pmsg_result_t pmsg_client_start(pmsg_client_t *client);

pmsg_result_t pmsg_client_send(pmsg_client_t *client, const void *message);

/** Next ServerMessage. Caller releases it with pmsg_client_message_free. */
pmsg_result_t pmsg_client_recv(pmsg_client_t *client, void **message);

void pmsg_client_message_free(pmsg_client_t *client, void *message);

void pmsg_client_stop(pmsg_client_t *client);

void pmsg_client_destroy(pmsg_client_t *client);

#ifdef __cplusplus
}
#endif

#endif
