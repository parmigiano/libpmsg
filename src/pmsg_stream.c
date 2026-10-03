#include "pmsg_frame.h"

#include <stdlib.h>
#include <string.h>

pmsg_result_t pmsg_pb_pack(const pmsg_protobuf_t *pb, int server_side, const void *message, uint32_t max_frame, uint8_t **out, size_t *out_len)
{
    size_t (*size_fn)(const void *);
    size_t (*pack_fn)(const void *, uint8_t *);

    size_t n;
    size_t wrote;
    uint8_t *buf;

    if (!pb || !message || !out || !out_len) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    size_fn = server_side ? pb->server_packed_size : pb->client_packed_size;
    pack_fn = server_side ? pb->server_pack : pb->client_pack;

    if (!size_fn || !pack_fn) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    n = size_fn(message);

    if ((max_frame && n > max_frame) || n > 0xffffffffu) {
        return PMSG_ERR_FRAME;
    }

    buf = (uint8_t *)malloc(n ? n : 1u);
    if (!buf) {
        return PMSG_ERR_NO_MEMORY;
    }

    wrote = pack_fn(message, buf);
    if ((max_frame && wrote > max_frame) || wrote > 0xffffffffu) {
        free(buf);

        return PMSG_ERR_FRAME;
    }

    *out = buf;
    *out_len = wrote;

    return PMSG_OK;
}

void *pmsg_pb_unpack(const pmsg_protobuf_t *pb, int server_side, const uint8_t *data, size_t len)
{
    void *(*unpack_fn)(void *, size_t, const uint8_t *);

    if (!pb || (!data && len)) {
        return NULL;
    }

    unpack_fn = server_side ? pb->server_unpack : pb->client_unpack;

    if (!unpack_fn) {
        return NULL;
    }

    return unpack_fn(NULL, len, data);
}

pmsg_result_t pmsg_stream_init(pmsg_stream_t *stream, const pmsg_protobuf_t *protobuf)
{
    pmsg_result_t st;

    if (!stream || !protobuf || !protobuf->client_unpack) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    st = (pmsg_result_t)pmsg_decoder_init(stream, 2u * 1024u * 1024u);

    if (st != PMSG_OK) {
        return st;
    }

    stream->protobuf = *protobuf;
    stream->has_protobuf = true;

    return PMSG_OK;
}

pmsg_result_t pmsg_stream_feed(pmsg_stream_t *stream, const void *data, size_t size)
{
    if (!stream || (size > 0 && !data)) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    return (pmsg_result_t)pmsg_decoder_push(stream, (const uint8_t *)data, size);
}

pmsg_result_t pmsg_stream_next_client(pmsg_stream_t *stream, void **message)
{
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    void *msg;
    int st;

    if (!stream || !message || !stream->has_protobuf) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    st = pmsg_decoder_pop(stream, &payload, &payload_len);

    if (st != PMSG_OK) {
        return (pmsg_result_t)st;
    }

    msg = pmsg_pb_unpack(&stream->protobuf, 0, payload, payload_len);

    free(payload);

    if (!msg) {
        return PMSG_ERR_PROTOBUF;
    }

    *message = msg;

    return PMSG_OK;
}

void pmsg_stream_client_free(pmsg_stream_t *stream, void *message)
{
    if (!stream || !message || !stream->protobuf.client_free) {
        return;
    }

    stream->protobuf.client_free(message, NULL);
}

void pmsg_stream_free(pmsg_stream_t *stream)
{
    if (!stream) {
        return;
    }

    pmsg_decoder_free(stream);

    stream->has_protobuf = false;
}

pmsg_result_t pmsg_server_message_encode(const pmsg_protobuf_t *protobuf, const void *message, void **data, size_t *size)
{
    uint8_t *payload = NULL;
    uint8_t *frame = NULL;
    size_t payload_len = 0;
    size_t frame_len = 0;

    pmsg_result_t st;

    if (!data || !size) {
        return PMSG_ERR_INVALID_ARGUMENT;
    }

    st = pmsg_pb_pack(protobuf, 1, message, 2u * 1024u * 1024u, &payload, &payload_len);

    if (st != PMSG_OK) {
        return st;
    }

    st = pmsg_frame_encode(payload, payload_len, &frame, &frame_len);

    free(payload);

    if (st != PMSG_OK) {
        return st;struct pmsg_client {
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
    }

    *data = frame;
    *size = frame_len;

    return PMSG_OK;
}
