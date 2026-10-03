#ifndef PMSG_FRAME_H
#define PMSG_FRAME_H

#include "libpmsg.h"

#include <stddef.h>
#include <stdint.h>

#define PMSG_MAX_QUEUED_FRAMES 32u
#define PMSG_DECODER_START 4096u

typedef pmsg_stream_t pmsg_decoder_t;

typedef struct pmsg_outq {
    uint8_t *data;
    size_t len;
    size_t cap;
    size_t off;
    uint32_t sizes[PMSG_MAX_QUEUED_FRAMES];
    uint32_t nframes;
    uint32_t head;
    size_t sent_in_frame;
} pmsg_outq_t;

int pmsg_decoder_init(pmsg_decoder_t *dec, uint32_t max_frame);

void pmsg_decoder_free(pmsg_decoder_t *dec);

int pmsg_decoder_push(pmsg_decoder_t *dec, const uint8_t *data, size_t len);

int pmsg_decoder_pop(pmsg_decoder_t *dec, uint8_t **payload, size_t *payload_len);

int pmsg_decoder_pending(const pmsg_decoder_t *dec);

void pmsg_outq_init(pmsg_outq_t *q);

void pmsg_outq_free(pmsg_outq_t *q);

int pmsg_outq_push(pmsg_outq_t *q, const uint8_t *frame, size_t len);

size_t pmsg_outq_pending(const pmsg_outq_t *q);

const uint8_t *pmsg_outq_data(const pmsg_outq_t *q, size_t *len);

void pmsg_outq_consume(pmsg_outq_t *q, size_t n);

pmsg_result_t pmsg_pb_pack(const pmsg_protobuf_t *pb, int server_side, const void *message, uint32_t max_frame, uint8_t **out, size_t *out_len);

void *pmsg_pb_unpack(const pmsg_protobuf_t *pb, int server_side, const uint8_t *data, size_t len);

#endif
