#include "pmsg_frame.h"

#include <stdlib.h>
#include <string.h>

static uint32_t pmsg_load_be32(const uint8_t *p)
{
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}

pmsg_result_t pmsg_frame_encode(const uint8_t *payload, size_t payload_len, uint8_t **out, size_t *out_len)
{
    uint8_t *frame;

    uint32_t n;

    if (!out || !out_len || (payload_len > 0 && !payload)) {
        return PMSG_ERR_ARG;
    }

    if (payload_len > 0xffffffffu) {
        return PMSG_ERR_RANGE;
    }

    frame = (uint8_t *)malloc(payload_len + 4u);

    if (!frame) {
        return PMSG_ERR_NOMEM;
    }

    n = (uint32_t)payload_len;

    frame[0] = (uint8_t)(n >> 24);

    frame[1] = (uint8_t)(n >> 16);

    frame[2] = (uint8_t)(n >> 8);

    frame[3] = (uint8_t)n;

    if (payload_len) {
        memcpy(frame + 4, payload, payload_len);
    }

    *out = frame;

    *out_len = payload_len + 4u;

    return PMSG_OK;
}

int pmsg_decoder_init(pmsg_decoder_t *dec, uint32_t max_frame)
{
    memset(dec, 0, sizeof(*dec));

    dec->max_frame = max_frame ? max_frame : (2u * 1024u * 1024u);

    dec->data = (uint8_t *)malloc(PMSG_DECODER_START);

    if (!dec->data) {
        return PMSG_ERR_NOMEM;
    }

    dec->cap = PMSG_DECODER_START;

    return PMSG_OK;
}

void pmsg_decoder_free(pmsg_decoder_t *dec)
{
    free(dec->data);

    memset(dec, 0, sizeof(*dec));
}

static int pmsg_decoder_grow(pmsg_decoder_t *dec, size_t need)
{
    size_t cap;

    uint8_t *data;

    if (need <= dec->cap) {
        return PMSG_OK;
    }

    cap = dec->cap ? dec->cap : (size_t)PMSG_DECODER_START;

    while (cap < need) {
        if (cap > (SIZE_MAX / 2u)) {
            return PMSG_ERR_NOMEM;
        }

        cap *= 2u;
    }

    data = (uint8_t *)realloc(dec->data, cap);

    if (!data) {
        return PMSG_ERR_NOMEM;
    }

    dec->data = data;

    dec->cap = cap;

    return PMSG_OK;
}

static void pmsg_decoder_shrink(pmsg_decoder_t *dec)
{
    uint8_t *data;

    if (dec->cap <= PMSG_DECODER_START || dec->len > PMSG_DECODER_START) {
        return;
    }

    data = (uint8_t *)realloc(dec->data, PMSG_DECODER_START);

    if (!data) {
        return;
    }

    dec->data = data;

    dec->cap = PMSG_DECODER_START;
}

static int pmsg_decoder_header_bad(const pmsg_decoder_t *dec)
{
    uint32_t n;

    if (dec->len < 4u) {
        return 0;
    }

    n = pmsg_load_be32(dec->data);

    return n == 0 || n > dec->max_frame;
}

int pmsg_decoder_push(pmsg_decoder_t *dec, const uint8_t *data, size_t len)
{
    int st;

    if (!len) {
        return pmsg_decoder_header_bad(dec) ? PMSG_ERR_RANGE : PMSG_OK;
    }

    if (!data) {
        return PMSG_ERR_ARG;
    }

    st = pmsg_decoder_grow(dec, dec->len + len);

    if (st != PMSG_OK) {
        return st;
    }

    memcpy(dec->data + dec->len, data, len);

    dec->len += len;

    if (pmsg_decoder_header_bad(dec)) {
        return PMSG_ERR_RANGE;
    }

    return PMSG_OK;
}

int pmsg_decoder_pop(pmsg_decoder_t *dec, uint8_t **payload, size_t *payload_len)
{
    uint32_t n;

    uint8_t *copy;

    size_t rest;

    if (dec->len < 4u) {
        return PMSG_AGAIN;
    }

    n = pmsg_load_be32(dec->data);

    if (n == 0 || n > dec->max_frame) {
        return PMSG_ERR_RANGE;
    }

    if (dec->len < 4u + (size_t)n) {
        return PMSG_AGAIN;
    }

    copy = (uint8_t *)malloc(n ? n : 1u);

    if (!copy) {
        return PMSG_ERR_NOMEM;
    }

    if (n) {
        memcpy(copy, dec->data + 4, n);
    }

    rest = dec->len - 4u - (size_t)n;

    if (rest) {
        memmove(dec->data, dec->data + 4u + n, rest);
    }

    dec->len = rest;

    pmsg_decoder_shrink(dec);

    *payload = copy;

    *payload_len = n;

    return PMSG_OK;
}

int pmsg_decoder_pending(const pmsg_decoder_t *dec)
{
    return dec->len > 0;
}

void pmsg_outq_init(pmsg_outq_t *q)
{
    memset(q, 0, sizeof(*q));
}

void pmsg_outq_free(pmsg_outq_t *q)
{
    free(q->data);

    pmsg_outq_init(q);
}

static void pmsg_outq_compact(pmsg_outq_t *q)
{
    if (q->off == 0) {
        return;
    }

    if (q->off == q->len) {
        q->off = 0;

        q->len = 0;

        return;
    }

    memmove(q->data, q->data + q->off, q->len - q->off);

    q->len -= q->off;

    q->off = 0;
}

int pmsg_outq_push(pmsg_outq_t *q, const uint8_t *frame, size_t len)
{
    size_t cap;

    uint8_t *data;

    uint32_t slot;

    if (!frame || len == 0 || len > 0xffffffffu) {
        return PMSG_ERR_ARG;
    }

    if (q->nframes >= PMSG_MAX_QUEUED_FRAMES) {
        return PMSG_ERR_BUSY;
    }

    if (q->off > 0 && q->off == q->len) {
        q->off = 0;

        q->len = 0;
    }

    cap = q->cap;

    if (q->len + len > cap) {
        cap = cap ? cap : 4096u;

        while (cap < q->len + len) {
            if (cap > (SIZE_MAX / 2u)) {
                return PMSG_ERR_NOMEM;
            }

            cap *= 2u;
        }

        data = (uint8_t *)realloc(q->data, cap);

        if (!data) {
            return PMSG_ERR_NOMEM;
        }

        q->data = data;

        q->cap = cap;
    }

    memcpy(q->data + q->len, frame, len);

    q->len += len;

    slot = (q->head + q->nframes) % PMSG_MAX_QUEUED_FRAMES;

    q->sizes[slot] = (uint32_t)len;

    q->nframes++;

    return PMSG_OK;
}

size_t pmsg_outq_pending(const pmsg_outq_t *q)
{
    if (q->len < q->off) {
        return 0;
    }

    return q->len - q->off;
}

const uint8_t *pmsg_outq_data(const pmsg_outq_t *q, size_t *len)
{
    *len = pmsg_outq_pending(q);

    if (*len == 0) {
        return NULL;
    }

    return q->data + q->off;
}

void pmsg_outq_consume(pmsg_outq_t *q, size_t n)
{
    while (n > 0 && q->nframes > 0) {
        uint32_t frame = q->sizes[q->head];

        size_t left = (size_t)frame - q->sent_in_frame;

        size_t take = n < left ? n : left;

        q->sent_in_frame += take;

        q->off += take;

        n -= take;

        if (q->sent_in_frame == frame) {
            q->head = (q->head + 1u) % PMSG_MAX_QUEUED_FRAMES;

            q->nframes--;

            q->sent_in_frame = 0;
        }
    }

    if (q->off > 65536u) {
        pmsg_outq_compact(q);
    }
}
