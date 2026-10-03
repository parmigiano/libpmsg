# libpmsg

C library for the Parmigiano TCP node. The public header is `include/libpmsg.h`. Callers hold `pmsg_server_t *` and `pmsg_client_t *`. The field layout of those two types stays in the `.c` files.

The wire format is [parmigiano-protocol](https://github.com/parmigiano/parmigiano-protocol): a big-endian `uint32` length and one protobuf message. libpmsg does not define those messages. The application generates them with `buf` from BSR and passes `pmsg_protobuf_t`. A proto change rebuilds the application, not libpmsg.

Connections are not threads. Linux uses one `epoll` thread for every connection. A client past `max_connections` is accepted and closed immediately, with no user-map entry.

`pmsg_send_user` and `pmsg_client_send` write one whole frame. The socket write is sliced into pieces of about 16 KiB. The receiver keeps those pieces until the length prefix is satisfied, then returns one message. A frame bigger than `max_frame` (default 2 MiB) is rejected with `PMSG_ERR_FRAME`. A 1024 KiB text fits. `PMSG_ERR_SEND` means 32 frames are already waiting on that connection.

The header is wrapped in `extern "C"`. C++ calls the same functions and the same structs.

## Build

```bash
make test
make lin-lib
```

Requires Linux and `gcc` with `pthread`.

## Protobuf

Fill the function table from generated protobuf-c before starting a server or a client. The library always passes `allocator` as `NULL`. `client_user_id` exists so the server can read the sender: the library does not know the `user_id` field offset.

```c
static uint64_t client_user_id(const void *message)
{
    return ((const Parmigiano__V1__ClientMessage *)message)->user_id;
}

static const pmsg_protobuf_t app_protobuf = {
    .client_packed_size = (size_t (*)(const void *))parmigiano__v1__client_message__get_packed_size,
    .client_pack = (size_t (*)(const void *, uint8_t *))parmigiano__v1__client_message__pack,
    .client_unpack = (void *(*)(void *, size_t, const uint8_t *))parmigiano__v1__client_message__unpack,
    .client_free = (void (*)(void *, void *))parmigiano__v1__client_message__free_unpacked,
    .client_user_id = client_user_id,
    .server_packed_size = (size_t (*)(const void *))parmigiano__v1__server_message__get_packed_size,
    .server_pack = (size_t (*)(const void *, uint8_t *))parmigiano__v1__server_message__pack,
    .server_unpack = (void *(*)(void *, size_t, const uint8_t *))parmigiano__v1__server_message__unpack,
    .server_free = (void (*)(void *, void *))parmigiano__v1__server_message__free_unpacked,
};
```

`pmsg_runtime_server` and `pmsg_client_create` copy the function pointers. The `pmsg_protobuf_t` object itself may go away after that call. The functions must stay loaded.

## Result codes

`PMSG_OK` means the call finished. `PMSG_AGAIN` means a full frame is not ready yet; for `pmsg_poll` the wait timed out. `PMSG_DISCONNECTED` means the socket is closed or that user is offline.

`PMSG_ERR_INVALID_ARGUMENT` is a NULL pointer or a call in the wrong state. `PMSG_ERR_NO_MEMORY` is allocation failure. `PMSG_ERR_FRAME` is a zero length, a length above `max_frame`, or an empty body. `PMSG_ERR_PROTOBUF` means unpack failed. `PMSG_ERR_SOCKET` is listen, connect, or a socket error. `PMSG_ERR_SEND` means 32 outbound frames are already queued, or the socket write failed. `PMSG_ERR_RECV` is a read error.

Older names remain as macros: `PMSG_ERR_RANGE`, `PMSG_ERR_OFFLINE`, `PMSG_ERR_BUSY`, `PMSG_ERR_CLOSED`, `PMSG_ERR_STATE`, `PMSG_ERR_ARG`, `PMSG_ERR_NOMEM`, `PMSG_ERR_PROTO`.

## Server

Order: `pmsg_runtime_init`, edit the config, `pmsg_runtime_server`, `pmsg_runtime_start`. Then loop on `pmsg_poll`. Send from any thread with `pmsg_send_user` or `pmsg_send_conn`. Finish with `pmsg_runtime_shutdown`.

```c
pmsg_runtime_t runtime;
pmsg_config_t cfg = pmsg_default_config();
pmsg_server_t *chat;

pmsg_runtime_init(&runtime);
cfg.port = 9000;
cfg.protobuf = &app_protobuf;
chat = pmsg_runtime_server(&runtime, "chat", &cfg);
pmsg_runtime_start(&runtime);

pmsg_event_t ev;
if (pmsg_poll(chat, &ev, 1000) == PMSG_OK && ev.kind == PMSG_EV_PACKET) {
    Parmigiano__V1__ClientMessage *incoming = ev.message;
    (void)incoming;
    pmsg_event_free(&ev);
}

Parmigiano__V1__ServerMessage outgoing = PARMIGIANO__V1__SERVER_MESSAGE__INIT;
pmsg_send_user(chat, 42, &outgoing);

pmsg_runtime_shutdown(&runtime);
```

`pmsg_default_config` returns a config by value. Bind address `0.0.0.0`, port `9000`, `max_connections` 100000, `max_frame` 2 MiB, socket buffers 16 KiB, `backlog` 4096, `protobuf` NULL. A zero limit is replaced with these defaults when the server is created. `bind_host` and `protobuf` must stay valid until `pmsg_runtime_server`: the library copies the address string and the function table.

`io_threads`, `idle_timeout_ms`, `recv_timeout_ms`, and `send_timeout_ms` are stored on the server. Linux still listens with one `epoll` thread.

`pmsg_runtime_init` zeroes `pmsg_runtime_t` and marks the runtime ready. Call it again on the same object after `pmsg_runtime_shutdown`.

`pmsg_runtime_server` adds a listening server and returns it. The name must be non-empty. A call after `pmsg_runtime_start` returns `NULL`. `config == NULL` uses `pmsg_default_config`. Without `protobuf`, an accepted frame fails to unpack and the connection is closed.

`pmsg_runtime_start` opens every server socket in this runtime and starts `epoll`. Port `0` binds an ephemeral port. If one server fails to start, the ones already started are shut down.

`pmsg_runtime_wait` blocks while the runtime is running. `pmsg_runtime_run` calls `pmsg_runtime_start`, then `pmsg_runtime_wait`.

`pmsg_runtime_shutdown` closes the servers, frees them, and clears the running flag. The runtime can be initialized again after that.

`pmsg_server_port` returns the port `bind` actually took. Before start it is `0`.

`pmsg_poll` takes one event. `timeout_ms == 0` returns immediately. Otherwise it waits up to that many milliseconds. `PMSG_AGAIN` means there is no event. `PMSG_EV_PACKET` puts an unpacked `ClientMessage` in `event->message`; `user_id` comes from `client_user_id`, and `conn_id` is the connection number. `PMSG_EV_CLOSE` means the connection closed and `message` is NULL. `message` stays valid until `pmsg_event_free`. Finish every successful `pmsg_poll` with `pmsg_event_free`, including `PMSG_EV_CLOSE`.

`pmsg_send_user` packs a `ServerMessage` and queues the frame on every live connection of that `user_id`. `pmsg_send_conn` does the same for one `conn_id`. Both return when the frame is queued, not when the socket has finished writing it. `PMSG_DISCONNECTED` means that user or connection is absent. The frame size is checked before the user lookup, so an oversized message to an offline user still returns `PMSG_ERR_FRAME`. `PMSG_ERR_SEND` means that connection already has 32 frames queued.

## Client

This is an outbound connection, for example the HTTP process connecting to `parmigiano-tcp:9000`. Store the client on the application struct and use the same pointer from a handler.

When `on_server_message` is `NULL`, `pmsg_client_recv` takes the replies. When the callback is set, messages are delivered there on the reader thread and `pmsg_client_recv` does not see them. The pointer inside the callback is valid only until the callback returns. `NULL` in the callback means TCP closed. `pmsg_client_send` may be called from the callback.

```c
pmsg_client_config_t tcp;
pmsg_client_t *client = NULL;

memset(&tcp, 0, sizeof(tcp));
tcp.host = "parmigiano-tcp";
tcp.port = 9000;
tcp.protobuf = &app_protobuf;

pmsg_client_create(&client, &tcp);
pmsg_client_start(client);

pmsg_client_send(client, &client_message);

void *reply = NULL;
if (pmsg_client_recv(client, &reply) == PMSG_OK) {
    Parmigiano__V1__ServerMessage *incoming = reply;
    (void)incoming;
    pmsg_client_message_free(client, reply);
}

pmsg_client_destroy(client);
```

`pmsg_client_create` allocates the client. It needs a non-empty `host`, a non-zero `port`, and `protobuf`. The socket is not open yet.

`pmsg_client_start` connects and starts the reader thread. A second start returns `PMSG_ERR_INVALID_ARGUMENT`.

`pmsg_client_send` packs a `ClientMessage` and writes the whole frame. Call it from a handler or from the callback.

`pmsg_client_recv` waits for the next `ServerMessage`. The caller owns `*message` and releases it with `pmsg_client_message_free`. `PMSG_DISCONNECTED` means the connection closed and the pointer is NULL. Do not call this from the reader thread.

`pmsg_client_stop` closes the socket and stops the reader thread. `pmsg_client_destroy` does that and frees the client. `pmsg_client_destroy(NULL)` does nothing.

## A frame without a socket

These functions decode bytes the application read itself.

`pmsg_frame_encode` builds a frame: 4 big-endian length bytes plus `payload`. The caller `free`s `*out`.

`pmsg_stream_init` prepares a stream and copies `protobuf`. Feed socket chunks into it afterwards.

`pmsg_stream_feed` appends bytes. An incomplete frame stays inside the stream.

`pmsg_stream_next_client` pops one `ClientMessage`. `PMSG_AGAIN` means the frame is not complete yet. Release the pointer with `pmsg_stream_client_free`.

`pmsg_stream_free` releases the stream buffer.

`pmsg_server_message_encode` packs a `ServerMessage` and wraps it in a TCP frame. The caller `free`s `*data`.
