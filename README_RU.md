# libpmsg

Библиотека на C для TCP-узла Parmigiano. Один публичный заголовок `include/libpmsg.h`. Снаружи видны указатели `pmsg_server_t *` и `pmsg_client_t *`. Поля этих структур лежат в `.c` и из приложения не читаются.

Провод — [parmigiano-protocol](https://github.com/parmigiano/parmigiano-protocol): длина `uint32` в big-endian и одно protobuf-сообщение. Структуры сообщений библиотека не содержит. Приложение генерирует их через `buf` из BSR и передаёт `pmsg_protobuf_t`. Если proto меняется, пересобирается приложение, не libpmsg.

На каждое соединение поток не создаётся. Один поток `epoll` обслуживает все сокеты. Клиент сверх `max_connections` принимается и сразу закрывается, в карту пользователей не попадает.

`pmsg_send_user` и `pmsg_client_send` ставят в сокет один целый кадр. Запись режется кусками около 16 КиБ. Приёмная сторона копит куски, пока не наберётся длина из заголовка, и только тогда отдаёт одно сообщение. Кадр больше `max_frame` (по умолчанию 2 МиБ) не уходит: `PMSG_ERR_FRAME`. Текст на 1024 КиБ в этот предел входит. `PMSG_ERR_SEND` значит, что на соединении уже ждут 32 кадра, а не что текст длиннее 16 КиБ.

Заголовок обёрнут в `extern "C"`. Из C++ вызываются те же функции и те же структуры.

## Сборка

```bash
make test
make lin-lib
```

Нужен Linux и `gcc` с `pthread`.

## Protobuf

Перед стартом сервера или клиента заполните таблицу функций из сгенерированного protobuf-c. `allocator` библиотека всегда передаёт как `NULL`. `client_user_id` нужен серверу, чтобы узнать отправителя: в самой библиотеке нет смещения поля `user_id`.

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

Указатели на функции копируются при `pmsg_runtime_server` и `pmsg_client_create`. Сама структура `pmsg_protobuf_t` после этого может умереть. Код функций должен остаться в процессе.

## Коды

`PMSG_OK` — вызов прошёл. `PMSG_AGAIN` — полного кадра ещё нет, для `pmsg_poll` это истёкший таймаут. `PMSG_DISCONNECTED` — сокет закрыт или пользователя нет в сети.

`PMSG_ERR_INVALID_ARGUMENT` — пустой указатель или вызов не в том состоянии. `PMSG_ERR_NO_MEMORY` — не хватило памяти. `PMSG_ERR_FRAME` — длина 0, длина больше `max_frame` или пустое тело. `PMSG_ERR_PROTOBUF` — unpack не собрал сообщение. `PMSG_ERR_SOCKET` — listen, connect или сокет. `PMSG_ERR_SEND` — очередь исходящих уже держит 32 кадра, либо запись в сокет оборвалась. `PMSG_ERR_RECV` — ошибка чтения.

Старые имена остаются макросами: `PMSG_ERR_RANGE`, `PMSG_ERR_OFFLINE`, `PMSG_ERR_BUSY`, `PMSG_ERR_CLOSED`, `PMSG_ERR_STATE`, `PMSG_ERR_ARG`, `PMSG_ERR_NOMEM`, `PMSG_ERR_PROTO`.

## Сервер

Порядок: `pmsg_runtime_init`, правка конфига, `pmsg_runtime_server`, `pmsg_runtime_start`. Дальше в цикле `pmsg_poll`. Отправка из любого потока: `pmsg_send_user` или `pmsg_send_conn`. В конце `pmsg_runtime_shutdown`.

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
pmsg_send_user(chat, user_id, &outgoing);

pmsg_runtime_shutdown(&runtime);
```

`pmsg_default_config` возвращает конфиг по значению. Адрес `0.0.0.0`, порт `9000`, `max_connections` 100000, `max_frame` 2 МиБ, буферы сокета 16 КиБ, `backlog` 4096, `protobuf` равен `NULL`. Ноль в лимитах при создании сервера заменяется этими значениями. `bind_host` и `protobuf` должны жить до `pmsg_runtime_server`: библиотека копирует строку адреса и таблицу функций.

`io_threads`, `idle_timeout_ms`, `recv_timeout_ms` и `send_timeout_ms` записываются в сервер. Слушание на Linux всё равно идёт одним потоком `epoll`.

`pmsg_runtime_init` обнуляет `pmsg_runtime_t` и помечает runtime готовым. Повторно вызывать его на том же объекте после `pmsg_runtime_shutdown`.

`pmsg_runtime_server` добавляет слушающий сервер в runtime и возвращает указатель. Имя не пустое. Вызов после `pmsg_runtime_start` возвращает `NULL`. `config == NULL` берёт `pmsg_default_config`. Без `protobuf` принятый кадр не распакуется, соединение закроется.

`pmsg_runtime_start` открывает сокеты всех серверов этого runtime и запускает `epoll`. Порт `0` в конфиге занимает свободный порт. Если один из серверов не поднялся, уже запущенные останавливаются.

`pmsg_runtime_wait` блокирует поток, пока runtime запущен. `pmsg_runtime_run` делает `pmsg_runtime_start`, затем `pmsg_runtime_wait`.

`pmsg_runtime_shutdown` закрывает серверы, освобождает их и снимает флаг запуска. После этого runtime можно инициализировать снова.

`pmsg_server_port` возвращает порт, который реально занял `bind`. До старта это `0`.

`pmsg_poll` забирает одно событие. `timeout_ms == 0` возвращает сразу. Иначе ждёт до указанного числа миллисекунд. `PMSG_AGAIN` — события нет. `PMSG_EV_PACKET` — в `event->message` лежит распакованный `ClientMessage`, `user_id` взят через `client_user_id`, `conn_id` — номер соединения. `PMSG_EV_CLOSE` — соединение закрыто, `message` пустой. Указатель `message` жив до `pmsg_event_free`. Каждый успешный `pmsg_poll` заканчивайте `pmsg_event_free`, в том числе для `PMSG_EV_CLOSE`.

`pmsg_send_user` пакует `ServerMessage` и ставит кадр всем живым соединениям этого `user_id`. `pmsg_send_conn` делает то же для одного `conn_id`. Обе возвращаются, когда кадр лежит в очереди, не когда он доехал до сокета. `PMSG_DISCONNECTED` — такого пользователя или соединения нет. `PMSG_ERR_FRAME` проверяется до поиска пользователя: слишком большой пакет офлайн-пользователю тоже даёт `PMSG_ERR_FRAME`. `PMSG_ERR_SEND` — у соединения уже 32 кадра в очереди.

## Клиент

Это исходящее соединение, например HTTP-процесс к `parmigiano-tcp:9000`. Клиент кладётся в структуру приложения, хендлер берёт тот же указатель.

Если `on_server_message` равен `NULL`, ответы забирает `pmsg_client_recv`. Если колбэк задан, сообщения приходят в него на потоке чтения и в `pmsg_client_recv` не попадают. Указатель в колбэке жив только до возврата. `NULL` в колбэке значит, что TCP закрылся. Из колбэка можно вызвать `pmsg_client_send`.

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

`pmsg_client_create` выделяет клиент. Нужны непустой `host`, ненулевой `port` и `protobuf`. Сокет ещё не открыт.

`pmsg_client_start` подключается и запускает поток чтения. Повторный старт возвращает `PMSG_ERR_INVALID_ARGUMENT`.

`pmsg_client_send` пакует `ClientMessage` и записывает весь кадр. Можно звать из хендлера и из колбэка.

`pmsg_client_recv` ждёт следующий `ServerMessage`. Вызвавший код владеет `*message` и отпускает его через `pmsg_client_message_free`. `PMSG_DISCONNECTED` — соединение закрыто, указатель пустой. Из потока чтения эту функцию не вызывают.

`pmsg_client_stop` закрывает сокет и останавливает поток чтения. `pmsg_client_destroy` делает то же и освобождает клиент. `pmsg_client_destroy(NULL)` ничего не делает.

## Кадр без сокета

Эти функции разбирают байты, которые приложение прочитало само.

`pmsg_frame_encode` собирает кадр: 4 байта длины в big-endian и `payload`. Вызвавший код делает `free` на `*out`.

`pmsg_stream_init` готовит поток и копирует `protobuf`. Дальше в него кормят куски сокета.

`pmsg_stream_feed` дописывает байты. Неполный кадр остаётся внутри.

`pmsg_stream_next_client` достаёт один `ClientMessage`. `PMSG_AGAIN` — кадр ещё не собран. Указатель освобождает `pmsg_stream_client_free`.

`pmsg_stream_free` освобождает буфер потока.

`pmsg_server_message_encode` пакует `ServerMessage` и сразу оборачивает его в TCP-кадр. `*data` освобождает вызвавший код через `free`.
