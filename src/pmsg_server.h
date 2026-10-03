#ifndef PMSG_SERVER_H
#define PMSG_SERVER_H

#include "libpmsg.h"

int pmsg_net_acquire(void);

void pmsg_net_release(void);

pmsg_server_t *pmsg_server_create(const char *name, const pmsg_config_t *config);

int pmsg_server_start(pmsg_server_t *server);

void pmsg_server_shutdown(pmsg_server_t *server);

#endif
