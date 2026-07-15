#ifndef GET_CFG_JSON_EXPORT_H
#define GET_CFG_JSON_EXPORT_H
#include <hiredis/hiredis.h>
#include <stdint.h>
#include "../include/general.h"

char *get_cfg_export_json(redisContext *ctx,
                          cmd_request_t *cmd);

#endif