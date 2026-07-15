#include "get_set_cfg.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include "json_helper.h"


int set_mqtt_cfg(redisContext *ctx, cJSON *data, int primary)
{
    char hash[32] = "";
    redisReply *reply;
    cJSON *item;

    if (!ctx || !data)
        return -1;

    /*-------------------------------------------------------
     * Find the required MQTT hash
     * primary = 1 -> mqtt_0_cfg or mqtt_1_cfg having primary=1
     * primary = 0 -> mqtt_0_cfg or mqtt_1_cfg having primary=0
     *------------------------------------------------------*/

    for (int i = 0; i < 2; i++)
    {
        char tmp[32];
        sprintf(tmp, "mqtt_%d_cfg", i);

        if (rget_int(ctx, tmp, "primary", -1) == primary)
        {
            strcpy(hash, tmp);
            break;
        }
    }

    if (hash[0] == '\0')
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                                  \
    do                                                                    \
    {                                                                     \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);          \
        if (cJSON_IsString(item) && item->valuestring)                    \
        {                                                                 \
            reply = redisCommand(ctx,                                     \
                    "HSET %s %s %s",                                      \
                    hash, REDIS_KEY, item->valuestring);                  \
            if (reply) freeReplyObject(reply);                            \
        }                                                                 \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                                   \
    do                                                                    \
    {                                                                     \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);          \
        if (cJSON_IsNumber(item))                                         \
        {                                                                 \
            reply = redisCommand(ctx,                                     \
                    "HSET %s %s %d",                                      \
                    hash, REDIS_KEY, item->valueint);                     \
            if (reply) freeReplyObject(reply);                            \
        }                                                                 \
    } while (0)

    /* Broker parameters */
    UPDATE_STR("BROKER_IP",        "broker_ip_url");
    UPDATE_STR("BROKER_PORT",      "broker_port");
    UPDATE_STR("CLIENT_ID",        "client_id");
    UPDATE_STR("USERNAME",         "username");
    UPDATE_STR("PASSWORD",         "password");

    /* Publish intervals */
    UPDATE_INT("HEALTH_INTERVAL",      "hc_pub_interval");
    UPDATE_INT("INST_INTERVAL",        "dlms_inst_pub_interval");
    UPDATE_INT("METER_DATA_INTERVAL",  "dlms_data_pub_interval");
    UPDATE_INT("MODBUS_INTERVAL",      "modbus_data_pub_interval");

#undef UPDATE_STR
#undef UPDATE_INT

    return 0;
}

int set_cfg_export_json(redisContext *ctx, cmd_request_t *cmd)
{
    if (!strcmp(cmd->data_type_req, "MQTT_BROKER_PRIMARY"))
        return set_mqtt_cfg(ctx, cmd, 1);

    if (!strcmp(cmd->data_type_req, "MQTT_BROKER_SECONDARY"))
        return set_mqtt_cfg(ctx, cmd, 0);

    if (!strcmp(cmd->data_type_req, "MODEM"))
        return set_modem_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "IPSEC"))
        return set_ipsec_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "MODBUS_TCP"))
        return set_modtcp_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "MODBUS_RTU"))
        return set_modrtu_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "IEC104"))
        return set_iec104_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "IEC101"))
        return set_iec101_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "FTP"))
        return set_ftp_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "NTP"))
        return set_ntp_cfg(ctx, cmd);

    return -1;
}