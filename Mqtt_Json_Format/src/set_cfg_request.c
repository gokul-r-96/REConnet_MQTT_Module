#include "get_set_cfg.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include "json_helper.h"

int set_mqtt_cfg(redisContext *ctx, cJSON *data, int primary)
{
    printf("Entering into mqtt config setting!!!\n");

    char *str = cJSON_Print(data);
    printf("DATA JSON = %s\n", str);
    free(str);

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

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                               \
    do                                                                \
    {                                                                 \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);      \
        if (cJSON_IsString(item) && item->valuestring)                \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %s",                     \
                                 hash, REDIS_KEY, item->valuestring); \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                                \
    do                                                                 \
    {                                                                  \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);       \
                                                                       \
        if (cJSON_IsNumber(item))                                      \
        {                                                              \
            reply = redisCommand(ctx,                                  \
                                 "HSET %s %s %d",                      \
                                 hash, REDIS_KEY, item->valueint);     \
            if (reply)                                                 \
                freeReplyObject(reply);                                \
        }                                                              \
        else if (cJSON_IsString(item) && item->valuestring)            \
        {                                                              \
            reply = redisCommand(ctx,                                  \
                                 "HSET %s %s %d",                      \
                                 hash, REDIS_KEY,                      \
                                 atoi(item->valuestring));             \
            if (reply)                                                 \
                freeReplyObject(reply);                                \
        }                                                              \
    } while (0)

    /* Broker parameters */
    UPDATE_STR("BROKER_IP", "broker_ip_url");
    UPDATE_INT("BROKER_PORT", "broker_port");
    UPDATE_STR("CLIENT_ID", "client_id");
    UPDATE_STR("USERNAME", "username");
    UPDATE_STR("PASSWORD", "password");

    /* Publish intervals */
    UPDATE_INT("HEALTH_INTERVAL", "hc_pub_interval");
    UPDATE_INT("INST_INTERVAL", "dlms_inst_pub_interval");
    UPDATE_INT("METER_DATA_INTERVAL", "dlms_data_pub_interval");
    UPDATE_INT("MODBUS_INTERVAL", "modbus_data_pub_interval");

#undef UPDATE_STR
#undef UPDATE_INT

    /* Restart MQTT process */
    reply = redisCommand(ctx, "SADD proc_restart re_mqtt_proc");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
                LOG_INFO("Restart requested for re_mqtt_proc");
            else
                LOG_INFO("Restart already pending for re_mqtt_proc");
        }

        freeReplyObject(reply);
    }

    return 0;
}

int set_modem_cfg(redisContext *ctx, cJSON *data)
{
    char hash[] = "modem_cfg";
    char field[32];
    redisReply *reply;
    cJSON *item;
    int sim = 0;

    if (!ctx || !data)
        return -1;

    /* SIM_SLOT is mandatory */
    item = cJSON_GetObjectItemCaseSensitive(data, "SIM_SLOT");

    if (cJSON_IsNumber(item))
    {
        sim = item->valueint;
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        sim = atoi(item->valuestring);
    }
    else
    {
        return -1;
    }

    if (sim != 1 && sim != 2)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_FMT)                          \
    do                                                           \
    {                                                            \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY); \
        if (cJSON_IsString(item) && item->valuestring)           \
        {                                                        \
            sprintf(field, REDIS_FMT, sim);                      \
            reply = redisCommand(ctx,                            \
                                 "HSET %s %s %s",                \
                                 hash,                           \
                                 field,                          \
                                 item->valuestring);             \
            if (reply)                                           \
                freeReplyObject(reply);                          \
        }                                                        \
    } while (0)

    UPDATE_STR("USERNAME", "username%d");
    UPDATE_STR("PASSWORD", "password%d");
    UPDATE_STR("APN", "apn%d");
    UPDATE_STR("DIAL_NUM", "phone_num%d");

#undef UPDATE_STR

    reply = redisCommand(ctx, "SADD proc_restart ppp_monitor.sh");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for ppp_monitor.sh");
            }
            else
            {
                LOG_INFO("Restart already pending for ppp_monitor.sh");
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}

int set_ipsec_cfg(redisContext *ctx, cJSON *data)
{
    char hash[32];
    redisReply *reply;
    cJSON *item;
    int tunnel = 0;

    if (!ctx || !data)
        return -1;

    /* Tunnel number is mandatory */
    item = cJSON_GetObjectItemCaseSensitive(data, "IPSEC_TUNNEL");

    if (cJSON_IsNumber(item))
    {
        tunnel = item->valueint;
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        tunnel = atoi(item->valuestring);
    }
    else
    {
        return -1;
    }

    if (tunnel != 1 && tunnel != 2)
        return -1;

    sprintf(hash, "ipsec_%d_cfg", tunnel - 1);

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
        if (cJSON_IsString(item) && item->valuestring)               \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET %s %s %s",                    \
                                 hash,                               \
                                 REDIS_KEY,                          \
                                 item->valuestring);                 \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                               \
    do                                                                \
    {                                                                 \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);      \
                                                                      \
        if (cJSON_IsNumber(item))                                     \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %d",                     \
                                 hash, REDIS_KEY, item->valueint);    \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
        else if (cJSON_IsString(item) && item->valuestring)           \
        {                                                             \
            int value;                                                \
                                                                      \
            if (!strcasecmp(item->valuestring, "YES"))                \
                value = 1;                                            \
            else if (!strcasecmp(item->valuestring, "NO"))            \
                value = 0;                                            \
            else                                                      \
                value = atoi(item->valuestring);                      \
                                                                      \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %d",                     \
                                 hash, REDIS_KEY, value);             \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
    } while (0)

    /* Parameters supported */
    UPDATE_STR("RIGHT_IP", "right_ip");
    UPDATE_STR("LEFT_ID", "left_id");
    UPDATE_STR("LEFT_SUBNET", "left_subnet");
    UPDATE_STR("RIGHT_ID", "right_id");
    UPDATE_STR("RIGHT_SUBNET", "right_subnet");
    UPDATE_STR("TUNNEL_NAME", "tunnel_name");

    /* Optional future parameters */
    UPDATE_STR("LEFT", "left");
    UPDATE_STR("LEFT_SRC_IP", "left_src_ip");
    UPDATE_STR("PRE_SHARED_KEY", "pre_shared_key");
    UPDATE_STR("KEYING_MODE", "keying_mode");
    UPDATE_STR("CONN_TYPE", "conn_type");
    UPDATE_STR("AUTO_MODE", "auto_mode");
    UPDATE_STR("DPD_ACTION", "dpd_action");
    UPDATE_STR("CLOSEACTION", "closeaction");
    UPDATE_STR("PHASE1_ENCRYPT", "phase1_encrpt");
    UPDATE_STR("PHASE1_AUTH", "phase1_authen");
    UPDATE_STR("PHASE1_DH", "phase1_dhgrp");
    UPDATE_STR("PHASE2_ENCRYPT", "phase2_encrpt");
    UPDATE_STR("PHASE2_AUTH", "phase2_authen");
    UPDATE_STR("PHASE2_DH", "phase2_dhgrp");

    UPDATE_INT("ENABLE_TUNNEL", "enable_tunnel");
    UPDATE_INT("PFS", "pfs");
    UPDATE_INT("NAT_TRAV", "nat_trav");
    UPDATE_INT("MOBIK_MODE", "mobik_mode");
    UPDATE_INT("AGGR_MODE", "aggr_mode");
    UPDATE_INT("FRAGMENTATION", "fragmentation");

    UPDATE_INT("DPD_DELAY", "dpd_delay");
    UPDATE_INT("DPD_TIMEOUT", "dpd_timeout");
    UPDATE_INT("IKELIFETIME", "ikelifetime");
    UPDATE_INT("KEY_LIFETIME", "key_life_time");
    UPDATE_INT("REKEY_MARGIN", "rekey_margin");

#undef UPDATE_STR
#undef UPDATE_INT

    reply = redisCommand(ctx, "SADD proc_restart ipsec_monitor.sh");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
                LOG_INFO("Restart requested for ipsec_monitor.sh");
            else
                LOG_INFO("Restart already pending for ipsec_monitor.sh");
        }
        freeReplyObject(reply);
    }

    return 0;
}


int set_ntp_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char field[64];
    int server = 0;

    if (!ctx || !data)
        return -1;

    /* NTP_SERVER is mandatory */
    item = cJSON_GetObjectItemCaseSensitive(data, "NTP_SERVER");

    if (cJSON_IsNumber(item))
    {
        server = item->valueint;
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        server = atoi(item->valuestring);
    }
    else
    {
        return -1;
    }

    if (server != 1 && server != 2)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_FMT)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
        if (cJSON_IsString(item) && item->valuestring)               \
        {                                                            \
            sprintf(field, REDIS_FMT, server);                       \
            reply = redisCommand(ctx,                                \
                                 "HSET ntp_cfg %s %s",               \
                                 field,                              \
                                 item->valuestring);                 \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_FMT)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
                                                                     \
        if (cJSON_IsNumber(item))                                    \
        {                                                            \
            sprintf(field, REDIS_FMT, server);                       \
            reply = redisCommand(ctx,                                \
                                 "HSET ntp_cfg %s %d",               \
                                 field,                              \
                                 item->valueint);                    \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
        else if (cJSON_IsString(item) && item->valuestring)          \
        {                                                            \
            int value;                                               \
                                                                     \
            if (!strcasecmp(item->valuestring, "YES"))               \
                value = 1;                                           \
            else if (!strcasecmp(item->valuestring, "NO"))           \
                value = 0;                                           \
            else                                                     \
                value = atoi(item->valuestring);                     \
                                                                     \
            sprintf(field, REDIS_FMT, server);                       \
            reply = redisCommand(ctx,                                \
                                 "HSET ntp_cfg %s %d",               \
                                 field,                              \
                                 value);                             \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

    /* Per-server parameters */
    UPDATE_STR("NTP_IP", "ntp%d_server_ip");
    UPDATE_INT("NTP_PORT", "ntp%d_port");

    /* Optional common parameters */
    UPDATE_INT("ENABLE_NTP", "enable_ntp%d");

    item = cJSON_GetObjectItemCaseSensitive(data, "SYNC_INTERVAL");
    if (item)
    {
        int value;

        if (cJSON_IsNumber(item))
            value = item->valueint;
        else if (cJSON_IsString(item) && item->valuestring)
        {
            if (!strcasecmp(item->valuestring, "YES"))
                value = 1;
            else if (!strcasecmp(item->valuestring, "NO"))
                value = 0;
            else
                value = atoi(item->valuestring);
        }
        else
            value = -1;

        if (value >= 0)
        {
            reply = redisCommand(ctx,"HSET ntp_cfg interval %d",value);
            if (reply)
                freeReplyObject(reply);
        }
    }

#undef UPDATE_STR
#undef UPDATE_INT

    reply = redisCommand(ctx, "SADD proc_restart ntp_time_sync.sh");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for ntp_time_sync.sh");
            }
            else
            {
                LOG_INFO("Restart already pending for ntp_time_sync.sh");
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}



int set_iec104_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;

    if (!ctx || !data)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
        if (cJSON_IsString(item) && item->valuestring)               \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET iec104_0_cfg %s %s",          \
                                 REDIS_KEY,                          \
                                 item->valuestring);                 \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
                                                                     \
        if (cJSON_IsNumber(item))                                    \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET iec104_0_cfg %s %d",          \
                                 REDIS_KEY,                          \
                                 item->valueint);                    \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
        else if (cJSON_IsString(item) && item->valuestring)          \
        {                                                            \
            int value;                                               \
                                                                     \
            if (!strcasecmp(item->valuestring, "YES"))               \
                value = 1;                                           \
            else if (!strcasecmp(item->valuestring, "NO"))           \
                value = 0;                                           \
            else                                                     \
                value = atoi(item->valuestring);                     \
                                                                     \
            reply = redisCommand(ctx,                                \
                                 "HSET iec104_0_cfg %s %d",          \
                                 REDIS_KEY,                          \
                                 value);                             \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

    /* Parameters from command */
    UPDATE_INT("ASDU_ADDRESS", "asdu_addr");
    UPDATE_INT("CYCLIC_INT", "cyclic_int");
    UPDATE_INT("DLMS_IOA_OFFSET", "ioa_offset");
    UPDATE_INT("MODBUS_IOA_OFFSET", "ioa_offset_modbus");
    UPDATE_INT("COMMANDS_IOA_OFFSET", "ioa_offset_command");

    /* Optional parameters for future use */
    UPDATE_INT("PORT", "port");
    UPDATE_INT("MAX_CONNECTIONS", "max_connections");
    UPDATE_INT("K", "k");
    UPDATE_INT("W", "w");
    UPDATE_INT("T0", "t0");
    UPDATE_INT("T1", "t1");
    UPDATE_INT("T2", "t2");
    UPDATE_INT("T3", "t3");
    UPDATE_INT("ENABLE_104", "enable_104");
    UPDATE_INT("ENABLE_TLS", "enable_tls");
    UPDATE_INT("MASTER0_ENABLE", "master_0_enabled");
    UPDATE_INT("MASTER1_ENABLE", "master_1_enabled");
    UPDATE_INT("ALLOWED_MASTER_CHECK", "allowed_master_check");

    UPDATE_STR("MASTER0_IP", "master_0_ip");
    UPDATE_STR("MASTER1_IP", "master_1_ip");
    UPDATE_STR("CA_CERTIFICATE", "ca_certificate");
    UPDATE_STR("SERVER_CERTIFICATE", "server_certificate");
    UPDATE_STR("SERVER_KEY", "server_key");
    UPDATE_STR("KEY_PASSWORD", "key_password");

#undef UPDATE_STR
#undef UPDATE_INT

    reply = redisCommand(ctx, "SADD proc_restart iec104_module");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for iec104_module");
            }
            else
            {
                LOG_INFO("Restart already pending for iec104_module");
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}



int set_iec101_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;

    if (!ctx || !data)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
        if (cJSON_IsString(item) && item->valuestring)               \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET iec101_0_cfg %s %s",          \
                                 REDIS_KEY,                          \
                                 item->valuestring);                 \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
                                                                     \
        if (cJSON_IsNumber(item))                                    \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET iec101_0_cfg %s %d",          \
                                 REDIS_KEY,                          \
                                 item->valueint);                    \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
        else if (cJSON_IsString(item) && item->valuestring)          \
        {                                                            \
            int value;                                               \
                                                                     \
            if (!strcasecmp(item->valuestring, "YES"))               \
                value = 1;                                           \
            else if (!strcasecmp(item->valuestring, "NO"))           \
                value = 0;                                           \
            else                                                     \
                value = atoi(item->valuestring);                     \
                                                                     \
            reply = redisCommand(ctx,                                \
                                 "HSET iec101_0_cfg %s %d",          \
                                 REDIS_KEY,                          \
                                 value);                             \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

    /* Parameters from your command */
    UPDATE_INT("ASDU_ADDRESS", "asdu_addr");
    UPDATE_INT("CYCLIC_INT", "cyclic_int");
    UPDATE_INT("DLMS_IOA_OFFSET", "ioa_offset");
    UPDATE_INT("MODBUS_IOA_OFFSET", "ioa_offset_modbus");
    UPDATE_INT("COMMANDS_IOA_OFFSET", "ioa_offset_command");

    /* Optional IEC101 parameters */
    UPDATE_INT("PORT", "port");
    UPDATE_INT("MAX_CONNECTIONS", "max_connections");
    UPDATE_INT("ENABLE_101", "enable_101");
    UPDATE_INT("ENABLE_TLS", "enable_tls");

    UPDATE_INT("LINK_ADDRESS", "link_addr");
    UPDATE_INT("LINK_ADDRESS_SIZE", "link_addr_size");
    UPDATE_INT("LINK_LAYER_TIMEOUT", "link_layer_timeout_ms");
    UPDATE_INT("LINK_LAYER_RETRIES", "link_layer_retries");
    UPDATE_INT("SINGLE_CHAR_ACK", "single_char_ack");
    UPDATE_INT("BALANCED_MODE", "balanced_mode");

    UPDATE_INT("MASTER0_ENABLE", "master_0_enabled");
    UPDATE_INT("MASTER1_ENABLE", "master_1_enabled");
    UPDATE_INT("ALLOWED_MASTER_CHECK", "allowed_master_check");

    UPDATE_STR("MASTER0_IP", "master_0_ip");
    UPDATE_STR("MASTER1_IP", "master_1_ip");

    UPDATE_STR("CA_CERTIFICATE", "ca_certificate");
    UPDATE_STR("SERVER_CERTIFICATE", "server_certificate");
    UPDATE_STR("SERVER_KEY", "server_key");
    UPDATE_STR("KEY_PASSWORD", "key_password");

#undef UPDATE_STR
#undef UPDATE_INT

    reply = redisCommand(ctx, "SADD proc_restart iec101_module");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for iec101_module");
            }
            else
            {
                LOG_INFO("Restart already pending for iec101_module");
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}



int set_ftp_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char field[64];
    int server = 0;

    if (!ctx || !data)
        return -1;

    /* FTP_SERVER is mandatory */
    item = cJSON_GetObjectItemCaseSensitive(data, "FTP_SERVER");

    if (cJSON_IsNumber(item))
    {
        server = item->valueint;
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        server = atoi(item->valuestring);
    }
    else
    {
        return -1;
    }

    if (server != 1 && server != 2)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_FMT)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
        if (cJSON_IsString(item) && item->valuestring)               \
        {                                                            \
            sprintf(field, REDIS_FMT, server);                       \
            reply = redisCommand(ctx,                                \
                                 "HSET ftp_cfg %s %s",               \
                                 field,                              \
                                 item->valuestring);                 \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_FMT)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
                                                                     \
        if (cJSON_IsNumber(item))                                    \
        {                                                            \
            sprintf(field, REDIS_FMT, server);                       \
            reply = redisCommand(ctx,                                \
                                 "HSET ftp_cfg %s %d",               \
                                 field,                              \
                                 item->valueint);                    \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
        else if (cJSON_IsString(item) && item->valuestring)          \
        {                                                            \
            int value;                                               \
                                                                     \
            if (!strcasecmp(item->valuestring, "YES"))               \
                value = 1;                                           \
            else if (!strcasecmp(item->valuestring, "NO"))           \
                value = 0;                                           \
            else                                                     \
                value = atoi(item->valuestring);                     \
                                                                     \
            sprintf(field, REDIS_FMT, server);                       \
            reply = redisCommand(ctx,                                \
                                 "HSET ftp_cfg %s %d",               \
                                 field,                              \
                                 value);                             \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

    UPDATE_STR("IP_ADDRESS", "ip_addr_%d");
    UPDATE_INT("PORT", "port_%d");
    UPDATE_STR("USERNAME", "username_%d");
    UPDATE_STR("PASSWORD", "password_%d");
    UPDATE_STR("REMOTE_DIRECTORY", "remote_directory_%d");
    UPDATE_INT("TIME_INTERVAL", "time_interval_%d");
    UPDATE_INT("ENABLE_SERVER", "server_enable_%d");

    /* Common FTP enable */
    item = cJSON_GetObjectItemCaseSensitive(data, "ENABLE_FTP");
    if (cJSON_IsNumber(item))
    {
        reply = redisCommand(ctx,
                             "HSET ftp_cfg ftp_enable %d",
                             item->valueint);
        if (reply)
            freeReplyObject(reply);
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        reply = redisCommand(ctx,
                             "HSET ftp_cfg ftp_enable %d",
                             atoi(item->valuestring));
        if (reply)
            freeReplyObject(reply);
    }

#undef UPDATE_STR
#undef UPDATE_INT

    const char *ftp_ser_sel = (server == 1) ? "ftp_pusher_1" : "ftp_pusher_2";

    reply = redisCommand(ctx, "SADD proc_restart %s", ftp_ser_sel);
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for %s", ftp_ser_sel);
            }
            else
            {
                LOG_INFO("Restart already pending for %s", ftp_ser_sel);
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}


int set_serial_port_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char hash[32];
    int port = 0;

    if (!ctx || !data)
        return -1;

    /* SERIAL_PORT is mandatory */
    item = cJSON_GetObjectItemCaseSensitive(data, "SERIAL_PORT");

    if (cJSON_IsNumber(item))
    {
        port = item->valueint;
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        port = atoi(item->valuestring);
    }
    else
    {
        return -1;
    }

    if (port != 1 && port != 2)
        return -1;

    sprintf(hash, "serial_port_%d_cfg", port - 1);

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                              \
    do                                                               \
    {                                                                \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);     \
                                                                     \
        if (cJSON_IsNumber(item))                                    \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET %s %s %d",                    \
                                 hash,                               \
                                 REDIS_KEY,                          \
                                 item->valueint);                    \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
        else if (cJSON_IsString(item) && item->valuestring)          \
        {                                                            \
            reply = redisCommand(ctx,                                \
                                 "HSET %s %s %d",                    \
                                 hash,                               \
                                 REDIS_KEY,                          \
                                 atoi(item->valuestring));           \
            if (reply)                                               \
                freeReplyObject(reply);                              \
        }                                                            \
    } while (0)

    UPDATE_INT("BAUD_RATE", "baudrate");
    UPDATE_INT("DATA_BITS", "databits");
    UPDATE_INT("STOP_BITS", "stopbits");

#undef UPDATE_INT

    /* PARITY */
    item = cJSON_GetObjectItemCaseSensitive(data, "PARITY");
    if (cJSON_IsString(item) && item->valuestring)
    {
        int parity = 0;

        if (!strcasecmp(item->valuestring, "none"))
            parity = 0;
        else if (!strcasecmp(item->valuestring, "odd"))
            parity = 1;
        else if (!strcasecmp(item->valuestring, "even"))
            parity = 2;
        else
            return -1;

        reply = redisCommand(ctx, "HSET %s parity %d", hash, parity);
        if (reply)
            freeReplyObject(reply);
    }

    return 0;
}


int set_modtcp_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char hash[32];
    char meter_name[64];
    char dev_name[64];

    if (!ctx || !data)
        return -1;

    item = cJSON_GetObjectItemCaseSensitive(data, "METER_NAME");
    if (!cJSON_IsString(item) || item->valuestring == NULL)
        return -1;

    strcpy(meter_name, item->valuestring);

    hash[0] = '\0';

    /* Find matching ModTCP device */
    for (int i = 0; i < 10; i++)
    {
        sprintf(hash, "modtcp_%d_cfg", i);

        if (!rhash_exists(ctx, hash))
            continue;

        if (rget_str(ctx, hash, "dev_name", dev_name, sizeof(dev_name)))
        {
            if (!strcmp(dev_name, meter_name))
                break;
        }

        hash[0] = '\0';
    }

    if (hash[0] == '\0')
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                               \
    do                                                                \
    {                                                                 \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);      \
        if (cJSON_IsString(item) && item->valuestring)                \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %s",                     \
                                 hash, REDIS_KEY, item->valuestring); \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                               \
    do                                                                \
    {                                                                 \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);      \
                                                                      \
        if (cJSON_IsNumber(item))                                     \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %d",                     \
                                 hash, REDIS_KEY, item->valueint);    \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
        else if (cJSON_IsString(item) && item->valuestring)           \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %d",                     \
                                 hash, REDIS_KEY,                     \
                                 atoi(item->valuestring));            \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
    } while (0)

    UPDATE_STR("IP_ADDRESS", "dev_ip");

    UPDATE_INT("PORT", "dev_port");
    UPDATE_INT("SLAVE_ID", "slave_id");
    UPDATE_INT("RETRIES", "retries");
    UPDATE_INT("POLL_SKIP_COUNT", "poll_faulty_cnt");
    UPDATE_INT("RESP_TIMEOUT", "resp_timeout");

#undef UPDATE_STR
#undef UPDATE_INT

    reply = redisCommand(ctx, "SADD proc_restart modtcp_master");
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for MODTCP_PROC");
            }
            else
            {
                LOG_INFO("Restart already pending for MODTCP_PROC");
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}


int set_modrtu_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char hash[64];
    char dev_name[64];
    char meter_name[64];
    int serial_port = 0;

    if (!ctx || !data)
        return -1;

    /* SERIAL_PORT */
    item = cJSON_GetObjectItemCaseSensitive(data, "SERIAL_PORT");

    if (cJSON_IsNumber(item))
    {
        serial_port = item->valueint;
    }
    else if (cJSON_IsString(item) && item->valuestring)
    {
        serial_port = atoi(item->valuestring);
    }
    else
    {
        return -1;
    }

    if (serial_port != 1 && serial_port != 2)
        return -1;

    /* METER_NAME */
    item = cJSON_GetObjectItemCaseSensitive(data, "METER_NAME");
    if (!cJSON_IsString(item) || !item->valuestring)
        return -1;

    strcpy(meter_name, item->valuestring);

    hash[0] = '\0';

    /* Search the selected serial port */
    for (int dev = 0; dev < MAX_RTU_DEVICES; dev++)
    {
        sprintf(hash, "modrtu_serial%d_%d_cfg", serial_port - 1, dev);

        if (!rhash_exists(ctx, hash))
            continue;

        if (rget_str(ctx, hash, "dev_name", dev_name, sizeof(dev_name)))
        {
            if (!strcmp(dev_name, meter_name))
                break;
        }

        hash[0] = '\0';
    }

    if (hash[0] == '\0')
        return -1;

#define UPDATE_INT(JSON_KEY, REDIS_KEY)                               \
    do                                                                \
    {                                                                 \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY);      \
                                                                      \
        if (cJSON_IsNumber(item))                                     \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %d",                     \
                                 hash, REDIS_KEY, item->valueint);    \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
        else if (cJSON_IsString(item) && item->valuestring)           \
        {                                                             \
            reply = redisCommand(ctx,                                 \
                                 "HSET %s %s %d",                     \
                                 hash,                                \
                                 REDIS_KEY,                           \
                                 atoi(item->valuestring));            \
            if (reply)                                                \
                freeReplyObject(reply);                               \
        }                                                             \
    } while (0)

    UPDATE_INT("SLAVE_ID", "slave_id");
    UPDATE_INT("RETRIES", "retries");
    UPDATE_INT("POLL_SKIP_COUNT", "poll_faulty_cnt");
    UPDATE_INT("RESP_TIMEOUT", "resp_timeout");

#undef UPDATE_INT

    const char *mod_serial = (serial_port == 1) ? "modrtu_master 0"
                                                : "modrtu_master 1";

    reply = redisCommand(ctx, "SADD proc_restart %s", mod_serial);
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for %s", mod_serial);
            }
            else
            {
                LOG_INFO("Restart already pending for %s", mod_serial);
            }
        }
        freeReplyObject(reply);
    }

    return 0;
}


int find_meter(redisContext *redis,
               const char *incoming_meter_name,
               const char *hash_name,
               int *meter_id)
{
    redisReply *reply = redisCommand(redis, "HGETALL %s", hash_name);

    if (!reply || reply->type != REDIS_REPLY_ARRAY)
    {
        if (reply)
            freeReplyObject(reply);
        return -1;
    }

    for (size_t i = 0; i < reply->elements; i += 2)
    {

        char *value = reply->element[i + 1]->str;

        if (strcmp(value, incoming_meter_name) == 0)
        {
            char *field = reply->element[i]->str;

            if (sscanf(field, "meter_loc[%d]", meter_id) == 1)
            {
                freeReplyObject(reply);
                return 0;
            }
        }
    }

    freeReplyObject(reply);
    return -1;
}

int set_dlms_serial_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char meter_name[64];
    int serial_port;
    char meter_address[64];
    int meter_id;

    if (!ctx || !data)
        return -1;

    /* SERIAL_PORT */
    item = cJSON_GetObjectItemCaseSensitive(data, "SERIAL_PORT");
    if (!cJSON_IsNumber(item))
        return -1;

    serial_port = item->valueint;

    if (serial_port != 0 && serial_port != 1)
    {
        LOG_INFO("Invalid serial port %d", serial_port);
        return -1;
    }

    /* METER_NAME */
    item = cJSON_GetObjectItemCaseSensitive(data, "METER_NAME");
    if (!cJSON_IsString(item) || !item->valuestring)
        return -1;

    snprintf(meter_name, sizeof(meter_name), "%s", item->valuestring);

    item = cJSON_GetObjectItemCaseSensitive(data, "METER_ADDRESS");

    if (cJSON_IsNumber(item))
    {
        snprintf(meter_address, sizeof(meter_address), "%d",
                 item->valueint);
    }
    else if (cJSON_IsString(item))
    {
        snprintf(meter_address, sizeof(meter_address), "%s",
                 item->valuestring);
    }
    else
    {
        return -1;
    }

    char hash_name[64];
    snprintf(hash_name, sizeof(hash_name),
             "serial_port_%d_cfg", serial_port);

    if (find_meter(ctx, meter_name, hash_name, &meter_id) != 0)
    {
        LOG_INFO("Meter not found");
        return -1;
    }

    char key[64];
    snprintf(key, sizeof(key),
             "meter_addr[%d]", meter_id);

    reply = redisCommand(ctx, "HSET %s %s %s", hash_name, key, meter_address);

    if (!reply)
    {
        LOG_ERROR("Redis HSET failed");
        return -1;
    }

    freeReplyObject(reply);

    const char *dlms_serial = (serial_port == 1) ? "SerDaProc 0" : "SerDaProc 1";
    reply = redisCommand(ctx, "SADD proc_restart %s", dlms_serial);
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for %s", dlms_serial);
            }
            else
            {
                LOG_INFO("Restart already pending for %s", dlms_serial);
            }
        }
        freeReplyObject(reply);
    }
    return 0;
}

int set_dlms_ethernet_cfg(redisContext *ctx, cJSON *data)
{

    redisReply *reply;
    cJSON *item;
    char meter_name[64];
    char ip_address[64];
    int meter_id;

    if (!ctx || !data)
        return -1;

    /* METER_NAME */
    item = cJSON_GetObjectItemCaseSensitive(data, "METER_NAME");
    if (!cJSON_IsString(item) || !item->valuestring)
        return -1;

    snprintf(meter_name, sizeof(meter_name), "%s", item->valuestring);

    item = cJSON_GetObjectItemCaseSensitive(data, "IP_ADDR");

    if (cJSON_IsString(item))
    {
        snprintf(ip_address, sizeof(ip_address), "%s",
                 item->valuestring);
    }
    else
    {
        return -1;
    }

    char hash_name[64];
    snprintf(hash_name, sizeof(hash_name),
             "ethernet_meter_cfg");

    if (find_meter(ctx, meter_name, hash_name, &meter_id) != 0)
    {
        LOG_INFO("Meter not found");
        return -1;
    }

    char key[64];
    snprintf(key, sizeof(key),
             "ip_addr[%d]", meter_id);

    reply = redisCommand(ctx, "HSET %s %s %s", hash_name, key, ip_address);

    if (!reply)
    {
        LOG_ERROR("Redis HSET failed");
        return -1;
    }

    freeReplyObject(reply);

    char eth_proc[64];
    snprintf(eth_proc, sizeof(eth_proc), "ethDaProc %d", meter_id);

    reply = redisCommand(ctx, "SADD proc_restart %s", eth_proc);
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
            {
                LOG_INFO("Restart requested for %s", eth_proc);
            }
            else
            {
                LOG_INFO("Restart already pending for %s", eth_proc);
            }
        }
        freeReplyObject(reply);
    }
    return 0;
}

int set_cfg_export_json(redisContext *ctx, cmd_request_t *cmd)
{
    if (!strcmp(cmd->data_type_req, "MQTT_BROKER_PRIMARY"))
        return set_mqtt_cfg(ctx, cmd->data, 1);

    if (!strcmp(cmd->data_type_req, "MQTT_BROKER_SECONDARY"))
        return set_mqtt_cfg(ctx, cmd->data, 0);

    if (!strcmp(cmd->data_type_req, "MODEM"))
        return set_modem_cfg(ctx, cmd->data);

    if (!strcmp(cmd->data_type_req, "IPSEC"))
        return set_ipsec_cfg(ctx, cmd->data); // eNABLE is enabled

    if (!strcmp(cmd->data_type_req, "MODBUS_TCP"))
        return set_modtcp_cfg(ctx, cmd->data);

    if (!strcmp(cmd->data_type_req, "MODBUS_RTU"))
        return set_modrtu_cfg(ctx, cmd->data);

    if (!strcmp(cmd->data_type_req, "IEC104"))
        return set_iec104_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcmp(cmd->data_type_req, "SERPORT"))
        return set_serial_port_cfg(ctx, cmd->data);

    if (!strcmp(cmd->data_type_req, "IEC101"))
        return set_iec101_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcmp(cmd->data_type_req, "FTP"))
        return set_ftp_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcmp(cmd->data_type_req, "NTP"))
        return set_ntp_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcmp(cmd->data_type_req, "DLMS_SERIAL"))
        return set_dlms_serial_cfg(ctx, cmd->data);

    if (!strcmp(cmd->data_type_req, "DLMS_ETHERNET"))
        return set_dlms_ethernet_cfg(ctx, cmd->data);

    return -1;
}