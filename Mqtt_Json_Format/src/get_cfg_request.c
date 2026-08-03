#include "get_set_cfg.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include "json_helper.h"

/* Uncomment to send ALL values as JSON strings */
#define MQTT_JSON_ALL_STRING

static int export_one_mqtt_cfg(jbuf_t *jb, redisContext *ctx, int instance, int is_last)
{
    char hash[32];
    char buf[256];

    sprintf(hash, "mqtt_%d_cfg", instance);

    jbuf_append(jb, "{");

#ifdef MQTT_JSON_ALL_STRING
    snprintf(buf, sizeof(buf), "%d", instance);
    jbuf_append(jb, "\"INSTANCE\":");
    jbuf_append_escaped(jb, buf);
#else
    jbuf_append(jb, "\"INSTANCE\":%d", instance);
#endif

#define ADD_STR(redis_field, json_field)                     \
    if (rget_str(ctx, hash, redis_field, buf, sizeof(buf)))  \
    {                                                        \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb, buf);                        \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                     \
    {                                                        \
        snprintf(buf, sizeof(buf), "%d",                     \
                 rget_int(ctx, hash, redis_field, 0));       \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb, buf);                        \
    }

#define ADD_BOOL(redis_field, json_field)                    \
    {                                                        \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb,                              \
            rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO"); \
    }

#else

#define ADD_INT(redis_field, json_field)                     \
    {                                                        \
        int v = rget_int(ctx, hash, redis_field, 0);         \
        jbuf_append(jb, ",\"" json_field "\":%d", v);        \
    }

#define ADD_BOOL(redis_field, json_field)                    \
    {                                                        \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb,                              \
            rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO"); \
    }

#endif

    /* Boolean fields */
    ADD_BOOL("primary",                 "PRIMARY");
    ADD_BOOL("enable_mqtt",             "ENABLE_MQTT");

    /* Broker */
    ADD_STR ("broker_ip_url",           "BROKER_IP_URL");
    ADD_INT ("broker_port",             "BROKER_PORT");

    /* Client */
    ADD_STR ("client_id",               "CLIENT_ID");
    ADD_STR ("username",                "USERNAME");
    ADD_STR ("password",                "PASSWORD");

    /* SSL */
    ADD_BOOL("enable_ssl",              "ENABLE_SSL");
    ADD_BOOL("encrypted_key",           "ENCRYPTED_KEY");
    ADD_BOOL("insecure",                "INSECURE");

    ADD_STR ("ca_certificate",          "CA_CERTIFICATE");
    ADD_STR ("client_certificate",      "CLIENT_CERTIFICATE");
    ADD_STR ("client_key",              "CLIENT_KEY");
    ADD_STR ("key_password",            "KEY_PASSWORD");

    /* MQTT */
    ADD_INT ("qos",                     "QOS");
    ADD_BOOL("clean_session",           "CLEAN_SESSION");
    ADD_INT ("keep_alive_interval",     "KEEP_ALIVE_INTERVAL");

    /* Topics */
    ADD_STR ("cmd_req_topic",           "CMD_REQ_TOPIC");
    ADD_STR ("cmd_resp_pub_topic",      "CMD_RESP_TOPIC");

    ADD_STR ("dlms_data_pub_topic",     "DLMS_DATA_PUB_TOPIC");
    ADD_INT ("dlms_data_pub_interval",  "DLMS_DATA_PUB_INTERVAL");

    ADD_STR ("dlms_inst_pub_topic",     "DLMS_INST_PUB_TOPIC");
    ADD_INT ("dlms_inst_pub_interval",  "DLMS_INST_PUB_INTERVAL");

    ADD_STR ("modbus_data_pub_topic",   "MODBUS_DATA_PUB_TOPIC");
    ADD_INT ("modbus_data_pub_interval","MODBUS_DATA_PUB_INTERVAL");

    ADD_STR ("hc_pub_topic",            "HC_PUB_TOPIC");
    ADD_INT ("hc_pub_interval",         "HC_PUB_INTERVAL");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    jbuf_append(jb, "}");

    if (!is_last)
        jbuf_append(jb, ",");

    return 1;
}

char *export_mqtt_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");
    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"MQTT_BROKER\",");

    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"MQTT_BROKERS\":[");

    int total = 0;
    for (int i = 0; i < 2; i++)
    {
        char hash[32];
        sprintf(hash, "mqtt_%d_cfg", i);
        if (rhash_exists(ctx, hash))
            total++;
    }

    int exported = 0;

    for (int i = 0; i < 2; i++)
    {
        char hash[32];
        sprintf(hash, "mqtt_%d_cfg", i);
        if (!rhash_exists(ctx, hash))
            continue;
        exported++;
        export_one_mqtt_cfg(&jb,ctx,i+1,exported == total);
    }

    jbuf_append(&jb, "]");
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}


static void export_modem_data(jbuf_t *jb, redisContext *ctx)
{
    char buf[256];

    jbuf_append(jb, "\"MODEM\":{");

#define ADD_STR(redis_field, json_field)                         \
    if (rget_str(ctx, "modem_cfg", redis_field, buf, sizeof(buf))) \
    {                                                            \
        jbuf_append(jb, "\"" json_field "\":");                  \
        jbuf_append_escaped(jb, buf);                            \
        jbuf_append(jb, ",");                                    \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                         \
    {                                                            \
        snprintf(buf, sizeof(buf), "%d",                         \
                 rget_int(ctx, "modem_cfg", redis_field, 0));    \
        jbuf_append(jb, "\"" json_field "\":");                  \
        jbuf_append_escaped(jb, buf);                            \
        jbuf_append(jb, ",");                                    \
    }

#define ADD_BOOL(redis_field, json_field)                        \
    {                                                            \
        jbuf_append(jb, "\"" json_field "\":");                  \
        jbuf_append_escaped(jb,                                  \
            rget_int(ctx, "modem_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(jb, ",");                                    \
    }

#else

#define ADD_INT(redis_field, json_field)                         \
    {                                                            \
        int v = rget_int(ctx, "modem_cfg", redis_field, 0);      \
        jbuf_append(jb, "\"" json_field "\":%d,", v);            \
    }

#define ADD_BOOL(redis_field, json_field)                        \
    {                                                            \
        jbuf_append(jb, "\"" json_field "\":");                  \
        jbuf_append_escaped(jb,                                  \
            rget_int(ctx, "modem_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(jb, ",");                                    \
    }

#endif

    /* Global settings */
    ADD_BOOL("enable_gprs", "ENABLE_GPRS");
    ADD_INT ("num_sims",    "NUM_SIMS");
    ADD_STR ("sim_sel",     "SIM_SELECTION");

    /* SIM1 */
    jbuf_append(jb, "\"SIM1\":{");

    ADD_STR("apn1",      "APN");
    ADD_STR("username1", "USERNAME");
    ADD_STR("password1", "PASSWORD");

    if (rget_str(ctx, "modem_cfg", "phone_num1", buf, sizeof(buf)))
    {
        jbuf_append(jb, "\"PHONE_NUMBER\":");
        jbuf_append_escaped(jb, buf);
    }

    jbuf_append(jb, "},");

    /* SIM2 */
    jbuf_append(jb, "\"SIM2\":{");

    ADD_STR("apn2",      "APN");
    ADD_STR("username2", "USERNAME");
    ADD_STR("password2", "PASSWORD");

    if (rget_str(ctx, "modem_cfg", "phone_num2", buf, sizeof(buf)))
    {
        jbuf_append(jb, "\"PHONE_NUMBER\":");
        jbuf_append_escaped(jb, buf);
    }

    jbuf_append(jb, "}");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    jbuf_append(jb, "}");
}

char *export_modem_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;

    if (jbuf_init(&jb) < 0)
        return NULL;

    /* Header */
    jbuf_append(&jb, "{");

    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");

    jbuf_append(&jb, "\"DATA_TYPE\":\"MODEM\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");

    export_modem_data(&jb, ctx);

    jbuf_append(&jb, "}");   /* DATA */

    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}

static int export_one_ipsec_cfg(jbuf_t *jb, redisContext *ctx, int instance, int is_last)
{
    char hash[32];
    char buf[256];

    sprintf(hash, "ipsec_%d_cfg", instance);

    jbuf_append(jb, "{");

#ifdef MQTT_JSON_ALL_STRING
    snprintf(buf, sizeof(buf), "%d", instance);
    jbuf_append(jb, "\"INSTANCE\":");
    jbuf_append_escaped(jb, buf);
#else
    jbuf_append(jb, "\"INSTANCE\":%d", instance);
#endif

#define ADD_STR(redis_field, json_field)                     \
    if (rget_str(ctx, hash, redis_field, buf, sizeof(buf)))  \
    {                                                        \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb, buf);                        \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                     \
    {                                                        \
        snprintf(buf, sizeof(buf), "%d",                     \
                 rget_int(ctx, hash, redis_field, 0));       \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb, buf);                        \
    }

#define ADD_BOOL(redis_field, json_field)                    \
    {                                                        \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb,                              \
            rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO"); \
    }

#else

#define ADD_INT(redis_field, json_field)                     \
    {                                                        \
        int v = rget_int(ctx, hash, redis_field, 0);         \
        jbuf_append(jb, ",\"" json_field "\":%d", v);        \
    }

#define ADD_BOOL(redis_field, json_field)                    \
    {                                                        \
        jbuf_append(jb, ",\"" json_field "\":");             \
        jbuf_append_escaped(jb,                              \
            rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO"); \
    }

#endif

    /* General */
    ADD_BOOL("enable_tunnel",    "ENABLE_TUNNEL");
    ADD_STR ("tunnel_name",      "TUNNEL_NAME");

    /* Local */
    ADD_STR ("left",             "LEFT");
    ADD_STR ("left_id",          "LEFT_ID");
    ADD_STR ("left_src_ip",      "LEFT_SRC_IP");
    ADD_STR ("left_subnet",      "LEFT_SUBNET");

    /* Remote */
    ADD_STR ("right_ip",         "RIGHT_IP");
    ADD_STR ("right_id",         "RIGHT_ID");
    ADD_STR ("right_subnet",     "RIGHT_SUBNET");

    /* Connection */
    ADD_STR ("conn_type",        "CONN_TYPE");
    ADD_STR ("keying_mode",      "KEYING_MODE");
    ADD_STR ("pre_shared_key",   "PRE_SHARED_KEY");

    /* Phase 1 */
    ADD_STR ("phase1_encrpt",    "PHASE1_ENCRYPT");
    ADD_STR ("phase1_authen",    "PHASE1_AUTH");
    ADD_STR ("phase1_dhgrp",     "PHASE1_DH_GROUP");

    /* Phase 2 */
    ADD_STR ("phase2_encrpt",    "PHASE2_ENCRYPT");
    ADD_STR ("phase2_authen",    "PHASE2_AUTH");
    ADD_STR ("phase2_dhgrp",     "PHASE2_DH_GROUP");

    /* Timers */
    ADD_INT ("ikelifetime",      "IKE_LIFETIME");
    ADD_INT ("key_life_time",    "KEY_LIFETIME");

    /* Options */
    ADD_BOOL("pfs",              "PFS");
    ADD_BOOL("nat_trav",         "NAT_TRAV");
    ADD_BOOL("fragmentation",    "FRAGMENTATION");
    ADD_BOOL("mobik_mode",       "MOBIKE");
    ADD_BOOL("aggr_mode",        "AGGRESSIVE_MODE");

    ADD_STR ("auto_mode",        "AUTO_MODE");
    ADD_STR ("closeaction",      "CLOSE_ACTION");

    ADD_INT ("dpd_delay",        "DPD_DELAY");
    ADD_INT ("dpd_timeout",      "DPD_TIMEOUT");

    ADD_STR ("dpd_action",       "DPD_ACTION");

    ADD_INT ("rekey_margin",     "REKEY_MARGIN");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    jbuf_append(jb, "}");

    if (!is_last)
        jbuf_append(jb, ",");

    return 1;
}

char *export_ipsec_cfg(redisContext *ctx,cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    if (jbuf_init(&jb) < 0)
        return NULL;
    jbuf_append(&jb, "{");
    jbuf_append(&jb,
        "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb,
        "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb,
        "\"DATA_TYPE\":\"IPSEC\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"IPSEC_TUNNELS\":[");
    int total = 0;
    for (int i = 0; i < 2; i++)
    {
        char hash[32];
        sprintf(hash, "ipsec_%d_cfg", i);
        if (rhash_exists(ctx, hash))
            total++;
    }

    int exported = 0;
    for (int i = 0; i < 2; i++)
    {
        char hash[32];
        sprintf(hash, "ipsec_%d_cfg", i);
        if (!rhash_exists(ctx, hash))
            continue;
        exported++;
        export_one_ipsec_cfg(&jb,ctx,i+1,exported == total);
    }

    jbuf_append(&jb, "]");
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}

char *export_iec104_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    char buf[256];

    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");

    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"IEC104\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"IEC104\":{");

#define ADD_STR(redis_field, json_field)                        \
    if (rget_str(ctx, "iec104_0_cfg", redis_field, buf, sizeof(buf))) \
    {                                                           \
        jbuf_append(&jb, "\"" json_field "\":");                \
        jbuf_append_escaped(&jb, buf);                          \
        jbuf_append(&jb, ",");                                  \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                        \
    {                                                           \
        snprintf(buf, sizeof(buf), "%d",                        \
                 rget_int(ctx, "iec104_0_cfg", redis_field, 0));\
        jbuf_append(&jb, "\"" json_field "\":");                \
        jbuf_append_escaped(&jb, buf);                          \
        jbuf_append(&jb, ",");                                  \
    }

#define ADD_BOOL(redis_field, json_field)                       \
    {                                                           \
        jbuf_append(&jb, "\"" json_field "\":");                \
        jbuf_append_escaped(&jb,                                \
            rget_int(ctx, "iec104_0_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(&jb, ",");                                  \
    }

#else

#define ADD_INT(redis_field, json_field)                        \
    {                                                           \
        int v = rget_int(ctx, "iec104_0_cfg", redis_field, 0);  \
        jbuf_append(&jb, "\"" json_field "\":%d,", v);          \
    }

#define ADD_BOOL(redis_field, json_field)                       \
    {                                                           \
        jbuf_append(&jb, "\"" json_field "\":");                \
        jbuf_append_escaped(&jb,                                \
            rget_int(ctx, "iec104_0_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(&jb, ",");                                  \
    }

#endif

    ADD_BOOL("enable_104",             "ENABLE_104");

    ADD_INT ("port",                   "PORT");
    ADD_INT ("max_connections",        "MAX_CONNECTIONS");

    ADD_BOOL("allowed_master_check",   "ALLOWED_MASTER_CHECK");

    ADD_STR ("master_0_ip",            "MASTER_0_IP");
    ADD_BOOL("master_0_enabled",       "MASTER_0_ENABLED");

    ADD_STR ("master_1_ip",            "MASTER_1_IP");
    ADD_BOOL("master_1_enabled",       "MASTER_1_ENABLED");

    ADD_BOOL("enable_tls",             "ENABLE_TLS");

    ADD_STR ("ca_certificate",         "CA_CERTIFICATE");
    ADD_STR ("server_certificate",     "SERVER_CERTIFICATE");
    ADD_STR ("server_key",             "SERVER_KEY");
    ADD_STR ("key_password",           "KEY_PASSWORD");

    ADD_BOOL("encrypted_key",          "ENCRYPTED_KEY");

    ADD_INT ("asdu_addr",              "ASDU_ADDR");
    ADD_INT ("asdu_addr_size",         "ASDU_ADDR_SIZE");

    ADD_INT ("cot_addr_size",          "COT_ADDR_SIZE");

    ADD_INT ("ioa_addr_size",          "IOA_ADDR_SIZE");

    ADD_INT ("ioa_offset",             "IOA_OFFSET");
    ADD_INT ("ioa_offset_modbus",      "IOA_OFFSET_MODBUS");
    ADD_INT ("ioa_offset_command",     "IOA_OFFSET_COMMAND");

    ADD_INT ("k",                      "K");
    ADD_INT ("w",                      "W");

    ADD_INT ("t0",                     "T0");
    ADD_INT ("t1",                     "T1");
    ADD_INT ("t2",                     "T2");
    ADD_INT ("t3",                     "T3");

    ADD_INT ("cyclic_int",             "CYCLIC_INTERVAL");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    /* Remove last comma */
    if (jb.len && jb.data[jb.len - 1] == ',')
        jb.data[--jb.len] = '\0';

    jbuf_append(&jb, "}");   /* IEC104 */
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}

char *export_iec101_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    char buf[256];

    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");

    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"IEC101\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"IEC101\":{");

#define ADD_STR(redis_field, json_field)                           \
    if (rget_str(ctx, "iec101_0_cfg", redis_field, buf, sizeof(buf))) \
    {                                                              \
        jbuf_append(&jb, "\"" json_field "\":");                   \
        jbuf_append_escaped(&jb, buf);                             \
        jbuf_append(&jb, ",");                                     \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                           \
    {                                                              \
        snprintf(buf, sizeof(buf), "%d",                           \
                 rget_int(ctx, "iec101_0_cfg", redis_field, 0));   \
        jbuf_append(&jb, "\"" json_field "\":");                   \
        jbuf_append_escaped(&jb, buf);                             \
        jbuf_append(&jb, ",");                                     \
    }

#define ADD_BOOL(redis_field, json_field)                          \
    {                                                              \
        jbuf_append(&jb, "\"" json_field "\":");                   \
        jbuf_append_escaped(&jb,                                   \
            rget_int(ctx, "iec101_0_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(&jb, ",");                                     \
    }

#else

#define ADD_INT(redis_field, json_field)                           \
    {                                                              \
        int v = rget_int(ctx, "iec101_0_cfg", redis_field, 0);     \
        jbuf_append(&jb, "\"" json_field "\":%d,", v);             \
    }

#define ADD_BOOL(redis_field, json_field)                          \
    {                                                              \
        jbuf_append(&jb, "\"" json_field "\":");                   \
        jbuf_append_escaped(&jb,                                   \
            rget_int(ctx, "iec101_0_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(&jb, ",");                                     \
    }

#endif

    /* General */
    ADD_BOOL("enable_101",            "ENABLE_101");

    ADD_INT ("port",                  "PORT");
    ADD_INT ("max_connections",       "MAX_CONNECTIONS");

    ADD_BOOL("allowed_master_check",  "ALLOWED_MASTER_CHECK");

    ADD_STR ("master_0_ip",           "MASTER_0_IP");
    ADD_BOOL("master_0_enabled",      "MASTER_0_ENABLED");

    ADD_STR ("master_1_ip",           "MASTER_1_IP");
    ADD_BOOL("master_1_enabled",      "MASTER_1_ENABLED");

    ADD_BOOL("enable_tls",            "ENABLE_TLS");

    ADD_STR ("ca_certificate",        "CA_CERTIFICATE");
    ADD_STR ("server_certificate",    "SERVER_CERTIFICATE");
    ADD_STR ("server_key",            "SERVER_KEY");
    ADD_STR ("key_password",          "KEY_PASSWORD");

    ADD_BOOL("encrypted_key",         "ENCRYPTED_KEY");

    ADD_INT ("asdu_addr",             "ASDU_ADDR");
    ADD_INT ("asdu_addr_size",        "ASDU_ADDR_SIZE");
    ADD_INT ("cot_addr_size",         "COT_ADDR_SIZE");
    ADD_INT ("ioa_addr_size",         "IOA_ADDR_SIZE");
    ADD_INT ("ioa_offset",            "IOA_OFFSET");
    ADD_INT ("ioa_offset_modbus",     "IOA_OFFSET_MODBUS");
    ADD_INT ("ioa_offset_command",    "IOA_OFFSET_COMMAND");

    /* IEC101 Specific */
    ADD_INT ("link_addr",             "LINK_ADDR");
    ADD_INT ("link_addr_size",        "LINK_ADDR_SIZE");

    ADD_BOOL("balanced_mode",         "BALANCED_MODE");
    ADD_BOOL("single_char_ack",       "SINGLE_CHAR_ACK");

    ADD_INT ("link_layer_timeout_ms", "LINK_LAYER_TIMEOUT_MS");
    ADD_INT ("link_layer_retries",    "LINK_LAYER_RETRIES");
    ADD_INT ("cyclic_int",            "CYCLIC_INTERVAL");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    /* Remove trailing comma */
    if (jb.len && jb.data[jb.len - 1] == ',')
        jb.data[--jb.len] = '\0';

    jbuf_append(&jb, "}");   /* IEC101 */
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}

char *export_ftp_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    char buf[256];

    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");

    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"FTP\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"FTP\":{");

#ifdef MQTT_JSON_ALL_STRING

    jbuf_append(&jb, "\"FTP_ENABLE\":");
    jbuf_append_escaped(&jb,
        rget_int(ctx, "ftp_cfg", "ftp_enable", 0) ? "YES" : "NO");
    jbuf_append(&jb, ",");

#else

    jbuf_append(&jb, "\"FTP_ENABLE\":");
    jbuf_append_escaped(&jb,
        rget_int(ctx, "ftp_cfg", "ftp_enable", 0) ? "YES" : "NO");
    jbuf_append(&jb, ",");

#endif

    jbuf_append(&jb, "\"SERVERS\":[");

    for (int i = 1; i <= 2; i++)
    {
        char field[64];

        jbuf_append(&jb, "{");

#ifdef MQTT_JSON_ALL_STRING
        snprintf(buf, sizeof(buf), "%d", i);
        jbuf_append(&jb, "\"INSTANCE\":");
        jbuf_append_escaped(&jb, buf);
#else
        jbuf_append(&jb, "\"INSTANCE\":%d,", i);
#endif

#ifdef MQTT_JSON_ALL_STRING
        sprintf(field, "server_enable_%d", i);
        jbuf_append(&jb, ",\"SERVER_ENABLE\":");
        jbuf_append_escaped(&jb,
            rget_int(ctx, "ftp_cfg", field, 0) ? "YES" : "NO");
#else
        sprintf(field, "server_enable_%d", i);
        jbuf_append(&jb, "\"SERVER_ENABLE\":");
        jbuf_append_escaped(&jb,
            rget_int(ctx, "ftp_cfg", field, 0) ? "YES" : "NO");
#endif

        sprintf(field, "ip_addr_%d", i);
        rget_str(ctx, "ftp_cfg", field, buf, sizeof(buf));
        jbuf_append(&jb, ",\"IP_ADDRESS\":");
        jbuf_append_escaped(&jb, buf);

        sprintf(field, "port_%d", i);

#ifdef MQTT_JSON_ALL_STRING
        snprintf(buf, sizeof(buf), "%d",
                 rget_int(ctx, "ftp_cfg", field, 21));
        jbuf_append(&jb, ",\"PORT\":");
        jbuf_append_escaped(&jb, buf);
#else
        jbuf_append(&jb, ",\"PORT\":%d",
                    rget_int(ctx, "ftp_cfg", field, 21));
#endif

        sprintf(field, "username_%d", i);
        rget_str(ctx, "ftp_cfg", field, buf, sizeof(buf));
        jbuf_append(&jb, ",\"USERNAME\":");
        jbuf_append_escaped(&jb, buf);

        sprintf(field, "password_%d", i);
        rget_str(ctx, "ftp_cfg", field, buf, sizeof(buf));
        jbuf_append(&jb, ",\"PASSWORD\":");
        jbuf_append_escaped(&jb, buf);

        sprintf(field, "remote_directory_%d", i);
        rget_str(ctx, "ftp_cfg", field, buf, sizeof(buf));
        jbuf_append(&jb, ",\"REMOTE_DIRECTORY\":");
        jbuf_append_escaped(&jb, buf);

        sprintf(field, "time_interval_%d", i);

#ifdef MQTT_JSON_ALL_STRING
        snprintf(buf, sizeof(buf), "%d",
                 rget_int(ctx, "ftp_cfg", field, 0));
        jbuf_append(&jb, ",\"TIME_INTERVAL\":");
        jbuf_append_escaped(&jb, buf);
#else
        jbuf_append(&jb, ",\"TIME_INTERVAL\":%d",
                    rget_int(ctx, "ftp_cfg", field, 0));
#endif

        jbuf_append(&jb, "}");

        if (i != 2)
            jbuf_append(&jb, ",");
    }

    jbuf_append(&jb, "]");   /* SERVERS */
    jbuf_append(&jb, "}");   /* FTP */
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}

char *export_ntp_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    char buf[256];

    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");

    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"NTP\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"NTP\":{");

#define ADD_STR(redis_field, json_field)                             \
    if (rget_str(ctx, "ntp_cfg", redis_field, buf, sizeof(buf)))      \
    {                                                                 \
        jbuf_append(&jb, "\"" json_field "\":");                      \
        jbuf_append_escaped(&jb, buf);                                \
        jbuf_append(&jb, ",");                                        \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                             \
    {                                                                 \
        snprintf(buf, sizeof(buf), "%d",                              \
                 rget_int(ctx, "ntp_cfg", redis_field, 0));            \
        jbuf_append(&jb, "\"" json_field "\":");                      \
        jbuf_append_escaped(&jb, buf);                                \
        jbuf_append(&jb, ",");                                        \
    }

#define ADD_BOOL(redis_field, json_field)                            \
    {                                                                 \
        jbuf_append(&jb, "\"" json_field "\":");                      \
        jbuf_append_escaped(&jb,                                      \
            rget_int(ctx, "ntp_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(&jb, ",");                                        \
    }

#else

#define ADD_INT(redis_field, json_field)                             \
    {                                                                 \
        int v = rget_int(ctx, "ntp_cfg", redis_field, 0);             \
        jbuf_append(&jb, "\"" json_field "\":%d,", v);                \
    }

#define ADD_BOOL(redis_field, json_field)                            \
    {                                                                 \
        jbuf_append(&jb, "\"" json_field "\":");                      \
        jbuf_append_escaped(&jb,                                      \
            rget_int(ctx, "ntp_cfg", redis_field, 0) ? "YES" : "NO"); \
        jbuf_append(&jb, ",");                                        \
    }

#endif

    ADD_BOOL("enable_ntp1", "ENABLE_NTP1");
    ADD_BOOL("enable_ntp2", "ENABLE_NTP2");

    ADD_STR("ntp1_server_ip", "NTP1_SERVER_IP");
    ADD_INT("ntp1_port", "NTP1_PORT");

    ADD_STR("ntp2_server_ip", "NTP2_SERVER_IP");
    ADD_INT("ntp2_port", "NTP2_PORT");

    ADD_INT("interval", "SYNC_INTERVAL_HOURS");

    ADD_STR("ntp_sync_status", "SYNC_STATUS");
    ADD_STR("ntp_sync_time", "LAST_SYNC_TIME");
    ADD_STR("next_sync_time", "NEXT_SYNC_TIME");
    ADD_STR("synced_server", "SYNCED_SERVER");
    ADD_STR("failure_reason", "FAILURE_REASON");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    /* Remove trailing comma */
    if (jb.len && jb.data[jb.len - 1] == ',')
        jb.data[--jb.len] = '\0';

    jbuf_append(&jb, "}");   /* NTP */
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}

static void export_modtcp_register_cfgs(jbuf_t *jb,redisContext *ctx,int dev_id)
{
    char hash[64];
    char buf[128];

    /* Register parameter names */
    jbuf_append(jb,
    "\"REGISTER_FIELDS\":[\"REGISTER_NO\",\"NAME\",\"ADDRESS\",\"FUNCTION\",\"DATA_TYPE\",\"NUM_REGS\",\"BYTE_ORDER\",\"SCALE_FACTOR\",\"ZERO_VALUE\",\"MAP_PROTO\",\"MAP_PROTO_101\",\"REPORT_MODE\",\"TOL_MIN\",\"TOL_MAX\",\"TOL_PER\",\"SEND_SMS\"],");

    /* Register values */
    jbuf_append(jb, "\"REGISTERS\":[");

    for (int r = 0; r < MAX_REGS_PER_DEV; r++)
    {
        sprintf(hash, "modtcp_%d_reg_%d_cfg", dev_id, r);

        if (!rhash_exists(ctx, hash))
            continue;

        jbuf_append(jb, "[");
        /* REGISTER_NO */
        jbuf_append(jb, "%d,", r);

        /* NAME */
        rget_str(ctx, hash, "name_id", buf, sizeof(buf));
        jbuf_append_escaped(jb, buf);
        jbuf_append(jb, ",");
        /* ADDRESS */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "start_addr", 0));
        /* FUNCTION */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "func_type", 0));
        /* DATA_TYPE */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "format", 0));
        /* NUM_REGS */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "num_regs", 0));
        /* BYTE_ORDER */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "byte_order", 0));
        /* SCALE_FACTOR */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "scale_fact", 0));

        /* ZERO_VALUE */
        rget_str(ctx, hash, "zero_val", buf, sizeof(buf));
        jbuf_append_numeric_value(jb, buf);
        jbuf_append(jb, ",");
        /* MAP_PROTO */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "map_proto", 0));
        /* MAP_PROTO_101 */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "map_proto_101", 0));
        /* REPORT_MODE */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "rep_mode", 0));
        /* TOL_MIN */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "tol_min", 0));
        /* TOL_MAX */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "tol_max", 0));
        /* TOL_PER */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "tol_per", 0));
        /* SEND_SMS */
        jbuf_append(jb, "%d", rget_int(ctx, hash, "send_sms", 0));
        jbuf_append(jb, "]");

        if (r != MAX_REGS_PER_DEV - 1)
            jbuf_append(jb, ",");
    }

    /* Remove trailing comma if last registers don't exist */
    if (jb->len && jb->data[jb->len - 1] == ',')
        jb->data[--jb->len] = '\0';

    jbuf_append(jb, "]");
}

char *export_modtcp_cfg(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;
    char buf[256];

    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");

    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"MODBUS_TCP\",");
    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");

    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"MODBUS_TCP\":[");

    int total = 0;

    for (int i = 0; i < MAX_TCP_DEVICES; i++)
    {
        char hash[32];
        sprintf(hash, "modtcp_%d_cfg", i);

        if (rhash_exists(ctx, hash))
            total++;
    }

    int exported = 0;

    for (int i = 0; i < MAX_TCP_DEVICES; i++)
    {
        char hash[32];
        sprintf(hash, "modtcp_%d_cfg", i);

        if (!rhash_exists(ctx, hash))
            continue;

        exported++;

        jbuf_append(&jb, "{");

#define ADD_STR(redis_field, json_field)                               \
        if (rget_str(ctx, hash, redis_field, buf, sizeof(buf)))         \
        {                                                               \
            jbuf_append(&jb, "\"" json_field "\":");                    \
            jbuf_append_escaped(&jb, buf);                              \
            jbuf_append(&jb, ",");                                      \
        }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                               \
        {                                                               \
            snprintf(buf, sizeof(buf), "%d",                            \
                     rget_int(ctx, hash, redis_field, 0));              \
            jbuf_append(&jb, "\"" json_field "\":");                    \
            jbuf_append_escaped(&jb, buf);                              \
            jbuf_append(&jb, ",");                                      \
        }

#define ADD_BOOL(redis_field, json_field)                              \
        {                                                               \
            jbuf_append(&jb, "\"" json_field "\":");                    \
            jbuf_append_escaped(&jb,                                    \
                rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO");    \
            jbuf_append(&jb, ",");                                      \
        }

#else

#define ADD_INT(redis_field, json_field)                               \
        {                                                               \
            int v = rget_int(ctx, hash, redis_field, 0);                \
            jbuf_append(&jb, "\"" json_field "\":%d,", v);              \
        }

#define ADD_BOOL(redis_field, json_field)                              \
        {                                                               \
            jbuf_append(&jb, "\"" json_field "\":");                    \
            jbuf_append_escaped(&jb,                                    \
                rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO");    \
            jbuf_append(&jb, ",");                                      \
        }

#endif

        ADD_INT ("dev_id",            "DEVICE_ID");
        ADD_BOOL("enable_device",     "ENABLE_DEVICE");

        ADD_STR ("dev_name",          "DEVICE_NAME");
        ADD_STR ("loc_name",          "LOCATION");

        ADD_STR ("dev_ip",            "IP_ADDRESS");
        ADD_INT ("dev_port",          "PORT");

        ADD_INT ("slave_id",          "SLAVE_ID");

        ADD_INT ("num_points",        "NUM_POINTS");

        ADD_INT ("resp_timeout",      "RESPONSE_TIMEOUT");
        ADD_INT ("retries",           "RETRIES");

        ADD_INT ("poll_faulty_cnt",   "POLL_FAULTY_COUNT");
        ADD_INT ("inter_frame_delay", "INTER_FRAME_DELAY");

        ADD_STR ("meter_type",        "METER_TYPE");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

        /* remove last comma */
        if (jb.len && jb.data[jb.len - 1] == ',')
            jb.data[--jb.len] = '\0';

        /* Add comma because REGISTERS will follow */
        jbuf_append(&jb, ",");

        /* Export all register configurations for this device */
        export_modtcp_register_cfgs(&jb, ctx, i);

        /* Close this device object */
        jbuf_append(&jb, "}");

        if (exported != total)
            jbuf_append(&jb, ",");
    }

    jbuf_append(&jb, "]");   /* MODBUS_TCP */
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */

    return jb.data;
}


static void export_modrtu_register_cfgs(jbuf_t *jb,redisContext *ctx,int port,int dev)
{
    char hash[64];
    char buf[128];

    jbuf_append(jb,
        "\"REGISTER_FIELDS\":[\"REGISTER_NO\",\"NAME\",\"ADDRESS\",\"FUNCTION\",\"DATA_TYPE\",\"NUM_REGS\",\"BYTE_ORDER\",\"SCALE_FACTOR\",\"MAP_PROTO\",\"MAP_PROTO_101\",\"REPORT_MODE\",\"TOL_MIN\",\"TOL_MAX\",\"TOL_PER\"],");

    jbuf_append(jb, "\"REGISTERS\":[");

    int first = 1;

    for (int reg = 0; reg < MAX_REGS_PER_DEV; reg++)
    {
        sprintf(hash,"modrtu_serial%d_%d_reg_%d_cfg",port,dev,reg);
        if (!rhash_exists(ctx, hash))
            continue;
        if (!first)
            jbuf_append(jb, ",");

        first = 0;
        jbuf_append(jb, "[");
        /* REGISTER_NO */
        jbuf_append(jb, "%d,", reg);

        /* NAME */
        rget_str(ctx, hash, "name_id", buf, sizeof(buf));
        jbuf_append_escaped(jb, buf);
        jbuf_append(jb, ",");
        /* ADDRESS */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "start_addr", 0));
        /* FUNCTION */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "func_type", 0));
        /* DATA_TYPE */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "format", 0));
        /* NUM_REGS */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "num_regs", 0));
        /* BYTE_ORDER */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "byte_order", 0));
        /* SCALE_FACTOR */
        rget_str(ctx, hash, "scale_fact", buf, sizeof(buf));
        if (buf[0] == '\0')
            strcpy(buf, "1");
        jbuf_append(jb, "%s,", buf);
        /* MAP_PROTO */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "map_proto", 0));
        /* MAP_PROTO_101 */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "map_proto_101", 0));
        /* REPORT_MODE */
        jbuf_append(jb, "%d,", rget_int(ctx, hash, "rep_mode", 0));
        /* TOL_MIN */
        rget_str(ctx, hash, "tol_min", buf, sizeof(buf));
        if (buf[0] == '\0')
            strcpy(buf, "0");
        jbuf_append(jb, "%s,", buf);
        /* TOL_MAX */
        rget_str(ctx, hash, "tol_max", buf, sizeof(buf));
        if (buf[0] == '\0')
            strcpy(buf, "0");
        jbuf_append(jb, "%s,", buf);
        /* TOL_PER */
        rget_str(ctx, hash, "tol_per", buf, sizeof(buf));
        if (buf[0] == '\0')
            strcpy(buf, "0");
        jbuf_append(jb, "%s", buf);

        jbuf_append(jb, "]");
    }

    jbuf_append(jb, "]");
}


static int export_one_modrtu_cfg(jbuf_t *jb, redisContext *ctx, int port, int dev, int is_last)
{
    char hash[64];
    char buf[256];

    sprintf(hash, "modrtu_serial%d_%d_cfg", port, dev);

    if (!rhash_exists(ctx, hash))
        return 0;

    jbuf_append(jb, "{");

#define ADD_STR(redis_field, json_field)                              \
    if (rget_str(ctx, hash, redis_field, buf, sizeof(buf)))           \
    {                                                                 \
        jbuf_append(jb, "\"" json_field "\":");                       \
        jbuf_append_escaped(jb, buf);                                 \
        jbuf_append(jb, ",");                                         \
    }

#ifdef MQTT_JSON_ALL_STRING

#define ADD_INT(redis_field, json_field)                              \
    {                                                                 \
        snprintf(buf, sizeof(buf), "%d",                              \
                 rget_int(ctx, hash, redis_field, 0));                \
        jbuf_append(jb, "\"" json_field "\":");                       \
        jbuf_append_escaped(jb, buf);                                 \
        jbuf_append(jb, ",");                                         \
    }

#define ADD_BOOL(redis_field, json_field)                             \
    {                                                                 \
        jbuf_append(jb, "\"" json_field "\":");                       \
        jbuf_append_escaped(jb,                                       \
            rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO");      \
        jbuf_append(jb, ",");                                         \
    }

#else

#define ADD_INT(redis_field, json_field)                              \
    {                                                                 \
        int v = rget_int(ctx, hash, redis_field, 0);                  \
        jbuf_append(jb, "\"" json_field "\":%d,", v);                 \
    }

#define ADD_BOOL(redis_field, json_field)                             \
    {                                                                 \
        jbuf_append(jb, "\"" json_field "\":");                       \
        jbuf_append_escaped(jb,                                       \
            rget_int(ctx, hash, redis_field, 0) ? "YES" : "NO");      \
        jbuf_append(jb, ",");                                         \
    }

#endif

    ADD_INT ("dev_id",            "DEVICE_ID");
    ADD_BOOL("enable_device",     "ENABLE_DEVICE");

    ADD_STR ("dev_name",          "DEVICE_NAME");
    ADD_STR ("loc_name",          "LOCATION");

    ADD_INT ("slave_id",          "SLAVE_ID");
    ADD_INT ("num_points",        "NUM_POINTS");

    ADD_INT ("resp_timeout",      "RESPONSE_TIMEOUT");
    ADD_INT ("retries",           "RETRIES");

    ADD_INT ("poll_faulty_cnt",   "POLL_FAULTY_COUNT");
    ADD_INT ("inter_frame_delay", "INTER_FRAME_DELAY");

    ADD_STR ("meter_type",        "METER_TYPE");

#undef ADD_STR
#undef ADD_INT
#undef ADD_BOOL

    /* Remove trailing comma */
    if (jb->len && jb->data[jb->len - 1] == ',')
        jb->data[--jb->len] = '\0';

    jbuf_append(jb, ",");

    export_modrtu_register_cfgs(jb, ctx, port, dev);

    jbuf_append(jb, "}");

    if (!is_last)
        jbuf_append(jb, ",");

    return 1;
}

char *export_modrtu_cfg(redisContext *ctx,cmd_request_t *cmd)
{
    printf("Entering MODRTU...\n");
    if (!ctx || !cmd)
        return NULL;

    jbuf_t jb;

    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{");

    /* Header */
    jbuf_append(&jb, "\"TYPE\":\"command_reply\",");
    jbuf_append(&jb, "\"SEQ_NUM\":");
    jbuf_append_escaped(&jb, cmd->transaction);
    jbuf_append(&jb, ",");
    jbuf_append(&jb, "\"DATA_TYPE\":\"MODBUS_RTU\",");

    jbuf_append(&jb, "\"CMD_STATUS\":\"0\",");
    jbuf_append(&jb, "\"CMD_MESSAGE\":\"SUCCESS\",");
    /* DATA */
    jbuf_append(&jb, "\"DATA\":{");
    jbuf_append(&jb, "\"MODBUS_RTU\":[");
    printf("Upto Data...\n");
    int total = 0;

    /* Count all RTU devices only on Modbus ports */
    for (int port = 0; port < MAX_SERIAL_PORTS; port++)
    {
        char port_hash[64];
        sprintf(port_hash, "serial_port_%d_cfg", port);
        if (!rhash_exists(ctx, port_hash))
            continue;
        if (rget_int(ctx, port_hash, "device_type", 0) != 2)
            continue;

        for (int dev = 0; dev < MAX_RTU_DEVICES; dev++)
        {
            char hash[64];
            sprintf(hash,"modrtu_serial%d_%d_cfg",port,dev);
            if (rhash_exists(ctx, hash))
                total++;
        }
    }

    int exported = 0;

    /* Export devices */
    for (int port = 0; port < MAX_SERIAL_PORTS; port++)
    {
        char port_hash[64];
        sprintf(port_hash, "serial_port_%d_cfg", port);

        if (!rhash_exists(ctx, port_hash))
            continue;

        if (rget_int(ctx, port_hash, "device_type", 0) != 2)
            continue;

        for (int dev = 0; dev < MAX_RTU_DEVICES; dev++)
        {
            char hash[64];
            sprintf(hash,"modrtu_serial%d_%d_cfg",port,dev);
            if (!rhash_exists(ctx, hash))
                continue;

            exported++;
            export_one_modrtu_cfg(&jb,ctx,port,dev,exported == total);
        }
    }

    jbuf_append(&jb, "]");   /* MODBUS_RTU */
    jbuf_append(&jb, "}");   /* DATA */
    jbuf_append(&jb, "}");   /* ROOT */
    printf("All build completed..\n");
    printf("%s\n",jb.data);
    return jb.data;
}

char *get_cfg_export_json(redisContext *ctx, cmd_request_t *cmd)
{
    if (!ctx || !cmd)
        return NULL;

    if (!strcmp(cmd->data_type_req, "MQTT_BROKER"))
        return export_mqtt_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "MODEM"))
        return export_modem_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "IPSEC"))
        return export_ipsec_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "MODBUS_TCP"))
        return export_modtcp_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "MODBUS_RTU"))
        return export_modrtu_cfg(ctx, cmd);

    // if (!strcmp(cmd->data_type_req, "DLMS"))
    //     return export_dlms_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "IEC104"))
        return export_iec104_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "IEC101"))
        return export_iec101_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "FTP"))
        return export_ftp_cfg(ctx, cmd);

    if (!strcmp(cmd->data_type_req, "NTP"))
        return export_ntp_cfg(ctx, cmd);


    return NULL;
}

