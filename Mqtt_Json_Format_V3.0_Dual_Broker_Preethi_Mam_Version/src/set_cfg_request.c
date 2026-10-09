#include <ctype.h>
#include "get_set_cfg.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <stdarg.h>
#include <time.h>
#include "json_helper.h"

/*
 * json_get_int_value()
 * --------------------
 * Reads a DATA field the same way for every config: accepts a JSON number
 * (7) or a string ("7", " 7 ", "007", "7\r\n"); optionally "YES"/"NO" -> 1/0.
 * Returns  0 : *out holds the integer
 *         -1 : key not present
 *         -2 : present but not a valid integer
 */
#define JSON_INT_OK 0
#define JSON_INT_ABSENT (-1)
#define JSON_INT_INVALID (-2)

static int json_get_int_value(cJSON *data, const char *key, int allow_yes_no, int *out)
{
    cJSON *item = cJSON_GetObjectItemCaseSensitive(data, key);

    if (item == NULL || cJSON_IsNull(item))
        return JSON_INT_ABSENT;

    if (cJSON_IsNumber(item))
    {
        double d = item->valuedouble;

        if (d != (double)(int)d) /* 1.5 is not an integer */
            return JSON_INT_INVALID;

        *out = (int)d;
        return JSON_INT_OK;
    }

    if (cJSON_IsString(item) && item->valuestring)
    {
        char buf[64];
        char *start = item->valuestring;
        char *end;
        size_t len;
        long v;

        /* trim leading / trailing whitespace (spaces, tabs, \r, \n) */
        while (*start == ' ' || *start == '\t' || *start == '\r' || *start == '\n')
            start++;

        snprintf(buf, sizeof(buf), "%s", start);
        len = strlen(buf);
        while (len > 0 && (buf[len - 1] == ' ' || buf[len - 1] == '\t' ||
                           buf[len - 1] == '\r' || buf[len - 1] == '\n'))
            buf[--len] = '\0';

        if (len == 0)
            return JSON_INT_INVALID;

        if (allow_yes_no)
        {
            if (!strcasecmp(buf, "YES") || !strcasecmp(buf, "TRUE"))
            {
                *out = 1;
                return JSON_INT_OK;
            }
            if (!strcasecmp(buf, "NO") || !strcasecmp(buf, "FALSE"))
            {
                *out = 0;
                return JSON_INT_OK;
            }
        }

        v = strtol(buf, &end, 10);
        if (end == buf || *end != '\0') /* "abc", "7x" -> invalid */
            return JSON_INT_INVALID;

        *out = (int)v;
        return JSON_INT_OK;
    }

    if (cJSON_IsBool(item) && allow_yes_no)
    {
        *out = cJSON_IsTrue(item) ? 1 : 0;
        return JSON_INT_OK;
    }

    return JSON_INT_INVALID;
}

/* Is the string value (trimmed, any case) equal to 'word'? */
static int json_str_equals(cJSON *data, const char *key, const char *word)
{
    cJSON *item = cJSON_GetObjectItemCaseSensitive(data, key);
    const char *p;
    size_t wlen = strlen(word);

    if (!cJSON_IsString(item) || item->valuestring == NULL)
        return 0;

    p = item->valuestring;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
        p++;

    if (strncasecmp(p, word, wlen) != 0)
        return 0;

    p += wlen;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
        p++;

    return *p == '\0';
}

/*
 * Failure reason of the last set_cfg / trans-mode request
 * -------------------------------------------------------
 * A handler that fails calls set_cfg_fail(param, fmt, ...) instead of a bare
 * "return -1", so processServerMsg() can tell the broker WHICH parameter
 * failed (DATA.PARAM / DATA.REASON in the FAILED reply).
 */
static char g_set_cfg_err_param[32];
static char g_set_cfg_err_reason[160];

static void set_cfg_clear_error(void)
{
    g_set_cfg_err_param[0] = '\0';
    g_set_cfg_err_reason[0] = '\0';
}

static int set_cfg_fail(const char *param, const char *fmt, ...)
{
    va_list ap;

    snprintf(g_set_cfg_err_param, sizeof(g_set_cfg_err_param), "%s", param ? param : "");
    va_start(ap, fmt);
    vsnprintf(g_set_cfg_err_reason, sizeof(g_set_cfg_err_reason), fmt, ap);
    va_end(ap);

    LOG_ERROR("set_cfg failed: param=%s reason=%s", g_set_cfg_err_param, g_set_cfg_err_reason);
    return -1;
}

const char *set_cfg_last_error_param(void) { return g_set_cfg_err_param; }
const char *set_cfg_last_error_reason(void) { return g_set_cfg_err_reason; }

/*
 * web_ui_notify()
 * ---------------
 * Tells the Web UI that a config section was changed over MQTT so it reloads
 * it: SADD mqtt_config_change_web <section>   (network / upstream / device / meter)
 */
static void web_ui_notify(redisContext *ctx, const char *section)
{
    redisReply *reply = redisCommand(ctx, "SADD mqtt_config_change_web %s", section);

    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
                LOG_INFO("Web UI update requested for %s", section);
            else
                LOG_INFO("Web UI update already pending for %s", section);
        }
        freeReplyObject(reply);
    }
    else
    {
        LOG_ERROR("Failed to execute Redis command: SADD mqtt_config_change_web %s", section);
    }
}

/*
 * hset_int_field()
 * ----------------
 * Common integer setter used by every set_*_cfg():
 *   number 9600 / string "9600" / " 9600 " / "YES" / "NO"  ->  HSET hash field <int>
 * A value that is not a valid integer is logged and NOT written
 * (previously atoi() silently stored "abc" as 0).
 * Returns 1 = written, 0 = key absent, -1 = invalid value.
 */
static int hset_int_field_scaled(redisContext *ctx, cJSON *data, const char *json_key,
                                 const char *hash, const char *field, int multiplier)
{
    redisReply *reply;
    int value = 0;
    int rc = json_get_int_value(data, json_key, 1, &value);

    if (rc == JSON_INT_ABSENT)
        return 0;

    if (rc != JSON_INT_OK)
    {
        LOG_ERROR("set_cfg: %s is not a valid number - %s %s not changed", json_key, hash, field);
        return -1;
    }

    if (multiplier != 1)
    {
        LOG_INFO("set_cfg: %s=%d -> %s %s = %d", json_key, value, hash, field, value * multiplier);
        value *= multiplier;
    }

    reply = redisCommand(ctx, "HSET %s %s %d", hash, field, value);
    if (reply)
        freeReplyObject(reply);

    return 1;
}

static int hset_int_field(redisContext *ctx, cJSON *data, const char *json_key,
                          const char *hash, const char *field)
{
    return hset_int_field_scaled(ctx, data, json_key, hash, field, 1);
}

/* RESP_TIMEOUT arrives in seconds (1-30) and is stored in Redis in ms */
#define RESP_TIMEOUT_SCALE 1000

/* Mandatory selector (SIM_SLOT, IPSEC_TUNNEL, FTP_SERVER, SERIAL_PORT, NTP_SERVER):
 * accepts 1 / "1" / 2 / "2". Returns 0 and sets *out, or -1. */
static int get_selector_1_2(cJSON *data, const char *json_key, int *out)
{
    int v = 0;

    if (json_get_int_value(data, json_key, 0, &v) != JSON_INT_OK)
        return set_cfg_fail(json_key, "%s missing or not a number", json_key);
    if (v != 1 && v != 2)
        return set_cfg_fail(json_key, "%s must be 1 or 2, got %d", json_key, v);

    *out = v;
    return 0;
}

int set_mqtt_cfg(redisContext *ctx, cJSON *data, int mqtt1)
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
     * Fixed mapping:
     *   MQTT_BROKER_1 / PRIMARY   (mqtt1 = 1) -> mqtt_0_cfg
     *   MQTT_BROKER_2 / SECONDARY (mqtt1 = 0) -> mqtt_1_cfg
     * (The old lookup by the "mqtt1" field returned -1 -> FAILED whenever
     *  no hash had mqtt1 == 0/1 as expected.)
     *------------------------------------------------------*/
    snprintf(hash, sizeof(hash), "%s", mqtt1 ? "mqtt_0_cfg" : "mqtt_1_cfg");

    if (!rhash_exists(ctx, hash))
        return set_cfg_fail("DATA_TYPE", "Redis hash %s does not exist", hash);

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

#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, hash, REDIS_KEY)

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

    /* No restart here: the SUCCESS reply has to reach the broker BEFORE
     * re_mqtt_proc is restarted, otherwise it is lost with the connection.
     * processServerMsg() sends the reply and then calls set_cfg_after_reply(),
     * which does the "SADD proc_restart re_mqtt_proc" that used to be here. */
    LOG_INFO("MQTT configuration written to %s, restart deferred until reply is sent", hash);

    return 0;
}

/* F1 (review 03-Oct): MODEM fields end up unquoted in shell/pppd files that
 * ppp_monitor.sh runs as root. Accept only safe characters; reject the whole
 * request otherwise. kind: 'a' = APN, 'd' = dial number, 'c' = credential. */
static int modem_value_ok(const char *v, char kind)
{
    size_t n = 0;

    if (v == NULL)
        return 0;

    for (; *v; v++, n++)
    {
        unsigned char c = (unsigned char)*v;

        if (kind == 'a')
        {
            if (!(isalnum(c) || c == '.' || c == '-' || c == '_'))
                return 0;
        }
        else if (kind == 'd')
        {
            if (!(isdigit(c) || c == '*' || c == '#' || c == '+'))
                return 0;
        }
        else /* credential */
        {
            if (c <= 0x20 || c >= 0x7F)             /* no space/control/non-ASCII */
                return 0;
            if (strchr("'\"`$\\;&|<>(){}*?[]~", c))   /* no shell metacharacters or glob chars */
                return 0;
        }
    }

    return n <= 63;
}

static int modem_field_ok(cJSON *data, const char *key, char kind)
{
    cJSON *it = cJSON_GetObjectItemCaseSensitive(data, key);

    if (it == NULL)
        return 1; /* field not sent: nothing to update */
    if (!cJSON_IsString(it) || !modem_value_ok(it->valuestring, kind))
    {
        LOG_ERROR("set_cfg MODEM: rejected invalid %s", key);
        set_cfg_fail(key, "%s contains characters that are not allowed or is too long", key);
        return 0;
    }
    return 1;
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

    /* SIM_SLOT is mandatory (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "SIM_SLOT", &sim) != 0)
        return -1;

    /* F1: validate every field before writing any of them */
    if (!modem_field_ok(data, "USERNAME", 'c') ||
        !modem_field_ok(data, "PASSWORD", 'c') ||
        !modem_field_ok(data, "APN", 'a') ||
        !modem_field_ok(data, "DIAL_NUM", 'd'))
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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "network");
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

    /* Tunnel number is mandatory (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "IPSEC_TUNNEL", &tunnel) != 0)
        return -1;

    sprintf(hash, "ipsec_%d_cfg", tunnel - 1);

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                          \
    do                                                           \
    {                                                            \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY); \
        if (cJSON_IsString(item) && item->valuestring)           \
        {                                                        \
            reply = redisCommand(ctx,                            \
                                 "HSET %s %s %s",                \
                                 hash,                           \
                                 REDIS_KEY,                      \
                                 item->valuestring);             \
            if (reply)                                           \
                freeReplyObject(reply);                          \
        }                                                        \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, hash, REDIS_KEY)

    /* Parameters supported */
    UPDATE_STR("RIGHT_IP", "right_ip");
    UPDATE_STR("LEFT_ID", "left_id");
    UPDATE_STR("LEFT_SUBNET", "left_subnet");
    UPDATE_STR("LEFT_SUBNET_MASK", "left_subnet_mask");
    UPDATE_STR("RIGHT_ID", "right_id");
    UPDATE_STR("RIGHT_SUBNET", "right_subnet");
    UPDATE_STR("RIGHT_SUBNET_MASK", "right_subnet_mask");
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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "network");
    return 0;
}

/*
 * NTP_INTERVAL handling
 * ---------------------
 *   "1"   -> sync every day   -> HSET ntp_cfg interval 24   (hours)
 *   "7"   -> sync every week  -> HSET ntp_cfg interval 168  (hours)
 *   "NOW" -> sync immediately -> HSET ntp_cfg ntp_sync_req Not_set_yet
 */
#define NTP_INTERVAL_DAY_HOURS 24
#define NTP_INTERVAL_WEEK_HOURS 168
#define NTP_SYNC_REQ_FIELD "ntp_sync_req"
#define NTP_SYNC_REQ_VALUE "Not_set_yet"

typedef enum
{
    NTP_INTERVAL_ABSENT = 0, /* key not present in DATA        */
    NTP_INTERVAL_HOURS,      /* valid "1" / "7" -> hours set   */
    NTP_INTERVAL_SYNC_NOW,   /* "NOW"                          */
    NTP_INTERVAL_INVALID     /* present but not 1 / 7 / NOW    */
} ntp_interval_kind_t;

static ntp_interval_kind_t parse_ntp_interval(cJSON *data, int *hours)
{
    int days = 0;
    int rc;

    *hours = 0;

    if (json_str_equals(data, "NTP_INTERVAL", "NOW"))
        return NTP_INTERVAL_SYNC_NOW;

    rc = json_get_int_value(data, "NTP_INTERVAL", 0, &days);
    if (rc == JSON_INT_ABSENT)
        return NTP_INTERVAL_ABSENT;
    if (rc != JSON_INT_OK)
        return NTP_INTERVAL_INVALID;

    if (days == 1)
        *hours = NTP_INTERVAL_DAY_HOURS;
    else if (days == 7)
        *hours = NTP_INTERVAL_WEEK_HOURS;
    else
        return NTP_INTERVAL_INVALID;

    return NTP_INTERVAL_HOURS;
}

int set_ntp_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char field[64];
    int server = 0;
    int interval_hours = 0;
    ntp_interval_kind_t interval_kind;

    if (!ctx || !data)
        return -1;

    /* NTP_SERVER is mandatory (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "NTP_SERVER", &server) != 0)
        return -1;

    /* Validate NTP_INTERVAL before writing anything, so a bad value
     * does not leave the config half-updated. */
    interval_kind = parse_ntp_interval(data, &interval_hours);
    if (interval_kind == NTP_INTERVAL_INVALID)
    {
        LOG_ERROR("set_ntp_cfg: invalid NTP_INTERVAL (allowed: \"1\", \"7\", \"NOW\")");
        return set_cfg_fail("NTP_INTERVAL", "NTP_INTERVAL must be \"1\", \"7\" or \"NOW\"");
    }

#define UPDATE_STR(JSON_KEY, REDIS_FMT)                          \
    do                                                           \
    {                                                            \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY); \
        if (cJSON_IsString(item) && item->valuestring)           \
        {                                                        \
            sprintf(field, REDIS_FMT, server);                   \
            reply = redisCommand(ctx,                            \
                                 "HSET ntp_cfg %s %s",           \
                                 field,                          \
                                 item->valuestring);             \
            if (reply)                                           \
                freeReplyObject(reply);                          \
        }                                                        \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_FMT)                           \
    do                                                            \
    {                                                             \
        snprintf(field, sizeof(field), REDIS_FMT, server);        \
        hset_int_field(ctx, data, JSON_KEY, "ntp_cfg", field); \
    } while (0)

    /* Per-server parameters */
    UPDATE_STR("NTP_IP", "ntp%d_server_ip");
    UPDATE_INT("NTP_PORT", "ntp%d_port");

    /* Optional common parameters */
    UPDATE_INT("ENABLE_NTP", "enable_ntp%d");

    /* NTP_INTERVAL: 1 -> 24 h, 7 -> 168 h, NOW -> immediate sync request */
    if (interval_kind == NTP_INTERVAL_HOURS)
    {
        reply = redisCommand(ctx, "HSET ntp_cfg interval %d", interval_hours);
        if (reply)
            freeReplyObject(reply);
        LOG_INFO("NTP sync interval set to %d hours", interval_hours);
    }
    else if (interval_kind == NTP_INTERVAL_SYNC_NOW)
    {
        reply = redisCommand(ctx, "HSET ntp_cfg %s %s", NTP_SYNC_REQ_FIELD, NTP_SYNC_REQ_VALUE);
        if (reply)
            freeReplyObject(reply);
        LOG_INFO("NTP immediate sync requested (%s = %s)", NTP_SYNC_REQ_FIELD, NTP_SYNC_REQ_VALUE);
    }

    /* Legacy key, only used when the new NTP_INTERVAL is not sent */
    if (interval_kind == NTP_INTERVAL_ABSENT)
        hset_int_field(ctx, data, "SYNC_INTERVAL", "ntp_cfg", "interval");

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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "network");
    return 0;
}

int set_iec104_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;

    if (!ctx || !data)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                          \
    do                                                           \
    {                                                            \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY); \
        if (cJSON_IsString(item) && item->valuestring)           \
        {                                                        \
            reply = redisCommand(ctx,                            \
                                 "HSET iec104_0_cfg %s %s",      \
                                 REDIS_KEY,                      \
                                 item->valuestring);             \
            if (reply)                                           \
                freeReplyObject(reply);                          \
        }                                                        \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, "iec104_0_cfg", REDIS_KEY)

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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "upstream");
    return 0;
}

int set_iec101_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;

    if (!ctx || !data)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_KEY)                          \
    do                                                           \
    {                                                            \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY); \
        if (cJSON_IsString(item) && item->valuestring)           \
        {                                                        \
            reply = redisCommand(ctx,                            \
                                 "HSET iec101_0_cfg %s %s",      \
                                 REDIS_KEY,                      \
                                 item->valuestring);             \
            if (reply)                                           \
                freeReplyObject(reply);                          \
        }                                                        \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, "iec101_0_cfg", REDIS_KEY)

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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "upstream");
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

    /* FTP_SERVER is mandatory (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "FTP_SERVER", &server) != 0)
        return -1;

#define UPDATE_STR(JSON_KEY, REDIS_FMT)                          \
    do                                                           \
    {                                                            \
        item = cJSON_GetObjectItemCaseSensitive(data, JSON_KEY); \
        if (cJSON_IsString(item) && item->valuestring)           \
        {                                                        \
            sprintf(field, REDIS_FMT, server);                   \
            reply = redisCommand(ctx,                            \
                                 "HSET ftp_cfg %s %s",           \
                                 field,                          \
                                 item->valuestring);             \
            if (reply)                                           \
                freeReplyObject(reply);                          \
        }                                                        \
    } while (0)

#define UPDATE_INT(JSON_KEY, REDIS_FMT)                           \
    do                                                            \
    {                                                             \
        snprintf(field, sizeof(field), REDIS_FMT, server);        \
        hset_int_field(ctx, data, JSON_KEY, "ftp_cfg", field); \
    } while (0)

    UPDATE_STR("IP_ADDRESS", "ip_addr_%d");
    UPDATE_INT("PORT", "port_%d");
    UPDATE_STR("USERNAME", "username_%d");
    UPDATE_STR("PASSWORD", "password_%d");
    UPDATE_STR("REMOTE_DIRECTORY", "remote_directory_%d");
    UPDATE_INT("TIME_INTERVAL", "time_interval_%d");
    UPDATE_INT("ENABLE_SERVER", "server_enable_%d");

    /* Common FTP enable */
    hset_int_field(ctx, data, "ENABLE_FTP", "ftp_cfg", "ftp_enable");

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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "upstream");
    return 0;
}

/* serial_port_N_cfg "device_type": which acquisition process owns the port */
#define SERPORT_DEV_DLMS 1
#define SERPORT_DEV_MODBUS 2

int set_serial_port_cfg(redisContext *ctx, cJSON *data)
{
    redisReply *reply;
    cJSON *item;
    char hash[32];
    int port = 0;
    int device_type;
    int parity = -1;
    char stop_bits[8] = ""; /* "1", "1.5" or "2"; empty = not sent */
    const char *proc_name;

    if (!ctx || !data)
        return -1;

    /* SERIAL_PORT is mandatory (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "SERIAL_PORT", &port) != 0)
        return -1;

    sprintf(hash, "serial_port_%d_cfg", port - 1);

    /* ---- 1. Check everything BEFORE writing, so a failure changes nothing ---- */
    if (!rhash_exists(ctx, hash))
        return set_cfg_fail("SERIAL_PORT", "Redis hash %s does not exist", hash);

    /* device_type decides which process must be restarted:
     *   1 = DLMS   -> SerDaProc_0      (port 1) / SerDaProc_1      (port 2)
     *   2 = Modbus -> modrtu_master_0  (port 1) / modrtu_master_1  (port 2) */
    device_type = rget_int(ctx, hash, "device_type", -1);
    if (device_type == SERPORT_DEV_DLMS)
        proc_name = (port == 1) ? "SerDaProc_0" : "SerDaProc_1";
    else if (device_type == SERPORT_DEV_MODBUS)
        proc_name = (port == 1) ? "modrtu_master_0" : "modrtu_master_1";
    else
        return set_cfg_fail("SERIAL_PORT", "%s device_type is %d (expected 1 = DLMS or 2 = Modbus)",
                            hash, device_type);

    item = cJSON_GetObjectItemCaseSensitive(data, "PARITY");
    if (cJSON_IsString(item) && item->valuestring)
    {
        if (!strcasecmp(item->valuestring, "none"))
            parity = 0;
        else if (!strcasecmp(item->valuestring, "odd"))
            parity = 1;
        else if (!strcasecmp(item->valuestring, "even"))
            parity = 2;
        else
            return set_cfg_fail("PARITY", "PARITY must be none, odd or even (got '%s')", item->valuestring);
    }

    /* STOP_BITS: "1", "1.5" or "2" (string or number), stored as that text.
     * Through hset_int_field() "1.5" was rejected as "not a valid number"
     * and silently never written. */
    item = cJSON_GetObjectItemCaseSensitive(data, "STOP_BITS");
    if (item != NULL && !cJSON_IsNull(item))
    {
        if (cJSON_IsNumber(item))
        {
            double d = item->valuedouble;

            if (d == 1.0)
                strcpy(stop_bits, "1");
            else if (d == 1.5)
                strcpy(stop_bits, "1.5");
            else if (d == 2.0)
                strcpy(stop_bits, "2");
            else
                return set_cfg_fail("STOP_BITS", "STOP_BITS must be 1, 1.5 or 2 (got %g)", d);
        }
        else if (cJSON_IsString(item) && item->valuestring)
        {
            const char *v = item->valuestring;
            size_t len;

            while (*v == ' ' || *v == '\t')
                v++;
            len = strlen(v);
            while (len > 0 && (v[len - 1] == ' ' || v[len - 1] == '\t' ||
                               v[len - 1] == '\r' || v[len - 1] == '\n'))
                len--;

            if (len == 1 && v[0] == '1')
                strcpy(stop_bits, "1");
            else if (len == 3 && !strncmp(v, "1.5", 3))
                strcpy(stop_bits, "1.5");
            else if (len == 1 && v[0] == '2')
                strcpy(stop_bits, "2");
            else
                return set_cfg_fail("STOP_BITS", "STOP_BITS must be 1, 1.5 or 2 (got '%s')", item->valuestring);
        }
        else
        {
            return set_cfg_fail("STOP_BITS", "STOP_BITS must be 1, 1.5 or 2");
        }
    }

    /* ---- 2. Write ---- */
#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, hash, REDIS_KEY)

    UPDATE_INT("BAUD_RATE", "baudrate");
    UPDATE_INT("DATA_BITS", "databits");

#undef UPDATE_INT

    if (stop_bits[0])
    {
        reply = redisCommand(ctx, "HSET %s stopbits %s", hash, stop_bits);
        if (reply)
            freeReplyObject(reply);
        LOG_INFO("Serial port %d: stopbits = %s", port, stop_bits);
    }

    if (parity >= 0)
    {
        reply = redisCommand(ctx, "HSET %s parity %d", hash, parity);
        if (reply)
            freeReplyObject(reply);
    }

    /* ---- 3. Restart the process that uses this port ---- */
    LOG_INFO("Serial port %d (%s) device_type=%d -> restarting %s",
             port, hash, device_type, proc_name);

    reply = redisCommand(ctx, "SADD proc_restart %s", proc_name);
    if (reply)
    {
        if (reply->type == REDIS_REPLY_INTEGER)
        {
            if (reply->integer == 1)
                LOG_INFO("Restart requested for %s", proc_name);
            else
                LOG_INFO("Restart already pending for %s", proc_name);
        }
        else if (reply->type == REDIS_REPLY_ERROR)
        {
            LOG_ERROR("Redis SADD proc_restart %s failed: %s", proc_name, reply->str);
        }
        freeReplyObject(reply);
    }
    else
    {
        LOG_ERROR("Failed to execute Redis command: SADD proc_restart %s", proc_name);
    }


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "device");
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
        return set_cfg_fail("METER_NAME", "METER_NAME missing or not a string");

    /* F3 (review 03-Oct): bounded copy; reject names that do not fit */
    if (strlen(item->valuestring) >= sizeof(meter_name))
        return set_cfg_fail("METER_NAME", "METER_NAME is too long (max %d characters)", (int)sizeof(meter_name) - 1);
    snprintf(meter_name, sizeof(meter_name), "%s", item->valuestring);

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
        return set_cfg_fail("METER_NAME", "Modbus TCP device '%s' not found", meter_name);

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

#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, hash, REDIS_KEY)

    UPDATE_STR("IP_ADDRESS", "dev_ip");

    UPDATE_INT("PORT", "dev_port");
    UPDATE_INT("SLAVE_ID", "slave_id");
    UPDATE_INT("RETRIES", "retries");
    UPDATE_INT("POLL_SKIP_COUNT", "poll_faulty_cnt");
    hset_int_field_scaled(ctx, data, "RESP_TIMEOUT", hash, "resp_timeout", RESP_TIMEOUT_SCALE); /* s -> ms */

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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "meter");
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

    /* SERIAL_PORT (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "SERIAL_PORT", &serial_port) != 0)
        return -1;

    /* METER_NAME */
    item = cJSON_GetObjectItemCaseSensitive(data, "METER_NAME");
    if (!cJSON_IsString(item) || !item->valuestring)
        return set_cfg_fail("METER_NAME", "METER_NAME missing or not a string");

    /* F3 (review 03-Oct): bounded copy; reject names that do not fit */
    if (strlen(item->valuestring) >= sizeof(meter_name))
        return set_cfg_fail("METER_NAME", "METER_NAME is too long (max %d characters)", (int)sizeof(meter_name) - 1);
    snprintf(meter_name, sizeof(meter_name), "%s", item->valuestring);

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
        return set_cfg_fail("METER_NAME", "Modbus RTU device '%s' not found on port %d", meter_name, serial_port);

#define UPDATE_INT(JSON_KEY, REDIS_KEY) \
    hset_int_field(ctx, data, JSON_KEY, hash, REDIS_KEY)

    UPDATE_INT("SLAVE_ID", "slave_id");
    UPDATE_INT("RETRIES", "retries");
    UPDATE_INT("POLL_SKIP_COUNT", "poll_faulty_cnt");
    hset_int_field_scaled(ctx, data, "RESP_TIMEOUT", hash, "resp_timeout", RESP_TIMEOUT_SCALE); /* s -> ms */

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


    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "meter");
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

    /* SERIAL_PORT (1 / "1" / 2 / "2") */
    if (get_selector_1_2(data, "SERIAL_PORT", &serial_port) != 0)
        return -1;

    /* METER_NAME */
    item = cJSON_GetObjectItemCaseSensitive(data, "METER_NAME");
    if (!cJSON_IsString(item) || !item->valuestring)
        return set_cfg_fail("METER_NAME", "METER_NAME missing or not a string");

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
        return set_cfg_fail("METER_ADDRESS", "METER_ADDRESS missing or not a number/string");
    }

    char hash_name[64];
    snprintf(hash_name, sizeof(hash_name),
             "serial_port_%d_cfg", serial_port - 1);

    if (find_meter(ctx, meter_name, hash_name, &meter_id) != 0)
    {
        LOG_INFO("Meter not found");
        return set_cfg_fail("METER_NAME", "DLMS serial meter '%s' not found on port %d", meter_name, serial_port);
    }

    char key[64];
    snprintf(key, sizeof(key),
             "meter_addr[%d]", meter_id);

    reply = redisCommand(ctx, "HSET %s %s %s", hash_name, key, meter_address);

    if (!reply)
    {
        LOG_ERROR("Redis HSET failed");
        return set_cfg_fail("", "Redis write failed");
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

    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "meter");
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
        return set_cfg_fail("METER_NAME", "METER_NAME missing or not a string");

    snprintf(meter_name, sizeof(meter_name), "%s", item->valuestring);

    item = cJSON_GetObjectItemCaseSensitive(data, "IP_ADDR");

    if (cJSON_IsString(item))
    {
        snprintf(ip_address, sizeof(ip_address), "%s",
                 item->valuestring);
    }
    else
    {
        return set_cfg_fail("IP_ADDR", "IP_ADDR missing or not a string");
    }

    char hash_name[64];
    snprintf(hash_name, sizeof(hash_name),
             "ethernet_meter_cfg");

    if (find_meter(ctx, meter_name, hash_name, &meter_id) != 0)
    {
        LOG_INFO("Meter not found");
        return set_cfg_fail("METER_NAME", "DLMS Ethernet meter '%s' not found", meter_name);
    }

    char key[64];
    snprintf(key, sizeof(key),
             "ip_addr[%d]", meter_id);

    reply = redisCommand(ctx, "HSET %s %s %s", hash_name, key, ip_address);

    if (!reply)
    {
        LOG_ERROR("Redis HSET failed");
        return set_cfg_fail("", "Redis write failed");
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

    /* Web UI update (as in the previous file) */
    web_ui_notify(ctx, "meter");
    return 0;
}

/* =========================================================================
 * TRANSPARENT MODE  (COMMAND_TYPE = START_TRANS_MODE / STOP_TRANS_MODE)
 * -------------------------------------------------------------------------
 *  DATA_TYPE  | DATA keys            | trans_dcu_mode_info written
 *  -----------+----------------------+-----------------------------------
 *  SERIAL     | PORT (1/2), DUR      | port=1|2            mode=1|0
 *  ETHERNET   | METER (serial), DUR  | port=3 met_id=0..29 mode=1|0
 *  FULL       | DUR                  | not implemented yet -> failure
 *
 *  START also writes: HSET features_cfg transparent_mode_dur <DUR*60>
 *  DUR (minutes) must be one of 15, 30, 45, 60, 120.
 *
 *  Ethernet met_id is looked up from the meter details using the
 *  meter serial number sent in "METER":
 *     hash meter_status, field meter_<a>_<b>_<serial>_details,
 *     value = JSON {"serial_number": "...", "met_id": "...", ...}
 *  (fallback: a Redis hash KEY named meter_*_<serial>_details)
 * ========================================================================= */

#define TRANS_FEATURES_HASH "features_cfg"
#define TRANS_DUR_FIELD "transparent_mode_dur"
#define TRANS_MODE_HASH "trans_dcu_mode_info"
#define TRANS_METER_STATUS_HASH "meter_status"

#define TRANS_PORT_SERIAL_1 1
#define TRANS_PORT_SERIAL_2 2
#define TRANS_PORT_ETHERNET 3

#define TRANS_MODE_STOP 0
#define TRANS_MODE_START 1

#define TRANS_MET_ID_MIN 0
#define TRANS_MET_ID_MAX 29

#define TRANS_SCAN_MAX_ROUNDS 200 /* bound for the fallback SCAN loop */

/* Return codes of trans_mode_request() */
#define TRANS_OK 0
#define TRANS_ERR (-1)
#define TRANS_NOT_SUPPORTED (-2)

static int trans_duration_allowed(int minutes)
{
    static const int allowed[] = {15, 30, 45, 60, 120};
    size_t i;

    for (i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++)
    {
        if (allowed[i] == minutes)
            return 1;
    }
    return 0;
}

/* Copy the trimmed "METER" string. Only [A-Za-z0-9_-] is accepted, which
 * also keeps it safe for use inside a Redis SCAN MATCH pattern. */
static int trans_get_meter_serial(cJSON *data, char *out, size_t out_len)
{
    cJSON *item = cJSON_GetObjectItemCaseSensitive(data, "METER");
    const char *p;
    size_t n = 0;

    if (!cJSON_IsString(item) || item->valuestring == NULL)
    {
        LOG_ERROR("trans_mode: METER missing");
        return -1;
    }

    p = item->valuestring;
    while (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')
        p++;

    while (*p && *p != ' ' && *p != '\t' && *p != '\r' && *p != '\n')
    {
        char c = *p++;

        if (!((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
              (c >= '0' && c <= '9') || c == '_' || c == '-'))
        {
            LOG_ERROR("trans_mode: METER contains invalid character '%c'", c);
            return -1;
        }
        if (n + 1 >= out_len)
        {
            LOG_ERROR("trans_mode: METER too long");
            return -1;
        }
        out[n++] = c;
    }
    out[n] = '\0';

    if (n == 0)
    {
        LOG_ERROR("trans_mode: METER is empty");
        return -1;
    }
    return 0;
}

/* Strict string -> int ("1", " 7 " ok; "", "1x" rejected). */
static int trans_str_to_int(const char *s, int *out)
{
    char *end;
    long v;

    if (s == NULL)
        return -1;
    while (*s == ' ' || *s == '\t')
        s++;
    if (*s == '\0')
        return -1;

    v = strtol(s, &end, 10);
    while (*end == ' ' || *end == '\t' || *end == '\r' || *end == '\n')
        end++;
    if (*end != '\0')
        return -1;

    *out = (int)v;
    return 0;
}

/* met_id inside a details JSON object (string "1" or number 1). */
static int trans_met_id_from_json(cJSON *obj, int *met_id)
{
    cJSON *id = cJSON_GetObjectItemCaseSensitive(obj, "met_id");

    if (cJSON_IsNumber(id))
    {
        *met_id = id->valueint;
        return 0;
    }
    if (cJSON_IsString(id) && id->valuestring)
        return trans_str_to_int(id->valuestring, met_id);

    return -1;
}

static int trans_ends_with(const char *s, const char *suffix)
{
    size_t ls = strlen(s);
    size_t lf = strlen(suffix);

    return ls >= lf && strcmp(s + ls - lf, suffix) == 0;
}

/* Layout A: hash "meter_status", field "..._details", value = JSON string */
static int trans_find_met_id_in_status_hash(redisContext *ctx, const char *serial, int *met_id)
{
    redisReply *reply;
    int found = -1;
    size_t i;

    reply = redisCommand(ctx, "HGETALL %s", TRANS_METER_STATUS_HASH);
    if (reply == NULL)
        return -1;

    if (reply->type != REDIS_REPLY_ARRAY)
    {
        freeReplyObject(reply);
        return -1; /* missing or not a hash -> caller tries layout B */
    }

    for (i = 0; i + 1 < reply->elements && found != 0; i += 2)
    {
        redisReply *f = reply->element[i];
        redisReply *v = reply->element[i + 1];
        cJSON *obj;
        cJSON *sn;

        if (f == NULL || v == NULL || f->type != REDIS_REPLY_STRING || v->type != REDIS_REPLY_STRING)
            continue;

        if (!trans_ends_with(f->str, "_details"))
            continue;

        obj = cJSON_Parse(v->str);
        if (obj == NULL)
            continue;

        sn = cJSON_GetObjectItemCaseSensitive(obj, "serial_number");
        if (cJSON_IsString(sn) && sn->valuestring && strcmp(sn->valuestring, serial) == 0)
        {
            if (trans_met_id_from_json(obj, met_id) == 0)
            {
                LOG_INFO("trans_mode: meter %s found in %s/%s, met_id=%d",
                         serial, TRANS_METER_STATUS_HASH, f->str, *met_id);
                found = 0;
            }
            else
            {
                LOG_ERROR("trans_mode: meter %s found in %s/%s but met_id is invalid",
                          serial, TRANS_METER_STATUS_HASH, f->str);
                found = -2; /* found but unusable: stop searching */
                cJSON_Delete(obj);
                break;
            }
        }
        cJSON_Delete(obj);
    }

    freeReplyObject(reply);
    return found;
}

/* Layout B: separate hash key "meter_*_<serial>_details" with field met_id */
static int trans_find_met_id_in_detail_keys(redisContext *ctx, const char *serial, int *met_id)
{
    char cursor[32] = "0";
    int rounds = 0;
    int found = -1;

    do
    {
        redisReply *reply = redisCommand(ctx, "SCAN %s MATCH meter_*_%s_details COUNT 100", cursor, serial);
        redisReply *keys;
        size_t i;

        if (reply == NULL)
            return -1;

        if (reply->type != REDIS_REPLY_ARRAY || reply->elements != 2 ||
            reply->element[0]->type != REDIS_REPLY_STRING ||
            reply->element[1]->type != REDIS_REPLY_ARRAY)
        {
            freeReplyObject(reply);
            return -1;
        }

        snprintf(cursor, sizeof(cursor), "%s", reply->element[0]->str);
        keys = reply->element[1];

        for (i = 0; i < keys->elements && found != 0; i++)
        {
            redisReply *sn;
            redisReply *id;
            const char *key = keys->element[i]->str;

            if (keys->element[i]->type != REDIS_REPLY_STRING)
                continue;

            /* confirm the serial number really matches */
            sn = redisCommand(ctx, "HGET %s serial_number", key);
            if (sn == NULL)
                continue;
            if (sn->type != REDIS_REPLY_STRING || strcmp(sn->str, serial) != 0)
            {
                freeReplyObject(sn);
                continue;
            }
            freeReplyObject(sn);

            id = redisCommand(ctx, "HGET %s met_id", key);
            if (id && id->type == REDIS_REPLY_STRING && trans_str_to_int(id->str, met_id) == 0)
            {
                LOG_INFO("trans_mode: meter %s found in key %s, met_id=%d", serial, key, *met_id);
                found = 0;
            }
            if (id)
                freeReplyObject(id);
        }

        freeReplyObject(reply);
    } while (found != 0 && strcmp(cursor, "0") != 0 && ++rounds < TRANS_SCAN_MAX_ROUNDS);

    return found;
}

static int trans_find_eth_met_id(redisContext *ctx, const char *serial, int *met_id)
{
    int rc = trans_find_met_id_in_status_hash(ctx, serial, met_id);

    if (rc == -1)
        rc = trans_find_met_id_in_detail_keys(ctx, serial, met_id);

    if (rc != 0)
    {
        LOG_ERROR("trans_mode: meter %s not found in meter details", serial);
        return -1;
    }

    if (*met_id < TRANS_MET_ID_MIN || *met_id > TRANS_MET_ID_MAX)
    {
        LOG_ERROR("trans_mode: meter %s has met_id %d, allowed %d..%d for ethernet meters",
                  serial, *met_id, TRANS_MET_ID_MIN, TRANS_MET_ID_MAX);
        return -1;
    }
    return 0;
}

/* Ethernet transparent mode runs over the IPsec tunnel, so at least one of
 * the two tunnels must be enabled (ipsec_0_cfg / ipsec_1_cfg enable_tunnel).
 * enable_tunnel is written as 1/0 by set_cfg; "yes"/"true"/"enable(d)" are
 * accepted too in case the Web UI stores text. Returns 1 if any is enabled. */
static int trans_ipsec_value_on(const char *v)
{
    return v != NULL &&
           (!strcmp(v, "1") || !strcasecmp(v, "yes") || !strcasecmp(v, "true") ||
            !strcasecmp(v, "enable") || !strcasecmp(v, "enabled"));
}

static int trans_ipsec_enabled(redisContext *ctx)
{
    int i;

    for (i = 0; i < 2; i++)
    {
        redisReply *r = redisCommand(ctx, "HGET ipsec_%d_cfg enable_tunnel", i);
        int on = 0;

        if (r)
        {
            if (r->type == REDIS_REPLY_STRING)
                on = trans_ipsec_value_on(r->str);
            else if (r->type == REDIS_REPLY_INTEGER)
                on = (r->integer != 0);
            LOG_INFO("trans_mode: ipsec_%d_cfg enable_tunnel=%s -> %s", i,
                     r->type == REDIS_REPLY_STRING ? r->str : "<not set>", on ? "enabled" : "disabled");
            freeReplyObject(r);
        }
        if (on)
            return 1;
    }
    return 0;
}

static int trans_redis_ok(redisReply *reply, const char *what)
{
    if (reply == NULL)
    {
        LOG_ERROR("trans_mode: Redis error while %s", what);
        return 0;
    }
    if (reply->type == REDIS_REPLY_ERROR)
    {
        LOG_ERROR("trans_mode: Redis error while %s: %s", what, reply->str ? reply->str : "?");
        freeReplyObject(reply);
        return 0;
    }
    freeReplyObject(reply);
    return 1;
}

/*
 * trans_mode_request()
 * --------------------
 * Handles START_TRANS_MODE / STOP_TRANS_MODE.
 * Returns TRANS_OK (0), TRANS_ERR (-1) or TRANS_NOT_SUPPORTED (-2, FULL).
 * Everything is validated before anything is written to Redis.
 */
int trans_mode_request(redisContext *ctx, cmd_request_t *cmd)
{
    cJSON *data;
    int start;
    int port = 0;
    int met_id = -1;
    int dur_min = 0;
    char meter_serial[64] = "";

    set_cfg_clear_error(); /* reason of a failure is reported in the reply */

    if (!ctx || !cmd || !cmd->data)
        return TRANS_ERR;

    data = cmd->data;

    if (!strcmp(cmd->type, "START_TRANS_MODE"))
        start = 1;
    else if (!strcmp(cmd->type, "STOP_TRANS_MODE"))
        start = 0;
    else
        return TRANS_ERR;

    /* ---- 1. Target (port / meter) -------------------------------------- */
    if (!strcasecmp(cmd->data_type_req, "SERIAL"))
    {
        if (get_selector_1_2(data, "PORT", &port) != 0)
            return TRANS_ERR;
        /* port is already 1 or 2 = TRANS_PORT_SERIAL_1 / _2 */
    }
    else if (!strcasecmp(cmd->data_type_req, "ETHERNET"))
    {
        /* START over Ethernet needs an IPsec tunnel; nothing is written if not */
        if (start && !trans_ipsec_enabled(ctx))
        {
            set_cfg_fail("IPSEC", "IPsec is not enabled in both tunnels");
            return TRANS_ERR;
        }
        if (trans_get_meter_serial(data, meter_serial, sizeof(meter_serial)) != 0)
        {
            set_cfg_fail("METER", "METER missing or invalid");
            return TRANS_ERR;
        }
        if (trans_find_eth_met_id(ctx, meter_serial, &met_id) != 0)
        {
            set_cfg_fail("METER", "Ethernet meter '%s' not found", meter_serial);
            return TRANS_ERR;
        }
        port = TRANS_PORT_ETHERNET;
    }
    else if (!strcasecmp(cmd->data_type_req, "FULL"))
    {
        LOG_INFO("trans_mode: %s FULL received - not implemented yet", start ? "START" : "STOP");
        set_cfg_fail("DATA_TYPE", "FULL transparent mode is not supported yet");
        return TRANS_NOT_SUPPORTED;
    }
    else
    {
        LOG_ERROR("trans_mode: unknown DATA_TYPE '%s'", cmd->data_type_req);
        return TRANS_ERR;
    }

    /* ---- 2. Duration (START only) ------------------------------------- */
    if (start)
    {
        if (json_get_int_value(data, "DUR", 0, &dur_min) != JSON_INT_OK)
        {
            LOG_ERROR("trans_mode: DUR missing or not a number");
            return TRANS_ERR;
        }
        if (!trans_duration_allowed(dur_min))
        {
            LOG_ERROR("trans_mode: DUR %d min not supported (15, 30, 45, 60, 120)", dur_min);
            return TRANS_ERR;
        }
    }

    /* ---- 3. Write: duration first, then mode info (mode last) --------- */
    if (start)
    {
        if (!trans_redis_ok(redisCommand(ctx, "HSET %s %s %d", TRANS_FEATURES_HASH,
                                         TRANS_DUR_FIELD, dur_min * 60),
                            "writing duration"))
            return TRANS_ERR;
    }

    if (port == TRANS_PORT_ETHERNET)
    {
        if (!trans_redis_ok(redisCommand(ctx, "HSET %s port %d met_id %d mode %d", TRANS_MODE_HASH,
                                         port, met_id, start ? TRANS_MODE_START : TRANS_MODE_STOP),
                            "writing mode info"))
            return TRANS_ERR;
    }
    else
    {
        if (!trans_redis_ok(redisCommand(ctx, "HSET %s port %d mode %d", TRANS_MODE_HASH,
                                         port, start ? TRANS_MODE_START : TRANS_MODE_STOP),
                            "writing mode info"))
            return TRANS_ERR;
    }

    if (start)
        LOG_INFO("trans_mode: START port=%d%s%s met_id=%d dur=%d min (%d s)", port,
                 meter_serial[0] ? " meter=" : "", meter_serial, met_id, dur_min, dur_min * 60);
    else
        LOG_INFO("trans_mode: STOP port=%d%s%s met_id=%d", port,
                 meter_serial[0] ? " meter=" : "", meter_serial, met_id);

    return TRANS_OK;
}

static int set_cfg_dispatch(redisContext *ctx, cmd_request_t *cmd)
{
    /* DATA_TYPE names match mqtt_cmd_validate.c (case-insensitive, spec name
     * plus the older alias), so a message that passed validation is never
     * dropped here as an "unknown" DATA_TYPE. */
    const char *dt = cmd->data_type_req;

    if (!strcasecmp(dt, "MQTT_BROKER_1") || !strcasecmp(dt, "MQTT_BROKER_PRIMARY"))
        return set_mqtt_cfg(ctx, cmd->data, 1);

    if (!strcasecmp(dt, "MQTT_BROKER_2") || !strcasecmp(dt, "MQTT_BROKER_SECONDARY"))
        return set_mqtt_cfg(ctx, cmd->data, 0);

    if (!strcasecmp(dt, "MODEM"))
        return set_modem_cfg(ctx, cmd->data);

    if (!strcasecmp(dt, "IPSEC"))
        return set_ipsec_cfg(ctx, cmd->data); // eNABLE is enabled

    if (!strcasecmp(dt, "MODBUS_TCP"))
        return set_modtcp_cfg(ctx, cmd->data);

    if (!strcasecmp(dt, "MODBUS_RTU"))
        return set_modrtu_cfg(ctx, cmd->data);

    if (!strcasecmp(dt, "IEC104"))
        return set_iec104_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcasecmp(dt, "SERPORT") || !strcasecmp(dt, "SERIAL PORT"))
        return set_serial_port_cfg(ctx, cmd->data);

    if (!strcasecmp(dt, "IEC101"))
        return set_iec101_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcasecmp(dt, "FTP"))
        return set_ftp_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcasecmp(dt, "NTP"))
        return set_ntp_cfg(ctx, cmd->data); // Enable is enabled

    if (!strcasecmp(dt, "DLMS_SERIAL"))
        return set_dlms_serial_cfg(ctx, cmd->data);

    if (!strcasecmp(dt, "DLMS_ETHERNET"))
        return set_dlms_ethernet_cfg(ctx, cmd->data);

    return set_cfg_fail("DATA_TYPE", "Unsupported set_cfg DATA_TYPE '%s'", dt);
}

int set_cfg_export_json(redisContext *ctx, cmd_request_t *cmd)
{
    int rc;

    set_cfg_clear_error();

    if (!ctx || !cmd || !cmd->data)
        return set_cfg_fail("", "Internal error (no Redis context or DATA)");

    rc = set_cfg_dispatch(ctx, cmd);

    /* A handler that failed without naming a parameter */
    if (rc != 0 && g_set_cfg_err_reason[0] == '\0')
        set_cfg_fail("", "%s update failed", cmd->data_type_req);

    return rc;
}

/*
 * set_cfg_after_reply()
 * ---------------------
 * Called by processServerMsg() AFTER the set_cfg SUCCESS reply has been
 * published (mqtt_send_msg() blocks until the broker confirms delivery).
 * Restart requests that would cut this process's own MQTT link go here, so
 * the reply is never lost when the supervisor restarts re_mqtt_proc.
 */
void set_cfg_after_reply(redisContext *ctx, cmd_request_t *cmd)
{
    const char *dt;
    redisReply *reply;

    if (!ctx || !cmd)
        return;
    dt = cmd->data_type_req;

    if (!strcasecmp(dt, "MQTT_BROKER_1") || !strcasecmp(dt, "MQTT_BROKER_PRIMARY") ||
        !strcasecmp(dt, "MQTT_BROKER_2") || !strcasecmp(dt, "MQTT_BROKER_SECONDARY"))
    {
        /* Restart MQTT process (moved here from set_mqtt_cfg) */
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
        else
        {
            LOG_ERROR("Failed to execute Redis command: SADD proc_restart re_mqtt_proc");
        }

        /* Web UI update (as in the previous file) */
        web_ui_notify(ctx, "upstream");
    }
}