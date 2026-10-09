/*
 * mqtt_cmd_validate.c
 * -------------------
 * Validation of MDAS -> DCU set_cfg and general commands (Reset,
 * START_TRANS_MODE, STOP_TRANS_MODE). See mqtt_cmd_validate.h.
 *
 * The spec gives message templates; its example values are not rules. Keys
 * the spec text calls mandatory must be present; every value that is present
 * gets the Web UI's validation for that field (e.g. an FTP REMOTE_DIRECTORY
 * like "D:\CMS_FTP_DATA" is rejected, as the UI rejects it).
 * MQTT_BROKER_PRIMARY and MQTT_BROKER_SECONDARY share one rule set.
 *
 * Every rule below names the Web UI rule it mirrors. Values may arrive as
 * JSON strings ("8883", as in the spec examples) or JSON numbers (8883).
 * Numbers must be plain digits (the UI's number inputs accept digits only).
 *
 * Status codes:
 *   8 (Invalid parameter)       - missing mandatory key, unknown key, duplicate
 *                                 key, bad envelope, nothing to set
 *   9 (Invalid parameter value) - value has the wrong type, format or range
 *   3 (Invalid meter name)      - ctx->meter_exists() says the meter is unknown
 *   7 (Unknown request)         - unrecognised COMMAND_TYPE
 *
 * NOT checked here (the UI checks these against the rest of the stored
 * configuration; do them where that config is available if needed):
 *   - MQTT: duplicate CLIENT_ID / BROKER_IP:PORT across brokers, primary rule
 *   - IEC104/101: DLMS/Modbus/command IOA zone overlap (iec10xDetectZoneOverlaps)
 *   - Modbus: duplicate device name / slave id / ip:port:slave across devices
 *   - COMMON_ADDR unique across all meters (validateCommonAddresses)
 *   - DLMS: duplicate meter name / address / IP within a port or Ethernet
 *   - FTP/NTP/meter IP vs. the DCU's own Ethernet IPs and gateways
 *   - RS232 "one enabled meter per port" rule
 */

#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mqtt_cmd_validate.h"

/* ================= Result helpers ================= */

static int fail(mqtt_val_result_t *res, int status, const char *field, const char *fmt, ...)
{
    va_list ap;

    if (res) {
        res->status = status;
        snprintf(res->field, sizeof(res->field), "%s", field ? field : "");
        va_start(ap, fmt);
        vsnprintf(res->msg, sizeof(res->msg), fmt, ap);
        va_end(ap);
    }
    return status;
}

static int ok(mqtt_val_result_t *res)
{
    if (res) {
        res->status = MQTT_CMD_ST_SUCCESS;
        res->field[0] = '\0';
        snprintf(res->msg, sizeof(res->msg), "SUCCESS");
    }
    return MQTT_CMD_ST_SUCCESS;
}

/* ================= String helpers ================= */

/* Case-insensitive compare that ignores leading/trailing whitespace in a
 * (the spec itself has " FetchDay " with spaces). */
static int name_eq(const char *a, const char *b)
{
    size_t la, lb, i;

    if (!a || !b)
        return 0;
    while (isspace((unsigned char)*a))
        a++;
    la = strlen(a);
    while (la > 0 && isspace((unsigned char)a[la - 1]))
        la--;
    lb = strlen(b);
    if (la != lb)
        return 0;
    for (i = 0; i < la; i++)
        if (tolower((unsigned char)a[i]) != tolower((unsigned char)b[i]))
            return 0;
    return 1;
}

static int is_blank(const char *s)
{
    for (; *s; s++)
        if (!isspace((unsigned char)*s))
            return 0;
    return 1;
}

/* Character count as the UI sees it (code points, not bytes). */
static long utf8_len(const char *s)
{
    long n = 0;

    for (; *s; s++)
        if (((unsigned char)*s & 0xC0) != 0x80)
            n++;
    return n;
}

/* Text of a string or number item. Numbers are rendered into buf.
 * Returns 0 if the item is neither (object, array, bool, null). */
static int item_text(const cJSON *it, char *buf, size_t len, const char **out)
{
    if (cJSON_IsString(it) && it->valuestring) {
        *out = it->valuestring;
        return 1;
    }
    if (cJSON_IsNumber(it)) {
        double d = it->valuedouble;
        if (d >= 0 && d < 1e9 && d == (double)(long)d)
            snprintf(buf, len, "%ld", (long)d);
        else
            snprintf(buf, len, "%g", d);
        *out = buf;
        return 1;
    }
    return 0;
}

/* Plain digits only, at most 9 of them (every limit here fits). */
static int parse_uint(const char *s, long *v)
{
    long n = 0;
    int nd = 0;

    if (!*s)
        return 0;
    for (; *s; s++) {
        if (!isdigit((unsigned char)*s) || ++nd > 9)
            return 0;
        n = n * 10 + (*s - '0');
    }
    *v = n;
    return 1;
}

/* ================= Address helpers ================= */

/* Shape a.b.c.d with digit groups; max_digits 3 = /\d{1,3}/ form, 0 = /\d+/.
 * Octet values are returned capped at 999; the caller range-checks them. */
static int parse_ipv4(const char *s, int max_digits, int oct[4])
{
    int i;

    for (i = 0; i < 4; i++) {
        long v = 0;
        int nd = 0;
        while (isdigit((unsigned char)*s)) {
            if (v < 1000)
                v = v * 10 + (*s - '0');
            nd++;
            s++;
        }
        if (nd == 0 || (max_digits && nd > max_digits))
            return 0;
        oct[i] = (int)(v > 999 ? 999 : v);
        if (i < 3) {
            if (*s != '.')
                return 0;
            s++;
        }
    }
    return *s == '\0';
}

/* UI isValidIPv4 (DeviceConfiguration.tsx): four numeric octets 0-255,
 * rejects 0.0.0.0 and 255.255.255.255. Leading zeros are accepted. */
static int is_ipv4_strict(const char *s)
{
    int o[4], i;

    if (!parse_ipv4(s, 0, o))
        return 0;
    for (i = 0; i < 4; i++)
        if (o[i] > 255)
            return 0;
    if (o[0] == 0 && o[1] == 0 && o[2] == 0 && o[3] == 0)
        return 0;
    if (o[0] == 255 && o[1] == 255 && o[2] == 255 && o[3] == 255)
        return 0;
    return 1;
}

/* UI HOSTNAME_PATTERN:
 * /^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$/ */
static int is_hostname(const char *s)
{
    const char *label = s;
    const char *dot;
    int nlabels = 0;
    size_t len, i;

    while ((dot = strchr(label, '.')) != NULL) {
        len = (size_t)(dot - label);
        if (len < 1 || len > 63)
            return 0;
        if (!isalnum((unsigned char)label[0]) || !isalnum((unsigned char)label[len - 1]))
            return 0;
        for (i = 0; i < len; i++)
            if (!isalnum((unsigned char)label[i]) && label[i] != '-')
                return 0;
        nlabels++;
        label = dot + 1;
    }
    if (nlabels == 0)
        return 0;
    len = strlen(label);
    if (len < 2)
        return 0;
    for (i = 0; i < len; i++)
        if (!isalpha((unsigned char)label[i]))
            return 0;
    return 1;
}

/* UI ipOrDomainSchema (NTP, FTP): if it looks like an IP
 * (/^\d{1,3}(\.\d{1,3}){3}$/) every octet must be 0-255 (0.0.0.0 allowed),
 * with no fall-through to the domain check; otherwise it must be a hostname. */
static int is_ip_or_domain(const char *s)
{
    int o[4], i;

    if (parse_ipv4(s, 3, o)) {
        for (i = 0; i < 4; i++)
            if (o[i] > 255)
                return 0;
        return 1;
    }
    return is_hostname(s);
}

/* ================= Field tables ================= */

enum {
    K_INT,          /* whole number lo..hi */
    K_INT_SET,      /* whole number from iset */
    K_STR,          /* string, length lo..hi (hi 0 = no max) */
    K_ENUM,         /* exact string from sset */
    K_ENUM_CI,      /* string from sset, case-insensitive */
    K_YESNO,        /* "YES" / "NO" */
    K_IPV4,         /* required, isValidIPv4 */
    K_IP_OR_DOMAIN, /* required, ipOrDomainSchema */
    K_RIGHT_IP,     /* required, isValidIPv4 (or hostname with v1.1) */
    K_DIAL,         /* modem dial number */
    K_FTP_DIR,      /* directorySchema */
    K_COMMON_ADDR   /* meter's own IEC104/101 Common Address */
};

#define F_MANDATORY 0x01u  /* must be present */
#define F_SELECTOR  0x02u  /* picks what is configured; not a value being set */
#define F_NONBLANK  0x04u  /* K_STR: trimmed value must not be empty */

typedef struct {
    const char        *key;
    int                kind;
    long               lo, hi;
    const long        *iset;   /* -1 terminated */
    const char *const *sset;   /* NULL terminated */
    unsigned           flags;
} fld_t;

struct grp;
typedef int (*post_fn)(const struct grp *g, const cJSON *data,
                       const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res);

typedef struct grp {
    const char  *data_type;
    const char  *alias;        /* alternative DATA_TYPE spelling, or NULL */
    const fld_t *fields;
    int          at_least_one; /* spec: "at least one of the remaining fields must be provided" */
    const char  *enable_key;   /* if this key is "NO", the other values are not checked */
    post_fn      post;
} grp_t;

#define FINT(k, lo, hi, f)  { k, K_INT, lo, hi, NULL, NULL, f }
#define FSET(k, set, f)     { k, K_INT_SET, 0, 0, set, NULL, f }
#define FSTR(k, lo, hi, f)  { k, K_STR, lo, hi, NULL, NULL, f }
#define FENUM(k, set, f)    { k, K_ENUM, 0, 0, NULL, set, f }
#define FENUMCI(k, set, f)  { k, K_ENUM_CI, 0, 0, NULL, set, f }
#define FKIND(k, kind, f)   { k, kind, 0, 0, NULL, NULL, f }
#define FEND                { NULL, 0, 0, 0, NULL, NULL, 0 }

#define NUM_MAX 999999999L

static const long SET_1_2[]          = { 1, 2, -1 };
/* FTPSection.tsx dropdown, stored as minutes by to_minutes_cms */
static const long SET_FTP_INTERVAL[] = { 5, 15, 30, 60, 240, 360, 720, 1440, -1 };
/* IEC104Section/IEC101Section.tsx cyclic interval dropdown, seconds, 0 = disabled */
static const long SET_IEC_CYCLIC[]   = { 0, 30, 60, 120, 180, 300, 600, 900, 1800, 3600, -1 };
/* SerialPortSection.tsx dropdowns */
static const long SET_BAUD[]         = { 1200, 2400, 4800, 9600, 19200, 38400, 57600, 115200, -1 };
static const long SET_DATA_BITS[]    = { 7, 8, -1 };
static const char *const SET_STOP_BITS[] = { "1", "1.5", "2", NULL };
static const char *const SET_PARITY[]    = { "none", "odd", "even", NULL };
/* Spec 4.5.1.5 NTP_INTERVAL, exact values only:
 *   "1"   = 1 day (24 hours)  - UI "24 hours", stored as interval 24
 *   "7"   = 1 week (7 days)   - UI "1 week",   stored as interval 168
 *   "NOW" = synchronise immediately */
static const char *const SET_NTP_INTERVAL[] = { "1", "7", "NOW", NULL };
/* ResetUnit.tsx TRANSPARENT_DURATION_OPTIONS / app.py allowed timeouts, minutes */
static const long SET_TRANS_DUR[]    = { 15, 30, 45, 60, 120, -1 };

/* --- set_cfg groups --- */

/* MQTTSection / deviceConfigSchema mqttServers */
static const fld_t F_MQTT[] = {
    FSTR("BROKER_IP", 0, 128, F_NONBLANK),     /* required, max 128, no format check in UI */
    FINT("BROKER_PORT", 1, 65535, 0),
    FSTR("CLIENT_ID", 0, 64, 0),
    FSTR("USERNAME", 0, 64, 0),
    FSTR("PASSWORD", 0, 64, 0),
    FINT("HEALTH_INTERVAL", 1, 1440, 0),       /* hc_pub_interval, minutes */
    FINT("INST_INTERVAL", 1, 1440, 0),         /* dlms_inst_pub_interval */
    FINT("METER_DATA_INTERVAL", 5, 1440, 0),   /* dlms_data_pub_interval */
    FINT("MODBUS_INTERVAL", 5, 1440, 0),       /* modbus_data_pub_interval */
    FEND
};

/* ModemSection / modemConfig (the active SIM's fields) */
static const fld_t F_MODEM[] = {
    FSET("SIM_SLOT", SET_1_2, F_MANDATORY | F_SELECTOR),
    FSTR("USERNAME", 0, 64, 0),
    FSTR("PASSWORD", 0, 64, 0),
    FSTR("APN", 0, 64, F_NONBLANK),            /* "APN is required for SIM n" */
    FKIND("DIAL_NUM", K_DIAL, 0),              /* phone_num1/2 */
    FEND
};

/* IPSecSection / ipsecTunnels */
static const fld_t F_IPSEC[] = {
    FSET("IPSEC_TUNNEL", SET_1_2, F_MANDATORY | F_SELECTOR),
    FKIND("ENABLE_TUNNEL", K_YESNO, 0),
    FKIND("RIGHT_IP", K_RIGHT_IP, 0),
    FSTR("LEFT_ID", 0, 64, 0),                 /* UI: max 64 only, no format */
    FSTR("LEFT_SUBNET", 0, 64, 0),
    FSTR("RIGHT_ID", 0, 64, 0),
    FSTR("RIGHT_SUBNET", 0, 64, 0),
    FSTR("TUNNEL_NAME", 0, 32, F_NONBLANK),
    FEND
};

/* NTPSection / ntpServers (UI validates these even for a disabled server) */
static const fld_t F_NTP[] = {
    FSET("NTP_SERVER", SET_1_2, F_MANDATORY | F_SELECTOR),
    FKIND("ENABLE_NTP", K_YESNO, 0),
    FKIND("NTP_IP", K_IP_OR_DOMAIN, 0),
    FINT("NTP_PORT", 1, 65535, 0),
    FENUM("NTP_INTERVAL", SET_NTP_INTERVAL, 0),
    FEND
};

/* FTPSection / ftpServers (UI validates these even for a disabled server) */
static const fld_t F_FTP[] = {
    FSET("FTP_SERVER", SET_1_2, F_MANDATORY | F_SELECTOR),
    FKIND("ENABLE_SERVER", K_YESNO, 0),
    FKIND("IP_ADDRESS", K_IP_OR_DOMAIN, 0),
    FINT("PORT", 1, 65535, 0),
    FSTR("USERNAME", 3, 128, 0),
    FSTR("PASSWORD", 3, 128, 0),
    FKIND("REMOTE_DIRECTORY", K_FTP_DIR, 0),
    FSET("TIME_INTERVAL", SET_FTP_INTERVAL, 0),
    FEND
};

/* IEC104Section / iec104Slaves. Base ranges here; size-dependent limits and
 * the offset gap rule are in post_iec(). */
static const fld_t F_IEC104[] = {
    FKIND("ENABLE_104", K_YESNO, 0),
    FINT("ASDU_ADDRESS", 0, 65535, 0),
    FSET("CYCLIC_INT", SET_IEC_CYCLIC, 0),
    FINT("DLMS_IOA_OFFSET", 0, NUM_MAX, 0),
    FINT("MODBUS_IOA_OFFSET", 0, NUM_MAX, 0),
    FINT("COMMANDS_IOA_OFFSET", 0, NUM_MAX, 0),
    FEND
};

static const fld_t F_IEC101[] = {
    FKIND("ENABLE_101", K_YESNO, 0),
    FINT("ASDU_ADDRESS", 0, 65535, 0),
    FSET("CYCLIC_INT", SET_IEC_CYCLIC, 0),
    FINT("DLMS_IOA_OFFSET", 0, NUM_MAX, 0),
    FINT("MODBUS_IOA_OFFSET", 0, NUM_MAX, 0),
    FINT("COMMANDS_IOA_OFFSET", 0, NUM_MAX, 0),
    FEND
};

/* SerialPortSection dropdowns (UI shows "None"/"Odd"/"Even") */
static const fld_t F_SERPORT[] = {
    FSET("SERIAL_PORT", SET_1_2, F_MANDATORY | F_SELECTOR),
    FSET("BAUD_RATE", SET_BAUD, 0),
    FENUMCI("PARITY", SET_PARITY, 0),
    FSET("DATA_BITS", SET_DATA_BITS, 0),
    FENUM("STOP_BITS", SET_STOP_BITS, 0),
    FEND
};

/* validateModbusTcpDevice */
static const fld_t F_MODTCP[] = {
    FSTR("METER_NAME", 0, 32, F_MANDATORY | F_SELECTOR | F_NONBLANK),
    FKIND("IP_ADDRESS", K_IPV4, 0),
    FINT("PORT", 1, 65535, 0),
    FINT("SLAVE_ID", 1, 247, 0),
    FINT("RETRIES", 0, 10, 0),
    FINT("POLL_SKIP_COUNT", 1, 100, 0),        /* poll_faulty_cnt */
    FINT("RESP_TIMEOUT", 1, 30, 0),            /* seconds (Redis stores ms) */
    FKIND("COMMON_ADDR", K_COMMON_ADDR, 0),
    FEND
};

/* validateModbusRtuDevice */
static const fld_t F_MODRTU[] = {
    FSET("SERIAL_PORT", SET_1_2, F_MANDATORY | F_SELECTOR),
    FSTR("METER_NAME", 0, 32, F_MANDATORY | F_SELECTOR | F_NONBLANK),
    FINT("SLAVE_ID", 1, 247, 0),
    FINT("RETRIES", 0, 10, 0),
    FINT("POLL_SKIP_COUNT", 1, 100, 0),
    FINT("RESP_TIMEOUT", 1, 30, 0),
    FKIND("COMMON_ADDR", K_COMMON_ADDR, 0),
    FEND
};

/* meterConfig serial1/serial2. Name: required, input maxLength 32.
 * Address: >= 1, no upper limit in the UI. LLS password: required for an
 * enabled meter. HLS password: no validation in the UI. */
static const fld_t F_DLMS_SERIAL[] = {
    FSET("SERIAL_PORT", SET_1_2, F_MANDATORY | F_SELECTOR),
    FSTR("METER_NAME", 0, 32, F_MANDATORY | F_SELECTOR | F_NONBLANK),
    FINT("METER_ADDRESS", 1, NUM_MAX, F_MANDATORY),
    FSTR("LLS_PASSWORD", 0, 0, F_NONBLANK),
    FSTR("HLS_PASSWORD", 0, 0, 0),
    FKIND("COMMON_ADDR", K_COMMON_ADDR, 0),
    FEND
};

/* meterConfig ethernet. IP: required only, the UI has no format check. */
static const fld_t F_DLMS_ETH[] = {
    FSTR("METER_NAME", 0, 32, F_MANDATORY | F_SELECTOR | F_NONBLANK),
    FINT("METER_ADDRESS", 1, NUM_MAX, F_MANDATORY),
    FSTR("IP_ADDR", 0, 0, F_NONBLANK),
    FSTR("LLS_PASSWORD", 0, 0, F_NONBLANK),
    FSTR("HLS_PASSWORD", 0, 0, 0),
    FKIND("COMMON_ADDR", K_COMMON_ADDR, 0),
    FEND
};

/* --- general command groups --- */

static const fld_t F_RESET[] = { FEND };

static const fld_t F_TRANS_START_SERIAL[] = {
    FSET("PORT", SET_1_2, F_MANDATORY | F_SELECTOR),
    FSET("DUR", SET_TRANS_DUR, F_MANDATORY),
    FEND
};
static const fld_t F_TRANS_START_ETH[] = {
    FSTR("METER", 0, 32, F_MANDATORY | F_SELECTOR | F_NONBLANK),
    FSET("DUR", SET_TRANS_DUR, F_MANDATORY),
    FEND
};
static const fld_t F_TRANS_START_FULL[] = {
    FSET("DUR", SET_TRANS_DUR, F_MANDATORY),
    FEND
};
static const fld_t F_TRANS_STOP_SERIAL[] = {
    FSET("PORT", SET_1_2, F_MANDATORY | F_SELECTOR),
    FEND
};
static const fld_t F_TRANS_STOP_ETH[] = {
    FSTR("METER", 0, 32, F_MANDATORY | F_SELECTOR | F_NONBLANK),
    FEND
};
static const fld_t F_TRANS_STOP_FULL[] = { FEND };

/* ================= Field checks ================= */

static const cJSON *get(const cJSON *obj, const char *key)
{
    return cJSON_GetObjectItemCaseSensitive(obj, key);
}

static const fld_t *find_field(const fld_t *fields, const char *key)
{
    for (; fields->key; fields++)
        if (strcmp(fields->key, key) == 0)
            return fields;
    return NULL;
}

static void list_iset(const long *set, char *buf, size_t len)
{
    size_t used = 0;

    buf[0] = '\0';
    for (; *set >= 0 && used < len; set++)
        used += (size_t)snprintf(buf + used, len - used, "%s%ld", used ? "," : "", *set);
}

static void list_sset(const char *const *set, char *buf, size_t len)
{
    size_t used = 0;

    buf[0] = '\0';
    for (; *set && used < len; set++)
        used += (size_t)snprintf(buf + used, len - used, "%s%s", used ? "," : "", *set);
}

static int check_field(const fld_t *f, const cJSON *it, const mqtt_val_ctx_t *ctx,
                       mqtt_val_result_t *res)
{
    char nbuf[48], list[96];
    const char *s, *p;
    const long *ip;
    const char *const *sp;
    long v, n;

    if (!item_text(it, nbuf, sizeof(nbuf), &s))
        return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s must be a string", f->key);

    switch (f->kind) {
    case K_INT:
        if (!parse_uint(s, &v))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be a whole number (got '%.40s')", f->key, s);
        if (v < f->lo || v > f->hi)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be %ld-%ld (got %ld)", f->key, f->lo, f->hi, v);
        break;

    case K_INT_SET:
        if (parse_uint(s, &v))
            for (ip = f->iset; *ip >= 0; ip++)
                if (*ip == v)
                    return 0;
        list_iset(f->iset, list, sizeof(list));
        return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                    "%s must be one of %s (got '%.40s')", f->key, list, s);

    case K_STR:
        if (!cJSON_IsString(it) && (f->flags & F_NONBLANK) == 0 && f->lo == 0 && f->hi == 0)
            break; /* no rule at all: accept a number too */
        if ((f->flags & F_NONBLANK) && is_blank(s))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s is required", f->key);
        n = utf8_len(s);
        if (n < f->lo)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be at least %ld characters", f->key, f->lo);
        if (f->hi && n > f->hi)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be %ld characters or less (got %ld)", f->key, f->hi, n);
        break;

    case K_ENUM:
    case K_ENUM_CI:
        for (sp = f->sset; *sp; sp++)
            if (f->kind == K_ENUM ? strcmp(s, *sp) == 0 : name_eq(s, *sp))
                return 0;
        list_sset(f->sset, list, sizeof(list));
        return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                    "%s must be one of %s (got '%.40s')", f->key, list, s);

    case K_YESNO:
        if (strcmp(s, "YES") != 0 && strcmp(s, "NO") != 0)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be YES or NO (got '%.40s')", f->key, s);
        break;

    case K_IPV4:
        if (is_blank(s))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s is required", f->key);
        if (!is_ipv4_strict(s))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s '%.40s' is not a valid IPv4 address", f->key, s);
        break;

    case K_IP_OR_DOMAIN:
        if (is_blank(s))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s is required", f->key);
        if (!is_ip_or_domain(s))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s '%.40s' is not a valid IP address or domain name", f->key, s);
        break;

    case K_RIGHT_IP:
        /* v1.1 off: isValidIPv4. v1.1 on: isValidIPv4OrHostname. Trimmed, required. */
        {
            char tb[256];
            size_t tl;
            while (isspace((unsigned char)*s))
                s++;
            tl = strlen(s);
            while (tl > 0 && isspace((unsigned char)s[tl - 1]))
                tl--;
            if (tl == 0)
                return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s is required", f->key);
            if (tl >= sizeof(tb))
                return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s is too long", f->key);
            memcpy(tb, s, tl);
            tb[tl] = '\0';
            if (!is_ipv4_strict(tb) && !(ctx->v11_enabled && is_hostname(tb)))
                return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                            ctx->v11_enabled ? "%s '%.40s' must be a valid IPv4 address or domain name"
                                             : "%s '%.40s' must be a valid IPv4 address",
                            f->key, tb);
        }
        break;

    case K_DIAL:
        /* Required for the active SIM, max 20, /^[0-9+\-\s#*]*$/ */
        if (is_blank(s))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s is required", f->key);
        n = utf8_len(s);
        if (n > 20)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be 20 characters or less (got %ld)", f->key, n);
        for (p = s; *p; p++)
            if (!isdigit((unsigned char)*p) && !isspace((unsigned char)*p) &&
                *p != '+' && *p != '-' && *p != '#' && *p != '*')
                return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                            "%s contains invalid characters (only digits, *, #, +, -, spaces allowed)",
                            f->key);
        break;

    case K_FTP_DIR:
        /* 3-255 characters, /^[a-zA-Z0-9_\-\/]+$/ */
        n = utf8_len(s);
        if (n < 3)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s cannot be empty", f->key);
        if (n > 255)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be 255 characters or less", f->key);
        for (p = s; *p; p++)
            if (!isalnum((unsigned char)*p) && *p != '_' && *p != '-' && *p != '/')
                return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                            "%s cannot contain spaces or special characters", f->key);
        break;

    case K_COMMON_ADDR:
        /* UI (v1.1 separate_common_addresses): optional, empty = use the IEC
         * instance's default CA; otherwise 1..commonAddressMax(), the smallest
         * limit among the ENABLED IEC104/101 instances (254 for a 1-byte ASDU
         * address, 65534 for 2 bytes; 65534 when neither is enabled). */
        if (!ctx->separate_common_addresses)
            return fail(res, MQTT_CMD_ST_INVALID_PARAM, f->key,
                        "%s is not supported (separate common addresses are off)", f->key);
        if (is_blank(s))
            break;
        n = 65534;
        if (ctx->iec104.enable && ctx->iec104.asdu_addr_size == 1)
            n = 254;
        if (ctx->iec101.enable && ctx->iec101.asdu_addr_size == 1)
            n = 254;
        if (!parse_uint(s, &v))
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be a whole number (got '%.40s')", f->key, s);
        if (v < 1 || v > n)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key,
                        "%s must be 1-%ld for the configured ASDU address size (got %ld)",
                        f->key, n, v);
        break;

    default:
        return fail(res, MQTT_CMD_ST_FAILED, f->key, "internal: unknown field kind");
    }
    return 0;
}

/* ================= Group engine ================= */

static int validate_group(const grp_t *g, const cJSON *data, const mqtt_val_ctx_t *ctx,
                          mqtt_val_result_t *res)
{
    const cJSON *it, *j;
    const fld_t *f;
    int updates = 0, disabled = 0, st;

    if (!cJSON_IsObject(data))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DATA", "DATA must be an object");

    /* 1. Every key is known for this DATA_TYPE and appears once */
    for (it = data->child; it; it = it->next) {
        if (!it->string)
            return fail(res, MQTT_CMD_ST_INVALID_PARAM, "", "DATA has an unnamed member");
        for (j = it->next; j; j = j->next)
            if (j->string && strcmp(j->string, it->string) == 0)
                return fail(res, MQTT_CMD_ST_INVALID_PARAM, it->string,
                            "Duplicate parameter %s", it->string);
        if (strcmp(it->string, "DCU") == 0)
            continue;
        if (!find_field(g->fields, it->string))
            return fail(res, MQTT_CMD_ST_INVALID_PARAM, it->string,
                        "Unknown parameter %s for %s", it->string, g->data_type);
    }

    /* 2. Mandatory keys */
    for (f = g->fields; f->key; f++)
        if ((f->flags & F_MANDATORY) && !get(data, f->key))
            return fail(res, MQTT_CMD_ST_INVALID_PARAM, f->key,
                        "Missing mandatory parameter %s", f->key);

    /* 3. Something to set */
    for (f = g->fields; f->key; f++)
        if (!(f->flags & F_SELECTOR) && get(data, f->key))
            updates++;
    if (g->at_least_one && updates == 0)
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "",
                    "At least one parameter to set is required for %s", g->data_type);

    /* 4. The UI skips checking a group it is disabling */
    if (g->enable_key) {
        it = get(data, g->enable_key);
        if (it && cJSON_IsString(it) && strcmp(it->valuestring, "NO") == 0)
            disabled = 1;
    }

    /* 5. Values */
    for (f = g->fields; f->key; f++) {
        it = get(data, f->key);
        if (!it)
            continue;
        if (disabled && !(f->flags & F_SELECTOR) && strcmp(f->key, g->enable_key) != 0) {
            if (!cJSON_IsString(it) && !cJSON_IsNumber(it))
                return fail(res, MQTT_CMD_ST_INVALID_VALUE, f->key, "%s must be a string", f->key);
            continue;
        }
        if ((st = check_field(f, it, ctx, res)) != 0)
            return st;
    }

    if (g->post)
        return g->post(g, data, ctx, res);
    return ok(res);
}

/* ================= Group post-checks ================= */

static int get_long(const cJSON *data, const char *key, long *v)
{
    char nbuf[48];
    const char *s;
    const cJSON *it = get(data, key);

    return it && item_text(it, nbuf, sizeof(nbuf), &s) && parse_uint(s, v);
}

/* deviceConfigSchema iec104Slaves / iec101Slaves superRefine: only when the
 * instance is enabled. Missing values come from the current configuration. */
static int post_iec(const char *enable_key, const mqtt_val_iec_cfg_t *cur,
                    const cJSON *data, mqtt_val_result_t *res)
{
    static const char *const off_keys[3] = {
        "DLMS_IOA_OFFSET", "MODBUS_IOA_OFFSET", "COMMANDS_IOA_OFFSET"
    };
    const cJSON *it;
    long v, asdu_max, ioa_max, off[3];
    int enabled = cur->enable, any_off = 0, i, k;

    it = get(data, enable_key);
    if (it && cJSON_IsString(it))
        enabled = strcmp(it->valuestring, "YES") == 0;
    if (!enabled)
        return ok(res);

    /* ASDU: 1..254 for a 1-byte ASDU address, 1..65534 for 2 bytes */
    asdu_max = cur->asdu_addr_size == 1 ? 254 : 65534;
    if (get_long(data, "ASDU_ADDRESS", &v) && (v < 1 || v > asdu_max))
        return fail(res, MQTT_CMD_ST_INVALID_VALUE, "ASDU_ADDRESS",
                    "ASDU_ADDRESS must be 1-%ld for %d-byte ASDU size (got %ld)",
                    asdu_max, cur->asdu_addr_size == 1 ? 1 : 2, v);

    /* IOA offsets: 0..255 / 0..65535 / 0..16777215 for a 1/2/3-byte IOA */
    ioa_max = cur->ioa_addr_size == 1 ? 255L : cur->ioa_addr_size == 2 ? 65535L : 16777215L;
    off[0] = cur->ioa_offset;
    off[1] = cur->ioa_offset_modbus;
    off[2] = cur->ioa_offset_command;
    for (i = 0; i < 3; i++) {
        if (!get_long(data, off_keys[i], &v))
            continue;
        any_off = 1;
        if (v > ioa_max)
            return fail(res, MQTT_CMD_ST_INVALID_VALUE, off_keys[i],
                        "%s must be 0-%ld for %d-byte IOA (got %ld)", off_keys[i], ioa_max,
                        cur->ioa_addr_size == 1 || cur->ioa_addr_size == 2 ? cur->ioa_addr_size : 3, v);
        off[i] = v;
    }

    /* Every pair of offsets at least 1000 apart (checked when an offset is being set) */
    if (any_off)
        for (i = 0; i < 3; i++)
            for (k = i + 1; k < 3; k++)
                if (labs(off[i] - off[k]) < 1000)
                    return fail(res, MQTT_CMD_ST_INVALID_VALUE, off_keys[k],
                                "IOA offsets must be at least 1000 apart (%s=%ld, %s=%ld, gap=%ld)",
                                off_keys[i], off[i], off_keys[k], off[k], labs(off[i] - off[k]));
    return ok(res);
}

static int post_iec104(const grp_t *g, const cJSON *data, const mqtt_val_ctx_t *ctx,
                       mqtt_val_result_t *res)
{
    (void)g;
    return post_iec("ENABLE_104", &ctx->iec104, data, res);
}

static int post_iec101(const grp_t *g, const cJSON *data, const mqtt_val_ctx_t *ctx,
                       mqtt_val_result_t *res)
{
    (void)g;
    return post_iec("ENABLE_101", &ctx->iec101, data, res);
}

static int check_meter(mqtt_meter_kind_t kind, const char *port_key, const char *name_key,
                       const cJSON *data, const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res)
{
    const cJSON *name = get(data, name_key);
    long port = 0;

    if (!ctx->meter_exists || !name || !cJSON_IsString(name))
        return ok(res);
    if (port_key && !get_long(data, port_key, &port))
        port = 0;
    if (!ctx->meter_exists(kind, (int)port, name->valuestring, ctx->user))
        return fail(res, MQTT_CMD_ST_INVALID_METER, name_key,
                    "Invalid meter name '%.40s'", name->valuestring);
    return ok(res);
}

static int post_modtcp(const grp_t *g, const cJSON *d, const mqtt_val_ctx_t *c, mqtt_val_result_t *r)
{
    (void)g;
    return check_meter(MQTT_METER_MODBUS_TCP, NULL, "METER_NAME", d, c, r);
}

static int post_modrtu(const grp_t *g, const cJSON *d, const mqtt_val_ctx_t *c, mqtt_val_result_t *r)
{
    (void)g;
    return check_meter(MQTT_METER_MODBUS_RTU, "SERIAL_PORT", "METER_NAME", d, c, r);
}

static int post_dlms_serial(const grp_t *g, const cJSON *d, const mqtt_val_ctx_t *c, mqtt_val_result_t *r)
{
    (void)g;
    return check_meter(MQTT_METER_DLMS_SERIAL, "SERIAL_PORT", "METER_NAME", d, c, r);
}

static int post_dlms_eth(const grp_t *g, const cJSON *d, const mqtt_val_ctx_t *c, mqtt_val_result_t *r)
{
    (void)g;
    return check_meter(MQTT_METER_DLMS_ETHERNET, NULL, "METER_NAME", d, c, r);
}

static int post_trans_eth(const grp_t *g, const cJSON *d, const mqtt_val_ctx_t *c, mqtt_val_result_t *r)
{
    (void)g;
    return check_meter(MQTT_METER_DLMS_ETHERNET, NULL, "METER", d, c, r);
}

/* ================= Group lists ================= */

static const grp_t SET_CFG_GROUPS[] = {
    { "MQTT_BROKER_PRIMARY",   NULL,          F_MQTT,        1, NULL,            NULL },
    { "MQTT_BROKER_SECONDARY", NULL,          F_MQTT,        1, NULL,            NULL },
    { "MODEM",                 NULL,          F_MODEM,       0, NULL,            NULL },
    { "IPSEC",                 NULL,          F_IPSEC,       1, "ENABLE_TUNNEL", NULL },
    { "NTP",                   NULL,          F_NTP,         1, NULL,            NULL },
    { "FTP",                   NULL,          F_FTP,         1, NULL,            NULL },
    { "IEC104",                NULL,          F_IEC104,      1, NULL,            post_iec104 },
    { "IEC101",                NULL,          F_IEC101,      1, NULL,            post_iec101 },
    { "SERPORT",               "SERIAL PORT", F_SERPORT,     1, NULL,            NULL },
    { "MODBUS_TCP",            NULL,          F_MODTCP,      1, NULL,            post_modtcp },
    { "MODBUS_RTU",            NULL,          F_MODRTU,      0, NULL,            post_modrtu },
    { "DLMS_SERIAL",           NULL,          F_DLMS_SERIAL, 0, NULL,            post_dlms_serial },
    { "DLMS_ETHERNET",         NULL,          F_DLMS_ETH,    0, NULL,            post_dlms_eth },
    { NULL, NULL, NULL, 0, NULL, NULL }
};

static const grp_t G_RESET = { "Reset", NULL, F_RESET, 0, NULL, NULL };

static const grp_t TRANS_START_GROUPS[] = {
    { "SERIAL",   NULL, F_TRANS_START_SERIAL, 0, NULL, NULL },
    { "ETHERNET", NULL, F_TRANS_START_ETH,    0, NULL, post_trans_eth },
    { "FULL",     NULL, F_TRANS_START_FULL,   0, NULL, NULL },
    { NULL, NULL, NULL, 0, NULL, NULL }
};

static const grp_t TRANS_STOP_GROUPS[] = {
    { "SERIAL",   NULL, F_TRANS_STOP_SERIAL, 0, NULL, NULL },
    { "ETHERNET", NULL, F_TRANS_STOP_ETH,    0, NULL, post_trans_eth },
    { "FULL",     NULL, F_TRANS_STOP_FULL,   0, NULL, NULL },
    { NULL, NULL, NULL, 0, NULL, NULL }
};

static const grp_t *find_group(const grp_t *groups, const char *data_type)
{
    for (; groups->data_type; groups++)
        if (name_eq(data_type, groups->data_type) ||
            (groups->alias && name_eq(data_type, groups->alias)))
            return groups;
    return NULL;
}

/* Recognised commands that this module leaves to their own handlers */
static const char *const NOT_VALIDATED_CMDS[] = {
    "GetDay", "FetchDay", "get_cfg", "ReadModbus", "SET_METER_CFG",
    "OD_TIMESYNC_MESSAGE", "OD_PROF_CAP_PERIOD_MESSAGE", NULL
};

/* ================= Public API ================= */

void mqtt_val_ctx_init(mqtt_val_ctx_t *ctx)
{
    static const mqtt_val_iec_cfg_t iec_default = { 1, 2, 3, 1000, 10000, 50000 };

    memset(ctx, 0, sizeof(*ctx));
    ctx->iec104 = iec_default;
    ctx->iec101 = iec_default;
}

int mqtt_validate_set_cfg(const char *data_type, const cJSON *data,
                          const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res)
{
    mqtt_val_ctx_t dflt;
    const grp_t *g;

    if (!ctx) {
        mqtt_val_ctx_init(&dflt);
        ctx = &dflt;
    }
    if (!data_type || is_blank(data_type))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DATA_TYPE", "DATA_TYPE is required for set_cfg");
    g = find_group(SET_CFG_GROUPS, data_type);
    if (!g)
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DATA_TYPE",
                    "Unknown set_cfg DATA_TYPE '%.40s'", data_type);
    return validate_group(g, data, ctx, res);
}

int mqtt_validate_general_cmd(const char *command_type, const char *data_type, const cJSON *data,
                              const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res)
{
    mqtt_val_ctx_t dflt;
    const grp_t *groups, *g;

    if (!ctx) {
        mqtt_val_ctx_init(&dflt);
        ctx = &dflt;
    }

    /* Reset: DATA carries only DCU (app.py /api/device/reset takes no parameters) */
    if (name_eq(command_type, "Reset"))
        return validate_group(&G_RESET, data, ctx, res);

    if (name_eq(command_type, "START_TRANS_MODE"))
        groups = TRANS_START_GROUPS;
    else if (name_eq(command_type, "STOP_TRANS_MODE"))
        groups = TRANS_STOP_GROUPS;
    else
        return fail(res, MQTT_CMD_ST_UNKNOWN_REQUEST, "COMMAND_TYPE", "Unknown request");

    if (!data_type || is_blank(data_type))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DATA_TYPE",
                    "DATA_TYPE (SERIAL, ETHERNET or FULL) is required");
    g = find_group(groups, data_type);
    if (!g)
        return fail(res, MQTT_CMD_ST_INVALID_VALUE, "DATA_TYPE",
                    "DATA_TYPE must be SERIAL, ETHERNET or FULL (got '%.40s')", data_type);
    return validate_group(g, data, ctx, res);
}

int mqtt_validate_command(const cJSON *root, const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res)
{
    mqtt_val_ctx_t dflt;
    const cJSON *type, *seq, *cmd, *dt, *data, *dcu;
    const char *dts = NULL;
    const char *const *nv;

    if (!ctx) {
        mqtt_val_ctx_init(&dflt);
        ctx = &dflt;
    }
    if (!cJSON_IsObject(root))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "", "Message is not a JSON object");

    type = get(root, "TYPE");
    if (!cJSON_IsString(type) || !name_eq(type->valuestring, "command"))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "TYPE", "TYPE must be \"command\"");

    seq = get(root, "SEQ_NUM");
    if (!seq || !((cJSON_IsString(seq) && !is_blank(seq->valuestring)) || cJSON_IsNumber(seq)))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "SEQ_NUM", "SEQ_NUM is required");

    cmd = get(root, "COMMAND_TYPE");
    if (!cJSON_IsString(cmd) || is_blank(cmd->valuestring))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "COMMAND_TYPE", "COMMAND_TYPE is required");

    dt = get(root, "DATA_TYPE");
    if (dt) {
        if (!cJSON_IsString(dt))
            return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DATA_TYPE", "DATA_TYPE must be a string");
        dts = dt->valuestring;
    }

    data = get(root, "DATA");
    if (!cJSON_IsObject(data))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DATA", "DATA object is required");

    dcu = get(data, "DCU");
    if (!cJSON_IsString(dcu) || is_blank(dcu->valuestring))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DCU", "DATA.DCU is required");
    if (ctx->dcu_serial && ctx->dcu_serial[0] && !name_eq(dcu->valuestring, ctx->dcu_serial))
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "DCU",
                    "DCU '%.40s' does not match this DCU", dcu->valuestring);

    if (name_eq(cmd->valuestring, "set_cfg"))
        return mqtt_validate_set_cfg(dts, data, ctx, res);

    if (name_eq(cmd->valuestring, "Reset") ||
        name_eq(cmd->valuestring, "START_TRANS_MODE") ||
        name_eq(cmd->valuestring, "STOP_TRANS_MODE"))
        return mqtt_validate_general_cmd(cmd->valuestring, dts, data, ctx, res);

    for (nv = NOT_VALIDATED_CMDS; *nv; nv++)
        if (name_eq(cmd->valuestring, *nv))
            return fail(res, MQTT_CMD_NOT_VALIDATED, "COMMAND_TYPE",
                        "%s is not validated by this module", *nv);

    return fail(res, MQTT_CMD_ST_UNKNOWN_REQUEST, "COMMAND_TYPE", "Unknown request");
}

int mqtt_validate_command_json(const char *json, const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res)
{
    cJSON *root;
    int st;

    if (!json)
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "", "Empty message");
    root = cJSON_Parse(json);
    if (!root)
        return fail(res, MQTT_CMD_ST_INVALID_PARAM, "", "Message is not valid JSON");
    st = mqtt_validate_command(root, ctx, res);
    cJSON_Delete(root);
    return st;
}
