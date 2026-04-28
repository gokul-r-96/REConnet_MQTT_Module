/**
 * @file  modbus_json_export.c
 * @brief Generate a JSON snapshot of all Modbus device data from Redis
 *
 * Reads the following Redis hashes:
 *   DCU nameplate:     dcu_info
 *   modrtu device cfg: modrtu_serial{port}_{dev}_cfg
 *   modrtu reg cfg:    modrtu_serial{port}_{dev}_reg_{reg}_cfg
 *   modrtu values:     modrtu:{port}:{dev}:{reg}:{dev_name}
 *   modrtu status:     modrtu_serial{port}_{dev}_status
 *   modtcp device cfg: modtcp_{dev}_cfg
 *   modtcp reg cfg:    modtcp_{dev}_reg_{reg}_cfg
 *   modtcp values:     modtcp:eth:{dev}:{reg}:{dev_name}
 *   modtcp status:     modtcp_{dev}_status
 */

#include "modbus_json_export.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>

/* Max limits — must match module config headers */
#define MAX_DEVICES         32
#define MAX_REGS_PER_DEV    128

/* =========================================================================
 * Growable JSON buffer
 * ========================================================================= */

#define JBUF_INIT_CAP  8192

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} jbuf_t;

static int jbuf_init(jbuf_t *jb)
{
    jb->data = malloc(JBUF_INIT_CAP);
    if (!jb->data) return -1;
    jb->len = 0;
    jb->cap = JBUF_INIT_CAP;
    jb->data[0] = '\0';
    return 0;
}

static int jbuf_grow(jbuf_t *jb, size_t need)
{
    if (jb->len + need < jb->cap)
        return 0;
    size_t new_cap = jb->cap * 2;
    while (jb->len + need >= new_cap)
        new_cap *= 2;
    char *tmp = realloc(jb->data, new_cap);
    if (!tmp) return -1;
    jb->data = tmp;
    jb->cap = new_cap;
    return 0;
}

static void jbuf_append(jbuf_t *jb, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) return;

    if (jbuf_grow(jb, (size_t)n + 1) < 0) return;

    va_start(ap, fmt);
    vsnprintf(jb->data + jb->len, jb->cap - jb->len, fmt, ap);
    va_end(ap);
    jb->len += (size_t)n;
}

/* Remove trailing comma if present (for JSON arrays/objects) */
static void jbuf_trim_comma(jbuf_t *jb)
{
    while (jb->len > 0 &&
           (jb->data[jb->len - 1] == ',' ||
            jb->data[jb->len - 1] == '\n' ||
            jb->data[jb->len - 1] == ' ')) {
        jb->len--;
    }
    jb->data[jb->len] = '\0';
}

/* =========================================================================
 * Redis helpers (self-contained — no dependency on module headers)
 * ========================================================================= */

static int rget_str(redisContext *ctx, const char *key, const char *field,
                    char *buf, size_t buf_sz)
{
    redisReply *r = redisCommand(ctx, "HGET %s %s", key, field);
    if (!r || r->type != REDIS_REPLY_STRING || !r->str) {
        if (r) freeReplyObject(r);
        buf[0] = '\0';
        return 0;
    }
    snprintf(buf, buf_sz, "%s", r->str);
    freeReplyObject(r);
    return 1;
}

static int rget_int(redisContext *ctx, const char *key, const char *field,
                    int def)
{
    char buf[64];
    if (!rget_str(ctx, key, field, buf, sizeof(buf)))
        return def;
    return atoi(buf);
}

/* Check if a Redis hash key exists (has at least one field) */
static int rhash_exists(redisContext *ctx, const char *key)
{
    redisReply *r = redisCommand(ctx, "EXISTS %s", key);
    if (!r) return 0;
    int exists = (r->type == REDIS_REPLY_INTEGER && r->integer > 0);
    freeReplyObject(r);
    return exists;
}

/* =========================================================================
 * JSON escape helper (for string values that may contain special chars)
 * ========================================================================= */

static void jbuf_append_escaped(jbuf_t *jb, const char *s)
{
    jbuf_append(jb, "\"");
    for (; *s; s++) {
        switch (*s) {
        case '"':  jbuf_append(jb, "\\\""); break;
        case '\\': jbuf_append(jb, "\\\\"); break;
        case '\n': jbuf_append(jb, "\\n");  break;
        case '\r': jbuf_append(jb, "\\r");  break;
        case '\t': jbuf_append(jb, "\\t");  break;
        default:   jbuf_append(jb, "%c", *s); break;
        }
    }
    jbuf_append(jb, "\"");
}

/* =========================================================================
 * Value formatting: coil bits → JSON array, numeric → JSON number
 * ========================================================================= */

/*
 * Coil/discrete values are stored in Redis as dash-separated bit strings:
 *   "0-1-0-1-1-0-0-1"
 * Convert to JSON array: [0, 1, 0, 1, 1, 0, 0, 1]
 */
static void jbuf_append_coil_value(jbuf_t *jb, const char *val_str)
{
    jbuf_append(jb, "[");
    int first = 1;
    const char *p = val_str;
    while (*p) {
        if (*p == '-') { p++; continue; }
        if (*p == '0' || *p == '1') {
            if (!first) jbuf_append(jb, ", ");
            jbuf_append(jb, "%c", *p);
            first = 0;
        }
        p++;
    }
    jbuf_append(jb, "]");
}

/*
 * Numeric values: output as JSON number.
 * If the string contains a decimal point, output as float; otherwise int.
 */
static void jbuf_append_numeric_value(jbuf_t *jb, const char *val_str)
{
    if (strchr(val_str, '.')) {
        double v = atof(val_str);
        /* Trim trailing zeros but keep at least one decimal */
        char tmp[64];
        snprintf(tmp, sizeof(tmp), "%.6f", v);
        /* Remove trailing zeros */
        char *dot = strchr(tmp, '.');
        if (dot) {
            char *end = tmp + strlen(tmp) - 1;
            while (end > dot + 1 && *end == '0') *end-- = '\0';
        }
        jbuf_append(jb, "%s", tmp);
    } else {
        long v = atol(val_str);
        jbuf_append(jb, "%ld", v);
    }
}

/* =========================================================================
 * DCU nameplate
 * ========================================================================= */

static void export_dcu_nameplate(jbuf_t *jb, redisContext *ctx)
{
    const char *key = "dcu_info";
    char dcu_name[128], dcu_sernum[128];
    char attr1[128], attr2[128], attr3[128], attr4[128], attr5[128];

    // rget_str(ctx, key, "dcu_name",   dcu_name,   sizeof(dcu_name));
    // rget_str(ctx, key, "dcu_sernum", dcu_sernum,  sizeof(dcu_sernum));
    // rget_str(ctx, key, "attr1",      attr1,       sizeof(attr1));
    // rget_str(ctx, key, "attr2",      attr2,       sizeof(attr2));
    // rget_str(ctx, key, "attr3",      attr3,       sizeof(attr3));
    // rget_str(ctx, key, "attr4",      attr4,       sizeof(attr4));
    // rget_str(ctx, key, "attr5",      attr5,       sizeof(attr5));

    //Gokul added the below fields from redis ---> 08/04/2026
    rget_str(ctx, key, "device_name",   dcu_name,   sizeof(dcu_name));
    rget_str(ctx, key, "serial_num", dcu_sernum,  sizeof(dcu_sernum));
    rget_str(ctx, key, "dcu_uptime",      attr1,       sizeof(attr1));
    rget_str(ctx, key, "updatetime",      attr2,       sizeof(attr2));
    rget_str(ctx, key, "device_type",      attr3,       sizeof(attr3));
    rget_str(ctx, key, "fw_ver",      attr4,       sizeof(attr4));

    /* Current date/time */
    char datetime[32];
    time_t now = time(NULL);
    struct tm tm_buf;
    localtime_r(&now, &tm_buf);
    strftime(datetime, sizeof(datetime), "%Y-%m-%d %H:%M:%S", &tm_buf);

    jbuf_append(jb, "\"dcu_nameplate\":\n{\n");
    jbuf_append(jb, "    \"dcu_name\":"); jbuf_append_escaped(jb, dcu_name);   jbuf_append(jb, ",\n");
    jbuf_append(jb, "    \"dcu_sernum\":"); jbuf_append_escaped(jb, dcu_sernum); jbuf_append(jb, ",\n");
    jbuf_append(jb, "    \"dcu_uptime\":"); jbuf_append_escaped(jb, attr1); jbuf_append(jb, ",\n");
    jbuf_append(jb, "    \"updatetime\":"); jbuf_append_escaped(jb, attr2); jbuf_append(jb, ",\n");
    jbuf_append(jb, "    \"device_type\":"); jbuf_append_escaped(jb, attr3); jbuf_append(jb, ",\n");
    jbuf_append(jb, "    \"fw_ver\":"); jbuf_append_escaped(jb, attr4); jbuf_append(jb, ",\n");
    // jbuf_append(jb, "    \"attr5\":"); jbuf_append_escaped(jb, attr5); jbuf_append(jb, ",\n");
    jbuf_append(jb, "    \"date_time\":"); jbuf_append_escaped(jb, datetime); jbuf_append(jb, "\n");
    jbuf_append(jb, "}");
}

/* =========================================================================
 * Register export (shared for modrtu and modtcp)
 * ========================================================================= */

static void export_register(jbuf_t *jb, redisContext *ctx,
                             const char *reg_cfg_key,
                             const char *value_hash, int is_last)
{
    char name[64];
    if (!rget_str(ctx, reg_cfg_key, "name_id", name, sizeof(name)))
        return;  /* register config doesn't exist */

    int func_type  = rget_int(ctx, reg_cfg_key, "func_type", 3);
    int start_addr = rget_int(ctx, reg_cfg_key, "start_addr", 0);

    /* Read current value from data hash */
    char val_field[64];
    char val_str[128];
    snprintf(val_field, sizeof(val_field), "reg_%d", start_addr);
    if (!rget_str(ctx, value_hash, val_field, val_str, sizeof(val_str)))
        val_str[0] = '\0';

    int is_coil = (func_type == 1 || func_type == 2);  /* FC01 or FC02 */

    jbuf_append(jb, "{\n");
    jbuf_append(jb, "\"address\": %d,\n", start_addr);
    jbuf_append(jb, "\"name\": "); jbuf_append_escaped(jb, name); jbuf_append(jb, ",\n");
    jbuf_append(jb, "\"type\": %d,\n", func_type);
    jbuf_append(jb, "\"value\": ");

    if (val_str[0] == '\0') {
        jbuf_append(jb, "null");
    } else if (is_coil) {
        jbuf_append_coil_value(jb, val_str);
    } else {
        jbuf_append_numeric_value(jb, val_str);
    }

    jbuf_append(jb, "\n}%s\n", is_last ? "" : ",");
}

/* =========================================================================
 * Device export
 * ========================================================================= */

static int export_modrtu_device(jbuf_t *jb, redisContext *ctx,
                                 uint16_t port_id, uint16_t dev_idx,
                                 int is_last_device)
{
    char cfg_key[128];
    snprintf(cfg_key, sizeof(cfg_key), "modrtu_serial%u_%u_cfg",
             port_id, dev_idx);

    /* Check if device is enabled */
    int enabled = rget_int(ctx, cfg_key, "enable_device", -1);
    if (enabled <= 0)
        return 0;  /* not enabled or hash missing */

    char dev_name[64], loc_name[64];
    rget_str(ctx, cfg_key, "dev_name", dev_name, sizeof(dev_name));
    rget_str(ctx, cfg_key, "loc_name", loc_name, sizeof(loc_name));
    int slave_id   = rget_int(ctx, cfg_key, "slave_id", 1);
    int num_points = rget_int(ctx, cfg_key, "num_points", 0);

    /* Read status */
    char status_key[128];
    snprintf(status_key, sizeof(status_key), "modrtu_serial%u_%u_status",
             port_id, dev_idx);

    char status_str[64], last_comm[64];
    rget_str(ctx, status_key, "status", status_str, sizeof(status_str));
    rget_str(ctx, status_key, "last_communication", last_comm, sizeof(last_comm));

    /* Map status string */
    const char *status_out = "Not Connected";
    if (strcmp(status_str, "communicating") == 0)
        status_out = "Connected";

    jbuf_append(jb, "{\n");
    jbuf_append(jb,"\"type\": \"RTU\",\n");
    jbuf_append(jb, "\"slave_id\": %d,\n", slave_id);
    jbuf_append(jb, "\"ip_addr\": \"\",\n");  /* RTU — no IP */
    jbuf_append(jb, "\"last_update_time\": "); jbuf_append_escaped(jb, last_comm); jbuf_append(jb, ",\n");
    jbuf_append(jb, "\"name\": "); jbuf_append_escaped(jb, dev_name); jbuf_append(jb, ",\n");
    jbuf_append(jb, "\"num_vals\": %d,\n", num_points);
    jbuf_append(jb, "\"registers\":[\n");

    /* Export each register */
    int max_regs = num_points;
    if (max_regs > MAX_REGS_PER_DEV)
        max_regs = MAX_REGS_PER_DEV;

    int reg_written = 0;
    for (int r = 0; r < max_regs; r++) {
        char reg_cfg_key[128];
        snprintf(reg_cfg_key, sizeof(reg_cfg_key),
                 "modrtu_serial%u_%u_reg_%d_cfg", port_id, dev_idx, r);

        /* Check if register exists */
        char name_check[64];
        if (!rget_str(ctx, reg_cfg_key, "name_id", name_check, sizeof(name_check)))
            continue;

        /* Read func_type to filter — only include read function codes */
        int fc = rget_int(ctx, reg_cfg_key, "func_type", 3);
        if (fc > 4 && fc != 23)
            continue;  /* skip write-only FCs */

        /* Build value hash key */
        char value_hash[256];
        snprintf(value_hash, sizeof(value_hash), "modrtu:%u:%u:%d:%s",
                 port_id, dev_idx, r, dev_name);

        reg_written++;
        int is_last_reg = 0;

        /* Peek ahead to see if this is the last register */
        int next_exists = 0;
        for (int rr = r + 1; rr < max_regs; rr++) {
            char peek_key[128];
            snprintf(peek_key, sizeof(peek_key),
                     "modrtu_serial%u_%u_reg_%d_cfg", port_id, dev_idx, rr);
            char peek_name[64];
            if (rget_str(ctx, peek_key, "name_id", peek_name, sizeof(peek_name))) {
                int peek_fc = rget_int(ctx, peek_key, "func_type", 3);
                if (peek_fc <= 4 || peek_fc == 23) {
                    next_exists = 1;
                    break;
                }
            }
        }
        is_last_reg = !next_exists;

        export_register(jb, ctx, reg_cfg_key, value_hash, is_last_reg);
    }

    /* Handle empty register list */
    if (reg_written == 0) {
        /* nothing to trim */
    }

    jbuf_append(jb, "],\n");
    jbuf_append(jb, "\"status\": "); jbuf_append_escaped(jb, status_out);
    jbuf_append(jb, "\n}%s\n", is_last_device ? "" : ",");

    return 1;  /* device was exported */
}

static int export_modtcp_device(jbuf_t *jb, redisContext *ctx,
                                 uint16_t dev_idx, int is_last_device)
{
    char cfg_key[128];
    snprintf(cfg_key, sizeof(cfg_key), "modtcp_%u_cfg", dev_idx);

    /* Check if device is enabled */
    int enabled = rget_int(ctx, cfg_key, "enable_device", -1);
    if (enabled <= 0)
        return 0;

    char dev_name[64], dev_ip[48];
    rget_str(ctx, cfg_key, "dev_name", dev_name, sizeof(dev_name));
    rget_str(ctx, cfg_key, "dev_ip",   dev_ip,   sizeof(dev_ip));
    int slave_id   = rget_int(ctx, cfg_key, "slave_id", 1);
    int num_points = rget_int(ctx, cfg_key, "num_points", 0);

    /* Read status */
    char status_key[128];
    snprintf(status_key, sizeof(status_key), "modtcp_%u_status", dev_idx);

    char status_str[64], last_comm[64];
    rget_str(ctx, status_key, "status", status_str, sizeof(status_str));
    rget_str(ctx, status_key, "last_communication", last_comm, sizeof(last_comm));

    const char *status_out = "Not Connected";
    if (strcmp(status_str, "communicating") == 0)
        status_out = "Connected";

    jbuf_append(jb, "{\n");
    jbuf_append(jb,"\"type\": \"TCP\",\n");
    jbuf_append(jb, "\"slave_id\": %d,\n", slave_id);
    jbuf_append(jb, "\"ip_addr\": "); jbuf_append_escaped(jb, dev_ip); jbuf_append(jb, ",\n");
    jbuf_append(jb, "\"last_update_time\": "); jbuf_append_escaped(jb, last_comm); jbuf_append(jb, ",\n");
    jbuf_append(jb, "\"name\": "); jbuf_append_escaped(jb, dev_name); jbuf_append(jb, ",\n");
    jbuf_append(jb, "\"num_vals\": %d,\n", num_points);
    jbuf_append(jb, "\"registers\":[\n");

    int max_regs = num_points;
    if (max_regs > MAX_REGS_PER_DEV)
        max_regs = MAX_REGS_PER_DEV;

    int reg_written = 0;
    for (int r = 0; r < max_regs; r++) {
        char reg_cfg_key[128];
        snprintf(reg_cfg_key, sizeof(reg_cfg_key),
                 "modtcp_%u_reg_%d_cfg", dev_idx, r);

        char name_check[64];
        if (!rget_str(ctx, reg_cfg_key, "name_id", name_check, sizeof(name_check)))
            continue;

        int fc = rget_int(ctx, reg_cfg_key, "func_type", 3);
        if (fc > 4 && fc != 23)
            continue;

        char value_hash[256];
        snprintf(value_hash, sizeof(value_hash), "modtcp:eth:%u:%d:%s",
                 dev_idx, r, dev_name);

        reg_written++;

        /* Peek ahead for trailing comma */
        int next_exists = 0;
        for (int rr = r + 1; rr < max_regs; rr++) {
            char peek_key[128];
            snprintf(peek_key, sizeof(peek_key),
                     "modtcp_%u_reg_%d_cfg", dev_idx, rr);
            char peek_name[64];
            if (rget_str(ctx, peek_key, "name_id", peek_name, sizeof(peek_name))) {
                int peek_fc = rget_int(ctx, peek_key, "func_type", 3);
                if (peek_fc <= 4 || peek_fc == 23) {
                    next_exists = 1;
                    break;
                }
            }
        }

        export_register(jb, ctx, reg_cfg_key, value_hash, !next_exists);
    }

    jbuf_append(jb, "],\n");
    jbuf_append(jb, "\"status\": "); jbuf_append_escaped(jb, status_out);
    jbuf_append(jb, "\n}%s\n", is_last_device ? "" : ",");

    return 1;
}

/* =========================================================================
 * Public API
 * ========================================================================= */

char *modbus_export_json(redisContext *ctx, uint16_t num_serial_ports)
{
    if (!ctx)
        return NULL;

    jbuf_t jb;
    if (jbuf_init(&jb) < 0)
        return NULL;

    jbuf_append(&jb, "{\n");

    /* --- DCU nameplate --- */
    export_dcu_nameplate(&jb, ctx);
    jbuf_append(&jb, ",\n\n");

    /* --- Collect all enabled devices (modrtu + modtcp) to handle trailing comma --- */

    /* First pass: count total enabled devices */
    int total_devices = 0;

    for (uint16_t p = 0; p < num_serial_ports; p++) {
        for (uint16_t d = 0; d < MAX_DEVICES; d++) {
            char cfg_key[128];
            snprintf(cfg_key, sizeof(cfg_key), "modrtu_serial%u_%u_cfg", p, d);
            if (rget_int(ctx, cfg_key, "enable_device", 0) > 0)
                total_devices++;
        }
    }

    for (uint16_t d = 0; d < MAX_DEVICES; d++) {
        char cfg_key[128];
        snprintf(cfg_key, sizeof(cfg_key), "modtcp_%u_cfg", d);
        if (rget_int(ctx, cfg_key, "enable_device", 0) > 0)
            total_devices++;
    }

    /* --- Export devices --- */
    jbuf_append(&jb, "\"modbus_devices\":[\n");

    int dev_count = 0;

    /* modrtu devices */
    for (uint16_t p = 0; p < num_serial_ports; p++) {
        for (uint16_t d = 0; d < MAX_DEVICES; d++) {
            dev_count++;
            int is_last = (dev_count == total_devices);
            if (export_modrtu_device(&jb, ctx, p, d, is_last))
                ;  /* exported */
            else
                dev_count--;  /* wasn't exported, adjust count */
        }
    }

    /* modtcp devices */
    for (uint16_t d = 0; d < MAX_DEVICES; d++) {
        dev_count++;
        int is_last = (dev_count == total_devices);
        if (export_modtcp_device(&jb, ctx, d, is_last))
            ;
        else
            dev_count--;
    }

    /* Safety: trim any stray trailing comma */
    jbuf_trim_comma(&jb);
    jbuf_append(&jb, "\n]\n");

    jbuf_append(&jb, "}\n");

    return jb.data;
}
