/**
 * @file dlms_cdf_gen.c
 * @brief DLMS Meter CDF (Common Data Format) File Generator
 *
 * This module reads DLMS meter data from Redis and generates CDF XML files.
 * It supports multiple data types (Instantaneous, Load Profile, Billing, etc.)
 * and runs on the iMX board with rotating log support.
 *
 * Usage: dlms_cdf_gen <meter_serial_number> <data_type>
 *   data_type: 1=Instantaneous, 2=LoadProfile, 3=Billing, 4=Nameplate, 5=EventLog
 *
 * Dependencies: hiredis, cJSON, libxml2
 *
 * @author  Embedded Team
 * @date    2025
 */

#include "../include/general.h"
#include <ctype.h>
#include <unistd.h>
#include <sys/wait.h>

extern int event_cmd_redis_resp;
extern int ls_cmd_redis_resp;
extern int billing_cmd_redis_resp;
extern int midnight_cmd_redis_resp;
extern int get_day_cmd;
extern int check_redis_resp;

extern cmd_request_t cpy_cmd;
extern cmd_request_t fd_cmd;
extern int seq_num;

 int multi_month_billing = 0;
/**
 * @brief Return current date string (for filenames/CDF DATE attribute).
 * @param buf    Output buffer.
 * @param buflen Buffer length.
 */
void get_date_str(char *buf, size_t buflen)
{
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(buf, buflen, "%Y-%m-%d", t);
}

/* ============================================================
 *  Redis helpers
 * ============================================================ */

/**
 * @brief Open a Redis connection.
 * @return Pointer to redisContext, or NULL on failure.
 */
redisContext *redis_connect(void)
{
    struct timeval timeout = {REDIS_TIMEOUT_SEC, 0};
    redisContext *ctx = redisConnectWithTimeout(REDIS_HOST, REDIS_PORT, timeout);
    if (!ctx || ctx->err)
    {
        LOG_ERROR("Redis connect failed: %s",
                  ctx ? ctx->errstr : "allocation error");
        if (ctx)
            redisFree(ctx);
        return NULL;
    }
    LOG_INFO("Redis connected to %s:%d", REDIS_HOST, REDIS_PORT);
    return ctx;
}

/* F10 (review 03-Oct): safe string read from the meter details JSON.
 * Returns "" when the key is missing or the value is not a string, instead
 * of dereferencing NULL (which crashed the daemon on every cycle). */
/* Make a malloc'd/owned value safe inside a raw "%s" JSON string (NP block):
 * trim surrounding whitespace (modem IMEI arrives as "\n866..."), map other
 * control chars to space, '"' to '\'' and '\\' to '/'. NULL-safe. */
static void json_clean_inplace(char *s)
{
    size_t len, start = 0, i;
    if (!s)
        return;
    len = strlen(s);
    while (start < len && (unsigned char)s[start] <= ' ')
        start++;
    while (len > start && (unsigned char)s[len - 1] <= ' ')
        len--;
    if (start > 0)
        memmove(s, s + start, len - start);
    len -= start;
    s[len] = '\0';
    for (i = 0; i < len; i++)
    {
        unsigned char c = (unsigned char)s[i];
        if (c < 0x20 || c == 0x7f)
            s[i] = ' ';
        else if (c == '"')
            s[i] = '\'';
        else if (c == '\\')
            s[i] = '/';
    }
}

static const char *js_str(const cJSON *obj, const char *key)
{
    const cJSON *it = obj ? cJSON_GetObjectItem(obj, key) : NULL;
    if (cJSON_IsString(it) && it->valuestring)
        return it->valuestring;
    LOG_DEBUG("meter JSON: key '%s' missing or not a string", key);
    return "";
}

/* F8 (review 03-Oct): "HSCAN <hash> 0 MATCH <pat>" returns only the first
 * page (~10 slots) and MATCH filters after paging, so meters whose field was
 * not on page 1 were reported as missing at random. This follows the cursor
 * until a page with a match is found (or the scan ends). It returns a reply
 * of the same shape as a single HSCAN ([cursor, [k1, v1, ...]]), so callers
 * are unchanged. Caller frees with freeReplyObject(). */
redisReply *hscan_first_match(redisContext *ctx, const char *hash, const char *pattern)
{
    char cursor[32] = "0";
    int pages = 0;

    for (;;)
    {
        redisReply *r = redisCommand(ctx, "HSCAN %s %s MATCH %s COUNT 200",
                                     hash, cursor, pattern);

        if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2 ||
            r->element[0]->type != REDIS_REPLY_STRING ||
            r->element[1]->type != REDIS_REPLY_ARRAY)
            return r; /* let the caller's existing error path handle it */

        if (r->element[1]->elements > 0)
            return r; /* found a page with matches */

        snprintf(cursor, sizeof(cursor), "%s", r->element[0]->str);
        if (strcmp(cursor, "0") == 0 || ++pages > 10000)
            return r; /* scan finished, no match: empty page */

        freeReplyObject(r);
    }
}

/**
 * @brief Fetch a field from a Redis hash.
 *
 * Caller must free() the returned string.
 *
 * @param ctx    Redis context.
 * @param hash   Hash name.
 * @param field  Field (key) within the hash.
 * @return       Allocated string with the value, or NULL.
 */
char *redis_hget(redisContext *ctx, const char *hash, const char *field)
{
    redisReply *reply = (redisReply *)redisCommand(ctx, "HGET %s %s", hash, field);
    if (!reply)
    {
        LOG_ERROR("HGET %s %s: no reply", hash, field);
        return NULL;
    }
    char *result = NULL;
    if (reply->type == REDIS_REPLY_STRING && reply->str)
    {
        result = strdup(reply->str);
    }
    else
    {
        LOG_WARN("HGET %s %s: key not found or wrong type", hash, field);
    }
    freeReplyObject(reply);
    return result;
}

/* ============================================================
 *  OBIS conversion utilities
 * ============================================================ */

/**
 * @brief Convert a decimal OBIS string to hex-octet CDF format.
 *
 * Input:  "1_0_31_7_0_255"
 * Output: "01_00_1f_07_00_ff"
 *
 * @param obis_dec  Decimal OBIS string (underscore-separated).
 * @param obis_hex  Output buffer (must be at least 24 bytes).
 * @return          0 on success, -1 on parse error.
 */
int obis_dec_to_hex(const char *obis_dec, char *obis_hex)
{
    unsigned int a, b, c, d, e, f;
    /* OBIS groups are separated by underscores */
    int parsed = sscanf(obis_dec, "%u_%u_%u_%u_%u_%u", &a, &b, &c, &d, &e, &f);
    if (parsed != 6)
    {
        LOG_ERROR("obis_dec_to_hex: cannot parse '%s'", obis_dec);
        return -1;
    }
    snprintf(obis_hex, 24, "%02x_%02x_%02x_%02x_%02x_%02x", a, b, c, d, e, f);
    return 0;
}


/* ============================================================
 *  Instantaneous data: Redis → InstSnapshot
 * ============================================================ */

/**
 * @brief Read instantaneous meter data from Redis and populate an InstSnapshot.
 *
 * Reads the JSON stored in REDIS_HASH_INST_INFO and uses the
 * obis_list/val_list directly. No OBIS parameter mapping is performed.
 *
 * @param ctx        Redis context.
 * @param serial     Meter serial number string.
 * @param snapshot   Output structure (caller must call inst_snapshot_free()).
 * @return           0 on success, -1 on error.
 */
static int read_instantaneous_data(redisContext *ctx,
                                   const char *serial,
                                   InstSnapshot *snapshot)
{
    memset(snapshot, 0, sizeof(*snapshot));
    snprintf(snapshot->meter_serial, sizeof(snapshot->meter_serial), "%s", serial);

    /* Build the field name */
    char field[64];
    snprintf(field, sizeof(field), "meter_*_*_%s", serial);

    LOG_INFO("Fetching instantaneous data: hash=%s field=%s",
             REDIS_HASH_INST_INFO, field);

    redisReply *r = hscan_first_match(ctx, REDIS_HASH_INST_INFO, field);

    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2)
    {
        LOG_ERROR("%s entry missing for %s", REDIS_HASH_INST_INFO, field);
        if (r)
            freeReplyObject(r);
        return -1;
    }

    redisReply *data = r->element[1];

    if (data->type != REDIS_REPLY_ARRAY || data->elements == 0)
    {
        LOG_ERROR("No matching fields found");
        freeReplyObject(r);
        return -1;
    }

    char *json_str = NULL;

    for (size_t i = 0; i < data->elements; i += 2)
    {
        char *field_name = data->element[i]->str;
        char *value = data->element[i + 1]->str;

        if (strstr(field_name, serial))
        {
            json_str = strdup(value);
            break;
        }
    }

    if (!json_str)
    {
        LOG_ERROR("No instantaneous data found for meter %s", serial);
        freeReplyObject(r);
        return -1;
    }

    /* DR-40: the whole instantaneous JSON was printed to stdout here every cycle */

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    freeReplyObject(r);

    if (!root)
    {
        LOG_ERROR("Failed to parse instantaneous JSON for meter %s", serial);
        return -1;
    }

    /* Extract obis_list and val_list arrays */
    cJSON *obis_arr = cJSON_GetObjectItemCaseSensitive(root, "obis_list");
    cJSON *val_arr = cJSON_GetObjectItemCaseSensitive(root, "val_list");

    if (!cJSON_IsArray(obis_arr) || !cJSON_IsArray(val_arr))
    {
        LOG_ERROR("Missing obis_list or val_list in JSON for meter %s", serial);
        cJSON_Delete(root);
        return -1;
    }

    int total = cJSON_GetArraySize(obis_arr);
    LOG_INFO("Meter %s: found %d OBIS entries", serial, total);

    /* Allocate parameter array (at most total - 1 real params, timestamp excluded) */
    snapshot->params = (InstParam *)calloc(total, sizeof(InstParam));
    if (!snapshot->params)
    {
        LOG_ERROR("Memory allocation failed for InstParam array");
        cJSON_Delete(root);
        return -1;
    }

    int val_count = cJSON_GetArraySize(val_arr);
    int param_idx = 0;

    for (int i = 0; i < total && i < val_count; i++)
    {
        cJSON *obis_item = cJSON_GetArrayItem(obis_arr, i);
        cJSON *val_item = cJSON_GetArrayItem(val_arr, i);

        if (!cJSON_IsString(obis_item) || !obis_item->valuestring)
            continue;
        if (!cJSON_IsString(val_item) || !val_item->valuestring)
            continue;

        const char *obis = obis_item->valuestring;
        const char *val = val_item->valuestring;

        /* First entry is the timestamp OBIS */
        if (strcmp(obis, OBIS_TIMESTAMP) == 0)
        {
            snprintf(snapshot->snapshot_date, sizeof(snapshot->snapshot_date),
                     "%s.000", val);
            LOG_DEBUG("Snapshot timestamp: %s", snapshot->snapshot_date);
            continue;
        }

        /* Resolve OBIS → hex */
        char obis_hex[24];
        if (obis_dec_to_hex(obis, obis_hex) != 0)
        {
            LOG_WARN("Skipping unparseable OBIS: %s", obis);
            continue;
        }

        /* Populate parameter entry */
        InstParam *p = &snapshot->params[param_idx];
        snprintf(p->obis_code, sizeof(p->obis_code), "%s", obis);
        snprintf(p->obis_hex, sizeof(p->obis_hex), "%s", obis_hex);
        snprintf(p->value, sizeof(p->value), "%s", val);

        /* No OBIS mapping for instantaneous data.
         * Keep the OBIS and value exactly from obis_list/val_list.
         * param_code, param_name and unit remain empty. */
        p->param_code[0] = '\0';
        p->param_name[0] = '\0';
        p->unit[0] = '\0';

        LOG_DEBUG("Param[%d]: obis=%s hex=%s val=%s",
                  param_idx, p->obis_code, p->obis_hex, p->value);

        param_idx++;
    }

    snapshot->param_count = param_idx;
    cJSON_Delete(root);

    LOG_INFO("Meter %s: parsed %d parameters (timestamp: %s)",
             serial, snapshot->param_count, snapshot->snapshot_date);
    return 0;
}

/**
 * @brief Free memory owned by an InstSnapshot.
 * @param snapshot Pointer to the snapshot to free.
 */
static void inst_snapshot_free(InstSnapshot *snapshot)
{
    if (snapshot && snapshot->params)
    {
        free(snapshot->params);
        snapshot->params = NULL;
    }
}

/* ============================================================
 *  CDF XML generation helpers
 * ============================================================ */

/**
 * @brief Write the CDF XML file header (<?xml ...> + root element open tag).
 * @param fp      Output file pointer.
 * @param date    Date string for the DATE attribute.
 */
void cdf_write_header(FILE *fp, const char *date)
{
    fprintf(fp, "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    fprintf(fp,
            "<CDF>\n"
            "\t<UTILITYTYPE CODE=\"%s\" DATE=\"%s\" DATA_TYPE=\"%s\">\n",
            UTILITY_CODE, date, UTILITY_DATA_TYPE);
}

/**
 * @brief Write the GENERAL section (DCU + meter details).
 * @param fp      Output file pointer.
 * @param serial  Meter serial number.
 * @param dt_str  Current date-time string (for the TIME attribute).
 */
void cdf_write_general(redisContext *ctx, FILE *fp, const char *serial, const char *dt_str)
{
    (void)serial; /* Future: look up meter-specific DCU details */

    char field_key[128];
    snprintf(field_key, sizeof(field_key),
             "meter_*_*_%s_details", serial);

    LOG_INFO("Fetching IPaddress and meter name from meter_status[%s]", field_key);

    redisReply *r = hscan_first_match(ctx, "meter_status", field_key);

    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2)
    {
        LOG_ERROR("meter_status entry missing for %s", field_key);
        if (r)
            freeReplyObject(r);
        return;
    }

    redisReply *data = r->element[1];

    if (data->type != REDIS_REPLY_ARRAY || data->elements == 0)
    {
        LOG_ERROR("No matching meter_status entry for %s", field_key);
        freeReplyObject(r);
        return;
    }

    char *json_str = NULL;

    for (size_t i = 0; i < data->elements; i += 2)
    {
        char *key = data->element[i]->str;
        char *value = data->element[i + 1]->str;

        if (strstr(key, serial))
        {
            json_str = value;
            break;
        }
    }

    if (!json_str)
    {
        LOG_ERROR("No valid JSON found for %s", field_key);
        freeReplyObject(r);
        return;
    }

    char *json_copy = strdup(json_str);
    freeReplyObject(r);

    cJSON *j = cJSON_Parse(json_copy);
    free(json_copy);
    if (!j)
    {
        LOG_ERROR("Failed to parse D1 JSON");

        return;
    }

    const char *location = js_str(j, "location");
    const char *port = js_str(j, "port");

    char *dcu_name = redis_hget(ctx, DCU_HASH, "device");
    char *attr1 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute1");
    char *attr2 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute2");
    char *attr3 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute3");
    char *attr4 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute4");
    char *attr5 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute5");
    char *dcu_ser = redis_hget(ctx, DCU_HASH, "serial_num");

    if (strcmp(port, "2") == 0)
    {
        const char *met_id = js_str(j, "met_id");
        int int_met_id = atoi(met_id);
        char ip_addr_key[64] = {0};

        snprintf(ip_addr_key, sizeof(ip_addr_key), "ip_addr[%d]", int_met_id);
        char *ipv4_address = redis_hget(ctx, "ethernet_meter_cfg", ip_addr_key);

        fprintf(fp,
                "\t\t<GENERAL>\n"
                "\t\t\t<DCU_DETAILS"
                " Name=\"%s\""
                " ATTRIBUTE_1=\"%s\""
                " ATTRIBUTE_2=\"%s\""
                " ATTRIBUTE_3=\"%s\""
                " ATTRIBUTE_4=\"%s\""
                " ATTRIBUTE_5=\"%s\""
                " SERIALNUM=\"%s\""
                " TIME=\"%s\"/>\n"
                "\t\t\t<METER_DETAILS BAY=\"%s\" IP_ADDRESS=\"%s\"/>\n"
                "\t\t</GENERAL>\n",
                dcu_name, attr1, attr2, attr3,
                attr4, attr5, dcu_ser, dt_str,
                location, ipv4_address);

        free(ipv4_address);
    }
    else
    {
        fprintf(fp,
                "\t\t<GENERAL>\n"
                "\t\t\t<DCU_DETAILS"
                " Name=\"%s\""
                " ATTRIBUTE_1=\"%s\""
                " ATTRIBUTE_2=\"%s\""
                " ATTRIBUTE_3=\"%s\""
                " ATTRIBUTE_4=\"%s\""
                " ATTRIBUTE_5=\"%s\""
                " SERIALNUM=\"%s\""
                " TIME=\"%s\"/>\n"
                "\t\t\t<METER_DETAILS BAY=\"%s\" IP_ADDRESS=\"\"/>\n"
                "\t\t</GENERAL>\n",
                dcu_name, attr1, attr2, attr3,
                attr4, attr5, dcu_ser, dt_str,
                location);
    }
}

/**
 * @brief Write the D1 (Nameplate) section with fixed macro values.
 *
 * In a future version these will be fetched from Redis per the
 * meter's nameplate profile.
 *
 * @param fp Output file pointer.
 */
// static void cdf_write_d1(FILE *fp)
// {
//     fprintf(fp, "\t\t<!--Nameplate Profile-->\n\t\t<D1>\n");

// #define NP_ENTRY(code, obis, name, val)                     \
//     fprintf(fp,                                             \
//             "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\"" \
//             " NAME=\"%s\" VALUE=\"%s\"/>\n",                \
//             (code), (obis), (name), (val))

//     NP_ENTRY("G1", OBIS_METER_SERIAL, "Meter_Serial_Number", NP_METER_SERIAL_VAL);
//     NP_ENTRY("G22", OBIS_MANUFACTURER, "Manufacturer_Name", NP_MANUFACTURER_VAL);
//     NP_ENTRY("G17", OBIS_FW_VERSION, "Firmware_Version", NP_FW_VERSION_VAL);
//     NP_ENTRY("G15", OBIS_METER_TYPE, "Meter_Type", NP_METER_TYPE_VAL);
//     NP_ENTRY("G8", OBIS_CT_RATIO, "Internal_CT_Ratio", NP_CT_RATIO_VAL);
//     NP_ENTRY("G7", OBIS_VT_RATIO, "Internal_VT_Ratio", NP_VT_RATIO_VAL);

// #undef NP_ENTRY

//     fprintf(fp, "\t\t</D1>\n");
// }

void cdf_write_d1(FILE *out, redisContext *rc, const char *meter_sn)
{
    char field_key[128];
    snprintf(field_key, sizeof(field_key),
             "meter_*_*_%s_details", meter_sn);

    LOG_INFO("Fetching D1 from meter_status[%s]", field_key);

    redisReply *r = hscan_first_match(rc, "meter_status", field_key);

    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2)
    {
        LOG_ERROR("meter_status entry missing for %s", field_key);
        if (r)
            freeReplyObject(r);
        return;
    }

    redisReply *data = r->element[1];

    if (data->type != REDIS_REPLY_ARRAY || data->elements == 0)
    {
        LOG_ERROR("No matching meter_status entry for %s", field_key);
        freeReplyObject(r);
        return;
    }

    char *json_str = NULL;

    for (size_t i = 0; i < data->elements; i += 2)
    {
        char *key = data->element[i]->str;
        char *value = data->element[i + 1]->str;

        if (strstr(key, meter_sn))
        {
            json_str = value;
            break;
        }
    }

    if (!json_str)
    {
        LOG_ERROR("No valid JSON found for %s", field_key);
        freeReplyObject(r);
        return;
    }

    char *json_copy = strdup(json_str);
    freeReplyObject(r);

    cJSON *j = cJSON_Parse(json_copy);
    free(json_copy);
    if (!j)
    {
        LOG_ERROR("Failed to parse D1 JSON");

        return;
    }

    const char *serial_number = js_str(j, "serial_number");
    const char *update_time = js_str(j, "update_time");
    const char *pt_ratio = js_str(j, "PT_ratio");
    const char *ct_ratio = js_str(j, "CT_ratio");
    const char *meter_type = js_str(j, "meter_type");
    const char *firmware_version = js_str(j, "firmware_version");
    const char *manufacturer = js_str(j, "manufacturer");
    const char *meter_category = js_str(j, "meter_category");
    const char *curr_rating = js_str(j, "current_rating");
    const char *year_of_manuf = js_str(j, "year_of_manufacture");
    // rithika 28May2026
    // const char *ipv4_address = cJSON_GetObjectItem(j, "ipv4_address")->valuestring;
    const char *hdlc_device_address = js_str(j, "hdlc_device_address");
    const char *tranfmr_volt = js_str(j, "transfrmr_volt");
    const char *extra_obis_1 = js_str(j, "extra_obis_1");
    const char *extra_obis_2 = js_str(j, "extra_obis_2");
    const char *extra_obis_3 = js_str(j, "extra_obis_3");

    // rithika 28May2026
    const char *port = js_str(j, "port");

    fprintf(out, "\t\t<!--Nameplate Profile-->\n\t\t<D1>\n");

    if (strcmp(port, "2") == 0)
    {
        const char *met_id = js_str(j, "met_id");
        int int_met_id = atoi(met_id);
        char ip_addr_key[64] = {0};
        snprintf(ip_addr_key, sizeof(ip_addr_key), "ip_addr[%d]", int_met_id);
        char *ipv4_address = redis_hget(rc, "ethernet_meter_cfg", ip_addr_key);
        printf("ipv4_address %s ip_addr_key %s\n", ipv4_address, ip_addr_key);

        fprintf(out,
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n",
                "G1", OBIS_METER_SERIAL, "Meter_Serial_Number", serial_number,
                "G22", OBIS_MANUFACTURER, "Manufacturer_Name", manufacturer,
                "G17", OBIS_FW_VERSION, "Firmware_Version", firmware_version,
                "G15", OBIS_METER_TYPE, "Meter_Type", meter_type,
                "G8", OBIS_CT_RATIO, "Internal_CT_Ratio", ct_ratio, /* DR-01: was swapped */
                "G7", OBIS_VT_RATIO, "Internal_VT_Ratio", pt_ratio,
                "", OBIS_METER_CATEGORY, "meter_category", meter_category,
                "", OBIS_CURR_RATING, "current_rating", curr_rating,
                "", OBIS_YR_OF_MANUF, "year_of_manufacture", year_of_manuf,
                "", EXTRA_OBIS_1, "", extra_obis_1,
                "", IPV4_ADDRESS, "", ipv4_address,
                "", EXTRA_OBIS_2, "", extra_obis_2,
                "", HDLC_SETUP, "", hdlc_device_address,
                "", TRANSFRMR_RATIO_VOLTAGE, "", tranfmr_volt,
                "", EXTRA_OBIS_3, "", extra_obis_3);
    }
    else
    {
        fprintf(out,
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n"
                "\t\t\t<NAMEPARAM CODE=\"%s\" OBIS_CODE=\"%s\" NAME=\"%s\" VALUE=\"%s\"/>\n",
                "G1", OBIS_METER_SERIAL, "Meter_Serial_Number", serial_number,
                "G22", OBIS_MANUFACTURER, "Manufacturer_Name", manufacturer,
                "G17", OBIS_FW_VERSION, "Firmware_Version", firmware_version,
                "G15", OBIS_METER_TYPE, "Meter_Type", meter_type,
                "G8", OBIS_CT_RATIO, "Internal_CT_Ratio", ct_ratio, /* DR-01: was swapped */
                "G7", OBIS_VT_RATIO, "Internal_VT_Ratio", pt_ratio,
                "", OBIS_METER_CATEGORY, "meter_category", meter_category,
                "", OBIS_CURR_RATING, "current_rating", curr_rating,
                "", OBIS_YR_OF_MANUF, "year_of_manufacture", year_of_manuf,
                "", EXTRA_OBIS_1, "", extra_obis_1,
                "", IPV4_ADDRESS, "", "",
                "", EXTRA_OBIS_2, "", extra_obis_2,
                "", HDLC_SETUP, "", hdlc_device_address,
                "", TRANSFRMR_RATIO_VOLTAGE, "", tranfmr_volt,
                "", EXTRA_OBIS_3, "", extra_obis_3);
    }
    fprintf(out, "\t\t</D1>\n");
    cJSON_Delete(j);

    LOG_INFO("D1 section written");
}

// cJSON *get_param_name_json(redisContext *ctx, char *key)
// {
//     redisReply *rly = redisCommand(ctx, "Hget datatype_param_name %s", key);
//     if (!rly || rly->type != REDIS_REPLY_STRING)
//     {
//         LOG_WARN("Redis key %s not found in the hash %s or not string\n", key, "datatype_param_name");
//         if (rly)
//             freeReplyObject(rly);
//         return NULL;
//     }

//     cJSON *root = cJSON_Parse(rly->str);

//     freeReplyObject(rly);

//     if (!root || !cJSON_IsObject(root))
//     {
//         LOG_WARN("Invalid JSON object\n");
//         if (root)
//             cJSON_Delete(root);
//         return NULL;
//     }

//     return root;
// }

// int chk_param_name_hash_exists(redisContext *ctx, char *key)
// {
//     redisReply *rly = redisCommand(ctx, "HEXISTS datatype_param_name %s", key);

//     if (!rly)
//     {
//         LOG_WARN("Redis command failed");
//         return -1;
//     }

//     int ret = -1;

//     if (rly->type == REDIS_REPLY_INTEGER && rly->integer == 1)
//     {
//         ret = 0; // exists
//     }
//     else
//     {
//         LOG_WARN("%s is not available in redis hash datatype_param_name", key);
//     }

//     freeReplyObject(rly);

//     return ret;
// }

/**
 * @brief Write the D2 (Instantaneous) section from a populated InstSnapshot.
 * @param fp        Output file pointer.
 * @param snapshot  Populated snapshot data.
 */
static void cdf_write_d2(FILE *fp, redisContext *ctx, const InstSnapshot *snapshot)
{
    // cJSON *root = NULL;
    // int paramname_avlb = chk_param_name_hash_exists(ctx, "inst_param");

    // if (paramname_avlb == 0) // 0 means it is available in redis
    // {
    //     root = get_param_name_json(ctx, "inst_param");
    //     if (!root)
    //         return;
    // }

    fprintf(fp, "\t\t<!--Instantaneous Profile-->\n\t\t<D2>\n");
    fprintf(fp, "\t\t\t<SNAPSHOT DATE=\"%s\">\n", snapshot->snapshot_date);

    for (int i = 0; i < snapshot->param_count; i++)
    {
        const InstParam *p = &snapshot->params[i];
        // const char *name = p->param_name; // default fallback

        // if (paramname_avlb == 0) // 0 means it is available in redis
        // {
        //     cJSON *item = cJSON_GetObjectItem(root, p->obis_code);

        //     if (item && cJSON_IsString(item) &&
        //         item->valuestring && item->valuestring[0] != '\0')
        //     {
        //         name = item->valuestring;
        //     }
        // }

        if (p->param_name[0] != '\0')
        {
            fprintf(fp,
                    "\t\t\t\t<INSTAPARAM"
                    " CODE=\"%s\""
                    " OBIS_CODE=\"%s\""
                    " NAME=\"%s\""
                    " VALUE=\"%s\""
                    " UNIT=\"%s\"/>\n",
                    p->param_code,
                    p->obis_hex,
                    p->param_name, // name,
                    p->value,
                    p->unit);
        }
    }

    fprintf(fp, "\t\t\t</SNAPSHOT>\n");
    fprintf(fp, "\t\t</D2>\n");

    // if (root)
    //     cJSON_Delete(root);
}

/**
 * @brief Write the closing tags of the CDF XML document.
 * @param fp Output file pointer.
 */
void cdf_write_footer(FILE *fp)
{
    fprintf(fp, "\t</UTILITYTYPE>\n</CDF>\n");
}

/* ============================================================
 *  CDF file generation entry points per data type
 * ============================================================ */

/**
 * @brief Generate a CDF file for Instantaneous data (type 1).
 *
 * @param ctx     Redis context.
 * @param serial  Meter serial number.
 * @return        0 on success, -1 on error.
 */
// cdf_result_t generate_instantaneous_cdf(redisContext *ctx, const char *serial)
// {
//     cdf_result_t result;
//     result.status = -1;
//     result.filesize = 0;
//     result.filename[0] = '\0';

//     LOG_INFO("Generating Instantaneous CDF for meter %s", serial);

//     /* 1. Read data from Redis */
//     InstSnapshot snapshot;
//     if (read_instantaneous_data(ctx, serial, &snapshot) != 0)
//     {
//         return result;
//     }

//     /* 2. Build output file path */
//     char date_str[32], dt_str[32];
//     get_date_str(date_str, sizeof(date_str));
//     get_datetime_str(dt_str, sizeof(dt_str));

//     // rithika 17Feb2026
//     // mkdir(CDF_OUTPUT_DIR, 0755);
//     // snprintf(out_path, sizeof(out_path),
//     //          "%sCDF_INST_%s_%s.xml", CDF_OUTPUT_DIR, serial, date_str);

//     // rithika 22Jun2026
//     char base_path[256];

//     if (get_base_path(base_path, sizeof(base_path)) == 0)
//     {
//         snprintf(result.filename,
//                  sizeof(result.filename),
//                  "%s/data/CDF_INST_%s_%s.xml",
//                  base_path, serial, date_str);

//         // snprintf(result.filename, sizeof(result.filename),
//         //          "%sCDF_INST_%s_%s.xml", CDF_OUTPUT_DIR, serial, date_str);
//     }

//     FILE *fp = fopen(result.filename, "w");
//     if (!fp)
//     {
//         LOG_ERROR("Cannot open output file: %s (%s)", result.filename, strerror(errno));
//         inst_snapshot_free(&snapshot);
//         return result;
//     }

//     /* 3. Write CDF XML */
//     cdf_write_header(fp, date_str);
//     cdf_write_general(ctx, fp, serial, dt_str);
//     cdf_write_d1(fp, ctx, serial);
//     cdf_write_d2(fp, ctx, &snapshot);
//     cdf_write_footer(fp);

//     /* Get file size */
//     fseek(fp, 0, SEEK_END);
//     result.filesize = ftell(fp);

//     fclose(fp);
//     inst_snapshot_free(&snapshot);
//     result.status = 0;

//     LOG_INFO("Instantaneous CDF written: %s", result.filename);
//     printf("CDF file generated: %s\n", result.filename);
//     return result;
// }

/* DR-29: write s as the body of a JSON string (SEQ_NUM comes from the server) */
static void fput_json_escaped(FILE *fp, const char *s)
{
    for (; s && *s; s++)
    {
        unsigned char c = (unsigned char)*s;
        if (c == '"' || c == '\\')
            fprintf(fp, "\\%c", c);
        else if (c < 0x20)
            fprintf(fp, "\\u%04x", c);
        else
            fputc(c, fp);
    }
}

/*
 * Cyclic message sequence number
 * ------------------------------
 * One counter for ALL cyclic messages (HEALTH, INST_DATA, METER_DATA and
 * MODBUS_DATA), so every message the DCU publishes gets the next number.
 * Uses the existing global seq_num (main.c sets it to 1 at start-up).
 *
 * The messages are built by different threads, so the counter is taken
 * with an atomic fetch-and-add: two messages can never get the same number
 * and no number is skipped by a race. Format "%04u": 0001 .. 9999, then it
 * wraps to 0001 (CYCLIC_SEQ_MAX).
 */
#define CYCLIC_SEQ_MAX 9999u

static unsigned int next_cyclic_seq(void)
{
    unsigned int v = (unsigned int)__sync_fetch_and_add(&seq_num, 1);
    unsigned int s = v % CYCLIC_SEQ_MAX;

    return s ? s : CYCLIC_SEQ_MAX;
}

void cyclic_seq_str(char *buf, size_t len)
{
    snprintf(buf, len, "%04u", next_cyclic_seq());
}

void json_write_header(FILE *fp, char *data_Type)
{
    printf("111111111111\n");
    printf("cpy cmd transc = %s", cpy_cmd.transaction);
    fprintf(fp, "{\n");

    if (event_cmd_redis_resp == 1 || ls_cmd_redis_resp == 1 || billing_cmd_redis_resp == 1 || midnight_cmd_redis_resp == 1 || get_day_cmd == 1)
    {
        fprintf(fp, "  \"TYPE\": \"OD_RESP_MESSAGE\",\n");
    }
    else
    {
        fprintf(fp, "  \"TYPE\": \"CYCLIC_MESSAGE\",\n");
    }

    if (get_day_cmd)
    {

        fprintf(fp, "  \"SEQ_NUM\": \"");
        fput_json_escaped(fp, cpy_cmd.transaction);
        fprintf(fp, "\",\n");
    }
    else if (check_redis_resp) /* DR-07: FetchDay data keeps its own SEQ_NUM */
    {
        fprintf(fp, "  \"SEQ_NUM\": \"");
        fput_json_escaped(fp, fd_cmd.transaction);
        fprintf(fp, "\",\n");
    }
    else
    {
        char seq[16];

        cyclic_seq_str(seq, sizeof(seq)); /* cyclic: next shared number */
        fprintf(fp, "  \"SEQ_NUM\": \"%s\",\n", seq);
    }

    fprintf(fp, "  \"DATATYPE\": \"%s\",\n", data_Type);
}

/*
 * clean_modem_field()
 * -------------------
 * The modem scripts store the raw AT response in Redis, e.g.
 *   imei     = "\r\n860710088983823\r\n\r\nOK\r\n"
 *   operator = "\r\nViIndia\r\n\r\nOK "
 * Printed as-is, the CR/LF break the JSON (a raw newline inside a JSON string
 * is invalid) and "OK" ends up in the value. Keep only the useful text:
 *   - the first line that is not empty, "OK", "ERROR" or an echoed "AT+..."
 *   - for a line like  +COPS: 0,0,"Vi India",7  the part inside the quotes
 *   - digits_only (IMEI): only 0-9
 *   - otherwise: no control characters, quotes or backslashes, trimmed,
 *     and no trailing " OK" (also when CR/LF were already turned into spaces)
 * Works in place (the result is never longer than the input).
 */
static void clean_modem_field(char *s, int digits_only)
{
    char *line, *end, *next, *q1, *q2, *w, *r;
    size_t n = 0;
    int found = 0;

    if (s == NULL)
        return;

    line = s;
    end = s;
    while (*line)
    {
        end = line;
        while (*end && *end != '\r' && *end != '\n')
            end++;
        next = end;
        while (*next == '\r' || *next == '\n')
            next++;

        while (line < end && isspace((unsigned char)*line))
            line++;
        while (end > line && isspace((unsigned char)end[-1]))
            end--;
        n = (size_t)(end - line);

        if (n == 0 ||
            (n == 2 && memcmp(line, "OK", 2) == 0) ||
            (n == 5 && memcmp(line, "ERROR", 5) == 0) ||
            (n >= 3 && (memcmp(line, "AT+", 3) == 0 || memcmp(line, "at+", 3) == 0)))
        {
            line = next;
            continue;
        }
        found = 1;
        break;
    }

    if (!found)
    {
        s[0] = '\0';
        return;
    }

    /* +COPS: 0,0,"Vi India",7  ->  Vi India */
    q1 = memchr(line, '"', n);
    if (q1 != NULL)
    {
        q2 = memchr(q1 + 1, '"', (size_t)(end - q1 - 1));
        if (q2 != NULL)
        {
            line = q1 + 1;
            n = (size_t)(q2 - line);
        }
    }

    memmove(s, line, n);
    s[n] = '\0';

    /* filter characters */
    for (r = s, w = s; *r; r++)
    {
        unsigned char c = (unsigned char)*r;

        if (digits_only ? isdigit(c) : !(iscntrl(c) || c == '"' || c == '\\'))
            *w++ = (char)c;
    }
    *w = '\0';

    /* final trim */
    while (w > s && isspace((unsigned char)w[-1]))
        *--w = '\0';
    for (r = s; *r && isspace((unsigned char)*r); r++)
        ;
    if (r != s)
        memmove(s, r, strlen(r) + 1);

    /* A reader that already turned CR/LF into spaces leaves "ViIndia    OK":
     * drop a trailing " OK" / " ERROR" word and a value that is only that. */
    for (;;)
    {
        size_t len = strlen(s);

        if (len >= 3 && strcmp(s + len - 2, "OK") == 0 && isspace((unsigned char)s[len - 3]))
            s[len - 2] = '\0';
        else if (len >= 6 && strcmp(s + len - 5, "ERROR") == 0 && isspace((unsigned char)s[len - 6]))
            s[len - 5] = '\0';
        else
            break;
        len = strlen(s);
        while (len > 0 && isspace((unsigned char)s[len - 1]))
            s[--len] = '\0';
    }
    if (strcmp(s, "OK") == 0 || strcmp(s, "ERROR") == 0)
        s[0] = '\0';
}

void json_write_general(redisContext *ctx, FILE *fp, const char *serial, const char *dt_str)
{
    (void)serial; /* Future: look up meter-specific DCU details */

    char field_key[128];
    snprintf(field_key, sizeof(field_key),
             "meter_*_*_%s_details", serial);

    LOG_INFO("Fetching IPaddress and meter name from meter_status[%s]", field_key);

    redisReply *r = hscan_first_match(ctx, "meter_status", field_key);

    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2)
    {
        LOG_ERROR("meter_status entry missing for %s", field_key);
        if (r)
            freeReplyObject(r);
        return;
    }

    redisReply *data = r->element[1];

    if (data->type != REDIS_REPLY_ARRAY || data->elements == 0)
    {
        LOG_ERROR("No matching meter_status entry for %s", field_key);
        freeReplyObject(r);
        return;
    }

    char *json_str = NULL;

    for (size_t i = 0; i < data->elements; i += 2)
    {
        char *key = data->element[i]->str;
        char *value = data->element[i + 1]->str;

        if (strstr(key, serial))
        {
            json_str = value;
            break;
        }
    }

    if (!json_str)
    {
        LOG_ERROR("No valid JSON found for %s", field_key);
        freeReplyObject(r);
        return;
    }

    char *json_copy = strdup(json_str);
    freeReplyObject(r);

    cJSON *j = cJSON_Parse(json_copy);
    free(json_copy);

    if (!j)
    {
        LOG_ERROR("Failed to parse meter JSON");
        return;
    }

    const char *location = js_str(j, "location");
    const char *port = js_str(j, "port");

    char *dcu_name = redis_hget(ctx, DCU_HASH, "device");
    char *attr1 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute1");
    char *attr2 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute2");
    char *attr3 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute3");
    char *attr4 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute4");
    char *attr5 = redis_hget(ctx, HASH_GENERAL_CDF, "attribute5");
    char *dcu_ser = redis_hget(ctx, DCU_HASH, "serial_num");
    char *fw_ver = redis_hget(ctx, DCU_HASH, "fw_ver");
    char *modem_imei = redis_hget(ctx, "modem_status", "imei");
    clean_modem_field(modem_imei, 1); /* raw AT response -> digits only (NULL-safe) */
    char *dcu_loc = redis_hget(ctx, DCU_HASH, "dcu_loc");

    /* DR-05: these go into JSON via raw "%s" below */
    json_clean_inplace(dcu_name);
    json_clean_inplace(attr1);
    json_clean_inplace(attr2);
    json_clean_inplace(attr3);
    json_clean_inplace(attr4);
    json_clean_inplace(attr5);
    json_clean_inplace(dcu_ser);
    json_clean_inplace(fw_ver);
    json_clean_inplace(modem_imei);
    json_clean_inplace(dcu_loc);

    fprintf(fp, "  \"NP\": {\n");
    fprintf(fp, "    \"DEVNAME\": \"%s\",\n", dcu_name);
    fprintf(fp, "    \"DEV_LOC\": \"%s\",\n", dcu_loc);
    fprintf(fp, "    \"SN\": \"%s\",\n", dcu_ser);
    fprintf(fp, "    \"IMEI\": \"%s\",\n", modem_imei);
    fprintf(fp, "    \"FW_VER\": \"%s%s%s\",\n", fw_ver, FW_BUILD_SEP, BUILD_ID);
    fprintf(fp, "    \"TS\": \"%s\"\n", dt_str);
    fprintf(fp, "  },\n");

    fprintf(fp, "  \"DCU_DETAILS\": {\n");
    fprintf(fp, "    \"ATTRIBUTE_1\": \"%s\",\n", attr1);
    fprintf(fp, "    \"ATTRIBUTE_2\": \"%s\",\n", attr2);
    fprintf(fp, "    \"ATTRIBUTE_3\": \"%s\",\n", attr3);
    fprintf(fp, "    \"ATTRIBUTE_4\": \"%s\",\n", attr4);
    fprintf(fp, "    \"ATTRIBUTE_5\": \"%s\"\n", attr5);
    fprintf(fp, "  },\n");

    fprintf(fp, "  \"DATA\": {\n");

    fprintf(fp, "    \"METER_DETAILS\": {\n");
    fprintf(fp, "      \"BAY\": \"%s\",\n", location);

    if (strcmp(port, "2") == 0)
    {
        const char *met_id = js_str(j, "met_id");

        int int_met_id = atoi(met_id);

        char ip_addr_key[64] = {0};

        snprintf(ip_addr_key,
                 sizeof(ip_addr_key),
                 "ip_addr[%d]",
                 int_met_id);

        char *ipv4_address =
            redis_hget(ctx,
                       "ethernet_meter_cfg",
                       ip_addr_key);

        fprintf(fp,
                "      \"IP_ADDRESS\": \"%s\"\n",
                ipv4_address ? ipv4_address : "");

        if (ipv4_address)
            free(ipv4_address);
    }
    else
    {
        fprintf(fp,
                "      \"IP_ADDRESS\": \"\"\n");
    }

    fprintf(fp, "    },\n");

    fprintf(fp,
            "    \"FIELDS\": [\"OBIS_CODE\", \"VALUE\"],\n");

    cJSON_Delete(j);

    free(dcu_name);
    free(attr1);
    free(attr2);
    free(attr3);
    free(attr4);
    free(attr5);
    free(dcu_ser);
    free(fw_ver);
    free(modem_imei);
    free(dcu_loc);
}

void json_write_d1(FILE *out, redisContext *rc, const char *meter_sn)
{
    char field_key[128];

    snprintf(field_key, sizeof(field_key),
             "meter_*_*_%s_details", meter_sn);

    LOG_INFO("Fetching D1 from meter_status[%s]", field_key);

    redisReply *r = hscan_first_match(rc, "meter_status", field_key);

    if (!r || r->type != REDIS_REPLY_ARRAY || r->elements != 2)
    {
        LOG_ERROR("meter_status entry missing for %s", field_key);

        if (r)
            freeReplyObject(r);

        return;
    }

    redisReply *data = r->element[1];

    if (data->type != REDIS_REPLY_ARRAY || data->elements == 0)
    {
        LOG_ERROR("No matching meter_status entry");

        freeReplyObject(r);
        return;
    }

    char *json_str = NULL;

    for (size_t i = 0; i < data->elements; i += 2)
    {
        char *key = data->element[i]->str;
        char *value = data->element[i + 1]->str;

        if (strstr(key, meter_sn))
        {
            json_str = strdup(value);
            break;
        }
    }

    freeReplyObject(r);

    if (!json_str)
    {
        LOG_ERROR("No JSON found");
        return;
    }

    cJSON *j = cJSON_Parse(json_str);
    free(json_str);

    if (!j)
    {
        LOG_ERROR("JSON Parse failed");
        return;
    }

    const char *serial_number =
        js_str(j, "serial_number");

    const char *pt_ratio =
        js_str(j, "PT_ratio");

    const char *ct_ratio =
        js_str(j, "CT_ratio");

    const char *meter_type =
        js_str(j, "meter_type");

    const char *firmware_version =
        js_str(j, "firmware_version");

    const char *manufacturer =
        js_str(j, "manufacturer");

    const char *meter_category =
        js_str(j, "meter_category");

    const char *curr_rating =
        js_str(j, "current_rating");

    const char *year_of_manuf =
        js_str(j, "year_of_manufacture");

    const char *ipv4_address =
        js_str(j, "ipv4_address");

    const char *hdlc_device_address =
        js_str(j, "hdlc_device_address");

    const char *tranfmr_volt =
        js_str(j, "transfrmr_volt");

    const char *extra_obis_1 =
        js_str(j, "extra_obis_1");

    const char *extra_obis_2 =
        js_str(j, "extra_obis_2");

    const char *extra_obis_3 =
        js_str(j, "extra_obis_3");


    /*
     * NAMEPLATE_PROFILE contains only:
     *
     * [OBIS_CODE, VALUE]
     *
     * No CODE, NAME or UNIT.
     */

    fprintf(out, "    \"NAMEPLATE_PROFILE\": [\n");

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_METER_SERIAL,
            serial_number);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_MANUFACTURER,
            manufacturer);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_FW_VERSION,
            firmware_version);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_METER_TYPE,
            meter_type);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_CT_RATIO,
            ct_ratio); /* DR-01: CT OBIS gets CT_ratio (was PT) */

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_VT_RATIO,
            pt_ratio);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_METER_CATEGORY,
            meter_category);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_CURR_RATING,
            curr_rating);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            OBIS_YR_OF_MANUF,
            year_of_manuf);

    /* DR-35: EXTRA_OBIS_2 has the same OBIS as IPV4_ADDRESS
     * (00_00_19_01_00_ff); write the key once - the IP if known, else the
     * extra_obis_2 value - instead of two rows with the same key. */
    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            IPV4_ADDRESS,
            ipv4_address[0] ? ipv4_address : extra_obis_2);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            HDLC_SETUP,
            hdlc_device_address);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            TRANSFRMR_RATIO_VOLTAGE,
            tranfmr_volt);

    fprintf(out,
            "      [\"%s\",\"%s\"],\n",
            EXTRA_OBIS_1,
            extra_obis_1);

    fprintf(out,
            "      [\"%s\",\"%s\"]\n",
            EXTRA_OBIS_3,
            extra_obis_3);

    fprintf(out, "    ],\n");

    cJSON_Delete(j);

    LOG_INFO("JSON D1 written");
}

void json_write_d2(FILE *fp, redisContext *ctx, const InstSnapshot *snapshot)
{
    (void)ctx;

    fprintf(fp, "    \"INSTANTANEOUS_PROFILE\": {\n");
    fprintf(fp, "      \"DATE\": \"%s\",\n", snapshot->snapshot_date);
    fprintf(fp, "      \"VALUES\": [\n");

    int first = 1;

    for (int i = 0; i < snapshot->param_count; i++)
    {
        const InstParam *p = &snapshot->params[i];

        /* No mapping is used for instantaneous data.
         * Therefore do not filter on param_name. Write every OBIS/value pair. */
        if (!first)
            fprintf(fp, ",\n");

        fprintf(fp,
                "        [\"%s\",\"%s\"]",
                p->obis_hex,
                p->value);

        first = 0;
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ]\n");
    fprintf(fp, "    }\n");
}

void json_write_footer(FILE *fp)
{
    fprintf(fp, "  }\n");
    fprintf(fp, "}\n");
}

cdf_result_t generate_instantaneous_json(redisContext *ctx, const char *serial)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';
    LOG_INFO("Generating Instantaneous JSON for meter %s", serial);
    /* Read data from Redis */
    InstSnapshot snapshot;
    if (read_instantaneous_data(ctx, serial, &snapshot) != 0)
    {
        return result;
    }
    char date_str[32];
    char dt_str[32];
    get_date_str(date_str, sizeof(date_str));
    get_datetime_str(dt_str, sizeof(dt_str));
    /* DR-40: write under <base>/data/ like the other generators, not into the
     * process's working directory */
    char base_path[256];
    if (get_base_path(base_path, sizeof(base_path)) == 0)
        snprintf(result.filename, sizeof(result.filename), "%s/data/INST_%s_%s.json", base_path, serial, date_str);
    else
        snprintf(result.filename, sizeof(result.filename), "INST_%s_%s.json", serial, date_str);
    FILE *fp = fopen(result.filename, "w");

    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s", result.filename);

        inst_snapshot_free(&snapshot);

        return result;
    }

    /* JSON writing functions - we'll create these next */
    json_write_header(fp, "INST_DATA_MESSAGE");
    json_write_general(ctx, fp, serial, dt_str);
    json_write_d1(fp, ctx, serial);
    json_write_d2(fp, ctx, &snapshot);
    json_write_footer(fp);

    fflush(fp);
    fseek(fp, 0, SEEK_END);

    result.filesize = ftell(fp);
    fclose(fp);
    inst_snapshot_free(&snapshot);
    result.status = 0;
    LOG_INFO("Instantaneous JSON written: %s", result.filename);

    return result;
}

/**
 * @brief Stub for Nameplate-only CDF generation (data type 4).
 *
 * @note  To be implemented in a future version.
 */
static int generate_nameplate_cdf(redisContext *ctx, const char *serial)
{

    LOG_INFO("Generating Nameplate-only CDF for meter %s", serial);

    char date_str[32], dt_str[32];
    get_date_str(date_str, sizeof(date_str));
    get_datetime_str(dt_str, sizeof(dt_str));

    char out_path[512];
    // mkdir(CDF_OUTPUT_DIR, 0755);
    // rithika 22Jun2026
    char base_path[256];

    if (get_base_path(base_path, sizeof(base_path)) == 0)
    {
        snprintf(out_path, sizeof(out_path), "%s/data/CDF_NP_%s_%s.xml", base_path, serial, date_str);
        // snprintf(out_path, sizeof(out_path), "%sCDF_NP_%s_%s.xml", CDF_OUTPUT_DIR, serial, date_str);
    }
    // snprintf(out_path, sizeof(out_path), "CDF_NP_%s_%s.xml", serial, date_str);

    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", out_path, strerror(errno));
        return -1;
    }

    cdf_write_header(fp, date_str);
    cdf_write_general(ctx, fp, serial, dt_str);
    cdf_write_d1(fp, ctx, serial);
    cdf_write_footer(fp);

    fclose(fp);
    LOG_INFO("Nameplate CDF written: %s", out_path);
    printf("CDF file generated: %s\n", out_path);
    return 0;
}

// int concatenate_files(char *outfile, const char *ls_file, const char *mn_file, const char *event_file, const char *billing_file)
// {
//     FILE *out = fopen(outfile, "w"); // open output in binary mode
//     if (!out)
//     {
//         perror("Failed to open output file");
//         return -1;
//     }

//     const char *inputs[NUM_CONCATENATE_FILES] = {ls_file, mn_file, event_file, billing_file};
//     char buffer[4096];

//     for (int i = 0; i < NUM_CONCATENATE_FILES; i++)
//     {
//         FILE *in = fopen(inputs[i], "r");
//         if (!in)
//         {
//             perror(inputs[i]);
//             fclose(out);
//             return -1;
//         }

//         int start_copy = (i == 0) ? 1 : 0;

//         while (fgets(buffer, sizeof(buffer), in))
//         {
//             if (i != 0 && !start_copy)
//             {
//                 if (strstr(buffer, "</D1>"))
//                 {
//                     start_copy = 1;
//                 }
//                 continue;
//             }
//             if ((i != NUM_CONCATENATE_FILES - 1) && strstr(buffer, "</UTILITYTYPE>"))
//             {
//                 break;
//             }
//             else if ((i == NUM_CONCATENATE_FILES - 1) && strstr(buffer, "</UTILITYTYPE>"))
//             {
//                 fprintf(out, "<!--Custom Profile-->\n\t<D18>\n\t<D18/>\n");
//             }

//             fputs(buffer, out);
//         }
//         fclose(in); // close input file
//     }

//     fclose(out); // close output file
//     return 0;
// }

// int concatenate_files(char *outfile,const char *ls_file,const char *mn_file,const char *event_file,const char *billing_file)
// {
//     FILE *out = fopen(outfile, "w");
//     if (!out)
//     {
//         perror("Failed to open output file");
//         return -1;
//     }
//     const char *inputs[NUM_CONCATENATE_FILES] =
//     {
//         ls_file,
//         mn_file,
//         event_file,
//         billing_file
//     };
//     char line[4096];

//     for (int i = 0; i < NUM_CONCATENATE_FILES; i++)
//     {
//         FILE *in = fopen(inputs[i], "r");
//         if (!in)
//         {
//             perror(inputs[i]);
//             fclose(out);
//             return -1;
//         }

//         while (fgets(line, sizeof(line), in))
//         {
//             /* First JSON : copy everything except final two } */
//             if (i == 0)
//             {
//                 if (strstr(line, "}\n") && feof(in))
//                     continue;

//                 if (strcmp(line, "}\n") == 0)
//                     continue;

//                 fputs(line, out);
//             }
//             else
//             {
//                 /* Skip beginning of next JSON until DATA object */
//                 if (strstr(line, "\"BLOCK_PROFILE\"") ||
//                     strstr(line, "\"DAILY_PROFILE\"") ||
//                     strstr(line, "\"EVENT_PROFILE\"") ||
//                     strstr(line, "\"BILLING_PROFILE\""))
//                 {
//                     fprintf(out, ",\n");
//                     fputs(line, out);

//                     while (fgets(line, sizeof(line), in))
//                     {
//                         if (strcmp(line, "}\n") == 0)
//                             continue;

//                         fputs(line, out);
//                     }
//                     break;
//                 }
//             }
//         }
//         fclose(in);
//     }
//     fprintf(out, "\n  }\n}\n");
//     fclose(out);
//     return 0;
// }
static cJSON *load_json_file(const char *filename)
{
    FILE *fp = fopen(filename, "rb");
    if (!fp)
    {
        perror(filename);
        return NULL;
    }

    fseek(fp, 0, SEEK_END);
    long len = ftell(fp);
    rewind(fp);

    char *buf = (char *)malloc(len + 1);
    if (!buf)
    {
        fclose(fp);
        return NULL;
    }

    fread(buf, 1, len, fp);
    buf[len] = '\0';
    fclose(fp);

    cJSON *root = cJSON_Parse(buf);
    free(buf);

    if (!root)
    {
        printf("JSON Parse Error : %s\n", cJSON_GetErrorPtr());
    }

    return root;
}

static void copy_item(cJSON *dst, cJSON *src, const char *name)
{
    cJSON *item = cJSON_GetObjectItem(src, name);

    if (item)
    {
        cJSON_AddItemToObject(dst, name, cJSON_Duplicate(item, 1));
    }
}

int concatenate_files(char *outfile, const char *ls_file, const char *mn_file, const char *event_file, const char *billing_file)
{
    cJSON *ls_root = load_json_file(ls_file);
    cJSON *mn_root = load_json_file(mn_file);
    cJSON *event_root = load_json_file(event_file);
    cJSON *bill_root = load_json_file(billing_file);

    if (!ls_root ||
        !mn_root ||
        !event_root ||
        !bill_root)
    {
        if (ls_root)
            cJSON_Delete(ls_root);
        if (mn_root)
            cJSON_Delete(mn_root);
        if (event_root)
            cJSON_Delete(event_root);
        if (bill_root)
            cJSON_Delete(bill_root);

        return -1;
    }

    cJSON *out_root = cJSON_CreateObject();

    // copy_item(out_root, ls_root, "TYPE");
    // copy_item(out_root, ls_root, "SEQ_NUM");
    // copy_item(out_root, ls_root, "DATATYPE");

    if (get_day_cmd == 1)
    {
        cJSON_AddStringToObject(out_root, "TYPE", "OD_RESP_MESSAGE");
    }
    else
    {
        cJSON_AddStringToObject(out_root, "TYPE", "CYCLIC_MESSAGE");
    }

    /* SEQ_NUM: same rule as json_write_header() - GetDay / FetchDay replies
     * keep the command's SEQ_NUM, cyclic messages take the next number. */
    if (get_day_cmd)
    {
        cJSON_AddStringToObject(out_root, "SEQ_NUM", cpy_cmd.transaction);
    }
    else if (check_redis_resp)
    {
        cJSON_AddStringToObject(out_root, "SEQ_NUM", fd_cmd.transaction);
    }
    else
    {
        char seq[16];

        cyclic_seq_str(seq, sizeof(seq));
        cJSON_AddStringToObject(out_root, "SEQ_NUM", seq);
    }
    cJSON_AddStringToObject(out_root, "DATATYPE", "METER_DATA_MESSAGE");
    copy_item(out_root, ls_root, "NP");
    copy_item(out_root, ls_root, "DCU_DETAILS");

    cJSON *out_data = cJSON_CreateObject();
    cJSON_AddItemToObject(out_root, "DATA", out_data);
    cJSON *ls_data = cJSON_GetObjectItem(ls_root, "DATA");
    copy_item(out_data, ls_data, "METER_DETAILS");
    cJSON *profiles = cJSON_CreateArray();

    cJSON_AddItemToObject(out_data, "PROFILES", profiles);

    cJSON *profile;
    cJSON *new_profile;

    /*************** BLOCK LOAD PROFILE ****************/

    profile = cJSON_GetObjectItem(ls_data, "BLOCK_PROFILE");

    if (profile)
    {
        new_profile = cJSON_CreateObject();

        cJSON_AddStringToObject(new_profile, "PROFILE", "BLOCK_LOAD_PROFILE");

        copy_item(new_profile, profile, "DATE");
        copy_item(new_profile, profile, "BLOCK_INTERVAL");
        copy_item(new_profile, profile, "FIELDS");
        copy_item(new_profile, profile, "PARAMS");

        cJSON *blocks = cJSON_GetObjectItem(profile, "BLOCKS");
        if (!blocks)
            blocks = cJSON_GetObjectItem(profile, "VALUES");

        if (blocks)
        {
            cJSON_AddItemToObject(new_profile, "BLOCKS", cJSON_Duplicate(blocks, 1));
        }

        cJSON_AddItemToArray(profiles, new_profile);
    }

    /*************** DAILY PROFILE ****************/

    cJSON *mn_data = cJSON_GetObjectItem(mn_root, "DATA");

    profile = cJSON_GetObjectItem(mn_data,
                                  "DAILY_PROFILE");

    if (profile)
    {
        new_profile = cJSON_CreateObject();

        cJSON_AddStringToObject(new_profile, "PROFILE", "DAILY_LOAD_PROFILE");

        copy_item(new_profile, profile, "FIELDS");
        copy_item(new_profile, profile, "PARAMS");

        cJSON *records = cJSON_GetObjectItem(profile, "RECORDS");

        if (!records)
            records = cJSON_GetObjectItem(profile, "VALUES");

        if (records)
        {
            cJSON_AddItemToObject(new_profile, "RECORDS", cJSON_Duplicate(records, 1));
        }
        cJSON_AddItemToArray(profiles, new_profile);
    }

    /*************** EVENT PROFILE ****************/

    cJSON *event_data = cJSON_GetObjectItem(event_root, "DATA");

    profile = cJSON_GetObjectItem(event_data, "EVENT_PROFILE");

    if (profile)
    {
        new_profile = cJSON_CreateObject();

        cJSON_AddStringToObject(new_profile, "PROFILE", "EVENT_PROFILE");

        copy_item(new_profile, profile, "FIELDS");
        copy_item(new_profile, profile, "PARAMS");

        cJSON *records = cJSON_GetObjectItem(profile, "RECORDS");

        if (!records)
            records = cJSON_GetObjectItem(profile, "VALUES");

        if (records)
        {
            cJSON_AddItemToObject(new_profile, "RECORDS", cJSON_Duplicate(records, 1));
        }

        cJSON_AddItemToArray(profiles, new_profile);
    }

    /*************** BILLING PROFILE ****************/

    cJSON *bill_data = cJSON_GetObjectItem(bill_root, "DATA");

    profile = cJSON_GetObjectItem(bill_data, "BILLING_PROFILE");

    if (profile)
    {
        new_profile = cJSON_CreateObject();

        cJSON_AddStringToObject(new_profile, "PROFILE", "BILLING_PROFILE");

        copy_item(new_profile, profile, "FIELDS");
        copy_item(new_profile, profile, "PARAMS");

        cJSON *records = cJSON_GetObjectItem(profile, "RECORDS");

        if (!records)
            records = cJSON_GetObjectItem(profile, "VALUES");

        if (records)
        {
            cJSON_AddItemToObject(new_profile, "RECORDS", cJSON_Duplicate(records, 1));
        }

        cJSON_AddItemToArray(profiles, new_profile);
    }

    char *json = cJSON_Print(out_root);

    FILE *fp = fopen(outfile, "w");

    if (!fp)
    {
        perror(outfile);

        free(json);

        cJSON_Delete(ls_root);
        cJSON_Delete(mn_root);
        cJSON_Delete(event_root);
        cJSON_Delete(bill_root);
        cJSON_Delete(out_root);

        return -1;
    }

    fputs(json, fp);

    fclose(fp);

    free(json);

    cJSON_Delete(ls_root);
    cJSON_Delete(mn_root);
    cJSON_Delete(event_root);
    cJSON_Delete(bill_root);
    cJSON_Delete(out_root);

    return 0;
}

// int generate_zip_file(const char *zip_file_name)
// {
//     char cmd[512];

//     int ret = snprintf(cmd, sizeof(cmd), "tar -czf %s.tar.gz %s", zip_file_name, zip_file_name);

//     if (ret < 0 || ret >= sizeof(cmd))
//     {
//         LOG_ERROR(stderr, "Command too long\n");
//         return -1;
//     }

//     int status = system(cmd);

//     if (status != 0)
//     {
//         LOG_ERROR(stderr, "tar command failed\n");
//         return -1;
//     }

//     LOG_INFO("Zip file %s.tar.gz created successfully", zip_file_name);
//     return 0;
// }

int generate_zip_file(const char *base_name, size_t *zip_size)
{
    char cmd[512];
    char zip_file_name[256];

    /* Create final zip name */
    snprintf(zip_file_name, sizeof(zip_file_name),
             "%s.tar.gz", base_name);

    /* Create tar command */
    int ret = snprintf(cmd, sizeof(cmd),
                       "tar -czf %s %s",
                       zip_file_name,
                       base_name);

    if (ret < 0 || ret >= sizeof(cmd))
    {
        LOG_ERROR("Command too long for %s", base_name); /* DR-41: was LOG_ERROR(stderr, ...) */
        return -1;
    }

    pid_t pid = fork();

    if (pid < 0)
    {
        LOG_ERROR("Failed to fork for tar");
        return -1;
    }

    if (pid == 0)
    {
        execlp("tar", "tar", "-czf", zip_file_name, base_name, (char *)NULL);
        _exit(EXIT_FAILURE);
    }

    int status = 0;
    if (waitpid(pid, &status, 0) < 0)
    {
        LOG_ERROR("waitpid failed for tar");
        return -1;
    }

    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0)
    {
        LOG_ERROR("tar command failed");
        return -1;
    }

    /* Get file size */
    struct stat st;
    if (stat(zip_file_name, &st) != 0)
    {
        LOG_ERROR("Failed to get zip size of %s", zip_file_name); /* DR-41 */
        return -1;
    }

    *zip_size = st.st_size;

    LOG_INFO("Zip file %s created successfully (%zu bytes)",
             zip_file_name, *zip_size);

    return 0;
}
/* ============================================================
 *  Program entry point
 * ============================================================ */

/**
 * @brief Print usage information.
 * @param prog  Argv[0] program name.
 */
static void print_usage(const char *prog)
{
    fprintf(stderr,
            "Usage: %s <data_type> <meter_serial_number> [date] [event_type]\n\n"
            "  data_type            Integer type of data to generate:\n"
            "                         1 = Instantaneous\n"
            "                         2 = Load Survey (requires date: YYYY-MM-DD)\n"
            "                         3 = Billing (requires year-month: YYYY-MM)\n"
            "                         4 = Nameplate\n"
            "                         5 = Event Log (requires date and event_type)\n"
            "                         6 = Midnight (requires date: YYYY-MM-DD)\n\n"
            "                         7 = Concatenate files <output_file_name> <ls_file_name> <midnight_file_name> <event_file_name> <billing_file_name> \n"
            "                         8 = Generate_zip_file <filename> \n "
            "  meter_serial_number  Numeric serial number of the meter\n"
            "                       (e.g. 21268190)\n\n"
            "  date                 Format depends on data type:\n"
            "                         Type 2, 6: YYYY-MM-DD (e.g. 2025-04-02)\n"
            "                         Type 3:    YYYY-MM (e.g. 2025-04)\n"
            "                         Type 5:    YYYY-MM-DD or 'all'\n\n"
            "  event_type           For type 5 only:\n"
            "                         1-7 for specific event types, or 'all'\n"
            "                         1=Voltage, 2=Current, 3=Power failure,\n"
            "                         4=Transactional, 5=Other, 6=Roll over, 7=Control\n",
            prog);
}
/**
 * @brief Application main function.
 *
 * Parses arguments, connects to Redis, dispatches to the appropriate
 * CDF generator, and cleans up.
 *
 * @param argc  Argument count.
 * @param argv  Argument vector: argv[1]=serial, argv[2]=data_type.
 * @return      EXIT_SUCCESS or EXIT_FAILURE.
 */
// int main2(int argc, char *argv[])
// {
//     /* ---- Argument validation ---- */
//     if (argc < 3)
//     {
//         print_usage(argv[0]);
//         return EXIT_FAILURE;
//     }

//     int data_type = atoi(argv[1]);

//     const char *meter_serial = NULL;
//     const char *date = NULL;
//     const char *event_type = NULL;

//     char *output_file_name = NULL;
//     char *ls_filename = NULL;
//     char *mn_filename = NULL;
//     char *event_filename = NULL;
//     char *billing_filename = NULL;

//     char *zip_file_name = NULL;

//     if (data_type != GENERATE_ZIP_FILE && data_type != CONCATENATE_FILE)
//     {

//         meter_serial = argv[2];
//         date = (argc >= 4) ? argv[3] : NULL;
//         event_type = (argc >= 5) ? argv[4] : NULL;
//     }
//     else if (data_type == CONCATENATE_FILE)
//     {
//         output_file_name = argv[2];
//         ls_filename = argv[3];
//         mn_filename = argv[4];
//         event_filename = argv[5];
//         billing_filename = argv[6];
//     }
//     else
//     {
//         if (argc < 3)
//             return EXIT_FAILURE;
//         zip_file_name = argv[2];
//     }

//     if (data_type <= 0 || data_type >= DATA_TYPE_MAX)
//     {
//         fprintf(stderr, "ERROR: Invalid data_type '%s'. Must be 1-%d.\n",
//                 argv[1], DATA_TYPE_MAX - 1);
//         print_usage(argv[0]);
//         return EXIT_FAILURE;
//     }

//     /* LS (type 2), Billing (type 3), Event (type 5), and Midnight (type 6) require date/year-month argument */
//     if ((data_type == DATA_TYPE_LOAD_PROFILE ||
//          data_type == DATA_TYPE_BILLING ||
//          data_type == DATA_TYPE_EVENT_LOG ||
//          data_type == DATA_TYPE_MIDNIGHT) &&
//         !date)
//     {
//         fprintf(stderr, "ERROR: Types 2, 3, 5, and 6 require date/year-month argument.\n");
//         print_usage(argv[0]);
//         return EXIT_FAILURE;
//     }

//     /* Event Log (type 5) requires both date and event_type */
//     if (data_type == DATA_TYPE_EVENT_LOG && !event_type)
//     {
//         fprintf(stderr, "ERROR: Event Log (type 5) requires both date and event_type arguments.\n");
//         print_usage(argv[0]);
//         return EXIT_FAILURE;
//     }

//     /* ---- Initialise logging ---- */
//     if (log_init() != 0)
//     {
//         fprintf(stderr, "WARNING: Logging unavailable, continuing without log file.\n");
//     }

//     LOG_INFO("=== dlms_cdf_gen started: serial=%s data_type=%d date=%s ===",
//              meter_serial, data_type, date ? date : "N/A");

//     /* ---- Connect to Redis ---- */
//     redisContext *ctx = redis_connect();
//     if (!ctx)
//     {
//         LOG_ERROR("Cannot connect to Redis - aborting");
//         log_close();
//         return EXIT_FAILURE;
//     }

//     /* ---- Dispatch to generator ---- */
//     int rc = 0;
//     switch ((DataType)data_type)
//     {
//     case DATA_TYPE_INSTANTANEOUS:
//         rc = generate_instantaneous_cdf(ctx, meter_serial);
//         break;
//     case DATA_TYPE_LOAD_PROFILE:
//         rc = generate_load_profile_cdf(ctx, meter_serial, date);
//         break;
//     case DATA_TYPE_BILLING:
//         rc = generate_billing_cdf(ctx, meter_serial, date);
//         break;
//     case DATA_TYPE_NAMEPLATE:
//         rc = generate_nameplate_cdf(ctx, meter_serial);
//         break;
//     case DATA_TYPE_EVENT_LOG:
//         rc = generate_event_log_cdf(ctx, meter_serial, date, event_type);
//         break;
//     case DATA_TYPE_MIDNIGHT:
//         rc = generate_midnight_cdf(ctx, meter_serial, date);
//         break;
//     case CONCATENATE_FILE:
//         rc = concatenate_files(output_file_name, ls_filename, mn_filename, event_filename, billing_filename);
//         break;
//     case GENERATE_ZIP_FILE:
//         rc = generate_zip_file(zip_file_name);
//         break;
//     default:
//         LOG_ERROR("Unhandled data_type %d", data_type);
//         rc = -1;
//         break;
//     }

//     send_hc_msg();

//     /* ---- Cleanup ---- */
//     redisFree(ctx);
//     LOG_INFO("=== dlms_cdf_gen finished: rc=%d ===", rc);
//     log_close();

//     return (rc == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
// }

/* F2: delete a temporary part file; skip empty names, never use the shell. */
static void remove_part_file(const char *name)
{
    if (name == NULL || name[0] == '\0')
        return;
    if (unlink(name) == 0)
        LOG_INFO("%s is deleted successfully", name);
    else
        LOG_WARN("Failed to delete %s: %s", name, strerror(errno));
}

cdf_result_t generate_profile_cdf(redisContext *ctx, const char *serial, const char *date, const char *event_type)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char ls_file_name[128] = "";  /* F2: was uninitialised */
    char mn_file_name[128] = "";  /* F2: was uninitialised */
    char billing_file_name[128] = "";  /* F2: was uninitialised */
    char event_file_name[128] = "";  /* F2: was uninitialised */

    int rc1 = generate_load_profile_cdf(ctx, serial, date, ls_file_name);
    if (rc1 != 0)
    {
        // return result;  //rithika commented 28/04/2026
    }

    int y, m, d;
    char bill_date[64];

    sscanf(date, "%d-%d-%d", &y, &m, &d);

    const char *months[] = {
        "", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    sprintf(bill_date, "%s %d", months[m], y);

    int rc2 = generate_billing_cdf(ctx, serial, bill_date, billing_file_name);
    if (rc2 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    int rc3 = generate_midnight_cdf(ctx, serial, date, mn_file_name);
    if (rc3 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    int rc4 = generate_event_log_cdf(ctx, serial, date, event_type, event_file_name);
    if (rc4 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    char output_file_name[64] = "";
    char base_path[256];

    if (get_base_path(base_path, sizeof(base_path)) == 0)
    {
        snprintf(output_file_name,
                 sizeof(output_file_name),
                 "%s/data/%s_%s",
                 base_path, date, serial);

        // sprintf(output_file_name, "%s%s_%s", CDF_OUTPUT_DIR, date, serial);
    }
    /* Concatenate */
    if (concatenate_files(output_file_name,
                          ls_file_name,
                          mn_file_name,
                          event_file_name,
                          billing_file_name) != 0)
        ;
    // return result; //rithika commented 28/04/2026

    // rithika 18Apr2026
    if (remove(ls_file_name) == 0)
        LOG_INFO("%s is deleted successfully", ls_file_name);
    else
        LOG_WARN("Failed to delete %s: %s", ls_file_name, strerror(errno));

    if (remove(mn_file_name) == 0)
        LOG_INFO("%s is deleted successfully", mn_file_name);
    else
        LOG_WARN("Failed to delete %s: %s", mn_file_name, strerror(errno));

    if (remove(billing_file_name) == 0)
        LOG_INFO("%s is deleted successfully", billing_file_name);
    else
        LOG_WARN("Failed to delete %s: %s", billing_file_name, strerror(errno));

    if (remove(event_file_name) == 0)
        LOG_INFO("%s is deleted successfully", event_file_name);
    else
        LOG_WARN("Failed to delete %s: %s", event_file_name, strerror(errno));

    /* Zip */
    long zip_size = 0;
    if (generate_zip_file(output_file_name, &zip_size) != 0)
        return result;

    if (remove(output_file_name) == 0)
        LOG_INFO("%s is deleted successfully", output_file_name);
    else
        LOG_WARN("Failed to delete %s: %s", output_file_name, strerror(errno));

    /* Fill result */
    result.status = 0;
    result.filesize = zip_size;
    snprintf(result.filename, sizeof(result.filename), "%s.tar.gz", output_file_name);
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}

cdf_result_t generate_profile_json(redisContext *ctx, const char *serial, const char *date, const char *event_type)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char ls_file_name[128] = "";  /* F2: was uninitialised */
    char mn_file_name[128] = "";  /* F2: was uninitialised */
    char billing_file_name[128] = "";  /* F2: was uninitialised */
    char event_file_name[128] = "";  /* F2: was uninitialised */

    struct timespec start, bill_start, bill_end, min_start, min_end, event_start, event_end, end, zip_strt, zip_end;
    clock_gettime(CLOCK_MONOTONIC, &start);

    int rc1 = generate_load_profile_json(ctx, serial, date, ls_file_name);
    clock_gettime(CLOCK_MONOTONIC, &end);

    long elapsed_ms =
        (end.tv_sec - start.tv_sec) * 1000L +
        (end.tv_nsec - start.tv_nsec) / 1000000L;

    LOG_INFO("Meter %s - Time taken for load survey data: %ld ms (%.3f seconds)", serial, elapsed_ms, elapsed_ms / 1000.0);

    if (rc1 != 0)
    {
        // return result;  //rithika commented 28/04/2026
    }

    int y, m, d;
    char bill_date[64];

    sscanf(date, "%d-%d-%d", &y, &m, &d);

   

    sprintf(bill_date, "%d_%d", m, y);

    clock_gettime(CLOCK_MONOTONIC, &bill_start);
    int rc2 = generate_billing_json(ctx, serial, bill_date, bill_date, billing_file_name);
    clock_gettime(CLOCK_MONOTONIC, &bill_end);

    long elapsed_ms_bill =
        (bill_end.tv_sec - bill_start.tv_sec) * 1000L +
        (bill_end.tv_nsec - bill_start.tv_nsec) / 1000000L;

    LOG_INFO("Meter %s - Time taken for billing data: %ld ms (%.3f seconds)", serial, elapsed_ms_bill, elapsed_ms_bill / 1000.0);

    if (rc2 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    clock_gettime(CLOCK_MONOTONIC, &min_start);
    int rc3 = generate_midnight_json(ctx, serial, date, 1, mn_file_name);

    clock_gettime(CLOCK_MONOTONIC, &min_end);

    long elapsed_ms_mn =
        (min_end.tv_sec - min_start.tv_sec) * 1000L +
        (min_end.tv_nsec - min_start.tv_nsec) / 1000000L;

    LOG_INFO("Meter %s - Time taken for midnight data: %ld ms (%.3f seconds)", serial, elapsed_ms_mn, elapsed_ms_mn / 1000.0);

    if (rc3 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    clock_gettime(CLOCK_MONOTONIC, &event_start);
    int rc4 = generate_event_log_json(ctx, serial, date, date, event_type, event_file_name);
    clock_gettime(CLOCK_MONOTONIC, &event_end);

    long elapsed_ms_event =
        (event_end.tv_sec - event_start.tv_sec) * 1000L +
        (event_end.tv_nsec - event_start.tv_nsec) / 1000000L;
    LOG_INFO("Meter %s - Time taken for event data: %ld ms (%.3f seconds)", serial, elapsed_ms_event, elapsed_ms_event / 1000.0);

    if (rc4 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    char output_file_name[64] = "";
    char base_path[256];

    // clock_gettime(CLOCK_MONOTONIC, &zip_strt);
    if (get_base_path(base_path, sizeof(base_path)) == 0)
    {
        snprintf(output_file_name, sizeof(output_file_name), "%s/data/%s_%s", base_path, date, serial);

        // sprintf(output_file_name, "%s%s_%s", CDF_OUTPUT_DIR, date, serial);
    }
    /* Concatenate */
    if (concatenate_files(output_file_name, ls_file_name, mn_file_name, event_file_name, billing_file_name) != 0)
        ;
    // return result; //rithika commented 28/04/2026

    // rithika 18Apr2026

    /* F2 (review 03-Oct): was system("rm <name>") as root on names that
     * could be uninitialised stack data. Now unlink() only real names. */
    remove_part_file(ls_file_name);
    remove_part_file(mn_file_name);
    remove_part_file(billing_file_name);
    remove_part_file(event_file_name);

    /* Zip */
    // long zip_size = 0;
    // if (generate_zip_file(output_file_name, &zip_size) != 0)
    //     return result;

    // memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
    // sprintf(file_rem_cmd, "rm %s", output_file_name);
    // system(file_rem_cmd);

    // LOG_INFO("%s is deleted successfully", output_file_name);

    /* Fill result */
    // result.status = 0;
    // result.filesize = zip_size;
    // snprintf(result.filename, sizeof(result.filename), "%s.tar.gz", output_file_name);
    // printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    FILE *fp = fopen(output_file_name, "rb");
    if (!fp)
    {
        LOG_ERROR("Unable to open %s", output_file_name);
        return result;
    }

    fseek(fp, 0, SEEK_END);
    long json_size = ftell(fp);
    fclose(fp);
    result.status = 0;
    result.filesize = json_size;
    strncpy(result.filename, output_file_name, sizeof(result.filename) - 1);
    result.filename[sizeof(result.filename) - 1] = '\0';
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}

cdf_result_t generate_mqtt_ls_json(redisContext *ctx, const char *serial, const char *date)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char ls_file_name[128] = "";  /* F2: was uninitialised */

    int rc1 = generate_load_profile_json(ctx, serial, date, ls_file_name);
    if (rc1 != 0)
    {
        // return result;  //rithika commented 28/04/2026
    }

    // char output_file_name[64];
    // char base_path[256];

    // if (get_base_path(base_path, sizeof(base_path)) == 0)
    // {
    //     snprintf(output_file_name, sizeof(output_file_name), "%s/data/%s_%s", base_path, date, serial);

    //     // sprintf(output_file_name, "%s%s_%s", CDF_OUTPUT_DIR, date, serial);
    // }

    FILE *fp = fopen(ls_file_name, "rb");
    if (!fp)
    {
        LOG_ERROR("Unable to open %s", ls_file_name);
        return result;
    }

    fseek(fp, 0, SEEK_END);
    long json_size = ftell(fp);
    fclose(fp);
    result.status = 0;
    result.filesize = json_size;
    strncpy(result.filename, ls_file_name, sizeof(result.filename) - 1);
    result.filename[sizeof(result.filename) - 1] = '\0';
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}

#if 1
cdf_result_t generate_mqtt_billing_json(redisContext *ctx,
                                        const char *serial,
                                        const char *start_date,
                                        const char *end_date)
{

    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char billing_file_name[128] = "";  /* F2: was uninitialised */



    int rc2 = generate_billing_json(ctx, serial, start_date,end_date, billing_file_name);
    if (rc2 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    FILE *fp = fopen(billing_file_name, "rb");
    if (!fp)
    {
        LOG_ERROR("Unable to open %s", billing_file_name);
        return result;
    }

    fseek(fp, 0, SEEK_END);
    long json_size = ftell(fp);
    fclose(fp);
    result.status = 0;
    result.filesize = json_size;
    strncpy(result.filename, billing_file_name, sizeof(result.filename) - 1);
    result.filename[sizeof(result.filename) - 1] = '\0';
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}
#endif

cdf_result_t generate_mqtt_midnight_json(redisContext *ctx, const char *serial, const char *strt_date, int num_days)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char mn_file_name[128] = "";  /* F2: was uninitialised */

    int rc3 = generate_midnight_json(ctx, serial, strt_date, num_days, mn_file_name);
    if (rc3 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    FILE *fp = fopen(mn_file_name, "rb");
    if (!fp)
    {
        LOG_ERROR("Unable to open %s", mn_file_name);
        return result;
    }

    fseek(fp, 0, SEEK_END);
    long json_size = ftell(fp);
    fclose(fp);
    result.status = 0;
    result.filesize = json_size;
    strncpy(result.filename, mn_file_name, sizeof(result.filename) - 1);
    result.filename[sizeof(result.filename) - 1] = '\0';
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}

cdf_result_t generate_mqtt_event_json(redisContext *ctx, const char *serial, const char *strt_date, char *end_date, char *event_catgy)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char event_file_name[128] = "";  /* F2: was uninitialised */

    int rc4 = generate_event_log_json(ctx, serial, strt_date, end_date, event_catgy, event_file_name);
    if (rc4 != 0)
    {
        // return result; //rithika commented 28/04/2026
    }

    FILE *fp = fopen(event_file_name, "rb");
    if (!fp)
    {
        LOG_ERROR("Unable to open %s", event_file_name);
        return result;
    }

    fseek(fp, 0, SEEK_END);
    long json_size = ftell(fp);
    fclose(fp);
    result.status = 0;
    result.filesize = json_size;
    strncpy(result.filename, event_file_name, sizeof(result.filename) - 1);
    result.filename[sizeof(result.filename) - 1] = '\0';
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}