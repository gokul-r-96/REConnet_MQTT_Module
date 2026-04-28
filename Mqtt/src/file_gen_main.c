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
 *  OBIS parameter mapping
 * ============================================================ */

/**
 * @brief Parse a JSON key-value map stored in Redis and look up an OBIS key.
 *
 * The Redis value is expected to be a JSON object: { "obis1": "value1", ... }
 *
 * @param json_str  Raw JSON string from Redis.
 * @param obis_key  OBIS string to look up.
 * @param out_buf   Output buffer for the matched value.
 * @param out_len   Length of out_buf.
 * @return          0 on success, -1 if not found.
 */
int parse_obis_map(const char *json_str, const char *obis_key,
                   char *out_buf, size_t out_len)
{
    cJSON *root = cJSON_Parse(json_str);
    if (!root)
    {
        LOG_ERROR("parse_obis_map: JSON parse error");
        return -1;
    }
    cJSON *item = cJSON_GetObjectItemCaseSensitive(root, obis_key);
    int rc = -1;
    // printf("parse obis obis_key %s\n", obis_key); //Gokul commented

    if (cJSON_IsString(item) && item->valuestring)
    {
        // printf("11111\n"); //Gokul commented
        snprintf(out_buf, out_len, "%s", item->valuestring);
        // printf(" out_buf %s\n", out_buf); //Gokul commented
        rc = 0;
    }
    // printf("2222\n"); //Gokul commented
    cJSON_Delete(root);
    return rc;
}

/**
 * @brief Lookup param code, name and unit for a given OBIS from Redis.
 *
 * Reads three sub-hashes from REDIS_HASH_OBIS_MAP.
 *
 * @param ctx        Redis context.
 * @param obis       OBIS decimal string.
 * @param code_buf   Output: param code.
 * @param name_buf   Output: param name.
 * @param unit_buf   Output: param unit.
 */
static void lookup_obis_mapping(redisContext *ctx, const char *obis,
                                char *code_buf, size_t code_len,
                                char *name_buf, size_t name_len,
                                char *unit_buf, size_t unit_len)
{
    /* Default to empty strings on failure */
    code_buf[0] = name_buf[0] = unit_buf[0] = '\0';

    char *code_json = redis_hget(ctx, REDIS_HASH_OBIS_MAP, REDIS_FIELD_PARAM_CODE);
    char *name_json = redis_hget(ctx, REDIS_HASH_OBIS_MAP, REDIS_FIELD_PARAM_NAME);
    char *unit_json = redis_hget(ctx, REDIS_HASH_OBIS_MAP, REDIS_FIELD_PARAM_UNIT);

    if (code_json)
    {
        parse_obis_map(code_json, obis, code_buf, code_len);
        free(code_json);
    }
    if (name_json)
    {
        parse_obis_map(name_json, obis, name_buf, name_len);
        free(name_json);
    }
    if (unit_json)
    {
        parse_obis_map(unit_json, obis, unit_buf, unit_len);
        free(unit_json);
    }
}

/* ============================================================
 *  Instantaneous data: Redis → InstSnapshot
 * ============================================================ */

/**
 * @brief Read instantaneous meter data from Redis and populate an InstSnapshot.
 *
 * Reads the JSON stored at field "meter_0_1_<serial>" inside the
 * REDIS_HASH_INST_INFO hash, then resolves OBIS → param mappings.
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

    redisReply *r = redisCommand(ctx, "HSCAN %s 0 MATCH %s", REDIS_HASH_INST_INFO, field);

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

    printf("!!!! json str %s\n", json_str);

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

        /* Resolve name, code, unit from Redis mapping hash */
        lookup_obis_mapping(ctx, obis,
                            p->param_code, sizeof(p->param_code),
                            p->param_name, sizeof(p->param_name),
                            p->unit, sizeof(p->unit));

        LOG_DEBUG("Param[%d]: obis=%s hex=%s code=%s name=%s unit=%s val=%s",
                  param_idx, p->obis_code, p->obis_hex,
                  p->param_code, p->param_name, p->unit, p->value);

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

    char *dcu_name = redis_hget(ctx, DCU_HASH, "device_name");
    char *attr1 = redis_hget(ctx, DCU_HASH, "attribute_1");
    char *attr2 = redis_hget(ctx, DCU_HASH, "attribute_2");
    char *attr3 = redis_hget(ctx, DCU_HASH, "attribute_3");
    char *attr4 = redis_hget(ctx, DCU_HASH, "attribute_4");
    char *attr5 = redis_hget(ctx, DCU_HASH, "attribute_5");
    char *dcu_ser = redis_hget(ctx, DCU_HASH, "serial_num");

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
            METER_BAY, METER_IP);
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

    redisReply *r = redisCommand(rc,
                                 "HSCAN meter_status 0 MATCH %s", field_key);

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

    const char *serial_number = cJSON_GetObjectItem(j, "serial_number")->valuestring;
    const char *update_time = cJSON_GetObjectItem(j, "update_time")->valuestring;
    const char *pt_ratio = cJSON_GetObjectItem(j, "PT_ratio")->valuestring;
    const char *ct_ratio = cJSON_GetObjectItem(j, "CT_ratio")->valuestring;
    const char *meter_type = cJSON_GetObjectItem(j, "meter_type")->valuestring;
    const char *firmware_version = cJSON_GetObjectItem(j, "firmware_version")->valuestring;
    const char *manufacturer = cJSON_GetObjectItem(j, "manufacturer")->valuestring;
    const char *meter_category = cJSON_GetObjectItem(j, "meter_category")->valuestring;
    const char *curr_rating = cJSON_GetObjectItem(j, "current_rating")->valuestring;
    const char *year_of_manuf = cJSON_GetObjectItem(j, "year_of_manufacture")->valuestring;
    const char *ipv4_address = cJSON_GetObjectItem(j, "ipv4_address")->valuestring;
    const char *hdlc_device_address = cJSON_GetObjectItem(j, "hdlc_device_address")->valuestring;
    const char *tranfmr_volt = cJSON_GetObjectItem(j, "transfrmr_volt")->valuestring;
    const char *extra_obis_1 = cJSON_GetObjectItem(j, "extra_obis_1")->valuestring;
    const char *extra_obis_2 = cJSON_GetObjectItem(j, "extra_obis_2")->valuestring;
    const char *extra_obis_3 = cJSON_GetObjectItem(j, "extra_obis_3")->valuestring;

    fprintf(out, "\t\t<!--Nameplate Profile-->\n\t\t<D1>\n");

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
            "G8", OBIS_CT_RATIO, "Internal_CT_Ratio", pt_ratio,
            "G7", OBIS_VT_RATIO, "Internal_VT_Ratio", ct_ratio,
            "", OBIS_METER_CATEGORY, "meter_category", meter_category,
            "", OBIS_CURR_RATING, "current_rating", curr_rating,
            "", OBIS_YR_OF_MANUF, "year_of_manufacture", year_of_manuf,
            "", EXTRA_OBIS_1, "", ipv4_address,
            "", IPV4_ADDRESS, "", hdlc_device_address,
            "", EXTRA_OBIS_2, "", tranfmr_volt,
            "", HDLC_SETUP, "", extra_obis_1,
            "", TRANSFRMR_RATIO_VOLTAGE, "", extra_obis_2,
            "", EXTRA_OBIS_3, "", extra_obis_3);
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
cdf_result_t generate_instantaneous_cdf(redisContext *ctx, const char *serial)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    LOG_INFO("Generating Instantaneous CDF for meter %s", serial);

    /* 1. Read data from Redis */
    InstSnapshot snapshot;
    if (read_instantaneous_data(ctx, serial, &snapshot) != 0)
    {
        return result;
    }

    /* 2. Build output file path */
    char date_str[32], dt_str[32];
    get_date_str(date_str, sizeof(date_str));
    get_datetime_str(dt_str, sizeof(dt_str));

    // rithika 17Feb2026
    // mkdir(CDF_OUTPUT_DIR, 0755);
    // snprintf(out_path, sizeof(out_path),
    //          "%sCDF_INST_%s_%s.xml", CDF_OUTPUT_DIR, serial, date_str);

    snprintf(result.filename, sizeof(result.filename),
             "%sCDF_INST_%s_%s.xml", CDF_OUTPUT_DIR, serial, date_str);

    FILE *fp = fopen(result.filename, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", result.filename, strerror(errno));
        inst_snapshot_free(&snapshot);
        return result;
    }

    /* 3. Write CDF XML */
    cdf_write_header(fp, date_str);
    cdf_write_general(ctx, fp, serial, dt_str);
    cdf_write_d1(fp, ctx, serial);
    cdf_write_d2(fp, ctx, &snapshot);
    cdf_write_footer(fp);

    /* Get file size */
    fseek(fp, 0, SEEK_END);
    result.filesize = ftell(fp);

    fclose(fp);
    inst_snapshot_free(&snapshot);
    result.status = 0;

    LOG_INFO("Instantaneous CDF written: %s", result.filename);
    printf("CDF file generated: %s\n", result.filename);
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
    snprintf(out_path, sizeof(out_path), "%sCDF_NP_%s_%s.xml", CDF_OUTPUT_DIR, serial, date_str);

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

int concatenate_files(char *outfile, const char *ls_file, const char *mn_file, const char *event_file, const char *billing_file)
{
    FILE *out = fopen(outfile, "w"); // open output in binary mode
    if (!out)
    {
        perror("Failed to open output file");
        return -1;
    }

    const char *inputs[NUM_CONCATENATE_FILES] = {ls_file, mn_file, event_file, billing_file};
    char buffer[4096];

    for (int i = 0; i < NUM_CONCATENATE_FILES; i++)
    {
        FILE *in = fopen(inputs[i], "r");
        if (!in)
        {
            perror(inputs[i]);
            fclose(out);
            return -1;
        }

        int start_copy = (i == 0) ? 1 : 0;

        while (fgets(buffer, sizeof(buffer), in))
        {
            if (i != 0 && !start_copy)
            {
                if (strstr(buffer, "</D1>"))
                {
                    start_copy = 1;
                }
                continue;
            }

            if ((i != NUM_CONCATENATE_FILES - 1) && strstr(buffer, "</UTILITYTYPE>"))
            {
                break;
            }
            else if ((i == NUM_CONCATENATE_FILES - 1) && strstr(buffer, "</UTILITYTYPE>"))
            {
                fprintf(out, "<!--Custom Profile-->\n\t<D18>\n\t<D18/>\n");
            }

            fputs(buffer, out);
        }

        fclose(in); // close input file
    }

    fclose(out); // close output file
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
        LOG_ERROR(stderr, "Command too long\n");
        return -1;
    }

    int status = system(cmd);
    if (status != 0)
    {
        LOG_ERROR(stderr, "tar command failed\n");
        return -1;
    }

    /* Get file size */
    struct stat st;
    if (stat(zip_file_name, &st) != 0)
    {
        LOG_ERROR(stderr, "Failed to get zip size\n");
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

cdf_result_t generate_profile_cdf(redisContext *ctx, const char *serial, const char *date, const char *event_type)
{
    cdf_result_t result;
    result.status = -1;
    result.filesize = 0;
    result.filename[0] = '\0';

    char ls_file_name[128];
    char mn_file_name[128];
    char billing_file_name[128];
    char event_file_name[128];

    int rc1 = generate_load_profile_cdf(ctx, serial, date, ls_file_name);
    if (rc1 != 0)
    {
        return result;
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
        return result;
    }

    int rc3 = generate_midnight_cdf(ctx, serial, date, mn_file_name);
    if (rc3 != 0)
    {
        return result;
    }

    int rc4 = generate_event_log_cdf(ctx, serial, date, event_type, event_file_name);
    if (rc4 != 0)
    {
        return result;
    }

    char output_file_name[64];
    sprintf(output_file_name, "%s%s_%s", CDF_OUTPUT_DIR, date, serial);

    /* Concatenate */
    if (concatenate_files(output_file_name,
                          ls_file_name,
                          mn_file_name,
                          event_file_name,
                          billing_file_name) != 0)
        return result;

    // rithika 18Apr2026
    char file_rem_cmd[128];
    sprintf(file_rem_cmd, "rm %s", ls_file_name);
    system(file_rem_cmd);
    LOG_INFO("%s is deleted successfully", ls_file_name);

    memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
    sprintf(file_rem_cmd, "rm %s", mn_file_name);
    system(file_rem_cmd);
    LOG_INFO("%s is deleted successfully", mn_file_name);

    memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
    sprintf(file_rem_cmd, "rm \"%s\"", billing_file_name);
    system(file_rem_cmd);
    LOG_INFO("%s is deleted successfully", billing_file_name);

    memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
    sprintf(file_rem_cmd, "rm %s", event_file_name);
    system(file_rem_cmd);

    LOG_INFO("%s is deleted successfully", event_file_name);

    /* Zip */
    long zip_size = 0;
    if (generate_zip_file(output_file_name, &zip_size) != 0)
        return result;

    memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
    sprintf(file_rem_cmd, "rm %s", output_file_name);
    system(file_rem_cmd);

    LOG_INFO("%s is deleted successfully", output_file_name);

    /* Fill result */
    result.status = 0;
    result.filesize = zip_size;
    snprintf(result.filename, sizeof(result.filename), "%s.tar.gz", output_file_name);
    printf("status=%d size=%ld name=%s\n", result.status, result.filesize, result.filename);

    return result;
}