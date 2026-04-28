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

/* ============================================================
 *  OBIS parameter mapping
 * ============================================================ */

/**
 * @brief Map manufacturer string to short key.
 *
 * Checks for substrings: Schneider/LARSEN → lnt, GENUS → genus,
 * SECURE → secure, BENTEC → bentec, HPL → hpl.
 *
 * @param manuf_str   Full manufacturer string from Redis.
 * @param manuf_key   Output buffer for short key.
 * @param key_len     Length of manuf_key buffer.
 */
static void map_manufacturer_key(const char *manuf_str, char *manuf_key, size_t key_len)
{
    if (strcasestr(manuf_str, "Schneider") || strcasestr(manuf_str, "LARSEN"))
    {
        snprintf(manuf_key, key_len, "lnt");
    }
    else if (strcasestr(manuf_str, "GENUS"))
    {
        snprintf(manuf_key, key_len, "genus");
    }
    else if (strcasestr(manuf_str, "SECURE"))
    {
        snprintf(manuf_key, key_len, "secure");
    }
    else if (strcasestr(manuf_str, "BENTEC"))
    {
        snprintf(manuf_key, key_len, "bentec");
    }
    else if (strcasestr(manuf_str, "HPL"))
    {
        snprintf(manuf_key, key_len, "hpl");
    }
    else
    {
        snprintf(manuf_key, key_len, "unknown");
        LOG_WARN("Unknown manufacturer: %s", manuf_str);
    }
}

/* ============================================================
 *  Load Survey (LS) data helpers
 * ============================================================ */

/**
 * @brief Read meter status from Redis.
 *
 * Fetches field "meter_0_1_<serial>_details" from REDIS_HASH_METER_STATUS
 * and parses the JSON to extract manufacturer, dcu_serial, port, block_int,
 * and demand_int_period.
 *
 * @param ctx     Redis context.
 * @param serial  Meter serial number.
 * @param status  Output structure.
 * @return        0 on success, -1 on error.
 */
int read_meter_status(redisContext *ctx, const char *serial, MeterStatus *status)
{
    memset(status, 0, sizeof(*status));

    char field_key[64];

    snprintf(field_key, sizeof(field_key),
             "meter_*_*_%s_details", serial);

    LOG_INFO("Fetching D1 from meter_status[%s]", field_key);

    redisReply *r = redisCommand(ctx,
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

        if (strstr(field_key, serial))
        {
            json_str = value;
            break;
        }
    }

    if (!json_str)
    {
        LOG_ERROR("No meter status found for meter %s", serial);
        return -1;
    }

    cJSON *root = cJSON_Parse(json_str);
    free(json_str);
    if (!root)
    {
        LOG_ERROR("Failed to parse meter status JSON for meter %s", serial);
        return -1;
    }

    /* Extract fields */
    cJSON *item;

    item = cJSON_GetObjectItemCaseSensitive(root, "manufacturer");
    if (cJSON_IsString(item) && item->valuestring)
    {
        snprintf(status->manufacturer, sizeof(status->manufacturer), "%s", item->valuestring);
        map_manufacturer_key(status->manufacturer, status->manuf_key, sizeof(status->manuf_key));
    }

    item = cJSON_GetObjectItemCaseSensitive(root, "dcu_serial_number");
    if (cJSON_IsString(item) && item->valuestring)
    {
        snprintf(status->dcu_serial, sizeof(status->dcu_serial), "%s", item->valuestring);
    }

    item = cJSON_GetObjectItemCaseSensitive(root, "port");
    if (cJSON_IsString(item) && item->valuestring)
    {
        snprintf(status->port, sizeof(status->port), "%s", item->valuestring);
    }
    else if (cJSON_IsNumber(item))
    {
        snprintf(status->port, sizeof(status->port), "%d", item->valueint);
    }

    item = cJSON_GetObjectItemCaseSensitive(root, "block_int");
    if (cJSON_IsString(item) && item->valuestring)
    {
        status->block_int = atoi(item->valuestring);
    }

    item = cJSON_GetObjectItemCaseSensitive(root, "demand_int_period");
    if (cJSON_IsString(item) && item->valuestring)
    {
        status->demand_int_period = atoi(item->valuestring);
    }

    cJSON_Delete(root);

    LOG_INFO("Meter %s status: manuf=%s (key=%s) dcu=%s port=%s block_int=%d demand_int=%d",
             serial, status->manufacturer, status->manuf_key, status->dcu_serial,
             status->port, status->block_int, status->demand_int_period);

    return 0;
}

/**
 * @brief Lookup LS param code, name and unit for a given OBIS from Redis.
 *
 * Reads three sub-hashes from REDIS_HASH_LS_OBIS_MAP.
 *
 * @param ctx        Redis context.
 * @param obis       OBIS decimal string.
 * @param code_buf   Output: param code.
 * @param name_buf   Output: param name.
 * @param unit_buf   Output: param unit.
 */
static void lookup_ls_obis_mapping(redisContext *ctx, const char *obis,
                                   char *code_buf, size_t code_len,
                                   char *name_buf, size_t name_len,
                                   char *unit_buf, size_t unit_len)
{
    /* Default to empty strings on failure */
    code_buf[0] = name_buf[0] = unit_buf[0] = '\0';

    char *code_json = redis_hget(ctx, REDIS_HASH_LS_OBIS_MAP, REDIS_FIELD_PARAM_CODE);
    char *name_json = redis_hget(ctx, REDIS_HASH_LS_OBIS_MAP, REDIS_FIELD_PARAM_NAME);
    char *unit_json = redis_hget(ctx, REDIS_HASH_LS_OBIS_MAP, REDIS_FIELD_PARAM_UNIT);

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

/**
 * @brief Read LS data for a specific date from SQLite database.
 *
 * Opens the database, queries the table "ls_data_<manuf>_<dcu>_<port>_<serial>"
 * for all records matching the specified date (column "0_0_1_0_0_255"),
 * ordered by timestamp ascending.
 *
 * @param db_path     Path to SQLite database.
 * @param status      Meter status (contains table name components).
 * @param serial      Meter serial number.
 * @param date        Date string in YYYY-MM-DD format.
 * @param day_profile Output structure (caller must call ls_day_profile_free()).
 * @return            0 on success, -1 on error.
 */
static int read_ls_data(const char *db_path, const MeterStatus *status,
                        const char *serial, const char *date,
                        redisContext *ctx, LSDayProfile *day_profile)
{
    memset(day_profile, 0, sizeof(*day_profile));
    snprintf(day_profile->meter_serial, sizeof(day_profile->meter_serial), "%s", serial);
    snprintf(day_profile->date, sizeof(day_profile->date), "%s", date);
    day_profile->interval_period = status->block_int;
    day_profile->demand_integration_period = status->demand_int_period;

    /* Build table name */
    char table[128];
    snprintf(table, sizeof(table), "ls_data_%s_%s_%s_%s",
             status->manuf_key, status->dcu_serial, status->port, serial);

    LOG_INFO("Opening SQLite DB: %s, table: %s", db_path, table);

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK)
    {
        LOG_ERROR("Cannot open database: %s (%s)", db_path, sqlite3_errmsg(db));
        if (db)
            sqlite3_close(db);
        return -1;
    }

    /* Query all records for the specified date, ordered by time */
    char query[512];
    snprintf(query, sizeof(query),
             "SELECT * FROM %s WHERE DATE(\"0_0_1_0_0_255\") = '%s' "
             "ORDER BY \"0_0_1_0_0_255\" ASC",
             table, date);

    LOG_DEBUG("SQL query: %s", query);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) != SQLITE_OK)
    {
        LOG_ERROR("Failed to prepare query: %s", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    /* Count columns (first 2 are id and update_time, rest are OBIS columns) */
    int col_count = sqlite3_column_count(stmt);
    LOG_INFO("Query returned %d columns", col_count);

    /* Allocate for up to 96 intervals (15-min blocks: 24h * 4) */
    day_profile->intervals = (LSInterval *)calloc(96, sizeof(LSInterval));
    if (!day_profile->intervals)
    {
        LOG_ERROR("Memory allocation failed for LS intervals");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    int interval_idx = 0;

    /* Iterate over result rows */
    while (sqlite3_step(stmt) == SQLITE_ROW && interval_idx < 96)
    {
        LSInterval *interval = &day_profile->intervals[interval_idx];
        interval->interval_num = interval_idx;

        /* Allocate for parameters (col_count - 2, minus id and update_time) */
        interval->params = (LSParam *)calloc(col_count, sizeof(LSParam));
        if (!interval->params)
        {
            LOG_ERROR("Memory allocation failed for LSParam array");
            break;
        }

        int param_idx = 0;

        for (int col = 0; col < col_count; col++)
        {
            const char *col_name = sqlite3_column_name(stmt, col);

            /* Skip id and update_time */
            if (strcmp(col_name, "id") == 0 || strcmp(col_name, "update_time") == 0)
            {
                continue;
            }

            const char *obis = col_name; /* Column name is the OBIS code */
            const char *val_str = (const char *)sqlite3_column_text(stmt, col);
            if (!val_str)
                val_str = "0.0";

            LSParam *p = &interval->params[param_idx];

            /* First entry is timestamp (0_0_1_0_0_255) */
            if (strcmp(obis, OBIS_TIMESTAMP) == 0)
            {
                snprintf(interval->timestamp, sizeof(interval->timestamp), "%s.000", val_str);
                snprintf(p->obis_code, sizeof(p->obis_code), "%s", obis);
                snprintf(p->obis_hex, sizeof(p->obis_hex), "00_00_01_00_00_ff");
                snprintf(p->param_code, sizeof(p->param_code), "");
                snprintf(p->param_name, sizeof(p->param_name), "Date_Time");
                snprintf(p->unit, sizeof(p->unit), "count");
                snprintf(p->value, sizeof(p->value), "%s.000", val_str);
                param_idx++;
                continue;
            }

            /* Convert OBIS to hex */
            char obis_hex[24];
            if (obis_dec_to_hex(obis, obis_hex) != 0)
            {
                LOG_WARN("Skipping unparseable OBIS column: %s", obis);
                continue;
            }

            /* Populate parameter */
            snprintf(p->obis_code, sizeof(p->obis_code), "%s", obis);
            snprintf(p->obis_hex, sizeof(p->obis_hex), "%s", obis_hex);
            snprintf(p->value, sizeof(p->value), "%s", val_str);

            /* Lookup mapping from LS-specific hash */
            lookup_ls_obis_mapping(ctx, obis,
                                   p->param_code, sizeof(p->param_code),
                                   p->param_name, sizeof(p->param_name),
                                   p->unit, sizeof(p->unit));

            param_idx++;
        }

        interval->param_count = param_idx;
        interval_idx++;
    }

    day_profile->interval_count = interval_idx;

    sqlite3_finalize(stmt);
    sqlite3_close(db);

    LOG_INFO("Meter %s date %s: loaded %d intervals from DB",
             serial, date, day_profile->interval_count);

    return 0;
}

/**
 * @brief Free memory owned by an LSDayProfile.
 * @param profile Pointer to the profile to free.
 */
static void ls_day_profile_free(LSDayProfile *profile)
{
    if (!profile || !profile->intervals)
        return;

    for (int i = 0; i < profile->interval_count; i++)
    {
        if (profile->intervals[i].params)
        {
            free(profile->intervals[i].params);
        }
    }
    free(profile->intervals);
    profile->intervals = NULL;
}

/**
 * @brief Write the D4 (Load Survey / Block Profile) section from LSDayProfile.
 * @param fp       Output file pointer.
 * @param profile  Populated LS day profile data.
 */
static void cdf_write_d4(FILE *fp, redisContext *ctx, const LSDayProfile *profile)
{

    cJSON *root = NULL;
    int paramname_avlb = chk_param_name_hash_exists(ctx, "ls_param");

    if (paramname_avlb == 0) // 0 means it is available in redis
    {
        root = get_param_name_json(ctx, "ls_param");
        if (!root)
            return;
    }

    fprintf(fp, "\t\t<!--Block Profile-->\n");
    fprintf(fp, "\t\t<D4 INTERVALPERIOD=\"%d\" DEMAND_INTEGRATION_PERIOD=\"%d\">\n",
            profile->interval_period, profile->demand_integration_period);
    fprintf(fp, "\t\t\t<DAYPROFILE DATE=\"%s\">\n", profile->date);

    for (int i = 0; i < profile->interval_count; i++)
    {
        const LSInterval *interval = &profile->intervals[i];
        fprintf(fp, "\t\t\t\t<IP INTERVAL=\"%d\">\n", interval->interval_num);

        for (int j = 0; j < interval->param_count; j++)
        {

            const LSParam *p = &interval->params[j];
            const char *name = p->param_name; // default fallback

            if (paramname_avlb == 0) // 0 means it is available in redis
            {
                cJSON *item = cJSON_GetObjectItem(root, p->obis_code);
                printf("p->obis_code %s p->obis_hex %s\n", p->obis_code, p->obis_hex);

                if (item && cJSON_IsString(item) &&
                    item->valuestring && item->valuestring[0] != '\0')
                {
                    name = item->valuestring;
                }
            }

            fprintf(fp,
                    "\t\t\t\t\t<PARAMETER"
                    " CODE=\"%s\""
                    " OBIS_CODE=\"%s\""
                    " NAME=\"%s\""
                    " VALUE=\"%s\""
                    " UNIT=\"%s\"/>\n",
                    p->param_code,
                    p->obis_hex,
                    name, // p->param_name,
                    p->value,
                    p->unit);
        }
        fprintf(fp, "\t\t\t\t</IP>\n");
    }

    fprintf(fp, "\t\t\t</DAYPROFILE>\n");
    fprintf(fp, "\t\t</D4>\n");

    if (root)
        cJSON_Delete(root);
}

/**
 * @brief Generate a CDF file for Load Survey data (data type 2).
 *
 * Requires an additional date argument in argv[3] (YYYY-MM-DD format).
 *
 * @param ctx     Redis context.
 * @param serial  Meter serial number.
 * @param date    Date string for which to fetch LS data.
 * @return        0 on success, -1 on error.
 */
int generate_load_profile_cdf(redisContext *ctx, const char *serial, const char *date, char *output_file)
{
    LOG_INFO("Generating Load Survey CDF for meter %s date %s", serial, date);

    /* 1. Read meter status from Redis */
    MeterStatus status;
    if (read_meter_status(ctx, serial, &status) != 0)
    {
        LOG_ERROR("Cannot read meter status for meter %s", serial);
        return -1;
    }

    /* 2. Read LS data from SQLite */
    LSDayProfile day_profile;
    if (read_ls_data(SQLITE_DB_PATH, &status, serial, date, ctx, &day_profile) != 0)
    {
        LOG_ERROR("Cannot read LS data for meter %s date %s", serial, date);
        // return -1;
    }

    if (day_profile.interval_count == 0)
    {
        LOG_WARN("No LS data found for meter %s on date %s", serial, date);
        // ls_day_profile_free(&day_profile);
        // return -1;
    }

    /* 3. Build output file path */
    char date_str[32], dt_str[32];
    get_date_str(date_str, sizeof(date_str));
    get_datetime_str(dt_str, sizeof(dt_str));

    // mkdir(CDF_OUTPUT_DIR, 0755);

    char out_path[512];
    snprintf(out_path, sizeof(out_path),
             "CDF_LS_%s_%s.xml", serial, date);

    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", out_path, strerror(errno));
        ls_day_profile_free(&day_profile);
        return -1;
    }

    /* 4. Write CDF XML */
    cdf_write_header(fp, date_str);
    cdf_write_general(ctx, fp, serial, dt_str);
    cdf_write_d1(fp, ctx, serial);
    cdf_write_d4(fp, ctx, &day_profile);
    cdf_write_footer(fp);
    fclose(fp);
    ls_day_profile_free(&day_profile);

    strcpy(output_file, out_path);

    LOG_INFO("Load Survey CDF written: %s", out_path);
    printf("CDF file generated: %s\n", out_path);
    return 0;
}
