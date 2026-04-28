
#include "../include/general.h"

/**
 * @brief Lookup MN param code, name and unit for a given OBIS from Redis.
 *
 * Reads three sub-hashes from REDIS_HASH_MN_OBIS_MAP.
 *
 * @param ctx        Redis context.
 * @param obis       OBIS decimal string.
 * @param code_buf   Output: param code.
 * @param name_buf   Output: param name.
 * @param unit_buf   Output: param unit.
 */
static void lookup_mn_obis_mapping(redisContext *ctx, const char *obis,
                                   char *code_buf, size_t code_len,
                                   char *name_buf, size_t name_len,
                                   char *unit_buf, size_t unit_len)
{
    /* Default to empty strings on failure */
    code_buf[0] = name_buf[0] = unit_buf[0] = '\0';

    char *code_json = redis_hget(ctx, REDIS_HASH_MN_OBIS_MAP, REDIS_FIELD_PARAM_CODE);
    char *name_json = redis_hget(ctx, REDIS_HASH_MN_OBIS_MAP, REDIS_FIELD_PARAM_NAME);
    char *unit_json = redis_hget(ctx, REDIS_HASH_MN_OBIS_MAP, REDIS_FIELD_PARAM_UNIT);

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
 * @brief Read Midnight data for a specific date from SQLite database.
 *
 * Opens the database, queries the table "mn_data_<manuf>_<dcu>_<port>_<serial>"
 * for the single record matching the specified date (column "0_0_1_0_0_255").
 * There should be only one entry per day (midnight snapshot).
 *
 * @param db_path     Path to SQLite database.
 * @param status      Meter status (contains table name components).
 * @param serial      Meter serial number.
 * @param date        Date string in YYYY-MM-DD format.
 * @param ctx         Redis context for OBIS mapping lookups.
 * @param snapshot    Output structure (caller must call mn_snapshot_free()).
 * @return            0 on success, -1 on error.
 */
static int read_mn_data(const char *db_path, const MeterStatus *status,
                        const char *serial, const char *date,
                        redisContext *ctx, MNSnapshot *snapshot)
{
    memset(snapshot, 0, sizeof(*snapshot));
    snprintf(snapshot->meter_serial, sizeof(snapshot->meter_serial), "%s", serial);

    /* Build table name */
    char table[128];
    snprintf(table, sizeof(table), "daily_profile_data_%s_%s_%s_%s",
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

    /* Query the single midnight record for the specified date */
    char query[512];
    snprintf(query, sizeof(query),
             "SELECT * FROM %s WHERE DATE(\"0_0_1_0_0_255\") = '%s' "
             "ORDER BY \"0_0_1_0_0_255\" ASC LIMIT 1",
             table, date);

    LOG_DEBUG("SQL query: %s", query);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) != SQLITE_OK)
    {
        LOG_ERROR("Failed to prepare query: %s", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    /* Count columns */
    int col_count = sqlite3_column_count(stmt);
    LOG_INFO("Query returned %d columns", col_count);

    /* Allocate for parameters */
    snapshot->params = (MNParam *)calloc(col_count, sizeof(MNParam));
    if (!snapshot->params)
    {
        LOG_ERROR("Memory allocation failed for MNParam array");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    int param_idx = 0;

    /* Read the single row (if it exists) */
    if (sqlite3_step(stmt) == SQLITE_ROW)
    {
        for (int col = 0; col < col_count; col++)
        {
            const char *col_name = sqlite3_column_name(stmt, col);

            /* Skip id and update_time */
            if (strcmp(col_name, "id") == 0 || strcmp(col_name, "update_time") == 0)
            {
                continue;
            }

            const char *obis = col_name;
            const char *val_str = (const char *)sqlite3_column_text(stmt, col);
            if (!val_str)
                val_str = "0.0";

            MNParam *p = &snapshot->params[param_idx];

            /* First entry is timestamp (0_0_1_0_0_255) */
            if (strcmp(obis, OBIS_TIMESTAMP) == 0)
            {
                snprintf(snapshot->snapshot_date, sizeof(snapshot->snapshot_date),
                         "%s.000", val_str);
                LOG_DEBUG("Midnight snapshot date: %s", snapshot->snapshot_date);
                /* Don't include timestamp as a REGISTER in the output */
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

            /* Lookup mapping from MN-specific hash */
            lookup_mn_obis_mapping(ctx, obis,
                                   p->param_code, sizeof(p->param_code),
                                   p->param_name, sizeof(p->param_name),
                                   p->unit, sizeof(p->unit));

            LOG_DEBUG("MN param[%d]: obis=%s hex=%s code=%s name=%s unit=%s val=%s",
                      param_idx, p->obis_code, p->obis_hex, p->param_code,
                      p->param_name, p->unit, p->value);

            param_idx++;
        }
    }
    else
    {
        LOG_WARN("No midnight data found for meter %s on date %s", serial, date);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        free(snapshot->params);
        snapshot->params = NULL;
        return -1;
    }

    snapshot->param_count = param_idx;

    sqlite3_finalize(stmt);
    sqlite3_close(db);

    LOG_INFO("Meter %s date %s: loaded midnight snapshot with %d parameters",
             serial, date, snapshot->param_count);

    return 0;
}

/**
 * @brief Free memory owned by an MNSnapshot.
 * @param snapshot Pointer to the snapshot to free.
 */
static void mn_snapshot_free(MNSnapshot *snapshot)
{
    if (snapshot && snapshot->params)
    {
        free(snapshot->params);
        snapshot->params = NULL;
    }
}

/**
 * @brief Write the D6 (Midnight Profile) section from MNSnapshot.
 * @param fp        Output file pointer.
 * @param snapshot  Populated midnight snapshot data.
 */
static void cdf_write_d6(FILE *fp, redisContext *ctx, const MNSnapshot *snapshot)
{
    cJSON *root = NULL;
    int paramname_avlb = chk_param_name_hash_exists(ctx, "mn_param");

    if (paramname_avlb == 0) // 0 means it is available in redis
    {
        root = get_param_name_json(ctx, "mn_param");

        if (!root)
            return;
    }
    fprintf(fp, "\t\t<!--Midnight Profile-->\n");
    fprintf(fp, "\t\t<D6>\n");
    fprintf(fp, "\t\t\t<SNAPSHOT DATE=\"%s\">\n", snapshot->snapshot_date);

    for (int i = 0; i < snapshot->param_count; i++)
    {
        const MNParam *p = &snapshot->params[i];
        const char *name = p->param_name; // default fallback

        if (paramname_avlb == 0) // 0 means it is available in redis
        {
            cJSON *item = cJSON_GetObjectItem(root, p->obis_code);

            if (item && cJSON_IsString(item) &&
                item->valuestring && item->valuestring[0] != '\0')
            {
                name = item->valuestring;
            }
        }
        fprintf(fp,
                "\t\t\t\t<REGISTER"
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

    fprintf(fp, "\t\t\t</SNAPSHOT>\n");
    fprintf(fp, "\t\t</D6>\n");

    if (root)
        cJSON_Delete(root);
}

/**
 * @brief Generate a CDF file for Midnight/Billing data (data type 3).
 *
 * Requires an additional date argument in argv[3] (YYYY-MM-DD format).
 * Midnight data is a single snapshot per day taken at 00:00:00.
 *
 * @param ctx     Redis context.
 * @param serial  Meter serial number.
 * @param date    Date string for which to fetch midnight data.
 * @return        0 on success, -1 on error.
 */
int generate_midnight_cdf(redisContext *ctx, const char *serial, const char *date, char *output_file)
{
    LOG_INFO("Generating Midnight CDF for meter %s date %s", serial, date);

    /* 1. Read meter status from Redis */
    MeterStatus status;
    if (read_meter_status(ctx, serial, &status) != 0)
    {
        LOG_ERROR("Cannot read meter status for meter %s", serial);
        return -1;
    }

    /* 2. Read Midnight data from SQLite */
    MNSnapshot snapshot;
    if (read_mn_data(SQLITE_DB_PATH, &status, serial, date, ctx, &snapshot) != 0)
    {
        LOG_ERROR("Cannot read midnight data for meter %s date %s", serial, date);
        // return -1;
    }

    if (snapshot.param_count == 0)
    {
        LOG_WARN("No midnight data found for meter %s on date %s", serial, date);
        // mn_snapshot_free(&snapshot);
        // return -1;
    }

    /* 3. Build output file path */
    char date_str[32], dt_str[32];
    get_date_str(date_str, sizeof(date_str));
    get_datetime_str(dt_str, sizeof(dt_str));

    // mkdir(CDF_OUTPUT_DIR, 0755);

    char out_path[512];
    snprintf(out_path, sizeof(out_path),
             "CDF_MN_%s_%s.xml", serial, date);

    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", out_path, strerror(errno));
        mn_snapshot_free(&snapshot);
        return -1;
    }

    /* 4. Write CDF XML */
    cdf_write_header(fp, date_str);
    cdf_write_general(ctx, fp, serial, dt_str);
    cdf_write_d1(fp, ctx, serial);
    cdf_write_d6(fp, ctx, &snapshot);
    cdf_write_footer(fp);

    fclose(fp);
    mn_snapshot_free(&snapshot);

    strcpy(output_file, out_path);

    LOG_INFO("Midnight CDF written: %s", out_path);
    printf("CDF file generated: %s\n", out_path);
    return 0;
}
