
#include "../include/general.h"

/* DR-21: how long a profile read waits for a DA write lock */
#define PROFILE_DB_BUSY_MS 5000

extern int midnight_cmd_redis_resp;
extern int ls_cmd_redis_resp;
extern int billing_cmd_redis_resp;
extern int event_cmd_redis_resp;

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
                        redisContext *ctx, MNSnapshot *snapshot,
                        int drop_od_table)
{
    (void)ctx;
    memset(snapshot, 0, sizeof(*snapshot));
    snprintf(snapshot->meter_serial, sizeof(snapshot->meter_serial), "%s", serial);

    /* Build table name */
    char table[128];

    if (midnight_cmd_redis_resp == 1)
    {
        snprintf(table, sizeof(table), "daily_profile_data_od_%s_%s_%s_%s",
                 status->manuf_key, status->dcu_serial, status->port, serial);
    }
    else
    {
        snprintf(table, sizeof(table), "daily_profile_data_%s_%s_%s_%s",
                 status->manuf_key, status->dcu_serial, status->port, serial);
    }

    LOG_INFO("Opening SQLite DB: %s, table: %s", db_path, table);

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK)
    {
        LOG_ERROR("Cannot open database: %s (%s)", db_path, sqlite3_errmsg(db));
        if (db)
            sqlite3_close(db);
        return -1;
    }
    /* DR-21: wait for a DA write lock instead of failing at once */
    sqlite3_busy_timeout(db, PROFILE_DB_BUSY_MS);

    /* Query the single midnight record for the specified date */
    char query[512];
    /* Match the timestamp by prefix so SQLite can use an index on the column. */
    snprintf(query, sizeof(query),
             "SELECT * FROM %s WHERE \"0_0_1_0_0_255\" LIKE '%s%%' "
             "ORDER BY \"0_0_1_0_0_255\" ASC LIMIT 1",
             table, date);

    LOG_DEBUG("SQL query: %s", query);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) != SQLITE_OK)
    {
        const char *errmsg = sqlite3_errmsg(db);

        if (strstr(errmsg, "no such table") != NULL)
        {
            LOG_WARN("No midnight table found for meter %s date %s. "
                     "Treating as no data.",
                     serial, date);

            snapshot->params = NULL;
            snapshot->param_count = 0;

            sqlite3_close(db);

            return 0;
        }

        LOG_ERROR("Failed to prepare query: %s", errmsg);
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
    int step_rc = sqlite3_step(stmt);
    if (step_rc == SQLITE_ROW)
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
            snprintf(p->obis_code, sizeof(p->obis_code), "%s", obis_hex);
            snprintf(p->obis_hex, sizeof(p->obis_hex), "%s", obis_hex);
            snprintf(p->value, sizeof(p->value), "%s", val_str);

            LOG_DEBUG("MN param[%d]: obis=%s value=%s",
                      param_idx, p->obis_code, p->value);

            param_idx++;
        }
    }

    else if (step_rc != SQLITE_DONE)
    {
        /* DR-21: BUSY or another error is a failed read, not "no data"; the OD
         * table is kept so the request can be repeated */
        LOG_ERROR("Read of %s failed: %s (%d)", table, sqlite3_errmsg(db), step_rc);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }
    else
    {
        LOG_WARN("No midnight data found for meter %s on date %s",
                 serial, date);

        snapshot->param_count = 0;
    }

    snapshot->param_count = param_idx;

    sqlite3_finalize(stmt);

    /* For multi-day OD_MN_DATA, keep the temporary table until the
     * last requested date has been read. */
    /* DR-36: only the OD table this call read (not any name containing "od_") */
    if (midnight_cmd_redis_resp == 1 && drop_od_table)
    {
        drop_table(table, db);
    }

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
    // cJSON *root = NULL;
    // int paramname_avlb = chk_param_name_hash_exists(ctx, "mn_param");

    // if (paramname_avlb == 0) // 0 means it is available in redis
    // {
    //     root = get_param_name_json(ctx, "mn_param");

    //     if (!root)
    //         return;
    // }
    fprintf(fp, "\t\t<!--Midnight Profile-->\n");
    fprintf(fp, "\t\t<D6>\n");
    fprintf(fp, "\t\t\t<SNAPSHOT DATE=\"%s\">\n", snapshot->snapshot_date);

    for (int i = 0; i < snapshot->param_count; i++)
    {
        const MNParam *p = &snapshot->params[i];
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
                    "\t\t\t\t<REGISTER"
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
    fprintf(fp, "\t\t</D6>\n");

    // if (root)
    //     cJSON_Delete(root);
}

/* Increment a date in YYYY-MM-DD format by one day. */
static int is_leap_year(int year)
{
    return ((year % 4 == 0 && year % 100 != 0) ||
            (year % 400 == 0));
}

static int days_in_month(int month, int year)
{
    static const int days[] =
        {
            31, 28, 31, 30, 31, 30,
            31, 31, 30, 31, 30, 31};

    if (month < 1 || month > 12)
        return 0;

    if (month == 2)
        return is_leap_year(year) ? 29 : 28;

    return days[month - 1];
}

static int increment_date(char *date, size_t date_len)
{
    int year;
    int month;
    int day;

    if (!date)
        return -1;

    if (sscanf(date, "%d-%d-%d", &year, &month, &day) != 3)
        return -1;

    if (month < 1 || month > 12 ||
        day < 1 || day > days_in_month(month, year))
        return -1;

    day++;

    if (day > days_in_month(month, year))
    {
        day = 1;
        month++;

        if (month > 12)
        {
            month = 1;
            year++;
        }
    }

    snprintf(date, date_len, "%04d-%02d-%02d", year, month, day);
    return 0;
}

/* Write all requested Midnight snapshots into one JSON DAILY_PROFILE. */
static void json_write_d6_multi(FILE *fp,
                                redisContext *ctx,
                                MNSnapshot *snapshots,
                                int snapshot_count)
{
    (void)ctx;

    fprintf(fp, "    \"DAILY_PROFILE\": {\n");

    /* Every SQLite data column is represented by its OBIS code. */
    fprintf(fp, "      \"FIELDS\": [\"OBIS_CODE\"],\n");
    fprintf(fp, "      \"PARAMS\": [\n");

    if (snapshot_count > 0)
    {
        for (int i = 0; i < snapshots[0].param_count; i++)
        {
            const MNParam *p = &snapshots[0].params[i];

            if (i > 0)
                fprintf(fp, ",\n");

            fprintf(fp, "        [\"%s\"]", p->obis_hex);
        }
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ],\n");
    fprintf(fp, "      \"RECORDS\": [\n");

    int record_count = 0;

    for (int s = 0; s < snapshot_count; s++)
    {
        MNSnapshot *snapshot = &snapshots[s];

        /* Skip dates where no midnight data was available. */
        if (snapshot->param_count == 0)
            continue;

        if (record_count > 0)
            fprintf(fp, ",\n");

        fprintf(fp, "        [\"%s\",[", snapshot->snapshot_date);

        for (int p = 0; p < snapshot->param_count; p++)
        {
            if (p > 0)
                fprintf(fp, ", ");

            fprintf(fp, "\"%s\"", snapshot->params[p].value);
        }

        fprintf(fp, "]]");

        record_count++;
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ]\n");
    fprintf(fp, "    }\n");
}

static void json_write_d6(FILE *fp, redisContext *ctx, const MNSnapshot *snapshot)
{
    (void)ctx;

    fprintf(fp, "    \"DAILY_PROFILE\": {\n");
    fprintf(fp, "      \"FIELDS\": [\"OBIS_CODE\"],\n");
    fprintf(fp, "      \"PARAMS\": [\n");

    for (int i = 0; i < snapshot->param_count; i++)
    {
        const MNParam *p = &snapshot->params[i];

        if (i > 0)
            fprintf(fp, ",\n");

        fprintf(fp, "        [\"%s\"]", p->obis_hex);
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ],\n");
    fprintf(fp, "      \"RECORDS\": [\n");
    fprintf(fp, "        [\"%s\",[", snapshot->snapshot_date);

    for (int i = 0; i < snapshot->param_count; i++)
    {
        if (i > 0)
            fprintf(fp, ", ");

        fprintf(fp, "\"%s\"", snapshot->params[i].value);
    }

    fprintf(fp, "]]\n");
    fprintf(fp, "      ]\n");
    fprintf(fp, "    }\n");
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

    // rithika 23Jun2026
    char base_path_dir[256];
    char sqlite_db_path[256];
    if (get_base_path(base_path_dir, sizeof(base_path_dir)) == 0)
    {
        snprintf(sqlite_db_path,
                 sizeof(sqlite_db_path),
                 "%s/data/dcu_dlms.db",
                 base_path_dir);
    }

    /* 2. Read Midnight data from SQLite */
    MNSnapshot snapshot;
    if (read_mn_data(sqlite_db_path, &status, serial, date, ctx, &snapshot, 1) != 0)
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
    char base_path[256];

    if (get_base_path(base_path, sizeof(base_path)) == 0)
    {
        snprintf(out_path,
                 sizeof(out_path),
                 "%s/data/CDF_MN_%s_%s.xml",
                 base_path, serial, date);

        // snprintf(out_path, sizeof(out_path),
        //          "%sCDF_MN_%s_%s.xml", CDF_OUTPUT_DIR, serial, date);
    }

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

int generate_midnight_json(redisContext *ctx,
                           const char *serial,
                           const char *start_date,
                           int num_days,
                           char *output_file)
{
    LOG_INFO("Generating Midnight JSON for meter %s", serial);
    LOG_INFO("Start date : %s", start_date);
    LOG_INFO("Number of days : %d", num_days);

    if (!ctx || !serial || !start_date || !output_file || num_days <= 0)
    {
        LOG_ERROR("Invalid arguments for Midnight JSON generation");
        return -1;
    }

    /* 1. Read meter status from Redis */
    MeterStatus status;
    if (read_meter_status(ctx, serial, &status) != 0)
    {
        LOG_ERROR("Cannot read meter status for meter %s", serial);
        return -1;
    }

    /* 2. Database path */
    char base_path_dir[256];
    char sqlite_db_path[256];

    if (get_base_path(base_path_dir, sizeof(base_path_dir)) != 0)
    {
        LOG_ERROR("Failed to get base path");
        return -1;
    }

    snprintf(sqlite_db_path,
             sizeof(sqlite_db_path),
             "%s/data/dcu_dlms.db",
             base_path_dir);

    /* 3. Allocate one snapshot for each requested day. */
    MNSnapshot *snapshots =
        (MNSnapshot *)calloc(num_days, sizeof(MNSnapshot));

    if (!snapshots)
    {
        LOG_ERROR("Memory allocation failed for %d snapshots", num_days);
        return -1;
    }

    char current_date[32];
    snprintf(current_date, sizeof(current_date), "%s", start_date);

    int actual_count = 0;
    int day_index;

    /* 4. Read Midnight data for every requested date. */
    for (day_index = 0; day_index < num_days; day_index++)
    {
        LOG_INFO("Reading Midnight data for date %s (%d/%d)",
                 current_date,
                 day_index + 1,
                 num_days);

        /* Keep the OD table until the last requested date is read. */
        int drop_od_table =
            (day_index == num_days - 1) ? 1 : 0;

        if (read_mn_data(sqlite_db_path,
                         &status,
                         serial,
                         current_date,
                         ctx,
                         &snapshots[actual_count],
                         drop_od_table) != 0)
        {
            LOG_ERROR("Cannot read midnight data for meter %s date %s",
                      serial,
                      current_date);

            int i;
            for (i = 0; i < actual_count; i++)
                mn_snapshot_free(&snapshots[i]);

            free(snapshots);
            return -1;
        }

        if (snapshots[actual_count].param_count == 0)
        {
            LOG_WARN("No midnight data found for meter %s on date %s",
                     serial,
                     current_date);
        }

        actual_count++;

        if (day_index < num_days - 1)
        {
            if (increment_date(current_date,
                               sizeof(current_date)) != 0)
            {
                LOG_ERROR("Failed to increment date from %s", current_date);

                int i;
                for (i = 0; i < actual_count; i++)
                    mn_snapshot_free(&snapshots[i]);

                free(snapshots);
                return -1;
            }
        }
    }

    /* 5. Build output file path. */
    char dt_str[32];
    get_datetime_str(dt_str, sizeof(dt_str));

    char out_path[512];
    char base_path[256];

    if (get_base_path(base_path, sizeof(base_path)) != 0)
    {
        LOG_ERROR("Failed to get base path for output file");

        int i;
        for (i = 0; i < actual_count; i++)
            mn_snapshot_free(&snapshots[i]);

        free(snapshots);
        return -1;
    }

    snprintf(out_path,
             sizeof(out_path),
             "%s/data/MN_%s_%s.json",
             base_path,
             serial,
             start_date);

    /* 6. Open JSON file. */
    FILE *fp = fopen(out_path, "w");

    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)",
                  out_path,
                  strerror(errno));

        int i;
        for (i = 0; i < actual_count; i++)
            mn_snapshot_free(&snapshots[i]);

        free(snapshots);
        return -1;
    }

    /* 7. Write one JSON containing all requested dates. */
    json_write_header(fp, "MIDNIGHT_DATA_MESSAGE");
    json_write_general(ctx, fp, serial, dt_str);
    json_write_d1(fp, ctx, serial);
    json_write_d6_multi(fp, ctx, snapshots, actual_count);
    json_write_footer(fp);

    fclose(fp);

    /* 8. Free all snapshots. */
    int i;
    for (i = 0; i < actual_count; i++)
        mn_snapshot_free(&snapshots[i]);

    free(snapshots);

    strcpy(output_file, out_path);

    LOG_INFO("Midnight JSON written: %s", out_path);
    LOG_INFO("Total Midnight dates generated: %d", actual_count);
    printf("JSON file generated: %s\n", out_path);

    return 0;
}
