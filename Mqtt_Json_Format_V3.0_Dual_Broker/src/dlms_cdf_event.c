
#include "../include/general.h"

extern int event_cmd_redis_resp;
extern int ls_cmd_redis_resp;
extern int billing_cmd_redis_resp;
extern int midnight_cmd_redis_resp;

/** Event type mapping table */
static const EventTypeMap EVENT_TYPE_TABLE[] = {
    {1, "Voltage events", "0_0_96_11_0_255", "0_0_99_98_0_255"},
    {2, "Current events", "0_0_96_11_1_255", "0_0_99_98_1_255"},
    {3, "Power failure events", "0_0_96_11_2_255", "0_0_99_98_2_255"},
    {4, "Transactional events", "0_0_96_11_3_255", "0_0_99_98_3_255"},
    {5, "Other events", "0_0_96_11_4_255", "0_0_99_98_4_255"},
    {6, "Roll over events", "0_0_96_11_5_255", "0_0_99_98_5_255"},
    {7, "Control events", "0_0_96_11_6_255", "0_0_99_98_6_255"},
    {0, NULL, NULL, NULL} /* Sentinel */
};

/**
 * @brief Get event type mapping for a given event type number.
 * @param event_type Event type number (1-7).
 * @return Pointer to EventTypeMap, or NULL if not found.
 */
static const EventTypeMap *get_event_type_map(int event_type)
{
    for (int i = 0; EVENT_TYPE_TABLE[i].event_type_num != 0; i++)
    {
        if (EVENT_TYPE_TABLE[i].event_type_num == event_type)
        {
            return &EVENT_TYPE_TABLE[i];
        }
    }
    return NULL;
}

/*
 * Return the calendar date immediately after date (YYYY-MM-DD).
 * Used for an inclusive start/end date range.
 */
static int get_next_date(const char *date, char *next_date, size_t next_len)
{
    int year, month, day;
    struct tm tm_date;
    time_t t;

    if (!date || !next_date)
        return -1;

    if (sscanf(date, "%d-%d-%d", &year, &month, &day) != 3)
        return -1;

    memset(&tm_date, 0, sizeof(tm_date));
    tm_date.tm_year = year - 1900;
    tm_date.tm_mon = month - 1;
    tm_date.tm_mday = day;
    tm_date.tm_hour = 12;

    t = mktime(&tm_date);
    if (t == (time_t)-1)
        return -1;

    tm_date.tm_mday++;
    t = mktime(&tm_date);
    if (t == (time_t)-1)
        return -1;

    snprintf(next_date, next_len, "%04d-%02d-%02d",
             tm_date.tm_year + 1900,
             tm_date.tm_mon + 1,
             tm_date.tm_mday);

    return 0;
}

/**
 * @brief Read Event data from SQLite database with filtering.
 *
 * Filters by date and/or event_type. Special "all" values mean no filter.
 * - date="all", event_type="all" → all events for current month
 * - date="YYYY-MM-DD", event_type="all" → all events on that date
 * - date="all", event_type="3" → all events of type 3
 * - date="YYYY-MM-DD", event_type="3" → events of type 3 on that date
 *
 * @param db_path     Path to SQLite database.
 * @param status      Meter status (contains table name components).
 * @param serial      Meter serial number.
 * @param date        Date string (YYYY-MM-DD) or "all".
 * @param event_type  Event type string ("1"-"7") or "all".
 * @param ctx         Redis context for OBIS mapping lookups.
 * @param event_data  Output structure (caller must call event_data_free()).
 * @return            0 on success, -1 on error.
 */
static int read_event_data(const char *db_path, const MeterStatus *status,
                           const char *serial, const char *start_date,
                           const char *end_date, const char *event_type,
                           redisContext *ctx, EventData *event_data)
{
    memset(event_data, 0, sizeof(*event_data));
    snprintf(event_data->meter_serial, sizeof(event_data->meter_serial), "%s", serial);

    /* Build table name */
    char table[128];

    if (event_cmd_redis_resp == 1)
    {
        snprintf(table, sizeof(table), "event_data_od_%s_%s_%s_%s",
                 status->manuf_key, status->dcu_serial, status->port, serial);
    }
    else
    {
        snprintf(table, sizeof(table), "event_data_%s_%s_%s_%s",
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

    /* Build WHERE clause based on filters */
    char where_clause[1024] = "";
    int is_date_all = (strcmp(start_date, "all") == 0);
    int is_event_type_all = (strcmp(event_type, "all") == 0);

    if (is_date_all && is_event_type_all)
    {

        /* All events for current month - preserve existing behavior. */
        time_t now = time(NULL);
        struct tm *t = localtime(&now);
        char year_month[8];
        strftime(year_month, sizeof(year_month), "%Y-%m", t);
        snprintf(where_clause, sizeof(where_clause),
                 "WHERE \"0_0_1_0_0_255\" LIKE '%s%%'", year_month);
    }
    else if (!is_date_all && is_event_type_all)
    {

        if (end_date && strcmp(end_date, start_date) != 0)
        {
            char next_date[16];

            if (get_next_date(end_date, next_date, sizeof(next_date)) != 0)
            {
                LOG_ERROR("Invalid end date: %s", end_date);
                sqlite3_close(db);
                return -1;
            }

            /* Inclusive start_date through end_date. */
            snprintf(where_clause, sizeof(where_clause),
                     "WHERE \"0_0_1_0_0_255\" >= '%s' "
                     "AND \"0_0_1_0_0_255\" < '%s'",
                     start_date, next_date);
        }
        else
        {
            /* Preserve existing single-date behavior. */
            snprintf(where_clause, sizeof(where_clause),
                     "WHERE \"0_0_1_0_0_255\" LIKE '%s%%'", start_date);
        }
    }
    else if (is_date_all && !is_event_type_all)
    {

        /* All events of a specific type (no date filter). */
        snprintf(where_clause, sizeof(where_clause),
                 "WHERE event_type = '%s'", event_type);
    }
    else
    {

        if (end_date && strcmp(end_date, start_date) != 0)
        {
            char next_date[16];

            if (get_next_date(end_date, next_date, sizeof(next_date)) != 0)
            {
                LOG_ERROR("Invalid end date: %s", end_date);
                sqlite3_close(db);
                return -1;
            }

            snprintf(where_clause, sizeof(where_clause),
                     "WHERE \"0_0_1_0_0_255\" >= '%s' "
                     "AND \"0_0_1_0_0_255\" < '%s' "
                     "AND event_type = '%s'",
                     start_date, next_date, event_type);
        }
        else
        {
            snprintf(where_clause, sizeof(where_clause),
                     "WHERE \"0_0_1_0_0_255\" LIKE '%s%%' "
                     "AND event_type = '%s'",
                     start_date, event_type);
        }
    }

    /* Build query */
    char query[1024];
    if (event_cmd_redis_resp == 1 && billing_cmd_redis_resp == 1 && ls_cmd_redis_resp == 1 && midnight_cmd_redis_resp == 1)
    {
        snprintf(query, sizeof(query),
                 "SELECT * FROM %s ORDER BY \"0_0_1_0_0_255\" ASC",
                 table);
    }
    else
    {
        snprintf(query, sizeof(query),
                 "SELECT * FROM %s %s ORDER BY \"0_0_1_0_0_255\" ASC",
                 table, where_clause);
    }

    LOG_DEBUG("SQL query: %s", query);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, query, -1, &stmt, NULL) != SQLITE_OK)
    {
        LOG_ERROR("Failed to prepare query: %s", sqlite3_errmsg(db));
        sqlite3_close(db);
        return -1;
    }

    int col_count = sqlite3_column_count(stmt);
    LOG_INFO("Query returned %d columns", col_count);

    /* Allocate for events (assume max 1000) */
    event_data->entries = (EventEntry *)calloc(1000, sizeof(EventEntry));
    if (!event_data->entries)
    {
        LOG_ERROR("Memory allocation failed for EventEntry array");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    int entry_idx = 0;

    /* Process result rows */
    while (sqlite3_step(stmt) == SQLITE_ROW && entry_idx < 1000)
    {
        EventEntry *entry = &event_data->entries[entry_idx];

        /* Get event_type from this row */
        const char *row_event_type = NULL;
        for (int col = 0; col < col_count; col++)
        {
            const char *col_name = sqlite3_column_name(stmt, col);
            if (strcmp(col_name, "event_type") == 0)
            {
                row_event_type = (const char *)sqlite3_column_text(stmt, col);
                break;
            }
        }

        if (!row_event_type)
        {
            LOG_WARN("No event_type column found in row %d", entry_idx);
            continue;
        }

        int evt_type_num = atoi(row_event_type);
        const EventTypeMap *evt_map = get_event_type_map(evt_type_num);
        if (!evt_map)
        {
            LOG_WARN("Unknown event_type: %s", row_event_type);
            continue;
        }

        /* Convert profile OBIS to hex for EVT_CODE */
        if (obis_dec_to_hex(evt_map->profile_obis_code, entry->evt_code_hex) != 0)
        {
            LOG_WARN("Failed to convert profile OBIS for event type %d", evt_type_num);
            snprintf(entry->evt_code_hex, sizeof(entry->evt_code_hex), "00_00_00_00_00_00");
        }

        /* Allocate for parameters */
        entry->params = (EventParam *)calloc(col_count, sizeof(EventParam));
        if (!entry->params)
        {
            LOG_ERROR("Memory allocation failed for EventParam array");
            break;
        }

        int param_idx = 0;

        /* Process columns */
        for (int col = 0; col < col_count; col++)
        {
            const char *col_name = sqlite3_column_name(stmt, col);

            /* Skip metadata columns */
            if (strcmp(col_name, "id") == 0 ||
                strcmp(col_name, "update_time") == 0 ||
                strcmp(col_name, "event_type") == 0 ||
                strcmp(col_name, "unique_id") == 0)
            {
                continue;
            }

            const char *val_str = (const char *)sqlite3_column_text(stmt, col);
            if (!val_str)
                val_str = "0.0";

            /* Timestamp column (0_0_1_0_0_255) */
            if (strcmp(col_name, "0_0_1_0_0_255") == 0)
            {
                snprintf(entry->event_date, sizeof(entry->event_date), "%s.000", val_str);
                continue;
            }

            char obis_hex[24];

            if (obis_dec_to_hex(col_name, obis_hex) != 0)
            {
                LOG_WARN("Skipping invalid OBIS column: %s", col_name);
                continue;
            }

            if (strcasecmp(val_str, "FFFF") == 0 ||
                strcasecmp(val_str, "FFFFFFFF") == 0)
            {
                LOG_DEBUG("Skipping OBIS %s because value is %s",
                          col_name, val_str);
                continue;
            }

            EventParam *p = &entry->params[param_idx];

            snprintf(p->obis_code,
                     sizeof(p->obis_code),
                     "%s",
                     col_name);

            snprintf(p->obis_hex,
                     sizeof(p->obis_hex),
                     "%s",
                     obis_hex);

            snprintf(p->value,
                     sizeof(p->value),
                     "%s",
                     val_str);

            /*
             * No param_code / param_name / unit mapping.
             * These fields are intentionally left unused.
             */

            LOG_DEBUG("Event entry[%d] param[%d]: OBIS=%s HEX=%s VALUE=%s",
                      entry_idx,
                      param_idx,
                      p->obis_code,
                      p->obis_hex,
                      p->value);

            param_idx++;
        }

        entry->param_count = param_idx;
        entry_idx++;
    }

    event_data->entry_count = entry_idx;

    sqlite3_finalize(stmt);

    if (strstr(table, "od_"))
    {
        drop_table(table, db);
    }

    sqlite3_close(db);

    LOG_INFO("Meter %s: loaded %d event entries", serial, event_data->entry_count);

    if (event_data->entry_count == 0)
    {
        LOG_WARN("No event data found for specified filters");
        free(event_data->entries);
        event_data->entries = NULL;
        return -1;
    }

    return 0;
}

/**
 * @brief Free memory owned by an EventData structure.
 * @param event_data Pointer to the event data to free.
 */
static void event_data_free(EventData *event_data)
{
    if (!event_data || !event_data->entries)
        return;

    for (int i = 0; i < event_data->entry_count; i++)
    {
        if (event_data->entries[i].params)
        {
            free(event_data->entries[i].params);
        }
    }
    free(event_data->entries);
    event_data->entries = NULL;
}

/**
 * @brief Write the D5 (Event Profile) section from EventData.
 * @param fp          Output file pointer.
 * @param event_data  Populated event data.
 */
static void cdf_write_d5(FILE *fp, redisContext *ctx, const EventData *event_data)
{
    // cJSON *root = NULL;
    // int paramname_avlb = chk_param_name_hash_exists(ctx, "event_param");

    // if (paramname_avlb == 0) // 0 means it is available in redis
    // {
    //     root = get_param_name_json(ctx, "event_param");
    //     if (!root)
    //         return;
    // }

    fprintf(fp, "\t\t<!--Event Profile-->\n");
    fprintf(fp, "\t\t<D5>\n");

    for (int i = 0; i < event_data->entry_count; i++)
    {
        const EventEntry *entry = &event_data->entries[i];
        fprintf(fp, "\t\t\t<EVENT EVT_CODE=\"%s\" DATE=\"%s\">\n",
                entry->evt_code_hex, entry->event_date);

        for (int j = 0; j < entry->param_count; j++)
        {
            const EventParam *p = &entry->params[j];
            if (strcmp(p->value, "FFFFFFFF") != 0)
            {
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
                            "\t\t\t\t<SNAPSHOT"
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
        }

        fprintf(fp, "\t\t\t</EVENT>\n");
    }

    fprintf(fp, "\t\t</D5>\n");

    // if (root)
    //     cJSON_Delete(root);
}
static void json_write_d5(FILE *fp,
                          redisContext *ctx,
                          const EventData *event_data)
{
    (void)ctx;

    fprintf(fp, "    \"EVENT_PROFILE\": {\n");

    /*
     * Only OBIS_CODE is present in the JSON structure.
     */
    fprintf(fp, "      \"FIELDS\": [\"OBIS_CODE\"],\n");

    /*
     * PARAMS
     *
     * Each SQLite OBIS column is converted to hexadecimal.
     *
     * Example:
     *
     * SQLite:
     *     1_0_1_8_0_255
     *
     * JSON:
     *     [\"01_00_01_08_00_ff\"]
     */
    fprintf(fp, "      \"PARAMS\": [\n");

    int first_param = 1;

    if (event_data->entry_count > 0)
    {
        const EventEntry *entry = &event_data->entries[0];

        for (int j = 0; j < entry->param_count; j++)
        {
            const EventParam *p = &entry->params[j];

            if (p->obis_hex[0] == '\0')
                continue;

            if (strcasecmp(p->value, "FFFF") == 0 ||
                strcasecmp(p->value, "FFFFFFFF") == 0)
                continue;

            if (!first_param)
                fprintf(fp, ",\n");

            fprintf(fp,
                    "        [\"%s\"]",
                    p->obis_hex);

            first_param = 0;
        }
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ],\n");

    /*
     * RECORDS
     *
     * Format:
     *
     * [
     *     [
     *         \"date\",
     *         [\"value1\", \"value2\", ...]
     *     ]
     * ]
     */
    fprintf(fp, "      \"RECORDS\": [\n");

    int first_record = 1;

    for (int i = 0; i < event_data->entry_count; i++)
    {
        const EventEntry *entry = &event_data->entries[i];

        if (!first_record)
            fprintf(fp, ",\n");

        fprintf(fp,
                "        [\"%s\",[",
                entry->event_date);

        int first_value = 1;

        for (int j = 0; j < entry->param_count; j++)
        {
            const EventParam *p = &entry->params[j];

            if (p->obis_hex[0] == '\0')
                continue;

            if (strcasecmp(p->value, "FFFF") == 0 ||
                strcasecmp(p->value, "FFFFFFFF") == 0)
                continue;

            if (!first_value)
                fprintf(fp, ",");

            fprintf(fp,
                    "\"%s\"",
                    p->value);

            first_value = 0;
        }

        fprintf(fp, "]]");

        first_record = 0;
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ]\n");
    fprintf(fp, "    }\n");
}
/**
 * @brief Generate a CDF file for Event Log data (data type 5).
 *
 * Requires two additional arguments: date and event_type.
 * Special value "all" means no filter on that dimension.
 *
 * @param ctx         Redis context.
 * @param serial      Meter serial number.
 * @param date        Date string (YYYY-MM-DD) or "all".
 * @param event_type  Event type ("1"-"7") or "all".
 * @return            0 on success, -1 on error.
 */
int generate_event_log_cdf(redisContext *ctx, const char *serial,
                           const char *date, const char *event_type, char *output_file)
{
    LOG_INFO("Generating Event Log CDF for meter %s date=%s event_type=%s",
             serial, date, event_type);

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

    /* 2. Read Event data from SQLite */
    EventData event_data;
    memset(&event_data, 0, sizeof(event_data));

    if (read_event_data(sqlite_db_path, &status, serial, date, date, event_type,
                        ctx, &event_data) != 0)
    {
        LOG_ERROR("Cannot read event data for meter %s", serial);
    }

    if (event_data.entry_count == 0)
    {
        LOG_WARN("No event data found for meter %s", serial);
        // event_data_free(&event_data);
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
                 "%s/data/CDF_EVENT_%s_%s_%s.xml",
                 base_path, serial, date, event_type);

        // snprintf(out_path, sizeof(out_path),
        //          "%sCDF_EVENT_%s_%s_%s.xml", CDF_OUTPUT_DIR, serial, date, event_type);
    }

    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", out_path, strerror(errno));
        event_data_free(&event_data);
        return -1;
    }

    /* 4. Write CDF XML */
    cdf_write_header(fp, date_str);
    cdf_write_general(ctx, fp, serial, dt_str);
    cdf_write_d1(fp, ctx, serial);
    cdf_write_d5(fp, ctx, &event_data);
    cdf_write_footer(fp);

    fclose(fp);
    event_data_free(&event_data);

    strcpy(output_file, out_path);
    LOG_INFO("Event Log CDF written: %s", out_path);
    printf("CDF file generated: %s\n", out_path);
    return 0;
}

int generate_event_log_json(redisContext *ctx, const char *serial,
                            const char *start_date, const char *end_date,
                            const char *event_type, char *output_file)
{
    LOG_INFO("Generating Event Log JSON for meter %s start=%s end=%s event_type=%s",
             serial, start_date, end_date, event_type);
    /* 1. Read meter status from Redis */
    MeterStatus status;
    if (read_meter_status(ctx, serial, &status) != 0)
    {
        LOG_ERROR("Cannot read meter status for meter %s", serial);
        return -1;
    }
    /* Database path */
    char base_path_dir[256];
    char sqlite_db_path[256];
    if (get_base_path(base_path_dir, sizeof(base_path_dir)) == 0)
    {
        snprintf(sqlite_db_path,
                 sizeof(sqlite_db_path),
                 "%s/data/dcu_dlms.db",
                 base_path_dir);
    }
    /* 2. Read Event data */
    EventData event_data;
    memset(&event_data, 0, sizeof(event_data));

    if (read_event_data(sqlite_db_path, &status, serial, start_date, end_date,
                        event_type, ctx, &event_data) != 0)
    {
        LOG_ERROR("Cannot read event data for meter %s", serial);
    }

    if (event_data.entry_count == 0)
    {
        LOG_WARN("No event data found for meter %s", serial);
    }

    /* 3. Build output file path */
    char dt_str[32];
    get_datetime_str(dt_str, sizeof(dt_str));
    char out_path[512];
    char base_path[256];
    if (get_base_path(base_path, sizeof(base_path)) == 0)
    {
        snprintf(out_path, sizeof(out_path),
                 "%s/data/EVENT_%s_%s_%s_%s.json",
                 base_path, serial, start_date, end_date, event_type);
    }
    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)",
                  out_path,
                  strerror(errno));

        event_data_free(&event_data);
        return -1;
    }

    /* 4. Write JSON */
    json_write_header(fp, "EVENT_DATA_MESSAGE");
    json_write_general(ctx, fp, serial, dt_str);
    json_write_d1(fp, ctx, serial);
    json_write_d5(fp, ctx, &event_data);
    json_write_footer(fp);

    fclose(fp);

    event_data_free(&event_data);

    strcpy(output_file, out_path);

    LOG_INFO("Event Log JSON written: %s", out_path);
    printf("JSON file generated: %s\n", out_path);

    return 0;
}
