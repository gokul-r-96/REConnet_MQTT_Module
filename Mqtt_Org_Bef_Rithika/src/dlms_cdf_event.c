
#include "../include/general.h"
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

/**
 * @brief Lookup Event param code, name and unit for a given OBIS from Redis.
 *
 * Reads three sub-hashes from REDIS_HASH_EVENT_OBIS_MAP.
 *
 * @param ctx        Redis context.
 * @param obis       OBIS decimal string.
 * @param code_buf   Output: param code.
 * @param name_buf   Output: param name.
 * @param unit_buf   Output: param unit.
 */
static void lookup_event_obis_mapping(redisContext *ctx, const char *obis,
                                      char *code_buf, size_t code_len,
                                      char *name_buf, size_t name_len,
                                      char *unit_buf, size_t unit_len)
{
    /* Default to empty strings on failure */
    code_buf[0] = name_buf[0] = unit_buf[0] = '\0';

    char *code_json = redis_hget(ctx, REDIS_HASH_EVENT_OBIS_MAP, REDIS_FIELD_PARAM_CODE);
    char *name_json = redis_hget(ctx, REDIS_HASH_EVENT_OBIS_MAP, REDIS_FIELD_PARAM_NAME);
    char *unit_json = redis_hget(ctx, REDIS_HASH_EVENT_OBIS_MAP, REDIS_FIELD_PARAM_UNIT);

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
                           const char *serial, const char *date, const char *event_type,
                           redisContext *ctx, EventData *event_data)
{
    memset(event_data, 0, sizeof(*event_data));
    snprintf(event_data->meter_serial, sizeof(event_data->meter_serial), "%s", serial);

    /* Build table name */
    char table[128];
    snprintf(table, sizeof(table), "event_data_%s_%s_%s_%s",
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

    /* Build WHERE clause based on filters */
    char where_clause[512] = "";
    int is_date_all = (strcmp(date, "all") == 0);
    int is_event_type_all = (strcmp(event_type, "all") == 0);

    if (is_date_all && is_event_type_all)
    {
        /* All events for current month - use current year-month */
        time_t now = time(NULL);
        struct tm *t = localtime(&now);
        char year_month[8];
        strftime(year_month, sizeof(year_month), "%Y-%m", t);
        snprintf(where_clause, sizeof(where_clause),
                 "WHERE strftime('%%Y-%%m', \"0_0_1_0_0_255\") = '%s'", year_month);
    }
    else if (!is_date_all && is_event_type_all)
    {
        /* All events on a specific date */
        snprintf(where_clause, sizeof(where_clause),
                 "WHERE DATE(\"0_0_1_0_0_255\") = '%s'", date);
    }
    else if (is_date_all && !is_event_type_all)
    {
        /* All events of a specific type (no date filter) */
        snprintf(where_clause, sizeof(where_clause),
                 "WHERE event_type = '%s'", event_type);
    }
    else
    {
        /* Specific event type on a specific date */
        snprintf(where_clause, sizeof(where_clause),
                 "WHERE DATE(\"0_0_1_0_0_255\") = '%s' AND event_type = '%s'",
                 date, event_type);
    }

    /* Build query */
    char query[1024];
    snprintf(query, sizeof(query),
             "SELECT * FROM %s %s ORDER BY \"0_0_1_0_0_255\" ASC",
             table, where_clause);

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

            /* Event code column (matches evt_map->obis_code) */
            if (strcmp(col_name, evt_map->obis_code) == 0)
            {
                EventParam *p = &entry->params[param_idx];
                snprintf(p->obis_code, sizeof(p->obis_code), "%s", col_name);
                // obis_dec_to_hex(col_name, p->obis_hex, sizeof(p->obis_hex));
                obis_dec_to_hex(col_name, p->obis_hex);
                snprintf(p->param_code, sizeof(p->param_code), "");
                snprintf(p->param_name, sizeof(p->param_name), "");
                snprintf(p->unit, sizeof(p->unit), "count");
                snprintf(p->value, sizeof(p->value), "%s", val_str);
                param_idx++;
                continue;
            }

            /* Regular OBIS parameters */
            char obis_hex[24];
            if (obis_dec_to_hex(col_name, obis_hex) != 0)
            {
                continue;
            }

            EventParam *p = &entry->params[param_idx];
            snprintf(p->obis_code, sizeof(p->obis_code), "%s", col_name);
            snprintf(p->obis_hex, sizeof(p->obis_hex), "%s", obis_hex);
            snprintf(p->value, sizeof(p->value), "%s", val_str);

            /* Lookup mapping from Event-specific hash */
            lookup_event_obis_mapping(ctx, col_name,
                                      p->param_code, sizeof(p->param_code),
                                      p->param_name, sizeof(p->param_name),
                                      p->unit, sizeof(p->unit));

            param_idx++;
        }

        entry->param_count = param_idx;
        entry_idx++;
    }

    event_data->entry_count = entry_idx;

    sqlite3_finalize(stmt);
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
    cJSON *root = NULL;
    int paramname_avlb = chk_param_name_hash_exists(ctx, "event_param");

    if (paramname_avlb == 0) // 0 means it is available in redis
    {
        root = get_param_name_json(ctx, "event_param");
        if (!root)
            return;
    }

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
                        "\t\t\t\t<SNAPSHOT"
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
        }

        fprintf(fp, "\t\t\t</EVENT>\n");
    }

    fprintf(fp, "\t\t</D5>\n");

    if (root)
        cJSON_Delete(root);
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

    /* 2. Read Event data from SQLite */
    EventData event_data;
    if (read_event_data(SQLITE_DB_PATH, &status, serial, date, event_type,
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
    snprintf(out_path, sizeof(out_path),
             "CDF_EVENT_%s_%s_%s.xml", serial, date, event_type);

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
