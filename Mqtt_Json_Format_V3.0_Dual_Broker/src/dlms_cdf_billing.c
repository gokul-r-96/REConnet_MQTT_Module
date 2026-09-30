

#include "../include/general.h"

extern int billing_cmd_redis_resp;
extern int event_cmd_redis_resp;
extern int ls_cmd_redis_resp;
extern int midnight_cmd_redis_resp;

// rithika 13Aug2026
int int_cur_month = 0;
extern int multi_month_billing;
/* ============================================================
 *  Billing data helpers
 * ============================================================ */

/**
 * @brief Read Billing data for a specific year-month from SQLite database.
 *
 * Opens the database, queries the table "bill_data_<manuf>_<dcu>_<port>_<serial>"
 * for all records matching the specified year-month (column "bill_date"),
 * ordered by bill_date ascending. All matching entries are loaded.
 *
 * @param db_path     Path to SQLite database.
 * @param status      Meter status (contains table name components).
 * @param serial      Meter serial number.
 * @param year_month  Year-month string in YYYY-MM format.
 * @param ctx         Redis context for OBIS mapping lookups.
 * @param bill_data   Output structure (caller must call billing_data_free()).
 * @return            0 on success, -1 on error.
 */
static int read_billing_data(const char *db_path, const MeterStatus *status,
                             const char *serial, const char *year_month,
                             redisContext *ctx, BillingData *bill_data)
{
    memset(bill_data, 0, sizeof(*bill_data));
    snprintf(bill_data->meter_serial, sizeof(bill_data->meter_serial), "%s", serial);
    snprintf(bill_data->year_month, sizeof(bill_data->year_month), "%s", year_month);

    /* Build table name */
    char table[128];
    static char od_table[128] = {0};
    // if (billing_cmd_redis_resp == 1 && ls_cmd_redis_resp == 1 && midnight_cmd_redis_resp == 1 && event_cmd_redis_resp == 1)
    // {
    if (billing_cmd_redis_resp == 1)
    {
        snprintf(table, sizeof(table), "bill_data_od_%s_%s_%s_%s",
                 status->manuf_key, status->dcu_serial, status->port, serial);
        snprintf(od_table, sizeof(od_table), "%s", table);
    }
    else
    {
        snprintf(table, sizeof(table), "bill_data_%s_%s_%s_%s",
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

    /* Query all billing records for the specified year-month */
    char query[512];
    snprintf(query, sizeof(query),
             "SELECT * FROM %s WHERE bill_date like '%s' "
             "ORDER BY bill_date ASC",
             table, year_month);

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

    /* Allocate the first entry; grow dynamically for additional billing dates. */
    bill_data->entries = (BillingEntry *)calloc(1, sizeof(BillingEntry));
    if (!bill_data->entries)
    {
        LOG_ERROR("Memory allocation failed for BillingEntry array");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    int entry_idx = 0;

    /* Read all billing rows for this month. */
    while (sqlite3_step(stmt) == SQLITE_ROW)
    {
        BillingEntry *entry;

        if (entry_idx > 0)
        {
            BillingEntry *tmp = (BillingEntry *)realloc(
                bill_data->entries,
                (entry_idx + 1) * sizeof(BillingEntry));

            if (!tmp)
            {
                LOG_ERROR("Memory allocation failed for BillingEntry array");
                break;
            }

            bill_data->entries = tmp;
            memset(&bill_data->entries[entry_idx], 0, sizeof(BillingEntry));
        }

        entry = &bill_data->entries[entry_idx];

        /* Allocate for parameters */
        entry->params = (BillParam *)calloc(col_count, sizeof(BillParam));
        if (!entry->params)
        {
            LOG_ERROR("Memory allocation failed for BillParam array");
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

            const char *val_str = (const char *)sqlite3_column_text(stmt, col);
            if (!val_str)
                val_str = "0.0";

            /* bill_date is the billing timestamp */
            if (strcmp(col_name, "bill_date") == 0)
            {
                snprintf(entry->billing_date, sizeof(entry->billing_date), "%s.000", val_str);
                LOG_DEBUG("Billing entry[%d] date: %s", entry_idx, entry->billing_date);
                continue;
            }

            /* OBIS column (either actual OBIS or special column like 0_0_0_1_2_255) */
            const char *obis = col_name;
            BillParam *p = &entry->params[param_idx];

            /* Handle special billing date OBIS (0_0_0_1_2_255) */
            if (strcmp(obis, "0_0_0_1_2_255") == 0)
            {
                /* This is another form of billing date, skip it */
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

            LOG_DEBUG("Bill entry[%d] param[%d]: obis=%s hex=%s val=%s",
                      entry_idx, param_idx, p->obis_code, p->obis_hex, p->value);

            param_idx++;
        }

        entry->param_count = param_idx;
        entry_idx++;
    }

    bill_data->entry_count = entry_idx;

    sqlite3_finalize(stmt);

    // rithika 12Aug2026
    // if (billing_cmd_redis_resp == 0 && od_table[0] != '\0')
    // {
    if (int_cur_month == 0 && multi_month_billing == 0)
    {
        LOG_INFO("Deleting od table %s", od_table);
        drop_table(od_table, db);
    }

    sqlite3_close(db);

    LOG_INFO("Meter %s year-month %s: loaded %d billing entries from DB",
             serial, year_month, bill_data->entry_count);

    if (bill_data->entry_count == 0)
    {
        LOG_WARN("No billing data found for meter %s in %s", serial, year_month);
        free(bill_data->entries);
        bill_data->entries = NULL;
        return -1;
    }

    return 0;
}

/**
 * @brief Free memory owned by a BillingData structure.
 * @param bill_data Pointer to the billing data to free.
 */
static void billing_data_free(BillingData *bill_data)
{
    if (!bill_data || !bill_data->entries)
        return;

    for (int i = 0; i < bill_data->entry_count; i++)
    {
        if (bill_data->entries[i].params)
        {
            free(bill_data->entries[i].params);
        }
    }
    free(bill_data->entries);
    bill_data->entries = NULL;
}

/**
 * @brief Write the D3 (Billing Profile) section from BillingData.
 * @param fp         Output file pointer.
 * @param bill_data  Populated billing data (1-2 entries).
 */
static void cdf_write_d3(FILE *fp, redisContext *ctx, const BillingData *bill_data, const BillingData *bill_data_curr)
{
    // cJSON *root = NULL;
    // int paramname_avlb = chk_param_name_hash_exists(ctx, "billing_param");

    // if (paramname_avlb == 0) // 0 means it is available in redis
    // {
    //     root = get_param_name_json(ctx, "billing_param");
    //     if (!root)
    //         return;
    // }

    fprintf(fp, "\t\t<!--Billing Profile-->\n");
    fprintf(fp, "\t\t<D3>\n");

    for (int i = 0; i < bill_data->entry_count; i++)
    {
        const BillingEntry *entry = &bill_data->entries[i];
        fprintf(fp, "\t\t\t<BILLING DATE=\"%s\">\n", entry->billing_date);

        for (int j = 0; j < entry->param_count; j++)
        {
            const BillParam *p = &entry->params[j];
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
                        "\t\t\t\t<PROFILE"
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

        fprintf(fp, "\t\t\t</BILLING>\n");
    }

    for (int i = 0; i < bill_data_curr->entry_count; i++)
    {
        const BillingEntry *entry = &bill_data_curr->entries[i];
        fprintf(fp, "\t\t\t<BILLING DATE=\"%s\">\n", entry->billing_date);

        for (int j = 0; j < entry->param_count; j++)
        {
            const BillParam *p = &entry->params[j];
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
                        "\t\t\t\t<PROFILE"
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

        fprintf(fp, "\t\t\t</BILLING>\n");
    }

    fprintf(fp, "\t\t</D3>\n");

    // if (root)
    //     cJSON_Delete(root);
}

static void json_write_d3(FILE *fp, redisContext *ctx, const BillingData *bill_data, const BillingData *bill_data_curr)
{
    (void)ctx;
    fprintf(fp, "    \"BILLING_PROFILE\": {\n");
    fprintf(fp, "      \"FIELDS\":[\"OBIS_CODE\"],\n");
    fprintf(fp, "      \"PARAMS\":[\n");

    /* Print OBIS codes from SQLite columns only, in hexadecimal form. */
    if (bill_data->entry_count > 0)
    {
        const BillingEntry *entry = &bill_data->entries[0];
        int first = 1;

        for (int j = 0; j < entry->param_count; j++)
        {
            const BillParam *p = &entry->params[j];

            if (!first)
                fprintf(fp, ",\n");

            fprintf(fp, "        [\"%s\"]", p->obis_hex);
            first = 0;
        }
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ],\n");
    fprintf(fp, "      \"RECORDS\":[\n");

    int first_record = 1;

    /* Previous billing entries */
    for (int i = 0; i < bill_data->entry_count; i++)
    {
        const BillingEntry *entry = &bill_data->entries[i];

        if (!first_record)
            fprintf(fp, ",\n");

        fprintf(fp, "        [\"%s\",[", entry->billing_date);

        for (int j = 0; j < entry->param_count; j++)
        {
            const BillParam *p = &entry->params[j];

            if (j > 0)
                fprintf(fp, ",");

            fprintf(fp, "\"%s\"", p->value);
        }

        fprintf(fp, "]]");
        first_record = 0;
    }

    /* Current month billing entries */
    for (int i = 0; i < bill_data_curr->entry_count; i++)
    {
        const BillingEntry *entry = &bill_data_curr->entries[i];

        if (!first_record)
            fprintf(fp, ",\n");

        fprintf(fp, "        [\"%s\",[", entry->billing_date);

        for (int j = 0; j < entry->param_count; j++)
        {
            const BillParam *p = &entry->params[j];

            if (j > 0)
                fprintf(fp, ",");

            fprintf(fp, "\"%s\"", p->value);
        }

        fprintf(fp, "]]");
        first_record = 0;
    }

    fprintf(fp, "\n");
    fprintf(fp, "      ]\n");
    fprintf(fp, "    }\n");
}

/**
 * @brief Generate a CDF file for Billing data (data type 3).
 *
 * Requires a year-month argument in argv[3] (YYYY-MM format).
 * Billing data can contain multiple entries per month.
 *
 * @param ctx         Redis context.
 * @param serial      Meter serial number.
 * @param year_month  Year-month string for which to fetch billing data.
 * @return            0 on success, -1 on error.
 */
int generate_billing_cdf(redisContext *ctx, const char *serial, const char *year_month, char *output_file)
{

    time_t now = time(NULL);
    struct tm *curr_date = localtime(&now);
    char date[32];
    char curr_year[32] = {0};
    char month[32];
    char str_year[8];

    strftime(month, sizeof(month), "%b", curr_date);
    strftime(str_year, sizeof(str_year), "%Y", curr_date);

    if (strstr(month, "Jul"))
    {
        sprintf(date, "July %s", str_year);
    }
    else if (strstr(date, "Jun"))
    {
        sprintf(date, "June %s", str_year);
    }
    else
    {
        strftime(date, sizeof(date), "%b %Y", curr_date);
    }

    if (strcmp(date, year_month) == 0)
    {
        int year;
        sscanf(year_month, "%*s %d", &year);
        sprintf(curr_year, "curr mon %d", year);
    }
    LOG_INFO("date %s, year_month %s curr_year %s", date, year_month, curr_year);

    LOG_INFO("Generating Billing CDF for meter %s year-month %s date %s, year_month %s curr_year %s", serial, year_month, date, year_month, curr_year);

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

    /* 2. Read Billing data from SQLite */
    BillingData bill_data;
    if (read_billing_data(sqlite_db_path, &status, serial, year_month, ctx, &bill_data) != 0)
    {
        LOG_ERROR("Cannot read billing data for meter %s year-month %s", serial, year_month);
        // return -1;
    }

    BillingData bill_data_curr = {0};

    if (strstr(curr_year, "curr mon "))
    {

        if (read_billing_data(sqlite_db_path, &status, serial, curr_year, ctx, &bill_data_curr) != 0)
        {
            LOG_ERROR("Cannot read billing data for meter %s year-month %s", serial, curr_year);
            // return -1;
        }
    }

    if (bill_data.entry_count == 0)
    {
        LOG_WARN("No billing data found for meter %s in %s", serial, year_month);
        // billing_data_free(&bill_data);
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
                 "%s/data/CDF_BILL_%s_%s.xml",
                 base_path, serial, year_month);

        // snprintf(out_path, sizeof(out_path),
        //          "%sCDF_BILL_%s_%s.xml", CDF_OUTPUT_DIR, serial, year_month);
    }
    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", out_path, strerror(errno));
        billing_data_free(&bill_data);
        billing_data_free(&bill_data_curr);
        return -1;
    }

    /* 4. Write CDF XML */
    cdf_write_header(fp, date_str);
    cdf_write_general(ctx, fp, serial, dt_str);
    cdf_write_d1(fp, ctx, serial);
    cdf_write_d3(fp, ctx, &bill_data, &bill_data_curr);
    cdf_write_footer(fp);

    fclose(fp);
    billing_data_free(&bill_data);
    billing_data_free(&bill_data_curr);

    strcpy(output_file, out_path);
    LOG_INFO("Billing CDF written: %s", out_path);
    printf("CDF file generated: %s\n", out_path);
    return 0;
}

static int generate_billing_json_single_month(redisContext *ctx, const char *serial, const char *year_month, char *output_file)
{
    time_t now = time(NULL);
    struct tm *curr_date = localtime(&now);
    char date[32];
    char curr_year[32] = {0};

    char month[32];
    char str_year[8];
    int_cur_month = 0;

    strftime(month, sizeof(month), "%b", curr_date);
    strftime(str_year, sizeof(str_year), "%Y", curr_date);

    if (strstr(month, "Jul"))
    {
        sprintf(date, "July %s", str_year);
    }
    else if (strstr(month, "Jun"))
    {
        sprintf(date, "June %s", str_year);
    }
    else
    {
        strftime(date, sizeof(date), "%b %Y", curr_date);
    }

    if (strcmp(date, year_month) == 0)
    {
        int year;
        sscanf(year_month, "%*s %d", &year);
        sprintf(curr_year, "curr mon %d", year);
        int_cur_month++;
    }
    LOG_INFO("date %s, year_month %s curr_year %s", date, year_month, curr_year);

    LOG_INFO("Generating Billing JSON for meter %s year-month %s", serial, year_month);

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

    /* 2. Read Billing data */
    BillingData bill_data;
    if (read_billing_data(sqlite_db_path,
                          &status,
                          serial,
                          year_month,
                          ctx,
                          &bill_data) != 0)
    {
        LOG_ERROR("Cannot read billing data for meter %s year-month %s",
                  serial,
                  year_month);
    }

    BillingData bill_data_curr = {0};

    if (strstr(curr_year, "curr mon "))
    {
        int_cur_month--;
        if (read_billing_data(sqlite_db_path,
                              &status,
                              serial,
                              curr_year,
                              ctx,
                              &bill_data_curr) != 0)
        {
            LOG_ERROR("Cannot read billing data for meter %s year-month %s",
                      serial,
                      curr_year);
        }
    }

    if (bill_data.entry_count == 0)
    {
        LOG_WARN("No billing data found for meter %s in %s",
                 serial,
                 year_month);
    }

    /* 3. Build output file path */
    char dt_str[32];
    get_datetime_str(dt_str, sizeof(dt_str));

    char out_path[512];
    char base_path[256];

    if (get_base_path(base_path, sizeof(base_path)) == 0)
    {
        snprintf(out_path,
                 sizeof(out_path),
                 "%s/data/BILL_%s_%s.json",
                 base_path,
                 serial,
                 year_month);
    }

    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)",
                  out_path,
                  strerror(errno));

        billing_data_free(&bill_data);
        billing_data_free(&bill_data_curr);
        return -1;
    }

    /* 4. Write JSON */
    json_write_header(fp, "BILLING_DATA_MESSAGE");
    json_write_general(ctx, fp, serial, dt_str);
    json_write_d1(fp, ctx, serial);
    json_write_d3(fp, ctx, &bill_data, &bill_data_curr);
    json_write_footer(fp);

    fclose(fp);

    billing_data_free(&bill_data);
    billing_data_free(&bill_data_curr);

    strcpy(output_file, out_path);

    LOG_INFO("Billing JSON written: %s", out_path);
    printf("JSON file generated: %s\n", out_path);

    return 0;
}
/*
 * Append BillingEntry objects to a combined BillingData structure.
 * Ownership of each entry->params is transferred to dst.
 */
static int billing_data_append(BillingData *dst, BillingData *src, int max_entries)
{
    int i;
    int old_count;
    int add_count;
    BillingEntry *tmp;

    if (!dst || !src || !src->entries || src->entry_count <= 0)
        return 0;

    add_count = src->entry_count;
    if (max_entries > 0 && add_count > max_entries)
        add_count = max_entries;

    old_count = dst->entry_count;

    tmp = (BillingEntry *)realloc(dst->entries,
                                  (old_count + add_count) * sizeof(BillingEntry));
    if (!tmp)
    {
        LOG_ERROR("Failed to expand combined BillingData for %d entries", add_count);
        return -1;
    }

    dst->entries = tmp;

    for (i = 0; i < add_count; i++)
    {
        dst->entries[old_count + i] = src->entries[i];
        src->entries[i].params = NULL;
    }

    dst->entry_count = old_count + add_count;
    return add_count;
}

/*
 * Generate ONE billing JSON file for the complete requested month range.
 *
 * start_date / end_date format:
 *     "MM_YYYY"
 *
 * Example:
 *     start_date = "07_2026"
 *     end_date   = "09_2026"
 *
 * For normal months:
 *     all billing rows in that month are copied.
 *
 * For the current month:
 *     1. First billing entry of the month is copied from the normal month table.
 *     2. Current-date billing entry is copied from the existing "curr mon YYYY" data.
 *
 * All records are written into one JSON file.
 */
int generate_billing_json(redisContext *ctx,
                          const char *serial,
                          const char *start_date,
                          const char *end_date,
                          char *output_file)
{
    int start_month, start_year;
    int end_month, end_year;
    int year, month;
    int generated_entries = 0;
    int current_tm_month;
    int current_tm_year;
    time_t now;
    struct tm *curr_date;
    char dt_str[32];
    char base_path[256];
    char out_path[512];
    char bill_date[64];
    char curr_year[64];
    const char *months[] = {
        "", "Jan", "Feb", "Mar", "Apr", "May", "June",
        "July", "Aug", "Sep", "Oct", "Nov", "Dec"};

    BillingData all_bill_data;
    memset(&all_bill_data, 0, sizeof(all_bill_data));

    if (!ctx || !serial || !start_date || !end_date)
    {
        LOG_ERROR("Invalid argument to generate_billing_json");
        return -1;
    }

    if (sscanf(start_date, "%d_%d", &start_month, &start_year) != 2)
    {
        LOG_ERROR("Invalid start billing date format: %s", start_date);
        return -1;
    }

    if (sscanf(end_date, "%d_%d", &end_month, &end_year) != 2)
    {
        LOG_ERROR("Invalid end billing date format: %s", end_date);
        return -1;
    }

    if (start_month < 1 || start_month > 12 ||
        end_month < 1 || end_month > 12)
    {
        LOG_ERROR("Invalid month: start=%d end=%d", start_month, end_month);
        return -1;
    }

    if (start_year > end_year ||
        (start_year == end_year && start_month > end_month))
    {
        LOG_ERROR("Start billing date %s is after end billing date %s",
                  start_date, end_date);
        return -1;
    }

    now = time(NULL);
    curr_date = localtime(&now);
    if (!curr_date)
    {
        LOG_ERROR("Unable to get current date");
        return -1;
    }

    current_tm_month = curr_date->tm_mon + 1;
    current_tm_year = curr_date->tm_year + 1900;

    /* Keep the OD table until the complete range has been read. */
    multi_month_billing = 1;

    /*
     * The loop above is intentionally not used for the actual read because
     * read_billing_data() requires the MeterStatus. Obtain it once here.
     */
    {
        MeterStatus status;
        char base_path_dir[256];
        char sqlite_db_path[256];

        memset(&status, 0, sizeof(status));
        memset(base_path_dir, 0, sizeof(base_path_dir));
        memset(sqlite_db_path, 0, sizeof(sqlite_db_path));

        if (read_meter_status(ctx, serial, &status) != 0)
        {
            LOG_ERROR("Cannot read meter status for meter %s", serial);
            multi_month_billing = 0;
            return -1;
        }

        if (get_base_path(base_path_dir, sizeof(base_path_dir)) != 0)
        {
            LOG_ERROR("Cannot get DCU base path");
            multi_month_billing = 0;
            return -1;
        }

        snprintf(sqlite_db_path, sizeof(sqlite_db_path),
                 "%s/data/dcu_dlms.db", base_path_dir);

        year = start_year;
        month = start_month;

        while (year < end_year ||
               (year == end_year && month <= end_month))
        {
            BillingData month_data;
            BillingData current_data;
            int is_current_month;
            int rc;

            memset(&month_data, 0, sizeof(month_data));
            memset(&current_data, 0, sizeof(current_data));

            snprintf(bill_date, sizeof(bill_date), "%s %d", months[month], year);

            is_current_month = (year == current_tm_year &&
                                month == current_tm_month);

            LOG_INFO("Generating combined billing data for meter %s: %s%s",
                     serial,
                     bill_date,
                     is_current_month ? " (CURRENT MONTH)" : "");

            /* Allow normal OD-table cleanup after all range reads are complete. */
            if (!is_current_month && month == end_month)
            {
                multi_month_billing = 0;
            }

            rc = read_billing_data(sqlite_db_path,
                                   &status,
                                   serial,
                                   bill_date,
                                   ctx,
                                   &month_data);

            if (rc == 0 && month_data.entry_count > 0)
            {
                if (is_current_month)
                {
                    /* Current month: only the FIRST date from the normal table. */
                    if (billing_data_append(&all_bill_data, &month_data, 1) > 0)
                    {
                        generated_entries++;
                        LOG_INFO("Added first billing entry for current month %s",
                                 bill_date);
                    }
                }
                else
                {
                    /* Previous months: copy ALL available billing dates. */
                    int added = billing_data_append(&all_bill_data,
                                                    &month_data,
                                                    0);
                    if (added > 0)
                    {
                        generated_entries += added;
                        LOG_INFO("Added %d billing entries for %s",
                                 added, bill_date);
                    }
                }
            }
            else
            {
                LOG_WARN("No billing data found for meter %s in %s",
                         serial, bill_date);
            }

            billing_data_free(&month_data);

            if (is_current_month)
            {
                /*
                 * Existing current-month mechanism stores the current-date
                 * record using the key "curr mon YYYY".
                 */
                snprintf(curr_year, sizeof(curr_year),
                         "curr mon %d", year);

                /* Allow normal OD-table cleanup after all range reads are complete. */

                multi_month_billing = 0;

                if (read_billing_data(sqlite_db_path,
                                      &status,
                                      serial,
                                      curr_year,
                                      ctx,
                                      &current_data) == 0 &&
                    current_data.entry_count > 0)
                {
                    /* Add exactly one current-date entry. */
                    if (billing_data_append(&all_bill_data,
                                            &current_data,
                                            1) > 0)
                    {
                        generated_entries++;
                        LOG_INFO("Added current-date billing entry for %s",
                                 bill_date);
                    }
                }
                else
                {
                    LOG_WARN("Current-date billing entry not found for meter %s (%s)",
                             serial, curr_year);
                }

                billing_data_free(&current_data);
            }

            month++;
            if (month > 12)
            {
                month = 1;
                year++;
            }
        }
    }

    if (generated_entries == 0 || all_bill_data.entry_count == 0)
    {
        LOG_ERROR("No billing data found for meter %s from %s to %s",
                  serial, start_date, end_date);
        // billing_data_free(&all_bill_data);
        // return -1;
    }

    if (get_base_path(base_path, sizeof(base_path)) != 0)
    {
        LOG_ERROR("Cannot get DCU base path for billing JSON output");
        billing_data_free(&all_bill_data);
        return -1;
    }

    snprintf(out_path, sizeof(out_path),
             "%s/data/BILL_%s_%s_to_%s.json",
             base_path, serial, start_date, end_date);

    get_datetime_str(dt_str, sizeof(dt_str));

    {
        FILE *fp = fopen(out_path, "w");
        if (!fp)
        {
            LOG_ERROR("Cannot open output file: %s (%s)",
                      out_path, strerror(errno));
            billing_data_free(&all_bill_data);
            return -1;
        }

        json_write_header(fp, "BILLING_DATA_MESSAGE");
        json_write_general(ctx, fp, serial, dt_str);
        json_write_d1(fp, ctx, serial);

        /* All requested months are now in one BillingData structure. */
        {
            BillingData empty_current;
            memset(&empty_current, 0, sizeof(empty_current));
            json_write_d3(fp, ctx, &all_bill_data, &empty_current);
        }

        json_write_footer(fp);
        fclose(fp);
    }

    billing_data_free(&all_bill_data);

    strcpy(output_file, out_path);

    LOG_INFO("Combined Billing JSON written: %s entries=%d",
             out_path, generated_entries);

    printf("JSON file generated: %s entries=%d\n",
           output_file, generated_entries);

    return 0;
}
