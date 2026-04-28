

#include "../include/general.h"

extern int billing_cmd_redis_resp;
/* ============================================================
 *  Billing data helpers
 * ============================================================ */

/**
 * @brief Lookup Billing param code, name and unit for a given OBIS from Redis.
 *
 * Reads three sub-hashes from REDIS_HASH_BILL_OBIS_MAP.
 *
 * @param ctx        Redis context.
 * @param obis       OBIS decimal string.
 * @param code_buf   Output: param code.
 * @param name_buf   Output: param name.
 * @param unit_buf   Output: param unit.
 */
static void lookup_bill_obis_mapping(redisContext *ctx, const char *obis,
                                     char *code_buf, size_t code_len,
                                     char *name_buf, size_t name_len,
                                     char *unit_buf, size_t unit_len)
{
    /* Default to empty strings on failure */
    code_buf[0] = name_buf[0] = unit_buf[0] = '\0';

    char *code_json = redis_hget(ctx, REDIS_HASH_BILL_OBIS_MAP, REDIS_FIELD_PARAM_CODE);
    char *name_json = redis_hget(ctx, REDIS_HASH_BILL_OBIS_MAP, REDIS_FIELD_PARAM_NAME);
    char *unit_json = redis_hget(ctx, REDIS_HASH_BILL_OBIS_MAP, REDIS_FIELD_PARAM_UNIT);

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
 * @brief Read Billing data for a specific year-month from SQLite database.
 *
 * Opens the database, queries the table "bill_data_<manuf>_<dcu>_<port>_<serial>"
 * for all records matching the specified year-month (column "bill_date"),
 * ordered by bill_date ascending. Maximum 2 entries per month.
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
    if (billing_cmd_redis_resp == 1)
    {
    snprintf(table, sizeof(table), "bill_data_od_%s_%s_%s_%s",
             status->manuf_key, status->dcu_serial, status->port, serial);
    }
    else{
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
             "SELECT * FROM %s WHERE  bill_date like '%s' "
             "ORDER BY bill_date ASC LIMIT 2",
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

    /* Allocate for up to 2 billing entries */
    bill_data->entries = (BillingEntry *)calloc(2, sizeof(BillingEntry));
    if (!bill_data->entries)
    {
        LOG_ERROR("Memory allocation failed for BillingEntry array");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return -1;
    }

    int entry_idx = 0;

    /* Iterate over result rows (max 2) */
    while (sqlite3_step(stmt) == SQLITE_ROW && entry_idx < 2)
    {
        BillingEntry *entry = &bill_data->entries[entry_idx];

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

            /* Lookup mapping from Billing-specific hash */
            lookup_bill_obis_mapping(ctx, obis,
                                     p->param_code, sizeof(p->param_code),
                                     p->param_name, sizeof(p->param_name),
                                     p->unit, sizeof(p->unit));

            LOG_DEBUG("Bill entry[%d] param[%d]: obis=%s hex=%s code=%s name=%s unit=%s val=%s",
                      entry_idx, param_idx, p->obis_code, p->obis_hex, p->param_code,
                      p->param_name, p->unit, p->value);

            param_idx++;
        }

        entry->param_count = param_idx;
        entry_idx++;
    }

    bill_data->entry_count = entry_idx;

    sqlite3_finalize(stmt);
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

            if(p->param_name[0] != '\0'){
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

            if(p->param_name[0] != '\0'){
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

/**
 * @brief Generate a CDF file for Billing data (data type 3).
 *
 * Requires a year-month argument in argv[3] (YYYY-MM format).
 * Billing data can have up to 2 entries per month.
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
    strftime(date, sizeof(date), "%b %Y", curr_date);

    if (strcmp(date, year_month) == 0)
    {
        int year;
        sscanf(year_month, "%*s %d", &year);
        sprintf(curr_year, "curr mon %d", year);
    }

    LOG_INFO("Generating Billing CDF for meter %s year-month %s", serial, year_month);

    /* 1. Read meter status from Redis */
    MeterStatus status;
    if (read_meter_status(ctx, serial, &status) != 0)
    {
        LOG_ERROR("Cannot read meter status for meter %s", serial);
        return -1;
    }

    /* 2. Read Billing data from SQLite */
    BillingData bill_data;
    if (read_billing_data(SQLITE_DB_PATH, &status, serial, year_month, ctx, &bill_data) != 0)
    {
        LOG_ERROR("Cannot read billing data for meter %s year-month %s", serial, year_month);
        // return -1;
    }

    BillingData bill_data_curr = {0};

    if (strstr(curr_year, "curr mon "))
    {

        if (read_billing_data(SQLITE_DB_PATH, &status, serial, curr_year, ctx, &bill_data_curr) != 0)
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
    snprintf(out_path, sizeof(out_path),
             "%sCDF_BILL_%s_%s.xml", CDF_OUTPUT_DIR, serial, year_month);

    FILE *fp = fopen(out_path, "w");
    if (!fp)
    {
        LOG_ERROR("Cannot open output file: %s (%s)", out_path, strerror(errno));
        billing_data_free(&bill_data);
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

    strcpy(output_file, out_path);
    LOG_INFO("Billing CDF written: %s", out_path);
    printf("CDF file generated: %s\n", out_path);
    return 0;
}
