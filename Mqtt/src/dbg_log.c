#include "../include/general.h"
/* ============================================================
 *  Logging subsystem
 * ============================================================ */
#define DBGCFG_FILE_NAME "/usr/cms/config/debuglog.cfg"
#define DIAG_KEY "MQTT_PROC"

static FILE *g_log_fp = NULL;
static char g_log_path[256];

int dbgloglevel = 0;
time_t dbgcfgtime = 0;
extern redisContext *ctx;

/**
 * @brief Return current date-time as a formatted string.
 * @param buf    Output buffer.
 * @param buflen Buffer length.
 */
void get_datetime_str(char *buf, size_t buflen)
{
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    strftime(buf, buflen, "%Y-%m-%d %H:%M:%S", t);
}

// int add_process_diag(char *msg)
// {
//     if (!ctx){
//         return -1;
//     }
//     redisReply *reply = redisCommand(ctx, "hget diag_enable %s", DIAG_KEY);
//     if (reply == NULL)
//     {
//         return -1;
//     }
//     if (reply->type == REDIS_REPLY_NIL || reply->str == NULL)
//     {
//         if (reply)
//             freeReplyObject(reply);
//         return -1;
//     }
//     int diag_enbl = atoi(reply->str);
//     freeReplyObject(reply);

//     if (msg == NULL) return -1;

//     if (diag_enbl)
//     {
//         char *key = "diag_msgs:MQTT_PROC";
//         char json_buf[4096];

//         snprintf(json_buf, sizeof(json_buf), "%s", msg);
//         redisReply *rly = redisCommand(ctx, "LPUSH %s %s", key, json_buf);

//         if(rly == NULL){
//             return -1;
//         }
//         freeReplyObject(rly);
//     }
//     return 0;
// }

int add_process_diag(char *msg)
{
    static int diag_enbl_cached = 0;
    static time_t last_check = 0;

    if (!ctx || ctx->err || msg == NULL)
        return -1;

    time_t now = time(NULL);

    /* Refresh enable flag every 5 sec */
    if (now - last_check > 5)
    {
        redisReply *reply = redisCommand(ctx, "hget diag_enable %s", DIAG_KEY);
        if (reply && reply->str)
            diag_enbl_cached = atoi(reply->str);

        if (reply)
            freeReplyObject(reply);
        last_check = now;
    }

    if (!diag_enbl_cached)
        return 0;

    redisReply *rly = redisCommand(ctx, "LPUSH %s %s", "diag_msgs:MQTT_PROC", msg);

    if (!rly)
        return -1;

    freeReplyObject(rly);
    return 0;
}

/**
 * @brief Rotate log files if the current log exceeds LOG_MAX_SIZE_BYTES.
 *
 * Rotates: .log.4 → deleted, .log.3 → .log.4, ..., .log → .log.1
 */
void log_rotate(void)
{
    struct stat st;
    if (stat(g_log_path, &st) != 0)
        return;
    if (st.st_size < LOG_MAX_SIZE_BYTES)
        return;

    if (g_log_fp)
    {
        fclose(g_log_fp);
        g_log_fp = NULL;
    }

    char old_name[512], new_name[512];

    /* Delete the oldest backup */
    snprintf(old_name, sizeof(old_name), "%s.%d", g_log_path, LOG_MAX_FILES - 1);
    remove(old_name);

    /* Shift remaining backups */
    for (int i = LOG_MAX_FILES - 2; i >= 1; i--)
    {
        snprintf(old_name, sizeof(old_name), "%s.%d", g_log_path, i);
        snprintf(new_name, sizeof(new_name), "%s.%d", g_log_path, i + 1);
        rename(old_name, new_name);
    }

    /* Rename current log to .1 */
    snprintf(new_name, sizeof(new_name), "%s.1", g_log_path);
    rename(g_log_path, new_name);

    g_log_fp = fopen(g_log_path, "a");
}

/**
 * @brief Initialise the logging subsystem.
 * @return 0 on success, -1 on failure.
 */
int log_init(void)
{
    /* Ensure log directory exists */
    // rithika 17Feb2026
    // mkdir(LOG_DIR, 0755);

    snprintf(g_log_path, sizeof(g_log_path),
             "%s%s%s", LOG_DIR, LOG_FILE_BASE, LOG_FILE_EXT);

    // snprintf(g_log_path, sizeof(g_log_path),
    //          "%s%s", LOG_FILE_BASE, LOG_FILE_EXT);

    g_log_fp = fopen(g_log_path, "a");
    if (!g_log_fp)
    {
        fprintf(stderr, "ERROR: Cannot open log file: %s (%s)\n",
                g_log_path, strerror(errno));
        return -1;
    }
    return 0;
}

int checkDbgStatus(void)
{

    // static char fun_name[] = "checkDbgStatus()";
    // redisReply *reply;
    // reply = redisCommand(redis_conn, "HGET logs_rel_cfg enable_dbg_log");

    // if (reply == NULL || reply->type != REDIS_REPLY_STRING)
    // {
    //   fprintf(stderr, "Error fetching data from Redis\n");
    //   return -1;
    // }

    // dbgloglevel = atoi(reply->str);

    // return 0;

    static char fun_name[] = "checkDbgStatus()";
    struct stat sb;
    FILE *fp = NULL;

    memset(&sb, 0, sizeof(sb));

    if (stat(DBGCFG_FILE_NAME, &sb) == -1)
    {
        perror("stat");
        dbgloglevel = 1;
    }

    if ((dbgcfgtime == 0) || (dbgcfgtime != sb.st_mtime))
    {
        dbgcfgtime = sb.st_mtime;

        fp = fopen(DBGCFG_FILE_NAME, "r");
        if (fp != NULL)
        {
            fscanf(fp, "%d", &dbgloglevel);
            fclose(fp);
            printf("debug level %d\n", dbgloglevel);

            LOG_INFO("%s:debug log level changed %d\n", fun_name, dbgloglevel);
        }
    }

    return 0;
}

void iec104_log_sink_poll_network(void)
{
    dcu_netlog_poll();
}

static dcu_netlog_level_t netlog_level_of(const char *level)
{
    if (level == NULL)
        return DCU_NETLOG_INFO;

    if (strcmp(level, "DEBUG") == 0)
        return DCU_NETLOG_DEBUG;
    else if (strcmp(level, "INFO") == 0)
        return DCU_NETLOG_INFO;
    else if (strcmp(level, "WARN") == 0)
        return DCU_NETLOG_WARN;
    else if (strcmp(level, "ERROR") == 0)
        return DCU_NETLOG_ERROR;

    return DCU_NETLOG_INFO; // default
}

/**
 * @brief Write a formatted log entry.
 *
 * Format:  [YYYY-MM-DD HH:MM:SS] [LEVEL] message
 *
 * @param level  Severity label, e.g. "INFO", "WARN", "ERROR".
 * @param fmt    printf-style format string.
 */
// void log_write(const char *level, const char *fmt, ...)
// {
//     checkDbgStatus();

//     if (dbgloglevel == 0)
//     {
//         return;
//     }
//     if (!g_log_fp)
//         return;

//     log_rotate();

//     char dt[32];
//     get_datetime_str(dt, sizeof(dt));

//     va_list ap;

//     /* -------- Console Output -------- */
//     printf("[%s] [%s] ", dt, level);

//     va_start(ap, fmt);
//     vprintf(fmt, ap);
//     va_end(ap);

//     printf("\n");

//     /* -------- File Output -------- */
//     fprintf(g_log_fp, "[%s] [%s] ", dt, level);

//     va_start(ap, fmt);
//     vfprintf(g_log_fp, fmt, ap);
//     va_end(ap);

//     fprintf(g_log_fp, "\n");

//     fflush(g_log_fp);
// }

void log_write(const char *level, const char *fmt, ...)
{
    checkDbgStatus();

    char dt[32];
    get_datetime_str(dt, sizeof(dt));

    char log_buf[4096];

    va_list ap;

    /* -------- Format message once -------- */
    va_start(ap, fmt);
    vsnprintf(log_buf, sizeof(log_buf), fmt, ap);
    va_end(ap);

    /* -------- DIAG (ALWAYS or independently controlled) -------- */
    char diag_buf[4096];
    snprintf(diag_buf, sizeof(diag_buf), "[%s] [%s] %s", dt, level, log_buf);
    // add_process_diag(diag_buf);

    if (strstr(diag_buf, "[PAHO TRACE]") == NULL)
    {
        add_process_diag(diag_buf); // always called
    }
    memset(diag_buf,0,sizeof(diag_buf));
    snprintf(diag_buf, sizeof(diag_buf),  "%s",log_buf);
    dcu_netlog_send(netlog_level_of(level), "%s", diag_buf);

    /* -------- Logging only if enabled -------- */
    if (dbgloglevel == 0 || !g_log_fp)
    {
        return;
    }

    log_rotate();

    /* -------- Console Output -------- */
    printf("[%s] [%s] %s\n", dt, level, log_buf);
    /* -------- File Output -------- */
    fprintf(g_log_fp, "[%s] [%s] %s\n", dt, level, log_buf);
    fflush(g_log_fp);
}

/**
 * @brief Clean up logging subsystem.
 */
void log_close(void)
{
    if (g_log_fp)
    {
        fclose(g_log_fp);
        g_log_fp = NULL;
    }
}
