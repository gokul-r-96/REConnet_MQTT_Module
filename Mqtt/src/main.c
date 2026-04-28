#include <unistd.h>
#include <pthread.h>
#include "../include/general.h"
// #include "logger.h"

/* Global static instances */
mqtt_conn_t primary;
mqtt_conn_t secondary;
mqtt_conn_t *current_active = NULL;
redisContext *ctx;

#define PRI_BROKER_RECONNECT_PERIOD 30
#define NW_LOGGER_CHECK 30

char meter_serials[MAX_METERS][32];
int meter_count = 0;
int certificate_path_check_primary = 0;
int certificate_path_check_secondary = 0;
volatile sig_atomic_t stop_flag = 1;
// rithika 02April2026
time_t primary_mqtt_conn_time;
time_t secn_mqtt_conn_time;
int cur_active_mqtt = -1;
char dcu_ser_num[SIZE_32];

extern int check_redis_resp;
/**
 * mqtt_module_start()
 * -------------------
 * Initializes and starts MQTT subsystem.
 */

void mqtt_trace_callback(enum MQTTASYNC_TRACE_LEVELS level, char *message)
{
    LOG_INFO("[PAHO TRACE] %s", message);
}

/*Function to get redis string*/
void redis_get_str(redisContext *c, const char *hash, const char *key, char *out, size_t len)
{
    redisReply *r = redisCommand(c, "HGET %s %s", hash, key);
    if (r && r->type == REDIS_REPLY_STRING)
    {
        strncpy(out, r->str, len - 1);
        out[len - 1] = '\0';
    }
    freeReplyObject(r);
}

/**
 * Helper: read integer from Redis hash
 */
int redis_get_int(redisContext *c, const char *hash, const char *key)
{
    redisReply *r = redisCommand(c, "HGET %s %s", hash, key);
    int val = (r && r->type == REDIS_REPLY_STRING) ? atoi(r->str) : 0;
    freeReplyObject(r);
    return val;
}

void load_active_meters(redisContext *ctx)
{
    redisReply *reply;

    reply = redisCommand(ctx, "HGETALL meter_commn_status");

    if (!reply || reply->type != REDIS_REPLY_ARRAY)
        return;

    meter_count = 0;

    for (int i = 0; i < reply->elements; i += 2)
    {
        char *field = reply->element[i]->str;
        char *value = reply->element[i + 1]->str;

        /* Skip _time fields */
        if (strstr(field, "_time"))
            continue;

        /* Only communicating meters */
        if (strcmp(value, "Communicating") != 0)
            continue;

        /* Extract serial number */
        char *serial = strrchr(field, '_');
        if (!serial)
            continue;

        serial++; // skip '_'

        strncpy(meter_serials[meter_count], serial, 31);
        meter_serials[meter_count][31] = '\0';

        meter_count++;

        if (meter_count >= MAX_METERS)
            break;
    }

    freeReplyObject(reply);

    printf("Active meters: %d\n", meter_count);
}

void *mqtt_worker_thread(void *arg)
{
    char file_rem_cmd[128];

    time_t last_primary_retry = 0;
    time_t last_publish_inst = 0;
    time_t last_publish_profile = 0;
    time_t last_publish_hc = 0;
    time_t last_publish_modbus = 0;
    // rithika 16April2026
    time_t last_nw_logger_check = 0;

    int inst_data_interval = 0;
    int profile_data_interval = 0;
    int modbus_data_interval = 0;
    int health_check_data_interval = 0;

    int interval_sec;
    int elapsed;
    int remaining;
    while (stop_flag)
    {
        LOG_INFO("Message is Waiting to Publish in the given interval");
        time_t now = time(NULL);

        // rithika 16April2026
        if ((now - last_nw_logger_check) >= NW_LOGGER_CHECK)
        {
            last_nw_logger_check = now;
            iec104_log_sink_poll_network();
            send_hc_msg();
        }

        /* PRIMARY CONNECTED */

        if ((now - last_primary_retry) >= PRI_BROKER_RECONNECT_PERIOD)
        {
            last_primary_retry = now;

            if ((!primary.client || !MQTTAsync_isConnected(primary.client)) &&
                primary.cfg.enable_mqtt)
            {
                LOG_INFO("[GLOBAL RETRY] Trying primary again");
                mqtt_connect(&primary);
            }
        }

        if (primary.client && MQTTAsync_isConnected(primary.client))
        {
            primary.connected = true;
            current_active = &primary;
            // rithika 02April2026
            if (certificate_path_check_primary == 0)
                cur_active_mqtt = 1;
            else
                cur_active_mqtt = 2;
        }

        /* SECONDARY CONNECTED */
        else if (secondary.client && MQTTAsync_isConnected(secondary.client))
        {
            secondary.connected = true;
            current_active = &secondary;
            // rithika 02April2026
            if (certificate_path_check_secondary == 0)
                cur_active_mqtt = 1;
            else
                cur_active_mqtt = 2;

            // if ((now - last_primary_retry) >= PRI_BROKER_RECONNECT_PERIOD)
            // {
            //     last_primary_retry = now;

            //     if ((!primary.client || !MQTTAsync_isConnected(primary.client)) &&
            //         primary.cfg.enable_mqtt)
            //     {
            //         LOG_INFO("[RETRY] Trying primary again");
            //         mqtt_connect(&primary);
            //     }
            // }
        }

        /* NONE CONNECTED */
        else
        {
            current_active = NULL;

            // rithika 02April2026
            cur_active_mqtt = -1;

            if ((!primary.client || !MQTTAsync_isConnected(primary.client)) &&
                primary.cfg.enable_mqtt)
            {
                LOG_INFO("[RECONNECT] Trying primary broker");

                mqtt_connect(&primary);

                sleep(5);
            }

            if ((!primary.client || !MQTTAsync_isConnected(primary.client)) &&
                (!secondary.client || !MQTTAsync_isConnected(secondary.client)) &&
                secondary.cfg.enable_mqtt)
            {
                LOG_INFO("[FAILOVER] Connecting secondary broker");

                mqtt_connect(&secondary);
            }
        }
        printf("check_redis_resp %d\n\n",check_redis_resp);
        
        if (check_redis_resp == 1)
        {
            read_redis_resp(current_active);
        }

        /* Publish */
        // interval_sec = current_active->cfg.dlms_inst_pub_interval * 60;
        // elapsed = now - last_publish_inst;
        // remaining = interval_sec - elapsed;
        // if (remaining > 0)
        // {
        //     LOG_INFO("Instataneous Data will publish in %d minutes", remaining / 60);
        // }
        if (current_active && (now - last_publish_inst >= current_active->cfg.dlms_inst_pub_interval * 60))
        {
            last_publish_inst = now;
            load_active_meters(ctx);

            for (int i = 0; i < meter_count; i++)
            {
                const char *serial = meter_serials[i];
                cdf_result_t res = generate_instantaneous_cdf(ctx, serial);

                if (res.status == 0)
                {
                    mqtt_send_file(current_active, res.filename, INST_DATA_TOPIC);
                    // rithika 18Apr2026
                    memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
                    sprintf(file_rem_cmd, "rm %s", res.filename);
                    system(file_rem_cmd);
                    LOG_INFO("%s is deleted successfully", res.filename);
                }
            }
        }

        if (check_redis_resp == 1)
        {
            read_redis_resp(current_active);
        }

        // interval_sec = current_active->cfg.dlms_data_pub_interval * 60;
        // elapsed = now - last_publish_profile;
        // remaining = interval_sec - elapsed;
        // if (remaining > 0)
        // {
        //     LOG_INFO("Meter Profile Data will publish in %d minutes", remaining / 60);
        // }
        if (current_active && (now - last_publish_profile >= current_active->cfg.dlms_data_pub_interval * 60))
        {
            last_publish_profile = now;
            time_t t = time(NULL);
            struct tm *tm_det = localtime(&t);
            char today_date[16];
            strftime(today_date, sizeof(today_date), "%Y-%m-%d", tm_det);
            for (int i = 0; i < meter_count; i++)
            {
                const char *serial = meter_serials[i];
                cdf_result_t res = generate_profile_cdf(ctx, serial, today_date, "all");
                if (res.status == 0)
                {
                    LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
                    // LOG_INFO("Meter Profile Generated Successfully");
                    mqtt_send_file(current_active, res.filename, METER_DATA_TOPIC);
                    // rithika 18Apr2026
                    memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
                    sprintf(file_rem_cmd, "rm %s", res.filename);
                    system(file_rem_cmd);
                    LOG_INFO("%s is deleted successfully", res.filename);
                }
            }
        }

        if (check_redis_resp == 1)
        {
            read_redis_resp(current_active);
        }

        // interval_sec = current_active->cfg.hc_pub_interval * 60;
        // elapsed = now - last_publish_hc;
        // remaining = interval_sec - elapsed;
        // if (remaining > 0)
        // {
        //     LOG_INFO("Health check messages will publish in %d minutes", remaining / 60);
        // }
        if (current_active && (time(NULL) - last_publish_hc >= current_active->cfg.hc_pub_interval * 60))
        {
            last_publish_hc = time(NULL);
            char xml_buf[PAYLOAD_BUFFER_SIZE];
            int file_Size;

            build_health_status_xml(ctx, xml_buf, sizeof(xml_buf), &file_Size);

            mqtt_send_msg(current_active, xml_buf, file_Size, HEALTH_DATA_TOPIC);
        }

        if (check_redis_resp == 1)
        {
            read_redis_resp(current_active);
        }

        // Publish modbus messaged ---> 08/04/2026
        // LOG_INFO("Modbbus messages will Publish in %d minutes",current_active->cfg.modbus_data_pub_interval-(now - last_publish_modbus));
        // interval_sec = current_active->cfg.modbus_data_pub_interval * 60;
        // elapsed = now - last_publish_modbus;
        // remaining = interval_sec - elapsed;
        // if (remaining > 0)
        // {
        //     LOG_INFO("Modbus messages will publish in %d minutes", remaining / 60);
        // }
        if (current_active && (now - last_publish_modbus >= current_active->cfg.modbus_data_pub_interval * 60)) // every 60 sec
        {
            last_publish_modbus = now;

            char *json = modbus_export_json(ctx, 2); //  set your serial ports

            if (json)
            {
                LOG_INFO("Modbus JSON generated");

                mqtt_send_msg(current_active, json, strlen(json), MODBUS_DATA_TOPIC);

                free(json); // VERY IMPORTANT
            }
            else
            {
                LOG_ERROR("Failed to generate Modbus JSON");
            }
        }
        if (current_active)
        {
            interval_sec = current_active->cfg.modbus_data_pub_interval * 60;
            elapsed = now - last_publish_modbus;
            remaining = interval_sec - elapsed;
            if (remaining > 0)
            {
                LOG_INFO("Modbus messages will publish in %d minutes", remaining / 60);
            }
            interval_sec = current_active->cfg.hc_pub_interval * 60;
            elapsed = now - last_publish_hc;
            remaining = interval_sec - elapsed;
            if (remaining > 0)
            {
                LOG_INFO("Health check messages will publish in %d minutes", remaining / 60);
            }
            interval_sec = current_active->cfg.dlms_data_pub_interval * 60;
            elapsed = now - last_publish_profile;
            remaining = interval_sec - elapsed;
            if (remaining > 0)
            {
                LOG_INFO("Meter Profile Data will publish in %d minutes", remaining / 60);
            }
            interval_sec = current_active->cfg.dlms_inst_pub_interval * 60;
            elapsed = now - last_publish_inst;
            remaining = interval_sec - elapsed;
            if (remaining > 0)
            {
                LOG_INFO("Instataneous Data will publish in %d minutes", remaining / 60);
            }
        }

        sleep(3);
        LOG_INFO("Loop Completed.Going to start the next one");
    }
}

/*Loading MQTT Configuration for Broker1 and Broker2*/
// void load_mqtt_cfg(const char *hash, mqtt_cfg_t *cfg)
// {
//     int i;

//     cfg->enable_mqtt = redis_get_int(ctx, hash, "enable_mqtt");
//     cfg->broker_port = redis_get_int(ctx, hash, "broker_port");
//     cfg->keep_alive = redis_get_int(ctx, hash, "keep_alive_interval");
//     cfg->qos = redis_get_int(ctx, hash, "qos");
//     cfg->per_data_interval = redis_get_int(ctx, hash, "per_data_interval");
//     cfg->enable_ssl = redis_get_int(ctx, hash, "enable_ssl");

//     redis_get_str(ctx, hash, "broker_ip_url", cfg->broker_ip, MAX_STR_LEN);
//     redis_get_str(ctx, hash, "client_id", cfg->client_id, MAX_STR_LEN);
//     redis_get_str(ctx, hash, "username", cfg->username, MAX_STR_LEN);
//     redis_get_str(ctx, hash, "password", cfg->password, MAX_STR_LEN);
//     redis_get_str(ctx, hash, "per_data_topic", cfg->per_data_topic, MAX_TOPIC_LEN);
//     redis_get_str(ctx, hash, "event_pub_topic", cfg->event_pub_topic, MAX_TOPIC_LEN);
//     redis_get_str(ctx, hash, "diag_pub_topic", cfg->diag_pub_topic, MAX_TOPIC_LEN);

//     for (i = 0; i < MAX_SUB_TOPICS; i++)
//     {
//         char key[32];
//         snprintf(key, sizeof(key), "subscribe_topic%d", i + 1);
//         redis_get_str(ctx, hash, key, cfg->subscribe_topics[i], MAX_TOPIC_LEN);
//     }

//     // redisFree(ctx);
// }

void print_mqtt_full_cfg(const mqtt_cfg_t *cfg)
{
    int i;

    printf("\n========== MQTT FULL CONFIG ==========\n");

    // ---- Core ----
    printf("enable_mqtt        : %d\n", cfg->enable_mqtt);
    printf("primary            : %d\n", cfg->primary);
    printf("clean_session      : %d\n", cfg->clean_session);

    printf("broker_ip          : %s\n", cfg->broker_ip);
    printf("broker_port        : %d\n", cfg->broker_port);

    printf("client_id          : %s\n", cfg->client_id);
    printf("username           : %s\n", cfg->username);
    printf("password           : %s\n", cfg->password);

    printf("keep_alive         : %d\n", cfg->keep_alive);
    printf("qos                : %d\n", cfg->qos);
    printf("insecure           : %d\n", cfg->insecure);

    // ---- Publish Topics ----
    printf("\n--- Publish Topics ---\n");
    printf("modbus_data_topic  : %s\n", cfg->cyclic_modbus_data_topic);
    printf("dlms_data_topic    : %s\n", cfg->cyclic_dlms_data_topic);
    printf("dlms_inst_topic    : %s\n", cfg->inst_data_topic);
    printf("health_check_topic : %s\n", cfg->health_check_data_topic);
    printf("cmd_response_topic : %s\n", cfg->cmd_response_topic);

    // ---- Subscribe Topics ----
    printf("\n--- Subscribe Topics ---\n");
    for (i = 0; i < MAX_SUB_TOPICS; i++)
    {
        printf("subscribe_topic[%d] : %s\n", i + 1, cfg->subscribe_topics[i]);
    }

    // ---- Publish Intervals ----
    printf("\n--- Publish Intervals (minutes) ---\n");
    printf("hc_pub_interval          : %d\n", cfg->hc_pub_interval);
    printf("dlms_inst_pub_interval   : %d\n", cfg->dlms_inst_pub_interval);
    printf("dlms_data_pub_interval   : %d\n", cfg->dlms_data_pub_interval);
    printf("modbus_data_pub_interval : %d\n", cfg->modbus_data_pub_interval);

    // ---- SSL ----
    printf("\n--- SSL Config ---\n");
    printf("enable_ssl         : %d\n", cfg->enable_ssl);
    printf("ca_certificate     : %s\n", cfg->ca_certificate);
    printf("client_certificate : %s\n", cfg->client_certificate);
    printf("client_key         : %s\n", cfg->client_key);
    printf("key_password       : %s\n", cfg->key_password);
    printf("encrypted_key      : %d\n", cfg->encrypted_key);

    printf("======================================\n\n");
}

void load_mqtt_cfg(const char *hash, mqtt_cfg_t *cfg)
{
    int i;

    memset(cfg, 0, sizeof(mqtt_cfg_t));

    // ----------- INT VALUES -----------
    cfg->enable_mqtt = redis_get_int(ctx, hash, "enable_mqtt");
    cfg->broker_port = redis_get_int(ctx, hash, "broker_port");
    cfg->keep_alive = redis_get_int(ctx, hash, "keep_alive_interval");
    cfg->qos = redis_get_int(ctx, hash, "qos");
    cfg->primary = redis_get_int(ctx, hash, "primary");
    cfg->clean_session = redis_get_int(ctx, hash, "clean_session");
    cfg->insecure = redis_get_int(ctx, hash, "insecure");

    // ----------- INTERVALS (minutes → convert to seconds if needed) -----------
    cfg->hc_pub_interval = redis_get_int(ctx, hash, "hc_pub_interval");
    cfg->dlms_inst_pub_interval = redis_get_int(ctx, hash, "dlms_inst_pub_interval");
    cfg->dlms_data_pub_interval = redis_get_int(ctx, hash, "dlms_data_pub_interval");
    cfg->modbus_data_pub_interval = redis_get_int(ctx, hash, "modbus_data_pub_interval");

    // ----------- STRING VALUES -----------
    redis_get_str(ctx, hash, "broker_ip_url", cfg->broker_ip, SIZE_128);
    redis_get_str(ctx, hash, "client_id", cfg->client_id, MAX_STR_LEN);
    redis_get_str(ctx, hash, "username", cfg->username, MAX_STR_LEN);
    redis_get_str(ctx, hash, "password", cfg->password, MAX_STR_LEN);

    // ----------- PUBLISH TOPICS -----------
    redis_get_str(ctx, hash, "modbus_data_pub_topic", cfg->cyclic_modbus_data_topic, MAX_TOPIC_LEN);
    redis_get_str(ctx, hash, "dlms_data_pub_topic", cfg->cyclic_dlms_data_topic, MAX_TOPIC_LEN);
    redis_get_str(ctx, hash, "dlms_inst_pub_topic", cfg->inst_data_topic, MAX_TOPIC_LEN);
    redis_get_str(ctx, hash, "hc_pub_topic", cfg->health_check_data_topic, MAX_TOPIC_LEN);
    redis_get_str(ctx, hash, "cmd_resp_pub_topic", cfg->cmd_response_topic, MAX_TOPIC_LEN);

    // ----------- SSL -----------
    cfg->enable_ssl = redis_get_int(ctx, hash, "enable_ssl");
    cfg->encrypted_key = redis_get_int(ctx, hash, "encrypted_key");

    redis_get_str(ctx, hash, "ca_certificate", cfg->ca_certificate, MAX_STR_LEN);
    redis_get_str(ctx, hash, "client_certificate", cfg->client_certificate, MAX_STR_LEN);
    redis_get_str(ctx, hash, "client_key", cfg->client_key, MAX_STR_LEN);
    redis_get_str(ctx, hash, "key_password", cfg->key_password, MAX_STR_LEN);

    // ----------- SUBSCRIBE TOPICS -----------
    for (i = 0; i < MAX_SUB_TOPICS; i++)
    {
        char key[32];
        snprintf(key, sizeof(key), "cmd_req_topic");

        memset(cfg->subscribe_topics[i], 0, MAX_TOPIC_LEN);
        redis_get_str(ctx, hash, key, cfg->subscribe_topics[i], MAX_TOPIC_LEN);
    }

    // To get and store serial number from the redis hash !!!
    redis_get_str(ctx, "dcu_info", "serial_num", dcu_ser_num, SIZE_32);
    printf("dcu_ser_num %s\n", dcu_ser_num);
}
/**
 * load_mqtt_ssl_cfg()
 * ------------------
 * Reads SSL/TLS file paths from Redis.
 *
 * File Mapping:
 *  ssl_file1 -> CA certificate
 *  ssl_file2 -> Client certificate
 *  ssl_file3 -> Private key
 *  ssl_file4 -> Optional certificate chain
 */
void load_mqtt_ssl_cfg(const char *hash, mqtt_ssl_cfg_t *ssl)
{
    int i;

    ssl->num_files = 0;

    redisReply *r = redisCommand(ctx, "HGET %s num_mqtt_ssl_files", hash);
    if (r && r->type == REDIS_REPLY_STRING)
    {
        ssl->num_files = atoi(r->str);
        LOG_INFO("[TLS] Number of SSL Files %d", ssl->num_files);
    }
    freeReplyObject(r);

    for (i = 0; i < ssl->num_files && i < MAX_SSL_FILES; i++)
    {
        char key[32];
        snprintf(key, sizeof(key), "ssl_file_%d", i + 1);
        LOG_INFO("Key = %s", key);
        LOG_INFO("Inside SSL files redis get...");
        r = redisCommand(ctx, "HGET %s %s", hash, key);
        LOG_INFO("r->type %d", r->type);
        if (r && r->type == REDIS_REPLY_STRING)
        {
            strncpy(ssl->ssl_files[i], r->str, MAX_STR_LEN - 1);
            ssl->ssl_files[i][MAX_STR_LEN - 1] = '\0';
            LOG_INFO("[TLS] ssl_files[%d] = %s", i, ssl->ssl_files[i]);
        }
        freeReplyObject(r);
    }

    // redisFree(ctx);
}

void mqtt_module_start()
{
    MQTTAsync_setTraceCallback(mqtt_trace_callback);
    MQTTAsync_setTraceLevel(MQTTASYNC_TRACE_PROTOCOL);

    LOG_INFO("[INIT] Loading MQTT configs");
    int is_primary = redis_get_int(ctx, "mqtt_0_cfg", "primary");
    if (is_primary)
    {
        certificate_path_check_primary = 0;
        certificate_path_check_secondary = 1;
        LOG_INFO("Mqtt-1 is configured as primary!!!");
        load_mqtt_cfg("mqtt_0_cfg", &primary.cfg);
        LOG_INFO("Mqtt-2 is configured as secondary!!!");
        load_mqtt_cfg("mqtt_1_cfg", &secondary.cfg);
    }
    else
    {
        certificate_path_check_primary = 1;
        certificate_path_check_secondary = 0;
        LOG_INFO("Mqtt-2 is configured as primary!!!");
        load_mqtt_cfg("mqtt_1_cfg", &primary.cfg);
        LOG_INFO("Mqtt-1 is configured as secondary!!!");
        load_mqtt_cfg("mqtt_0_cfg", &secondary.cfg);
    }
    send_hc_msg();

    print_mqtt_full_cfg(&primary.cfg);
    printf("Primary cfg addr: %p\n", &primary.cfg);
    printf("Secondary cfg addr: %p\n", &secondary.cfg);
    // load_mqtt_cfg("mqtt_1_cfg", &secondary.cfg);
    print_mqtt_full_cfg(&secondary.cfg);
    // load_mqtt_ssl_cfg("mqtt_ssl_cfg", &primary.ssl);
    // load_mqtt_ssl_cfg("mqtt2_ssl_cfg", &secondary.ssl);

    printf("After Loading SSL config\n");

    /* Always try primary first */
    if (primary.cfg.enable_mqtt)
    {
        LOG_INFO("[START] Trying primary broker");

        mqtt_connect(&primary);
    }

    pthread_t worker;
    pthread_create(&worker, NULL, mqtt_worker_thread, NULL);
}

/* This function will handle the closing broker connections of both primary and secondary -- 27/04/2026 */
void mqtt_cleanup()
{
    LOG_INFO("Cleaning up MQTT connections...");

    if (primary.client)
    {
        MQTTAsync_disconnectOptions disc_opts =
            MQTTAsync_disconnectOptions_initializer;

        MQTTAsync_disconnect(primary.client, &disc_opts);
        MQTTAsync_destroy(&primary.client);
        primary.client = NULL;
    }

    if (secondary.client)
    {
        MQTTAsync_disconnectOptions disc_opts =
            MQTTAsync_disconnectOptions_initializer;

        MQTTAsync_disconnect(secondary.client, &disc_opts);
        MQTTAsync_destroy(&secondary.client);
        secondary.client = NULL;
    }

    LOG_INFO("MQTT cleanup completed");
}

void handle_signal(int sig)
{
    LOG_INFO("Received signal %d, shutting down...", sig);
    stop_flag = 0;
}


int main()
{

    printf("mqtt\n");
     //Signal Handling for MQTT Process -- Gokul added this 27/04/2026
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    //signal(SIGQUIT, handle_signal);
    printf("MQTT Process started\n");//Gokul added
    if (log_init() != 0)
    {
        fprintf(stderr, "WARNING: Logging unavailable, continuing without log file.\n");
    }

    ctx = redis_connect();
    if (!ctx)
    {
        LOG_ERROR("Cannot connect to Redis - aborting");
        log_close();
        return EXIT_FAILURE;
    }

    // rithika 16April2026
    char redis_key[64];
    snprintf(redis_key, sizeof(redis_key), "MQTT_PROC");
    dcu_netlog_init(redis_key);
    send_hc_msg();

    mqtt_module_start();

    while (stop_flag)
    {
        // rithika 16April2026
        iec104_log_sink_poll_network();
        sleep(1);
    }

    mqtt_cleanup();//Gokul added the mqtt cleanup function to shut down the process gracefully..!! 27/04/2026

    redisFree(ctx);

    log_close();
    // rithika 16April2026
    dcu_netlog_close();

    return 0;
}