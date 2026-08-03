#include <stdio.h>
#include "../include/general.h"
#include <pthread.h>
// #include "logger.h"

#define MQTT_1_CERTS_LOC "/usr/cms/config/mqtt_1_certs"
#define MQTT_2_CERTS_LOC "/usr/cms/config/mqtt_2_certs"

extern int certificate_path_check_primary;
extern int certificate_path_check_secondary;
extern char meter_serials[MAX_METERS][32];
extern int meter_count;
extern char dcu_ser_num[SIZE_32];
extern secondary_connecting;
extern primary_connecting;
// rithika 02april2026
extern redisContext *ctx;
extern int cur_active_mqtt;
time_t primary_mqtt_conn_time = 0;
time_t secn_mqtt_conn_time = 0;
int current_active_primary = -1;
int current_active_secondary = -1;

int ls_cmd_redis_resp = 0;
int billing_cmd_redis_resp = 0;
int event_cmd_redis_resp = 0;
int midnight_cmd_redis_resp = 0;

int check_redis_resp = 0;
/* Connection callbacks */
cmd_request_t cpy_cmd;

volatile int mqtt_status_update_req = 0;
char mqtt_status_value[16];

// Gokul added the time updation for mqtt --> 05/05/2026
volatile int mqtt_time_update_req = 0;
char mqtt_time_uptime[64];
char mqtt_time_lastmsg[32];
int mqtt_time_active = -1; // 0=primary, 1=secondary, -1=none
int mqtt_time_update_last = 0;

int primary_need_destroy = 0;
int secondary_need_destroy = 0;

time_t primary_destroy_time = 0;
time_t secondary_destroy_time = 0;

extern time_t last_primary_try;
extern time_t last_secondary_try;

time_t primary_lost_time = 0;
time_t secondary_lost_time = 0;

extern time_t last_publish_inst;
extern time_t last_publish_profile;
extern time_t last_publish_hc;
extern time_t last_publish_modbus;

pthread_mutex_t mqtt_api_mutex = PTHREAD_MUTEX_INITIALIZER;

char mqtt_cmd_buffer[4096];
volatile int mqtt_cmd_recv = 0;

pthread_mutex_t cmd_mutex = PTHREAD_MUTEX_INITIALIZER;

static void on_connect_success(void *mqtt_ctx,
                               MQTTAsync_successData *resp)
{
    mqtt_conn_t *conn = mqtt_ctx;
    conn->connected = true;
    LOG_INFO("[MQTT] Connected to %s ", conn->cfg.broker_ip);
    if (conn == &primary)
        primary_connecting = 0;
    else if (conn == &secondary)
        secondary_connecting = 0;

    /* -------- PRIMARY ALWAYS PREFERRED -------- */
    if (conn == &primary)
    {

        current_active = &primary;
        /* Reset publish scheduler after broker switch */
        /* Gokul added the below time resetting when it switches to another broker --> 14/05/2026 */
        // last_publish_inst = time(NULL);
        // last_publish_profile = time(NULL);
        // last_publish_hc = time(NULL);
        // last_publish_modbus = time(NULL);
        last_publish_inst = monotonic_sec();
        last_publish_profile = monotonic_sec();
        last_publish_hc = monotonic_sec();
        last_publish_modbus = monotonic_sec();

        LOG_INFO("[SCHEDULER] Publish timers reset for PRIMARY");

        mqtt_subscribe_topic(&primary);

        // rithika 02April2026
        if (certificate_path_check_primary == 0)
            current_active_primary = 0;
        else
            current_active_primary = 1;

        current_active_secondary = -1;
        // primary_mqtt_conn_time = time(NULL);
        primary_mqtt_conn_time = monotonic_sec();
        update_mqtt_status("connected");
        update_mqtt_time(0);

        /* If secondary was active, disconnect it */
        if (secondary.connected)
        {
            LOG_INFO("[SWITCH] Scheduling secondary disconnect");

            secondary.connected = false;

            secondary_need_destroy = 1;
            // secondary_destroy_time = time(NULL);
            secondary_destroy_time = monotonic_sec();
        }
    }

    /* -------- SECONDARY CONNECTED -------- */
    else if (conn == &secondary)
    {
        /* Only use secondary if primary is NOT connected */
        if (!primary.connected)
        {

            current_active = &secondary;
            /* Reset publish scheduler after broker switch */
            /* Gokul added the below time resetting information when it connects to new broker --> 14/05/2026 */
            // last_publish_inst = time(NULL);
            // last_publish_profile = time(NULL);
            // last_publish_hc = time(NULL);
            // last_publish_modbus = time(NULL);
            last_publish_inst = monotonic_sec();
            last_publish_profile = monotonic_sec();
            last_publish_hc = monotonic_sec();
            last_publish_modbus = monotonic_sec();

            LOG_INFO("[SCHEDULER] Publish timers reset for SECONDARY");

            mqtt_subscribe_topic(&secondary);

            // rithika 02April2026
            if (certificate_path_check_secondary == 0)
                current_active_secondary = 0;
            else
                current_active_secondary = 1;

            current_active_primary = -1;
            // secn_mqtt_conn_time = time(NULL);
            secn_mqtt_conn_time = monotonic_sec();
            update_mqtt_status("connected");
            update_mqtt_time(0);
        }
    }

    LOG_INFO("[STATE] Active broker is now %s", current_active ? current_active->cfg.broker_ip : "NONE");
}

// static void on_connect_failure(void *mqtt_ctx, MQTTAsync_failureData *resp)
// {
//     mqtt_conn_t *conn = mqtt_ctx;
//     conn->connected = false;
//     LOG_INFO("[MQTT] Connection failed to %s", conn->cfg.broker_ip);
//     if (conn == &primary)
//     {
//         primary_connecting = 0;

//         if (conn->client)
//         {
//             MQTTAsync_destroy(&conn->client);
//             conn->client = NULL;
//         }
//     }
//     else if (conn == &secondary)
//     {
//         secondary_connecting = 0;

//         if (conn->client)
//         {
//             MQTTAsync_destroy(&conn->client);
//             conn->client = NULL;
//         }
//     }

//     // rithika 04April2026
//     if (conn->cfg.primary)
//     {
//         LOG_ERROR("[MQTT] PRIMARY FAILED");
//         primary_mqtt_conn_time = 0;
//         current_active_primary = -1;
//     }
//     else
//     {
//         LOG_ERROR("[MQTT] SECONDARY FAILED");
//         secn_mqtt_conn_time = 0;
//         current_active_secondary = -1;
//     }
//     update_mqtt_status("disconnected");
//     primary_mqtt_conn_time = 0;
//     secn_mqtt_conn_time = 0;
//     update_mqtt_time(0);
//     // update_mqtt_status("disconnected"); //Gokul commented this line --> 02/05/2026
//     // update_mqtt_time(0);
// }

static void on_connect_failure(void *mqtt_ctx, MQTTAsync_failureData *resp)
{
    mqtt_conn_t *conn = (mqtt_conn_t *)mqtt_ctx;

    conn->connected = false;

    LOG_INFO("[MQTT] Connection failed to %s",
             conn->cfg.broker_ip);

    if (conn == &primary)
    {
        primary_connecting = 0;

        // last_primary_try = time(NULL);
        last_primary_try = monotonic_sec();

        primary_need_destroy = 1;
        // primary_destroy_time = time(NULL);
        primary_destroy_time = monotonic_sec();

        LOG_ERROR("[MQTT] PRIMARY FAILED");

        primary_mqtt_conn_time = 0;
        current_active_primary = -1;
    }
    else if (conn == &secondary)
    {
        secondary_connecting = 0;

        // last_secondary_try = time(NULL);
        last_secondary_try = monotonic_sec();

        secondary_need_destroy = 1;
        // secondary_destroy_time = time(NULL);
        secondary_destroy_time = monotonic_sec();

        LOG_ERROR("[MQTT] SECONDARY FAILED");

        secn_mqtt_conn_time = 0;
        current_active_secondary = -1;
    }
    if (current_active == conn)
    {
        current_active = NULL;
        cur_active_mqtt = -1;
    }

    // if ((primary.client && MQTTAsync_isConnected(primary.client)) ||
    //     (secondary.client && MQTTAsync_isConnected(secondary.client)))
    if (primary.connected || secondary.connected)
    {
        update_mqtt_status("connected");
    }
    else
    {
        update_mqtt_status("disconnected");
    }

    update_mqtt_time(0);
}

void on_send_success(void *context, MQTTAsync_successData *response)
{
    unsigned char *payload = (unsigned char *)context;
    free(payload);
}

// void connectionLost(void *context, char *cause)
// {
//     mqtt_conn_t *lost = (mqtt_conn_t *)context;

//     LOG_INFO("[MQTT] Connection lost from %s", lost->cfg.broker_ip);

//     lost->connected = false;

//     if (current_active == lost)
//     {
//         current_active = NULL;
//         // rithika 02April2026
//         cur_active_mqtt = -1;
//     }
//     // Gokul added this to clear the status --> 05/05/2026
//     update_mqtt_status("disconnected");
//     primary_mqtt_conn_time = 0;
//     secn_mqtt_conn_time = 0;
//     update_mqtt_time(0);
//     /* Destroy old MQTT client */
//     if (lost->client)
//     {
//         MQTTAsync_destroy(&lost->client);
//         lost->client = NULL;
//     }
//     if (lost == &primary)
//         primary_connecting = 0;
//     else if (lost == &secondary)
//         secondary_connecting = 0;
//     /* If primary lost -> immediately try secondary */
//     /* Gokul commented the below lines --> 02/05/2026 */

//     // if (lost == &primary)
//     // {
//     //     if (!secondary.connected && secondary.cfg.enable_mqtt)
//     //     {
//     //         LOG_INFO("[FAILOVER] Connecting secondary broker");
//     //         mqtt_connect(&secondary);
//     //     }
//     // }
// }

void connectionLost(void *context, char *cause)
{
    mqtt_conn_t *lost = (mqtt_conn_t *)context;

    LOG_INFO("[MQTT] Connection lost from %s", lost->cfg.broker_ip);

    lost->connected = false;

    if (current_active == lost)
    {
        current_active = NULL;
        cur_active_mqtt = -1;
    }

    // update_mqtt_status("disconnected");
    // if ((primary.client && MQTTAsync_isConnected(primary.client)) ||
    //     (secondary.client && MQTTAsync_isConnected(secondary.client)))
    if (primary.connected || secondary.connected)
    {
        update_mqtt_status("connected");
    }
    else
    {
        update_mqtt_status("disconnected");
    }
    primary_mqtt_conn_time = 0;
    secn_mqtt_conn_time = 0;

    update_mqtt_time(0);

    // ONLY FLAG — NO DESTROY HERE
    // if (lost == &primary)
    // {
    //     primary_connecting = 0;
    //     primary_need_destroy = 1;
    //     primary_destroy_time = time(NULL);
    // }
    // else if (lost == &secondary)
    // {
    //     secondary_connecting = 0;
    //     secondary_need_destroy = 1;
    //     secondary_destroy_time = time(NULL);
    // }

    if (lost == &primary)
    {
        primary_connecting = 0;

        primary_need_destroy = 1;
        // primary_destroy_time = time(NULL);
        primary_destroy_time = monotonic_sec();

        // primary_lost_time = time(NULL);
        primary_lost_time = monotonic_sec();
    }

    if (lost == &secondary)
    {
        secondary_connecting = 0;

        secondary_need_destroy = 1;
        // secondary_destroy_time = time(NULL);
        secondary_destroy_time = monotonic_sec();

        // secondary_lost_time = time(NULL);
        secondary_lost_time = monotonic_sec();
    }
}

/**
 * configure_tls()
 * ---------------
 * Configures MQTTAsync_SSLOptions using files loaded from Redis.
 *
 * This function does NOT allocate memory dynamically.
 */
// void configure_tls(mqtt_conn_t *conn)
// {
//     MQTTAsync_SSLOptions *ssl = &conn->ssl_opts;
//     *ssl = (MQTTAsync_SSLOptions)MQTTAsync_SSLOptions_initializer;

//     /* Mandatory: verify broker certificate */
//     ssl->enableServerCertAuth = 1;

//     if (conn->ssl.num_files >= 1)
//     {
//         LOG_INFO("SSL File 1 ==> %s", conn->ssl.ssl_files[0]);
//         ssl->trustStore = conn->ssl.ssl_files[0];
//         LOG_INFO("[TLS] CA cert: %s", ssl->trustStore);
//     }

//     if (conn->ssl.num_files >= 2)
//     {
//         LOG_INFO("SSL File 2 ==> %s", conn->ssl.ssl_files[1]);
//         ssl->keyStore = conn->ssl.ssl_files[1];
//         LOG_INFO("[TLS] Client cert: %s", ssl->keyStore);
//     }

//     if (conn->ssl.num_files >= 3)
//     {
//         LOG_INFO("SSL File 3 ==> %s", conn->ssl.ssl_files[2]);
//         ssl->privateKey = conn->ssl.ssl_files[2];
//         LOG_INFO("[TLS] Private key: %s", ssl->privateKey);
//     }

//     if (conn->ssl.num_files >= 4)
//     {
//         LOG_INFO("SSL File 4 ==> %s", conn->ssl.ssl_files[3]);
//         ssl->privateKeyPassword = NULL; /* If needed, extend */
//     }

//     /* Recommended TLS versions */
//     ssl->sslVersion = MQTT_SSL_VERSION_TLS_1_2;
// }

// Gokul commented this working logic to test the below one ---> 10/04/2026
//  void configure_tls(mqtt_conn_t *conn)
//  {
//      MQTTAsync_SSLOptions *ssl = &conn->ssl_opts;
//      mqtt_cfg_t *cfg = &conn->cfg;

//     static char ca_path[MAX_STR_LEN];
//     static char cert_path[MAX_STR_LEN];
//     static char key_path[MAX_STR_LEN];

//     const char *base_path;

//     *ssl = (MQTTAsync_SSLOptions)MQTTAsync_SSLOptions_initializer;

//     // ---- Select certificate directory ----
//     printf("Certificate Path Check Primary Variable = %d\n", certificate_path_check_primary);
//     printf("Certificate Path Check Secondary Variable = %d\n", certificate_path_check_secondary);
//     printf("Variables changed!!!\n");
//     if (cfg->primary == 1)
//     {
//         if (certificate_path_check_primary == 0)
//             base_path = MQTT_1_CERTS_LOC;
//         else
//             base_path = MQTT_2_CERTS_LOC;
//     }
//     else
//     {
//         if (certificate_path_check_secondary == 0)
//             base_path = MQTT_1_CERTS_LOC;
//         else
//             base_path = MQTT_2_CERTS_LOC;
//     }

//     LOG_INFO("[TLS] Using cert path: %s", base_path);

//     // ---- Server certificate verification ----
//     ssl->enableServerCertAuth = (cfg->insecure == 0) ? 1 : 0;

//     // ---- CA Certificate ----
//     if (strlen(cfg->ca_certificate) > 0)
//     {
//         snprintf(ca_path, sizeof(ca_path), "%s/%s", base_path, cfg->ca_certificate);
//         ssl->trustStore = ca_path;
//         LOG_INFO("[TLS] CA cert       : %s", ssl->trustStore);
//     }

//     // ---- Client Certificate ----
//     if (strlen(cfg->client_certificate) > 0)
//     {
//         snprintf(cert_path, sizeof(cert_path), "%s/%s", base_path, cfg->client_certificate);
//         ssl->keyStore = cert_path;
//         LOG_INFO("[TLS] Client cert   : %s", ssl->keyStore);
//     }

//     // ---- Private Key ----
//     if (strlen(cfg->client_key) > 0)
//     {
//         snprintf(key_path, sizeof(key_path), "%s/%s", base_path, cfg->client_key);
//         ssl->privateKey = key_path;
//         LOG_INFO("[TLS] Private key   : %s", ssl->privateKey);
//     }

//     // ---- Key Password ----
//     if (cfg->encrypted_key && strlen(cfg->key_password) > 0)
//     {
//         ssl->privateKeyPassword = cfg->key_password;
//         LOG_INFO("[TLS] Key password  : SET");
//     }
//     else
//     {
//         ssl->privateKeyPassword = NULL;
//     }

//     // ---- TLS Version ----
//     ssl->sslVersion = MQTT_SSL_VERSION_TLS_1_2;

//     LOG_INFO("[TLS] TLS Version     : TLS 1.2");
// }

void configure_tls(mqtt_conn_t *conn)
{
    MQTTAsync_SSLOptions *ssl = &conn->ssl_opts;
    mqtt_cfg_t *cfg = &conn->cfg;

    static char ca_path[MAX_STR_LEN];
    static char cert_path[MAX_STR_LEN];
    static char key_path[MAX_STR_LEN];

    const char *base_path;

    *ssl = (MQTTAsync_SSLOptions)MQTTAsync_SSLOptions_initializer;

    // ---- Select certificate directory ----
    printf("Certificate Path Check Primary Variable = %d\n", certificate_path_check_primary);
    printf("Certificate Path Check Secondary Variable = %d\n", certificate_path_check_secondary);
    printf("Variables changed!!!\n");
    // if (cfg->primary == 1)
    // {
    //     if (certificate_path_check_primary == 0)
    //         base_path = MQTT_1_CERTS_LOC;
    //     else
    //         base_path = MQTT_2_CERTS_LOC;
    // }
    // else
    // {
    //     if (certificate_path_check_secondary == 0)
    //         base_path = MQTT_1_CERTS_LOC;
    //     else
    //         base_path = MQTT_2_CERTS_LOC;
    // }

    // LOG_INFO("[TLS] Using cert path: %s", base_path);

    // ---- Server certificate verification ----
    ssl->enableServerCertAuth = (cfg->insecure == 0) ? 1 : 0;

    // ---- CA Certificate ----
    if (strlen(cfg->ca_certificate) > 0)
    {
        snprintf(ca_path, sizeof(ca_path), "%s", cfg->ca_certificate);
        ssl->trustStore = ca_path;
        LOG_INFO("[TLS] CA cert       : %s", ssl->trustStore);
    }

    // ---- Client Certificate ----
    if (strlen(cfg->client_certificate) > 0)
    {
        snprintf(cert_path, sizeof(cert_path), "%s", cfg->client_certificate);
        ssl->keyStore = cert_path;
        LOG_INFO("[TLS] Client cert   : %s", ssl->keyStore);
    }

    // ---- Private Key ----
    if (strlen(cfg->client_key) > 0)
    {
        snprintf(key_path, sizeof(key_path), "%s", cfg->client_key);
        ssl->privateKey = key_path;
        LOG_INFO("[TLS] Private key   : %s", ssl->privateKey);
    }

    // ---- Key Password ----
    if (cfg->encrypted_key && strlen(cfg->key_password) > 0)
    {
        ssl->privateKeyPassword = cfg->key_password;
        LOG_INFO("[TLS] Key password  : SET");
    }
    else
    {
        ssl->privateKeyPassword = NULL;
    }

    // ---- TLS Version ----
    ssl->sslVersion = MQTT_SSL_VERSION_TLS_1_2;

    LOG_INFO("[TLS] TLS Version     : TLS 1.2");
}

int mqtt_connect(mqtt_conn_t *conn)
{
    char url[256];

    snprintf(url, sizeof(url), "%s://%s:%d",
             conn->cfg.enable_ssl ? "ssl" : "tcp",
             conn->cfg.broker_ip,
             conn->cfg.broker_port);
    LOG_INFO("URL CONNECTION ---> %s", url);
    /* Create client only once */
    // if (conn->client && conn->connected == false)
    // {
    //     MQTTAsync_destroy(&conn->client);
    //     conn->client = NULL;
    // }
    if (conn->client == NULL)
    {
        MQTTAsync_create(&conn->client,
                         url,
                         conn->cfg.client_id,
                         MQTTCLIENT_PERSISTENCE_NONE,
                         NULL);

        MQTTAsync_setCallbacks(conn->client,
                               conn,
                               connectionLost,
                               on_message_arrived,
                               NULL);
    }

    MQTTAsync_connectOptions opts =
        MQTTAsync_connectOptions_initializer;

    opts.username = conn->cfg.username;
    opts.password = conn->cfg.password;
    opts.keepAliveInterval = conn->cfg.keep_alive;
    opts.cleansession = conn->cfg.clean_session;

    opts.connectTimeout = 5;
    opts.retryInterval = 0;

    // opts.automaticReconnect = 1;
    // opts.minRetryInterval = 3;
    // opts.maxRetryInterval = 10;

    opts.onSuccess = on_connect_success;
    opts.onFailure = on_connect_failure;
    opts.context = conn;

    if (conn->cfg.enable_ssl)
    {
        configure_tls(conn);
        opts.ssl = &conn->ssl_opts;
    }

    conn->connected = false;

    // return MQTTAsync_connect(conn->client, &opts);
    pthread_mutex_lock(&mqtt_api_mutex);

    int rc = MQTTAsync_connect(conn->client, &opts);

    pthread_mutex_unlock(&mqtt_api_mutex);

    return rc;
}

void mqtt_send_file(mqtt_conn_t *mqtt_cfg, const char *filename, int topic_type)
{
    int rc;
    char pub_topic[256];
    FILE *fp = fopen(filename, "rb");
    if (!fp)
    {
        LOG_INFO("Failed to open file");
        return;
    }

    unsigned char buffer[PAYLOAD_BUFFER_SIZE];
    size_t bytes_read;

    while ((bytes_read = fread(buffer, 1, PAYLOAD_BUFFER_SIZE, fp)) > 0)
    {
        unsigned char *payload = malloc(bytes_read);
        if (!payload)
            break;

        memcpy(payload, buffer, bytes_read);

        MQTTAsync_message msg = MQTTAsync_message_initializer;
        MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;

        msg.payload = payload;
        msg.payloadlen = bytes_read;
        msg.qos = mqtt_cfg->cfg.qos;
        msg.retained = 0;

        opts.onSuccess = on_send_success;
        opts.context = payload;

        if (topic_type == METER_DATA_TOPIC)
        {
            memset(pub_topic, 0, sizeof(pub_topic));
            snprintf(pub_topic, sizeof(pub_topic), "%s/%s", mqtt_cfg->cfg.cyclic_dlms_data_topic, dcu_ser_num);
            rc = MQTTAsync_sendMessage(mqtt_cfg->client,
                                       pub_topic,
                                       &msg,
                                       &opts);
        }
        else if (topic_type == INST_DATA_TOPIC)
        {
            memset(pub_topic, 0, sizeof(pub_topic));
            snprintf(pub_topic, sizeof(pub_topic), "%s/%s", mqtt_cfg->cfg.inst_data_topic, dcu_ser_num);
            rc = MQTTAsync_sendMessage(mqtt_cfg->client,
                                       pub_topic,
                                       &msg,
                                       &opts);
        }
        else
        {
            memset(pub_topic, 0, sizeof(pub_topic));
            snprintf(pub_topic, sizeof(pub_topic), "%s/%s", mqtt_cfg->cfg.cmd_response_topic, dcu_ser_num);
            rc = MQTTAsync_sendMessage(mqtt_cfg->client,
                                       pub_topic,
                                       &msg,
                                       &opts);
        }

        if (rc != MQTTASYNC_SUCCESS)
        {
            free(payload);
            break;
        }
    }

    fclose(fp);
    LOG_INFO("[FILE] Transfer complete");
}

void mqtt_send_msg(mqtt_conn_t *mqtt_cfg, const char *mqtt_msg, int msg_size, int topic_type)
{
    int rc;
    char *payload = malloc(msg_size);
    char pub_topic[256];
    if (!payload)
    {
        LOG_ERROR(stderr, "Memory allocation failed");
        return;
    }

    memcpy(payload, mqtt_msg, msg_size);

    MQTTAsync_message msg = MQTTAsync_message_initializer;
    MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;

    msg.payload = payload;
    msg.payloadlen = msg_size;
    msg.qos = mqtt_cfg->cfg.qos;
    msg.retained = 0;

    opts.onSuccess = on_send_success;
    opts.context = payload;

    if (topic_type == MODBUS_DATA_TOPIC)
    {
        memset(pub_topic, 0, sizeof(pub_topic));
        snprintf(pub_topic, sizeof(pub_topic), "%s/%s", mqtt_cfg->cfg.cyclic_modbus_data_topic, dcu_ser_num);
        rc = MQTTAsync_sendMessage(mqtt_cfg->client,
                                   pub_topic,
                                   &msg,
                                   &opts);
    }
    else if (topic_type == HEALTH_DATA_TOPIC)
    {
        memset(pub_topic, 0, sizeof(pub_topic));
        snprintf(pub_topic, sizeof(pub_topic), "%s/%s", mqtt_cfg->cfg.health_check_data_topic, dcu_ser_num);

        rc = MQTTAsync_sendMessage(mqtt_cfg->client,
                                   pub_topic,
                                   &msg,
                                   &opts);
    }
    else
    {
        memset(pub_topic, 0, sizeof(pub_topic));
        snprintf(pub_topic, sizeof(pub_topic), "%s/%s", mqtt_cfg->cfg.cmd_response_topic, dcu_ser_num);
        rc = MQTTAsync_sendMessage(mqtt_cfg->client,
                                   pub_topic,
                                   &msg,
                                   &opts);
    }

    if (rc != MQTTASYNC_SUCCESS)
    {
        free(payload);
        LOG_ERROR(stderr, "MQTT send failed");
        return;
    }
    if (topic_type == MODBUS_DATA_TOPIC)
    {
        LOG_INFO("[MODBUS Message] Transfer completed");
    }
    else if (topic_type == HEALTH_DATA_TOPIC)
    {
        LOG_INFO("[HEALTH CHECK Message] Transfer completed");
    }
    else
    {
        LOG_INFO("[COMMAND RESPONSE Message] Transfer completed");
    }

    update_mqtt_time(1);
}

void mqtt_subscribe_topic(mqtt_conn_t *mqtt_cfg)
{
    char sub_topic[256];
    MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
    LOG_INFO("Topics\n");

    for (int i = 0; i < MAX_SUB_TOPICS; i++)
    {
        // rithika 18Apr2026
        memset(sub_topic, 0, sizeof(sub_topic));
        snprintf(sub_topic, sizeof(sub_topic), "%s/%s", mqtt_cfg->cfg.subscribe_topics[i], dcu_ser_num);

        int rc = MQTTAsync_subscribe(mqtt_cfg->client, sub_topic, mqtt_cfg->cfg.qos, &opts);

        if (rc != MQTTASYNC_SUCCESS)
        {
            printf("Subscribe Topics Failed !!!\n");
            LOG_ERROR(stderr, "Failed to subscribe the topic %s", sub_topic);
            return;
        }
        printf("Topics Subscribed Successfully !!!\n");
        LOG_INFO("%s\n", sub_topic);
    }

    LOG_INFO("subscribed successfully\n");
}

/* -------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------- */

/**
 * @brief  Extract the value of a named XML attribute from a tag string.
 *
 *         Looks for: NAME="value" or NAME='value'
 *
 * @param  tag      Pointer to the start of the opening tag text (after '<').
 * @param  attr     Null-terminated attribute name to search for.
 * @param  out      Output buffer.
 * @param  out_len  Size of output buffer.
 * @return          0 on success, -1 if attribute not found or buffer too small.
 */
static int extract_attr(const char *restrict tag, const char *restrict attr, char *restrict out, uint32_t out_len)
{
    char needle[CMD_TYPE_MAX_LEN];
    const char *p = NULL;
    const char *start = NULL;
    const char *end = NULL;
    char quote = '"';
    uint32_t len = 0;

    /* Build search needle: ATTR=" */
    snprintf(needle, sizeof(needle), "%s=\"", attr);
    p = strstr(tag, needle);

    if (!p)
    {
        /* Try single-quote variant */
        snprintf(needle, sizeof(needle), "%s='", attr);
        p = strstr(tag, needle);
        if (!p)
            return -1;
        quote = '\'';
    }

    start = p + strlen(needle);
    end = strchr(start, quote);
    if (!end)
    {
        LOG_WARN("extract_attr: unterminated quote for attr '%s'", attr);
        return -1;
    }

    len = (uint32_t)(end - start);
    if (len >= out_len)
    {
        LOG_ERROR("extract_attr: value too long for attr '%s' (%u >= %u)",
                  attr, len, out_len);
        return -1;
    }

    memcpy(out, start, len);
    out[len] = '\0';
    return 0;
}

/**
 * @brief  Extract the inner text of the first occurrence of a named XML tag.
 *
 *         Handles: <TAG_NAME>inner text</TAG_NAME>
 *
 * @param  xml      Full XML string to search.
 * @param  tag_name Null-terminated tag name.
 * @param  out      Output buffer.
 * @param  out_len  Size of output buffer.
 * @return          0 on success, -1 if tag not found or buffer too small.
 */
static int extract_tag_inner(const char *restrict xml,
                             const char *restrict tag_name,
                             char *restrict out,
                             uint32_t out_len)
{
    char open_tag[CMD_TYPE_MAX_LEN];
    char close_tag[CMD_TYPE_MAX_LEN];
    const char *p_open = NULL;
    const char *start = NULL;
    const char *end = NULL;
    uint32_t len = 0;

    snprintf(open_tag, sizeof(open_tag), "<%s>", tag_name);
    snprintf(close_tag, sizeof(close_tag), "</%s>", tag_name);

    p_open = strstr(xml, open_tag);
    if (!p_open)
    {
        LOG_DEBUG("extract_tag_inner: tag '<%s>' not found", tag_name);
        return -1;
    }

    start = p_open + strlen(open_tag);
    end = strstr(start, close_tag);
    if (!end)
    {
        LOG_WARN("extract_tag_inner: no closing tag '</%s>'", tag_name);
        return -1;
    }

    len = (uint32_t)(end - start);
    if (len >= out_len)
    {
        LOG_ERROR("extract_tag_inner: inner text too long for tag '%s' (%u >= %u)",
                  tag_name, len, out_len);
        return -1;
    }

    memcpy(out, start, len);
    out[len] = '\0';
    return 0;
}

// Gokul commented the below cdf parse request --> 08/07/2026

// int parse_cmd_request(const char *xml, cmd_request_t *cmd)
// {
//     const char *cmd_info_tag = NULL;
//     const char *args_tag = NULL;
//     uint32_t i = 0;
//     int rc = 0;

//     if (!xml || !cmd)
//     {
//         LOG_ERROR("parse_cmd_request: NULL argument(s)");
//         return -1;
//     }

//     memset(cmd, 0, sizeof(*cmd));

//     /* ---- Validate outer wrapper ---------------------------------------- */
//     if (!strstr(xml, "<COMMAND_REQUEST>") || !strstr(xml, "</COMMAND_REQUEST>"))
//     {
//         LOG_ERROR("parse_cmd_request: missing <COMMAND_REQUEST> wrapper");
//         return -1;
//     }

//     /* ---- Locate <COMMAND_INFO …> opening tag ---------------------------- */
//     cmd_info_tag = strstr(xml, "<COMMAND_INFO");
//     if (!cmd_info_tag)
//     {
//         LOG_ERROR("parse_cmd_request: <COMMAND_INFO> tag not found");
//         return -1;
//     }

//     /* ---- Extract TYPE attribute ----------------------------------------- */
//     rc = extract_attr(cmd_info_tag, "TYPE",
//                       cmd->type, (uint32_t)sizeof(cmd->type));
//     if (rc != 0)
//     {
//         LOG_ERROR("parse_cmd_request: failed to extract TYPE attribute");
//         return -1;
//     }

//     /* ---- Extract TRANSACTION attribute ---------------------------------- */
//     rc = extract_attr(cmd_info_tag, "TRANSACTION",
//                       cmd->transaction, (uint32_t)sizeof(cmd->transaction));
//     if (rc != 0)
//     {
//         LOG_ERROR("parse_cmd_request: failed to extract TRANSACTION attribute");
//         return -1;
//     }

//     /* ---- Locate <ARGUMENTS …> tag --------------------------------------- */
//     args_tag = strstr(cmd_info_tag, "<ARGUMENTS");
//     if (!args_tag)
//     {
//         LOG_ERROR("parse_cmd_request: <ARGUMENTS> tag not found");
//         return -1;
//     }

//     /* ---- Extract COUNT attribute ---------------------------------------- */
//     {
//         char count_buf[16] = {0};
//         rc = extract_attr(args_tag, "COUNT",count_buf, (uint32_t)sizeof(count_buf));
//         if (rc != 0)
//         {
//             LOG_ERROR("parse_cmd_request: failed to extract COUNT attribute");
//             return -1;
//         }

//         cmd->arg_count = (uint32_t)strtoul(count_buf, NULL, 10);
//         if (cmd->arg_count > CMD_MAX_ARGS)
//         {
//             LOG_ERROR("parse_cmd_request: COUNT %u exceeds CMD_MAX_ARGS %u",cmd->arg_count, CMD_MAX_ARGS);
//             return -1;
//         }
//     }

//     /* ---- Extract each ARG_xx value ------------------------------------- */
//     for (i = 0; i < cmd->arg_count; i++)
//     {
//         char tag_name[16] = {0};
//         snprintf(tag_name, sizeof(tag_name), "ARG_%02u", i + 1);

//         rc = extract_tag_inner(args_tag, tag_name,
//                                cmd->args[i], (uint32_t)sizeof(cmd->args[i]));
//         if (rc != 0)
//         {
//             LOG_WARN("parse_cmd_request: tag <%s> missing or unreadable", tag_name);
//             /* Non-fatal: leave the slot empty, continue */
//         }
//     }

//     LOG_INFO("parse_cmd_request: OK type='%s' transaction='%s' args=%u",
//              cmd->type, cmd->transaction, cmd->arg_count);

//     return 0;
// }

// int parse_cmd_request(const char *json_str, cmd_request_t *cmd)
// {
//     cJSON *root = NULL;
//     cJSON *data = NULL;
//     cJSON *item = NULL;

//     if (!json_str || !cmd)
//     {
//         LOG_ERROR("parse_cmd_request: NULL argument(s)");
//         return -1;
//     }

//     memset(cmd, 0, sizeof(*cmd));

//     /* Parse JSON */
//     root = cJSON_Parse(json_str);
//     if (root == NULL)
//     {
//         LOG_ERROR("parse_cmd_request: Invalid JSON");
//         return -1;
//     }

//     /* Extract COMMAND_TYPE */
//     item = cJSON_GetObjectItemCaseSensitive(root, "COMMAND_TYPE");
//     if (!cJSON_IsString(item) || item->valuestring == NULL)
//     {
//         LOG_ERROR("parse_cmd_request: COMMAND_TYPE missing");
//         cJSON_Delete(root);
//         return -1;
//     }

//     strncpy(cmd->type, item->valuestring, sizeof(cmd->type) - 1);

//     /* Extract SEQ_NUM */
//     item = cJSON_GetObjectItemCaseSensitive(root, "SEQ_NUM");
//     if (!cJSON_IsString(item) || item->valuestring == NULL)
//     {
//         LOG_ERROR("parse_cmd_request: SEQ_NUM missing");
//         cJSON_Delete(root);
//         return -1;
//     }

//     strncpy(cmd->transaction, item->valuestring,sizeof(cmd->transaction) - 1);

//     item = cJSON_GetObjectItemCaseSensitive(root, "DATA_TYPE");
//     if (!cJSON_IsString(item) || item->valuestring == NULL)
//     {
//         LOG_ERROR("parse_cmd_request: DATA_TYPE missing");
//         cJSON_Delete(root);
//         return -1;
//     }

//     strncpy(cmd->data_type_req, item->valuestring,sizeof(cmd->data_type_req) - 1);

//     /* Extract DATA object */
//     data = cJSON_GetObjectItemCaseSensitive(root, "DATA");
//     if (!cJSON_IsObject(data))
//     {
//         LOG_ERROR("parse_cmd_request: DATA object missing");
//         cJSON_Delete(root);
//         return -1;
//     }

//     cmd->arg_count = 0;

//     /* Store every DATA value into args[] */
//     cJSON_ArrayForEach(item, data)
//     {
//         if (cmd->arg_count >= CMD_MAX_ARGS)
//             break;

//         if (cJSON_IsString(item) && item->valuestring != NULL)
//         {
//             strncpy(cmd->args[cmd->arg_count],item->valuestring,sizeof(cmd->args[0]) - 1);
//             cmd->args[cmd->arg_count][sizeof(cmd->args[0]) - 1] = '\0';
//             cmd->arg_count++;
//         }
//     }

//     LOG_INFO("parse_cmd_request: OK type='%s' transaction='%s' args=%u",cmd->type,cmd->transaction,cmd->arg_count);
//     cJSON_Delete(root);

//     return 0;
// }

int parse_cmd_request(const char *json_str, cmd_request_t *cmd)
{
    cJSON *root = NULL;
    cJSON *data = NULL;
    cJSON *item = NULL;

    if (!json_str || !cmd)
    {
        LOG_ERROR("parse_cmd_request: NULL argument(s)");
        return -1;
    }

    memset(cmd, 0, sizeof(*cmd));

    /* Parse JSON */
    root = cJSON_Parse(json_str);

    cmd->root = root;
    cmd->data = cJSON_GetObjectItemCaseSensitive(root, "DATA");

    if (root == NULL)
    {
        LOG_ERROR("parse_cmd_request: Invalid JSON");
        return -1;
    }

    /* COMMAND_TYPE */
    item = cJSON_GetObjectItemCaseSensitive(root, "COMMAND_TYPE");
    if (!cJSON_IsString(item) || item->valuestring == NULL)
    {
        LOG_ERROR("parse_cmd_request: COMMAND_TYPE missing");
        cJSON_Delete(root);
        return -1;
    }
    strncpy(cmd->type, item->valuestring, sizeof(cmd->type) - 1);

    /* SEQ_NUM */
    item = cJSON_GetObjectItemCaseSensitive(root, "SEQ_NUM");
    if (!cJSON_IsString(item) || item->valuestring == NULL)
    {
        LOG_ERROR("parse_cmd_request: SEQ_NUM missing");
        cJSON_Delete(root);
        return -1;
    }
    strncpy(cmd->transaction, item->valuestring,
            sizeof(cmd->transaction) - 1);

    /* DATA_TYPE */
    item = cJSON_GetObjectItemCaseSensitive(root, "DATA_TYPE");
    if (!cJSON_IsString(item) || item->valuestring == NULL)
    {
        LOG_ERROR("parse_cmd_request: DATA_TYPE missing");
        cJSON_Delete(root);
        return -1;
    }
    strncpy(cmd->data_type_req,
            item->valuestring,
            sizeof(cmd->data_type_req) - 1);

    /* DATA object */
    data = cJSON_GetObjectItemCaseSensitive(root, "DATA");
    if (!cJSON_IsObject(data))
    {
        LOG_ERROR("parse_cmd_request: DATA object missing");
        cJSON_Delete(root);
        return -1;
    }

    cmd->arg_count = 0;

    /* --------------------------------------------------------
     * Existing parser
     * Store only string values into args[]
     * -------------------------------------------------------- */
    cJSON_ArrayForEach(item, data)
    {
        if (cmd->arg_count >= CMD_MAX_ARGS)
            break;

        if (cJSON_IsString(item) && item->valuestring != NULL)
        {
            strncpy(cmd->args[cmd->arg_count],
                    item->valuestring,
                    sizeof(cmd->args[0]) - 1);

            cmd->args[cmd->arg_count][sizeof(cmd->args[0]) - 1] = '\0';

            cmd->arg_count++;
        }
    }

    /* --------------------------------------------------------
     * ReadModbus specific parsing
     * -------------------------------------------------------- */
    if (strcmp(cmd->type, "ReadModbus") == 0)
    {
        cJSON *slave_id;
        cJSON *registers;

        cmd->slave_id = 0;
        cmd->reg_count = 0;

        slave_id = cJSON_GetObjectItemCaseSensitive(data, "SLAVE_ID");
        if (cJSON_IsNumber(slave_id))
        {
            cmd->slave_id = slave_id->valueint;
        }

        registers = cJSON_GetObjectItemCaseSensitive(data, "REGISTERS");

        if (cJSON_IsArray(registers))
        {
            int total = cJSON_GetArraySize(registers);

            if (total > MAX_REGS_PER_DEV)
                total = MAX_REGS_PER_DEV;

            for (int i = 0; i < total; i++)
            {
                cJSON *reg = cJSON_GetArrayItem(registers, i);

                if (!cJSON_IsArray(reg))
                    continue;

                if (cJSON_GetArraySize(reg) < 3)
                    continue;

                cJSON *addr = cJSON_GetArrayItem(reg, 0);
                cJSON *type = cJSON_GetArrayItem(reg, 1);
                cJSON *count = cJSON_GetArrayItem(reg, 2);

                if (!cJSON_IsNumber(addr) ||
                    !cJSON_IsNumber(type) ||
                    !cJSON_IsNumber(count))
                    continue;

                cmd->regs[cmd->reg_count].address = addr->valueint;
                cmd->regs[cmd->reg_count].type = type->valueint;
                cmd->regs[cmd->reg_count].count = count->valueint;

                cmd->reg_count++;
            }
        }
    }

    LOG_INFO("parse_cmd_request: OK type='%s' transaction='%s' args=%u",
             cmd->type,
             cmd->transaction,
             cmd->arg_count);

    if (strcmp(cmd->type, "ReadModbus") == 0)
    {
        LOG_INFO("SLAVE_ID : %d", cmd->slave_id);
        LOG_INFO("REG COUNT: %d", cmd->reg_count);

        for (int i = 0; i < cmd->reg_count; i++)
        {
            LOG_INFO("REG[%d] ADDRESS=%d TYPE=%d COUNT=%d",
                     i,
                     cmd->regs[i].address,
                     cmd->regs[i].type,
                     cmd->regs[i].count);
        }
    }

    // cJSON_Delete(root); //Gokul commented this --> 15/07/2026

    return 0;
}

int generate_redis_list(cmd_request_t cmd)
{
    cpy_cmd = cmd;

    MeterStatus status;

    memset(&status, 0, sizeof(status));
    if (read_meter_status(ctx, cmd.args[0], &status) != 0)
    {
        LOG_ERROR("Cannot read meter status for meter %s", cmd.args[0]);
        return -1;
    }

    cJSON *root = cJSON_CreateObject();
    cJSON *data = cJSON_CreateObject();
    cJSON_AddStringToObject(root, "seq_no", cmd.transaction);

    if (!strcmp(cmd.data_type_req, "BLOCK"))
    {
        cJSON_AddStringToObject(root, "msgType", "OD_LS_DATA");
        cJSON_AddStringToObject(data, "startdate", cmd.args[1]); // 30-03-2026 format
    }
    else if (!strcmp(cmd.data_type_req, "DAILY"))
    {

        int day, month, year;
        char mn_date[32] = {0};

        if (sscanf(cmd.args[1], "%d-%d-%d", &day, &month, &year) == 3)
        {
            snprintf(mn_date, sizeof(mn_date), "%d_%d", month, year);
        }

        cJSON_AddStringToObject(root, "msgType", "OD_MN_DATA");
        cJSON_AddStringToObject(data, "startdate", mn_date);
    }
    else if (!strcmp(cmd.data_type_req, "EVENT"))
    {
        cJSON_AddStringToObject(root, "msgType", "OD_EVENT_DATA");
        cJSON_AddStringToObject(data, "startdate", "10");
        cJSON_AddStringToObject(data, "event_type", "all");
    }
    else if (!strcmp(cmd.data_type_req, "BILL"))
    {
        cJSON_AddStringToObject(root, "msgType", "OD_BILL_DATA");
        cJSON_AddStringToObject(data, "startdate", "current"); // 30-03-2026 format
    }

    cJSON_AddStringToObject(root, "init_source", "mqtt");

    cJSON_AddStringToObject(data, "port_id", status.port);
    cJSON_AddStringToObject(data, "num_days", "1");
    cJSON_AddStringToObject(data, "meter", cmd.args[0]);

    cJSON_AddItemToObject(root, "data", data);

    char *json_str = cJSON_Print(root);
    redisReply *reply = redisCommand(ctx, "LPUSH web_od_command %s", json_str);

    if (reply == NULL)
    {
        fprintf(stderr, "Redis command failed\n");

        return -1;
    }
    else
    {

        freeReplyObject(reply);
    }

    cJSON_Delete(root);
    free(json_str);
}

int is_list_empty()
{
    redisReply *rly = redisCommand(ctx, "LLEN mqtt_command_resp");
    if (rly == NULL)
    {
        fprintf(stderr, "Redis command failed\n");
        return -1;
    }

    int is_empty = rly->integer;
    freeReplyObject(rly);

    return is_empty;
}

int read_redis_resp(mqtt_conn_t *conn)
{
    int count = is_list_empty();
    static char start_date[32] = {0};
    char meter_ser[64] = {0};
    char output_msg[PAYLOAD_BUFFER_SIZE] = {0};
    int msg_size = 0;

    if (count == 0)
    {
        return -1;
    }

    redisReply *rly = redisCommand(ctx, "lpop mqtt_command_resp");
    if (rly == NULL)
    {
        fprintf(stderr, "Redis command failed\n");
        return -1;
    }
    if (rly->str == NULL)
    {
        freeReplyObject(rly);
        return -1;
    }

    char *cmd_resp = rly->str;
    printf("cmd_resp %s\n", cmd_resp);
    cJSON *root = cJSON_Parse(cmd_resp);
    freeReplyObject(rly);

    if (!root)
    {
        LOG_ERROR("Failed to parse meter status JSON");
        return -1;
    }

    cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");

    if (!cJSON_IsObject(data))
    {
        cJSON_Delete(root);
        check_redis_resp = 0;
        return -1;
    }

    cJSON *meter_ser_no = cJSON_GetObjectItemCaseSensitive(data, "meter");

    if (cJSON_IsString(meter_ser_no) && meter_ser_no->valuestring != NULL)
    {

        snprintf(meter_ser, sizeof(meter_ser), "%s", meter_ser_no->valuestring);
    }

    cJSON *data_type = cJSON_GetObjectItemCaseSensitive(root, "msg_type");

    if (cJSON_IsString(data_type) && data_type->valuestring != NULL)
    {
        if (strcmp(data_type->valuestring, "OD_LS_DATA") == 0)
        {
            cJSON *startdate = cJSON_GetObjectItemCaseSensitive(data, "start_date");

            if (cJSON_IsString(startdate) && startdate->valuestring != NULL)
            {
                int day, month, year;
                if (sscanf(startdate->valuestring, "%d-%d-%d", &day, &month, &year) == 3)
                {
                    snprintf(start_date, sizeof(start_date), "%04d-%02d-%02d", year, month, day);
                }
            }

            cdf_result_t res = generate_mqtt_ls_json(ctx, meter_ser, start_date);
            
            if (res.status == 0)
            {
                LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
                mqtt_send_file(current_active, res.filename, CMD_RESP_TOPIC);

                // rithika 18Apr2026
                // char file_rem_cmd[128];
                // sprintf(file_rem_cmd, "rm %s", res.filename);
                // system(file_rem_cmd);
                // remove(res.filename); ///01Aug2026
                LOG_INFO("%s is deleted successfully", res.filename);

                // msg_size = success_resp_msg(cpy_cmd, output_msg);
                // mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
            }
        }

        else if (strcmp(data_type->valuestring, "OD_MN_DATA") == 0)
        {
            midnight_cmd_redis_resp = 1;
        }
        else if (strcmp(data_type->valuestring, "OD_EVENT_DATA") == 0)
        {
            event_cmd_redis_resp = 1;
        }
        else if (strcmp(data_type->valuestring, "OD_BILL_DATA") == 0)
        {
            billing_cmd_redis_resp = 1;
        }
    }

    cJSON_Delete(root);

    LOG_INFO("read_redis_resp meter_ser %s start_date %s ls_cmd_redis_resp %d", meter_ser, start_date, ls_cmd_redis_resp);

    check_redis_resp = 0;
    return 0;
}

// rithika 01Aug2026
// int read_redis_resp(mqtt_conn_t *conn)
// {
//     int count = is_list_empty();
//     static char start_date[32] = {0};
//     char meter_ser[64] = {0};
//     char output_msg[PAYLOAD_BUFFER_SIZE] = {0};
//     int msg_size = 0;

//     if (count == 0)
//     {
//         return -1;
//     }

//     for (int i = 0; i < count; i++)
//     {
//         redisReply *rly = redisCommand(ctx, "lpop mqtt_command_resp");
//         if (rly == NULL)
//         {
//             fprintf(stderr, "Redis command failed\n");
//             return -1;
//         }
//         if (rly->str == NULL)
//         {
//             freeReplyObject(rly);
//             continue;
//         }

//         char *cmd_resp = rly->str;
//         printf("cmd_resp %s\n", cmd_resp);
//         cJSON *root = cJSON_Parse(cmd_resp);
//         freeReplyObject(rly);

//         if (!root)
//         {
//             LOG_ERROR("Failed to parse meter status JSON");
//             continue;
//         }

//         cJSON *data = cJSON_GetObjectItemCaseSensitive(root, "data");

//         if (!cJSON_IsObject(data))
//         {
//             cJSON_Delete(root);
//             continue;
//         }

//         cJSON *data_type = cJSON_GetObjectItemCaseSensitive(root, "msg_type");

//         if (cJSON_IsString(data_type) && data_type->valuestring != NULL)
//         {
//             if (strcmp(data_type->valuestring, "OD_LS_DATA") == 0)
//             {
//                 ls_cmd_redis_resp = 1;
//             }

//             else if (strcmp(data_type->valuestring, "OD_MN_DATA") == 0)
//             {
//                 midnight_cmd_redis_resp = 1;
//             }
//             else if (strcmp(data_type->valuestring, "OD_EVENT_DATA") == 0)
//             {
//                 event_cmd_redis_resp = 1;
//             }
//             else if (strcmp(data_type->valuestring, "OD_BILL_DATA") == 0)
//             {
//                 billing_cmd_redis_resp = 1;
//             }
//         }

//         if (strcmp(data_type->valuestring, "OD_LS_DATA") == 0)
//         {
//             cJSON *startdate = cJSON_GetObjectItemCaseSensitive(data, "start_date");

//             if (cJSON_IsString(startdate) && startdate->valuestring != NULL)
//             {
//                 int day, month, year;
//                 if (sscanf(startdate->valuestring, "%d-%d-%d", &day, &month, &year) == 3)
//                 {
//                     snprintf(start_date, sizeof(start_date), "%04d-%02d-%02d", year, month, day);
//                 }
//             }
//         }

//         cJSON *meter_ser_no = cJSON_GetObjectItemCaseSensitive(data, "meter");

//         if (cJSON_IsString(meter_ser_no) && meter_ser_no->valuestring != NULL)
//         {

//             snprintf(meter_ser, sizeof(meter_ser), "%s", meter_ser_no->valuestring);
//         }

//         cJSON_Delete(root);
//     }

//     LOG_INFO("read_redis_resp meter_ser %s start_date %s ls_cmd_redis_resp %d", meter_ser, start_date, ls_cmd_redis_resp);

//     if (ls_cmd_redis_resp == 1 && midnight_cmd_redis_resp == 1 && event_cmd_redis_resp == 1 && billing_cmd_redis_resp == 1)
//     {
//         // cdf_result_t res = generate_profile_cdf(ctx, meter_ser, start_date, "all");
//         cdf_result_t res = generate_profile_json(ctx, meter_ser, start_date, "all");
//         if (res.status == 0)
//         {
//             LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
//             mqtt_send_file(current_active, res.filename, CMD_RESP_TOPIC);

//             // rithika 18Apr2026
//             // char file_rem_cmd[128];
//             // sprintf(file_rem_cmd, "rm %s", res.filename);
//             // system(file_rem_cmd);
//             remove(res.filename);
//             LOG_INFO("%s is deleted successfully", res.filename);

//             msg_size = success_resp_msg(cpy_cmd, output_msg);
//             mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
//         }
//         check_redis_resp = 0;
//         ls_cmd_redis_resp = 0;
//         midnight_cmd_redis_resp = 0;
//         event_cmd_redis_resp = 0;
//         billing_cmd_redis_resp = 0;
//     }
//     return 0;
// }

// int processServerMsg(mqtt_conn_t *conn, const char *msg)
// {
//     int i;
//     int meter_avalb = 0;
//     cmd_request_t cmd;

//     char output_msg[PAYLOAD_BUFFER_SIZE] = {0};
//     int msg_size = 0;

//     send_hc_msg();

//     if (parse_cmd_request(msg, &cmd) != 0)
//     {
//         fprintf(stderr, "Failed to parse command request\n");
//         return 1;
//     }

//     LOG_INFO("TYPE        : %s", cmd.type);
//     LOG_INFO("TRANSACTION : %s", cmd.transaction);
//     LOG_INFO("ARG COUNT   : %u", cmd.arg_count);

//     for (i = 0; i < cmd.arg_count; i++)
//         LOG_INFO("ARG_%02u      : %s", i + 1, cmd.args[i]);

//     if (cmd.arg_count > 1)
//     {
//         for (i = 0; i < meter_count; i++)
//         {
//             if (strcmp(meter_serials[i], cmd.args[1]) == 0)
//             {
//                 meter_avalb = 1;
//                 break;
//             }
//         }

//         if (meter_avalb == 0)
//         {
//             LOG_INFO("Meter details are invalid");
//             msg_size = invalid_metsn_resp_msg(cmd, output_msg);
//             mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
//             return -1;
//         }
//     }

//     if (strcmp(cmd.type, "GetDay") && strcmp(cmd.type, "FetchDay") && strcmp(cmd.type, "Reset"))
//     {
//         LOG_INFO("Unknown cmd_type");
//         msg_size = unknown_req_resp_msg(cmd, output_msg);
//         mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
//         return -1;
//     }

//     printf("\033[0;32m ouput msg : %s\n size %d \033[0m\n", output_msg, msg_size);

//     if (strcmp(cmd.type, "GetDay") == 0 && cmd.args[1][0] != '\0')
//     {
//         time_t now = time(NULL);
//         struct tm *t = localtime(&now);
//         char today_date[32];
//         strftime(today_date, sizeof(today_date), "%Y-%m-%d", t);

//         cdf_result_t res = generate_profile_cdf(ctx, cmd.args[1], today_date, "all");
//         if (res.status == 0)
//         {
//             LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
//             mqtt_send_file(current_active, res.filename, CMD_RESP_TOPIC);

//             // rithika 18Apr2026
//             char file_rem_cmd[128];
//             sprintf(file_rem_cmd, "rm %s", res.filename);
//             system(file_rem_cmd);

//             LOG_INFO("%s is deleted successfully", res.filename);

//             msg_size = success_resp_msg(cmd, output_msg);
//             mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
//         }
//     }

//     else if (strcmp(cmd.type, "FetchDay") == 0 && cmd.args[1][0] != '\0')
//     {
//         check_redis_resp = 1;
//         generate_redis_list(cmd);
//     }
// }

int processServerMsg(mqtt_conn_t *conn, const char *msg)
{
    int i;
    int meter_avalb = 0;
    cmd_request_t cmd;

    char output_msg[PAYLOAD_BUFFER_SIZE] = {0};
    int msg_size = 0;

    send_hc_msg();

    if (parse_cmd_request(msg, &cmd) != 0)
    {
        fprintf(stderr, "Failed to parse command request\n");
        return 1;
    }

    LOG_INFO("TYPE        : %s", cmd.type);
    LOG_INFO("TRANSACTION : %s", cmd.transaction);
    LOG_INFO("ARG COUNT   : %u", cmd.arg_count);

    for (i = 0; i < cmd.arg_count; i++)
        LOG_INFO("ARG_%02u      : %s", i + 1, cmd.args[i]);

    // if (cmd.arg_count > 1)
    if ((!strcmp(cmd.type, "GetDay") || !strcmp(cmd.type, "FetchDay")) && cmd.arg_count > 1)
    {

        printf("------------------------------Received Meter Serial : %s\n", cmd.args[0]);
        printf("------------------------------Meter Count : %d\n", meter_count);
        printf("------------------------------Available Meter Serials:\n");
        for (i = 0; i < meter_count; i++)
        {
            printf("----------------------meter_serials[%d] = %s\n", i, meter_serials[i]);
            if (strcmp(meter_serials[i], cmd.args[0]) == 0)
            {
                meter_avalb = 1;
                break;
            }
        }

        if (meter_avalb == 0)
        {
            LOG_INFO("Meter details are invalid");
            msg_size = invalid_metsn_resp_msg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
            return -1; // Gokul commenting this for testing purpose ..--> 15/07/2026
        }
    }

    if (strcmp(cmd.type, "GetDay") && strcmp(cmd.type, "FetchDay") && strcmp(cmd.type, "Reset") && strcmp(cmd.type, "ReadModbus") && strcmp(cmd.type, "get_cfg") && strcmp(cmd.type, "set_cfg"))
    {
        LOG_INFO("Unknown cmd_type");
        msg_size = unknown_req_resp_msg(cmd, output_msg);
        mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        if (cmd.root)
        {
            cJSON_Delete(cmd.root);
            cmd.root = NULL;
            cmd.data = NULL;
        }
        return -1;
    }

    // printf("\033[0;32m ouput msg : %s\n size %d \033[0m\n", output_msg, msg_size);

    if (strcmp(cmd.type, "GetDay") == 0 && cmd.args[0][0] != '\0')
    {
        time_t now = time(NULL);
        struct tm *t = localtime(&now);
        char today_date[32];
        strftime(today_date, sizeof(today_date), "%Y-%m-%d", t);

        // cdf_result_t res = generate_profile_cdf(ctx, cmd.args[0], today_date, "all");
        cdf_result_t res = generate_profile_json(ctx, cmd.args[0], today_date, "all");
        if (res.status == 0)
        {
            LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
            mqtt_send_file(current_active, res.filename, CMD_RESP_TOPIC);

            // rithika 18Apr2026
            char file_rem_cmd[128];
            sprintf(file_rem_cmd, "rm %s", res.filename);
            system(file_rem_cmd);

            LOG_INFO("%s is deleted successfully", res.filename);

            msg_size = success_resp_msg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        }
    }

    else if (strcmp(cmd.type, "FetchDay") == 0 && cmd.args[0][0] != '\0')
    {
        check_redis_resp = 1;
        generate_redis_list(cmd);
    }

    else if (strcmp(cmd.type, "ReadModbus") == 0)
    {
        /* Validate DCU */
        char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
        printf("Welcome to Gokul Magical Showroom!!!\n");
        if (strcmp(cmd.args[0], dcu_sn) != 0)
        {
            unknown_req_resp_msg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
            return;
        }
        /* Send ACK immediately */
        msg_size = success_resp_msg(cmd, output_msg);
        printf("msg_size = %d\n", msg_size);
        printf("strlen(output_msg) = %zu\n", strlen(output_msg));
        printf("output_msg = [%s]\n", output_msg);
        mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        char *json = modbus_cmd_export_json(ctx, &cmd);
        if (json)
        {
            printf("Modbus JSON:\n%s\n", json);
            mqtt_send_msg(conn, json, strlen(json), CMD_RESP_TOPIC);
            free(json);
        }
        if (cmd.root)
        {
            cJSON_Delete(cmd.root);
            cmd.root = NULL;
            cmd.data = NULL;
        }
        return 0;
    }
    else if (strcmp(cmd.type, "get_cfg") == 0)
    {
        /* Validate DCU */
        char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
        if (strcmp(cmd.args[0], dcu_sn) != 0)
        {
            msg_size = unknown_req_resp_msg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
            return;
        }
        /* Send ACK */
        // msg_size = success_resp_msg(cmd, output_msg);
        msg_size = ack_msg_reply(2004, output_msg);
        printf("After ack msg building..\n");
        mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        printf("After sending ack message...\n");
        /* Export requested configuration */
        char *json = get_cfg_export_json(ctx, &cmd);
        printf("After exporting cfg messages...\n");
        if (json)
        {
            mqtt_send_msg(conn, json, strlen(json), CMD_RESP_TOPIC);
            free(json);
        }
        if (cmd.root)
        {
            cJSON_Delete(cmd.root);
            cmd.root = NULL;
            cmd.data = NULL;
        }
        return 0;
    }
    else if (strcmp(cmd.type, "set_cfg") == 0)
    {
        char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
        if (strcmp(cmd.args[0], dcu_sn) != 0)
        {
            msg_size = unknown_req_resp_msg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
            return 0;
        }

        msg_size = ack_msg_reply(2102, output_msg);
        mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        if (set_cfg_export_json(ctx, &cmd) == 0)
        {
            LOG_INFO("Configuration updated successfully");
            msg_size = success_resp_msg_set_cfg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        }
        else
        {
            LOG_ERROR("Configuration update failed");
            msg_size = failure_resp_msg_set_cfg(cmd, output_msg);
            mqtt_send_msg(conn, output_msg, msg_size, CMD_RESP_TOPIC);
        }
        if (cmd.root)
        {
            cJSON_Delete(cmd.root);
            cmd.root = NULL;
            cmd.data = NULL;
        }
        return 0;
    }
}

// int on_message_arrived(void *context,
//                        char *topicName,
//                        int topicLen,
//                        MQTTAsync_message *message)
// {
//     mqtt_conn_t *conn = (mqtt_conn_t *)context;
//     LOG_INFO("MQTT Messaeg is received");
//     LOG_INFO("[MQTT RX] Topic: %s", topicName);
//     LOG_INFO("[MQTT RX] Size: %d", message->payloadlen);

//     /* Example: copy payload */
//     char buf[2048];
//     int len = message->payloadlen;

//     if (len >= sizeof(buf))
//         len = sizeof(buf) - 1;

//     memcpy(buf, message->payload, len);
//     buf[len] = '\0';

//     LOG_INFO("[MQTT RX] Payload: %s", buf);

//     processServerMsg(conn, buf);

//     MQTTAsync_freeMessage(&message);
//     MQTTAsync_free(topicName);

//     return 1;
// }

int on_message_arrived(void *context, char *topicName, int topicLen, MQTTAsync_message *message)
{
    pthread_mutex_lock(&cmd_mutex);

    memset(mqtt_cmd_buffer, 0, sizeof(mqtt_cmd_buffer));

    memcpy(mqtt_cmd_buffer,
           message->payload,
           message->payloadlen);

    mqtt_cmd_recv = 1;

    pthread_mutex_unlock(&cmd_mutex);

    MQTTAsync_freeMessage(&message);
    MQTTAsync_free(topicName);

    return 1;
}

// int update_mqtt_status(char *status)
// {
//     printf("MQTT_Status before updating to redis = %s\n", status);
//     printf("current active primary  %d current activesecondary %d \n", current_active_primary, current_active_secondary);
//     // if (strcmp(status, "connected") == 0)
//     // {
//     //     if (current_active_primary == 0)
//     //     {
//     //         redisReply *rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", status);
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //         rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", "disconnected");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     //     else if (current_active_primary == 1)
//     //     {
//     //         redisReply *rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", status);
//     //         if (rly)
//     //             freeReplyObject(rly);

//     //         rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", "disconnected");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     //     else if (current_active_secondary == 0)
//     //     {
//     //         redisReply *rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", status);
//     //         if (rly)
//     //             freeReplyObject(rly);

//     //         rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", "disconnected");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     //     else if (current_active_secondary == 1)
//     //     {
//     //         redisReply *rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", status);
//     //         if (rly)
//     //             freeReplyObject(rly);

//     //         rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", "disconnected");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     // }
//     // else
//     // {
//     //     if (!primary.connected && !secondary.connected)
//     //     {
//     //         redisReply *rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", status);

//     //         rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", status);
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     //     else if (!primary.connected)
//     //     {
//     //         if (certificate_path_check_primary == 0)
//     //         {
//     //             redisReply *rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", status);
//     //             if (rly)
//     //                 freeReplyObject(rly);
//     //         }
//     //         else
//     //         {
//     //             redisReply *rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", status);
//     //             if (rly)
//     //                 freeReplyObject(rly);
//     //         }
//     //     }
//     //     else if (!secondary.connected)
//     //     {
//     //         if (certificate_path_check_secondary == 0)
//     //         {
//     //             redisReply *rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "connection_status", status);
//     //             if (rly)
//     //                 freeReplyObject(rly);
//     //         }
//     //         else
//     //         {
//     //             redisReply *rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "connection_status", status);
//     //             if (rly)
//     //                 freeReplyObject(rly);
//     //         }
//     //     }
//     // }
// }

int update_mqtt_status(char *status)
{
    strncpy(mqtt_status_value, status, sizeof(mqtt_status_value) - 1);
    mqtt_status_value[sizeof(mqtt_status_value) - 1] = '\0';

    mqtt_status_update_req = 1;

    return 0;
}

// int update_mqtt_time(int update_last_msg_time)
// {

//     time_t now = time(NULL);
//     redisReply *rly;

//     char pub_time[32];
//     strftime(pub_time, sizeof(pub_time), "%Y:%m:%d %H:%M:%S", localtime(&now));

//     if (current_active_primary == 0)
//     {
//         char uptime_str[64] = {0};
//         int uptime = now - primary_mqtt_conn_time;
//         int days = uptime / (24 * 3600);
//         uptime = uptime % (24 * 3600);

//         int hours = uptime / 3600;
//         uptime = uptime % 3600;

//         int minutes = uptime / 60;
//         int seconds = uptime % 60;

//         snprintf(uptime_str, sizeof(uptime_str),
//                  "%dd %0dh %0dm %0ds",
//                  days, hours, minutes, seconds);

//         if (update_last_msg_time == 1)
//         {
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//             if (rly)
//                 freeReplyObject(rly);
//             secn_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//         else
//         {
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", uptime_str);

//             if (rly)
//                 freeReplyObject(rly);
//             secn_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//     }
//     else if (current_active_primary == 1)
//     {

//         char uptime_str[64] = {0};
//         int uptime = now - primary_mqtt_conn_time;
//         int days = uptime / (24 * 3600);
//         uptime = uptime % (24 * 3600);

//         int hours = uptime / 3600;
//         uptime = uptime % 3600;

//         int minutes = uptime / 60;
//         int seconds = uptime % 60;

//         snprintf(uptime_str, sizeof(uptime_str),
//                  "%dd %0dh %0dm %0ds",
//                  days, hours, minutes, seconds);

//         if (update_last_msg_time == 1)
//         {

//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//             if (rly)
//                 freeReplyObject(rly);
//             secn_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//         else
//         {
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s ", "uptime", uptime_str);

//             if (rly)
//                 freeReplyObject(rly);
//             secn_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//     }

//     else if (current_active_secondary == 0)
//     {
//         int uptime = now - secn_mqtt_conn_time;

//         int days = uptime / (24 * 3600);
//         uptime = uptime % (24 * 3600);

//         int hours = uptime / 3600;
//         uptime = uptime % 3600;

//         int minutes = uptime / 60;
//         int seconds = uptime % 60;

//         char uptime_str[64];
//         snprintf(uptime_str, sizeof(uptime_str),
//                  "%dd %0dh %0dm %0ds",
//                  days, hours, minutes, seconds);

//         if (update_last_msg_time == 1)
//         {

//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//             if (rly)
//                 freeReplyObject(rly);
//             primary_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//         else
//         {
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", uptime_str);

//             if (rly)
//                 freeReplyObject(rly);
//             primary_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//     }

//     else if (current_active_secondary == 1)
//     {
//         int uptime = now - secn_mqtt_conn_time;

//         int days = uptime / (24 * 3600);
//         uptime = uptime % (24 * 3600);

//         int hours = uptime / 3600;
//         uptime = uptime % 3600;

//         int minutes = uptime / 60;
//         int seconds = uptime % 60;

//         char uptime_str[64];
//         snprintf(uptime_str, sizeof(uptime_str),
//                  "%dd %0dh %0dm %0ds",
//                  days, hours, minutes, seconds);

//         if (update_last_msg_time == 1)
//         {
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//             if (rly)
//                 freeReplyObject(rly);
//             primary_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//         else
//         {
//             rly = redisCommand(ctx, "HSET mqtt_1_status %s %s ", "uptime", uptime_str);

//             if (rly)
//                 freeReplyObject(rly);
//             primary_mqtt_conn_time = 0;
//             rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//             if (rly)
//                 freeReplyObject(rly);
//         }
//     }

//     if (!primary.connected && !secondary.connected)
//     {
//         rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//         if (rly)
//             freeReplyObject(rly);

//         rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//         if (rly)
//             freeReplyObject(rly);
//     }

//     ///////////////////
//     // if (certificate_path_check_primary == 0)
//     // {
//     //     char uptime_str[64] = {0};
//     //     int uptime = now - primary_mqtt_conn_time;
//     //     int days = uptime / (24 * 3600);
//     //     uptime = uptime % (24 * 3600);

//     //     int hours = uptime / 3600;
//     //     uptime = uptime % 3600;

//     //     int minutes = uptime / 60;
//     //     int seconds = uptime % 60;

//     //     snprintf(uptime_str, sizeof(uptime_str),
//     //              "%dd %0dh %0dm %0ds",
//     //              days, hours, minutes, seconds);

//     //     rly = redisCommand(ctx, "HSET mqtt_0_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//     //     if (rly)
//     //         freeReplyObject(rly);
//     //     secn_mqtt_conn_time = 0;
//     //     // rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//     //     // if (rly)
//     //     //     freeReplyObject(rly);
//     // }
//     // else if (certificate_path_check_primary == 1)
//     // {
//     //     char uptime_str[64] = {0};
//     //     int uptime = now - primary_mqtt_conn_time;
//     //     int days = uptime / (24 * 3600);
//     //     uptime = uptime % (24 * 3600);

//     //     int hours = uptime / 3600;
//     //     uptime = uptime % 3600;

//     //     int minutes = uptime / 60;
//     //     int seconds = uptime % 60;

//     //     snprintf(uptime_str, sizeof(uptime_str),
//     //              "%dd %0dh %0dm %0ds",
//     //              days, hours, minutes, seconds);

//     //     rly = redisCommand(ctx, "HSET mqtt_1_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//     //     if (rly)
//     //         freeReplyObject(rly);
//     //     secn_mqtt_conn_time = 0;
//     //     // rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//     //     // if (rly)
//     //     //     freeReplyObject(rly);
//     // }

//     // if (certificate_path_check_secondary == 0)
//     // {
//     //     int uptime = now - secn_mqtt_conn_time;

//     //     int days = uptime / (24 * 3600);
//     //     uptime = uptime % (24 * 3600);

//     //     int hours = uptime / 3600;
//     //     uptime = uptime % 3600;

//     //     int minutes = uptime / 60;
//     //     int seconds = uptime % 60;

//     //     char uptime_str[64];
//     //     snprintf(uptime_str, sizeof(uptime_str),
//     //              "%dd %0dh %0dm %0ds",
//     //              days, hours, minutes, seconds);

//     //     rly = redisCommand(ctx, "HSET mqtt_0_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//     //     if (rly)
//     //         freeReplyObject(rly);
//     //     primary_mqtt_conn_time = 0;
//     //     // rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//     //     // if (rly)
//     //     //     freeReplyObject(rly);
//     // }
//     // else if (certificate_path_check_secondary == 1)
//     // {
//     //     int uptime = now - secn_mqtt_conn_time;

//     //     int days = uptime / (24 * 3600);
//     //     uptime = uptime % (24 * 3600);

//     //     int hours = uptime / 3600;
//     //     uptime = uptime % 3600;

//     //     int minutes = uptime / 60;
//     //     int seconds = uptime % 60;

//     //     char uptime_str[64];
//     //     snprintf(uptime_str, sizeof(uptime_str),
//     //              "%dd %0dh %0dm %0ds",
//     //              days, hours, minutes, seconds);

//     //     rly = redisCommand(ctx, "HSET mqtt_1_status %s %s %s %s", "uptime", uptime_str, "last_message_time", pub_time);

//     //     if (rly)
//     //         freeReplyObject(rly);
//     //     primary_mqtt_conn_time = 0;
//     //     // rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//     //     // if (rly)
//     //     //     freeReplyObject(rly);
//     // }

//     // if (!primary.connected && !secondary.connected)
//     // {
//     //     rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//     //     if (rly)
//     //         freeReplyObject(rly);

//     //     rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//     //     if (rly)
//     //         freeReplyObject(rly);
//     // }
//     // else if (!primary.connected)
//     // {
//     //     if(certificate_path_check_primary == 0)
//     //     {
//     //         rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     //     else
//     //     {
//     //         rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     // }
//     // else if (!secondary.connected)
//     // {
//     //     if (certificate_path_check_secondary == 0)
//     //     {
//     //         rly = redisCommand(ctx, "HSET mqtt_0_status %s %s", "uptime", "0s");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     //     else
//     //     {
//     //         rly = redisCommand(ctx, "HSET mqtt_1_status %s %s", "uptime", "0s");
//     //         if (rly)
//     //             freeReplyObject(rly);
//     //     }
//     // }
// }

void format_uptime(int seconds, char *out, size_t size)
{
    int d = seconds / 86400;
    int h = (seconds % 86400) / 3600;
    int m = (seconds % 3600) / 60;
    int s = seconds % 60;

    snprintf(out, size, "%dd %dh %dm %ds", d, h, m, s);
}

// int update_mqtt_time(int update_last_msg_time)
// {
//     // if (!current_active)
//     //     return -1;

//     time_t now = time(NULL);
//     redisReply *rly;

//     char pub_time[32];
//     strftime(pub_time, sizeof(pub_time), "%Y:%m:%d %H:%M:%S", localtime(&now));

//     char uptime_str[64];
//     int uptime = 0;

//     if (current_active == &primary)
//     {
//         uptime = now - primary_mqtt_conn_time;

//         // snprintf(uptime_str, sizeof(uptime_str), "%d", uptime);
//         format_uptime(uptime, uptime_str, sizeof(uptime_str));

//         if (update_last_msg_time)
//         {
//             rly = redisCommand(ctx,
//                                "HSET mqtt_0_status uptime %s last_message_time %s",
//                                uptime_str, pub_time);
//             if (rly)
//                 freeReplyObject(rly);
//         }
//         else
//         {
//             rly = redisCommand(ctx,
//                                "HSET mqtt_0_status uptime %s",
//                                uptime_str);
//             if (rly)
//                 freeReplyObject(rly);
//         }

//         // reset other
//         rly = redisCommand(ctx, "HSET mqtt_1_status uptime 0");
//         if (rly)
//             freeReplyObject(rly);
//     }
//     else if (current_active == &secondary)
//     {
//         uptime = now - secn_mqtt_conn_time;

//         // snprintf(uptime_str, sizeof(uptime_str), "%d", uptime);
//         format_uptime(uptime, uptime_str, sizeof(uptime_str));

//         if (update_last_msg_time)
//         {
//             rly = redisCommand(ctx,
//                                "HSET mqtt_1_status uptime %s last_message_time %s",
//                                uptime_str, pub_time);
//             if (rly)
//                 freeReplyObject(rly);
//         }
//         else
//         {
//             rly = redisCommand(ctx,
//                                "HSET mqtt_1_status uptime %s",
//                                uptime_str);
//             if (rly)
//                 freeReplyObject(rly);
//         }

//         // reset other
//         rly = redisCommand(ctx, "HSET mqtt_0_status uptime 0");
//         if (rly)
//             freeReplyObject(rly);
//     }
//     else if (current_active == NULL)
//     {
//         redisReply *rly;

//         rly = redisCommand(ctx, "HSET mqtt_0_status uptime 0s");
//         if (rly)
//             freeReplyObject(rly);

//         rly = redisCommand(ctx, "HSET mqtt_1_status uptime 0s");
//         if (rly)
//             freeReplyObject(rly);

//         return 0;
//     }
//     return 0;
// }

int update_mqtt_time(int update_last_msg_time)
{
    // time_t now = time(NULL);
    time_t now = monotonic_sec();
    time_t pub_now = time(NULL);

    char pub_time[32];
    strftime(pub_time, sizeof(pub_time), "%Y-%m-%d %H:%M:%S", localtime(&pub_now));

    int uptime = 0;

    if (current_active == &primary)
    {
        uptime = now - primary_mqtt_conn_time;

        format_uptime(uptime, mqtt_time_uptime, sizeof(mqtt_time_uptime));

        mqtt_time_active = 0;
    }
    else if (current_active == &secondary)
    {
        uptime = now - secn_mqtt_conn_time;

        format_uptime(uptime, mqtt_time_uptime, sizeof(mqtt_time_uptime));

        mqtt_time_active = 1;
    }
    else
    {
        strcpy(mqtt_time_uptime, "0s");
        mqtt_time_active = -1;
    }

    if (update_last_msg_time)
        strncpy(mqtt_time_lastmsg, pub_time, sizeof(mqtt_time_lastmsg));

    mqtt_time_update_last = update_last_msg_time;

    mqtt_time_update_req = 1;
    return 0;
}