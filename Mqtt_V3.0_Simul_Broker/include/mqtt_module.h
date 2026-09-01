#ifndef MQTT_MODULE_H
#define MQTT_MODULE_H

#include <stdbool.h>
#include <MQTTAsync.h>
#include <string.h>

/* ================= Configuration Limits ================= */

#define MAX_STR_LEN        256
#define MAX_TOPIC_LEN      256
#define MAX_SUB_TOPICS     5
#define MAX_SSL_FILES      4

/* ================= Timing Parameters ================= */

#define HEALTH_CHECK_INTERVAL   30    /* seconds */
#define MQTT_CONNECT_WAIT       5     /* seconds */

/* ================= MQTT Configuration ================= */

/**
 * mqtt_cfg_t
 * ----------
 * Holds all MQTT-related configuration read from Redis.
 * Stored statically to avoid heap usage.
 */
typedef struct {
    int  enable_mqtt;
    char broker_ip[MAX_STR_LEN];
    int  broker_port;
    char client_id[MAX_STR_LEN];
    char username[MAX_STR_LEN];
    char password[MAX_STR_LEN];
    int  keep_alive;
    int  qos;

    char per_data_topic[MAX_TOPIC_LEN];
    char event_pub_topic[MAX_TOPIC_LEN];
    char diag_pub_topic[MAX_TOPIC_LEN];

    int  per_data_interval;
    int  enable_ssl;

    char subscribe_topics[MAX_SUB_TOPICS][MAX_TOPIC_LEN];
} mqtt_cfg_t;

/**
 * mqtt_ssl_cfg_t
 * --------------
 * TLS configuration read from Redis.
 */
typedef struct {
    int  num_files;
    char ssl_files[MAX_SSL_FILES][MAX_STR_LEN];
} mqtt_ssl_cfg_t;

/**
 * mqtt_conn_t
 * -----------
 * Runtime state of one MQTT connection.
 * Only ONE connection is active at any time.
 */
typedef struct {
    mqtt_cfg_t     cfg;
    mqtt_ssl_cfg_t ssl;
    MQTTAsync      client;
    bool           connected;
    MQTTAsync_SSLOptions ssl_opts;  //Gokul added 20/02/2026
} mqtt_conn_t;

/* ================= Global Objects ================= */

extern mqtt_conn_t primary;
extern mqtt_conn_t secondary;
extern mqtt_conn_t *current_active;
/* ================= Public APIs ================= */

void mqtt_module_start(void);
int  mqtt_connect(mqtt_conn_t *conn);
bool try_primary_health_check(mqtt_conn_t *primary);
void *mqtt_worker_thread(void *arg);
void load_mqtt_cfg(const char *hash, mqtt_cfg_t *cfg);
void configure_tls(mqtt_conn_t *conn);
void load_mqtt_ssl_cfg(const char *section, mqtt_ssl_cfg_t *cfg);
void connectionLost(void *context, char *cause);
void mqtt_send_file(mqtt_conn_t *ctx, const char *filename);

int update_mqtt_status(char *status);
int update_mqtt_time();
#endif
