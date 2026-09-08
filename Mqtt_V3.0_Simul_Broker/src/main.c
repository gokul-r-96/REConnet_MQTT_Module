#include <unistd.h>
#include <pthread.h>
#include "../include/general.h"
#include <pthread.h>
// #include "logger.h"

/* Global static instances */
mqtt_conn_t mqtt1;
mqtt_conn_t mqtt2;
mqtt_conn_t *current_active = NULL;
// mqtt_conn_t *current_active_mqtt1 = NULL;
// mqtt_conn_t *current_active_mqtt2 = NULL;
redisContext *ctx;
#define MQTT_LED_GPIO 87
volatile int mqtt_led_connected = 0;
volatile int mqtt_led_stop = 0;
pthread_t mqtt_led_thread;


#define PRI_BROKER_RECONNECT_PERIOD 60
#define NW_LOGGER_CHECK 30

char meter_serials[MAX_METERS][32];
int meter_count = 0;
int certificate_path_check_mqtt1 = 0;
int certificate_path_check_mqtt2 = 0;
volatile sig_atomic_t stop_flag = 1;
// rithika 02April2026
time_t mqtt1_mqtt_conn_time;
time_t secn_mqtt_conn_time;
int cur_active_mqtt = -1;
char dcu_ser_num[SIZE_32];

char mqtt1_status_hash[32];
char mqtt2_status_hash[32];

extern int check_redis_resp;
/*Gokul added the below variables for mqtt connecting --> 02/05/2026 */
int mqtt1_connecting = 0;
int mqtt2_connecting = 0;

time_t last_mqtt1_try = 0;
time_t last_mqtt2_try = 0;

time_t last_publish_inst = 0;
time_t last_publish_profile = 0;
time_t last_publish_hc = 0;
time_t last_publish_modbus = 0;


// #define mqtt1_RETRY_SEC 15
// #define mqtt2_RETRY_SEC 15

#define mqtt1_RETRY_SEC 60
#define mqtt2_RETRY_SEC 60

time_t last_mqtt1_inst = 0;
time_t last_mqtt1_profile = 0;
time_t last_mqtt1_hc = 0;
time_t last_mqtt1_modbus = 0;

time_t last_mqtt2_inst = 0;
time_t last_mqtt2_profile = 0;
time_t last_mqtt2_hc = 0;
time_t last_mqtt2_modbus = 0;

extern volatile int mqtt_cmd_broker;   /* 0=mqtt1, 1=mqtt2 */


extern volatile int mqtt_status_update_req;
extern char mqtt_status_value[16];

extern volatile int mqtt_time_update_req;
extern char mqtt_time_uptime[64];
extern char mqtt_time_lastmsg[32];
extern int mqtt_time_active;
extern int mqtt_time_update_last;

extern int mqtt1_need_destroy;
extern int mqtt2_need_destroy;

extern time_t mqtt1_destroy_time;
extern time_t mqtt2_destroy_time;

extern pthread_mutex_t mqtt_api_mutex;

extern time_t mqtt1_lost_time;
extern time_t mqtt2_lost_time;

extern time_t mqtt1_connect_start;
extern time_t mqtt2_connect_start;

extern char mqtt_cmd_buffer[4096];
extern volatile int mqtt_cmd_recv;
extern pthread_mutex_t cmd_mutex ;

/**
 * mqtt_module_start()
 * -------------------
 * Initializes and starts MQTT subsystem.
 */

// static inline time_t monotonic_sec(void)
// {
//     struct timespec ts;

//     clock_gettime(CLOCK_MONOTONIC, &ts);

//     return ts.tv_sec;
// }

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

int get_active_broker()
{
    if (current_active == &mqtt1 && mqtt1.connected)
        return 0; // mqtt1

    if (current_active == &mqtt2 && mqtt2.connected)
        return 1; // mqtt2

    return -1; // NONE
}

// void *mqtt_worker_thread(void *arg)
// {
//     char file_rem_cmd[128];
//     time_t last_mqtt1_retry = 0;
    
//     // rithika 16April2026
//     time_t last_nw_logger_check = 0;
//     int inst_data_interval = 0;
//     int profile_data_interval = 0;
//     int modbus_data_interval = 0;
//     int health_check_data_interval = 0;
//     int interval_sec;
//     int elapsed;
//     int remaining;
//     while (stop_flag)
//     {
//         send_hc_msg();
//         // time_t now = time(NULL);
//         time_t now = monotonic_sec();
//         // rithika 16April2026
//         if ((now - last_nw_logger_check) >= NW_LOGGER_CHECK)
//         {
//             last_nw_logger_check = now;
//             iec104_log_sink_poll_network();
//             send_hc_msg();
//         }
//         /* ------------------------------------------------ */
//         /* MQTT FAILOVER + FAILBACK LOGIC                   */
//         /* ------------------------------------------------ */

//         bool mqtt1_up = mqtt1.connected;
//         bool mqtt2_up = mqtt2.connected;

//         /* ---------- mqtt1 ACTIVE ---------- */
//         /* Gokul changed the mqtt1 and mqtt2 connections setup due to some failure cases --> 21/05/2026 */
//         if (mqtt1_up)
//         {
//             mqtt1.connected = true;
//             mqtt2.connected = mqtt2_up;

//             current_active = &mqtt1;

//             cur_active_mqtt =
//                 (certificate_path_check_mqtt1 == 0) ? 1 : 2;

//             /* No need mqtt2 when mqtt1 alive */
//             if (mqtt2_up)
//             {
//                 LOG_INFO("[SWITCHOVER] Disconnect mqtt2");

//                 // MQTTAsync_disconnect(mqtt2.client, NULL);
//                 pthread_mutex_lock(&mqtt_api_mutex);
//                 MQTTAsync_disconnect(mqtt2.client, NULL);
//                 pthread_mutex_unlock(&mqtt_api_mutex);

//                 mqtt2.connected = false;
//             }
//         }

//         /* ---------- mqtt2 ACTIVE ---------- */
//         else if (mqtt2_up)
//         {
//             mqtt2.connected = true;
//             mqtt1.connected = false;

//             current_active = &mqtt2;

//             cur_active_mqtt =
//                 (certificate_path_check_mqtt2 == 0) ? 1 : 2;

//             /* Retry mqtt1 periodically */
//             if ((now - last_mqtt1_retry) >= PRI_BROKER_RECONNECT_PERIOD)
//             {
//                 last_mqtt1_retry = now;

//                 LOG_INFO("[FAILBACK] Checking mqtt1 broker");

//                 /* clear stale connecting state */
//                 if (mqtt1_connecting)
//                 {
//                     if (!mqtt1.connected)
//                     {
//                         LOG_INFO("[FAILBACK] Clearing stale mqtt1 state");

//                         mqtt1_connecting = 0;

//                         if (mqtt1.client)
//                         {
//                             mqtt1_need_destroy = 1;
//                             // mqtt1_destroy_time = time(NULL);
//                             mqtt1_destroy_time = monotonic_sec();
//                         }
//                     }
//                 }

//                 /* retry mqtt1 */
//                 if (!mqtt1_connecting &&
//                     mqtt1.cfg.enable_mqtt)
//                 {
//                     LOG_INFO("[FAILBACK] Trying mqtt1 broker");

//                     mqtt1_connecting = 1;

//                     mqtt_connect(&mqtt1);
//                 }
//             }
//         }

//         // /* ---------- NO BROKER CONNECTED ---------- */
//         // else
//         // {
//         //     mqtt1.connected = false;
//         //     mqtt2.connected = false;

//         //     current_active = NULL;
//         //     cur_active_mqtt = -1;

//         //     /* Try mqtt1 */
//         //     if (mqtt1.cfg.enable_mqtt)
//         //     {
//         //         if ((!mqtt1.connected && !mqtt1_connecting) &&
//         //             (now - last_mqtt1_try >= mqtt1_RETRY_SEC))
//         //         {
//         //             LOG_INFO("[RECONNECT] Trying mqtt1");

//         //             mqtt1_connecting = 1;
//         //             last_mqtt1_try = now;

//         //             mqtt_connect(&mqtt1);
//         //         }
//         //     }

//         //     /* Try mqtt2 */
//         //     if (mqtt2.cfg.enable_mqtt)
//         //     {
//         //         if ((!mqtt2.connected && !mqtt2_connecting) &&
//         //             (now - last_mqtt2_try >= mqtt2_RETRY_SEC))
//         //         {
//         //             /* wait before switching to mqtt2 */
//         //             if ((now - mqtt1_lost_time) >= 10)
//         //             {
//         //                 LOG_INFO("[FAILOVER] Trying mqtt2");

//         //                 mqtt2_connecting = 1;
//         //                 last_mqtt2_try = now;

//         //                 mqtt_connect(&mqtt2);
//         //             }
//         //             else
//         //             {
//         //                 LOG_INFO("[FAILOVER] Waiting before mqtt2 retry...");
//         //             }
//         //         }
//         //     }
//         // }
//         /* ---------- NO BROKER CONNECTED ---------- */
//         else
//         {
//             mqtt1.connected = false;
//             mqtt2.connected = false;

//             current_active = NULL;
//             cur_active_mqtt = -1;

//             /* Try mqtt1 first */
//             if (mqtt1.cfg.enable_mqtt)
//             {
//                 if ((!mqtt1.connected && !mqtt1_connecting) &&
//                     (now - last_mqtt1_try >= mqtt1_RETRY_SEC))
//                 {
//                     LOG_INFO("[RECONNECT] Trying mqtt1");

//                     mqtt1_connecting = 1;
//                     last_mqtt1_try = now;

//                     mqtt_connect(&mqtt1);

//                     /* do not try mqtt2 immediately */
//                     goto mqtt_loop_end;
//                 }
//             }

//             /* Try mqtt2 only if mqtt1 not connecting */
//             if (mqtt2.cfg.enable_mqtt)
//             {
//                 if (!mqtt1_connecting)
//                 {
//                     if ((!mqtt2.connected && !mqtt2_connecting) &&
//                         (now - last_mqtt2_try >= mqtt2_RETRY_SEC))
//                     {
//                         LOG_INFO("[FAILOVER] Trying mqtt2");

//                         mqtt2_connecting = 1;
//                         last_mqtt2_try = now;

//                         mqtt_connect(&mqtt2);
//                     }
//                 }
//             }
//         }
//         if (mqtt1_need_destroy)
//         {
//             // if ((time(NULL) - mqtt1_destroy_time) >= 2)
//             if ((monotonic_sec() - mqtt1_destroy_time) >= 2)
//             {
//                 if (mqtt1.client)
//                 {
//                     LOG_INFO("[mqtt1] Destroying old client");

//                     // MQTTAsync_destroy(&mqtt1.client);
//                     pthread_mutex_lock(&mqtt_api_mutex);
//                     MQTTAsync_destroy(&mqtt1.client);
//                     pthread_mutex_unlock(&mqtt_api_mutex);
//                     mqtt1.client = NULL;
//                 }

//                 mqtt1_need_destroy = 0;
//                 mqtt1_destroy_time = 0;
//             }
//         }
//         if (mqtt2_need_destroy)
//         {
//             // if ((time(NULL) - mqtt2_destroy_time) >= 2)
//             if ((monotonic_sec() - mqtt2_destroy_time) >= 2)
//             {
//                 if (mqtt2.client)
//                 {
//                     LOG_INFO("[mqtt2] Destroying old client");
//                     pthread_mutex_lock(&mqtt_api_mutex);
//                     MQTTAsync_destroy(&mqtt2.client);
//                     pthread_mutex_unlock(&mqtt_api_mutex);
//                     mqtt2.client = NULL;
//                 }

//                 mqtt2_need_destroy = 0;
//                 mqtt2_destroy_time = 0;
//             }
//         }
//         // ///////////////////////////
//         printf("check_redis_resp %d\n\n", check_redis_resp);

//         if (check_redis_resp == 1)
//         {
//             read_redis_resp(current_active);
//         }

//         /* Publish */
//         // interval_sec = current_active->cfg.dlms_inst_pub_interval * 60;
//         // elapsed = now - last_publish_inst;
//         // remaining = interval_sec - elapsed;
//         // if (remaining > 0)
//         // {
//         //     LOG_INFO("Instataneous Data will publish in %d minutes", remaining / 60);
//         // }
//         if (current_active && (now - last_publish_inst >= current_active->cfg.dlms_inst_pub_interval * 60))
//         {
//             last_publish_inst = now;
//             load_active_meters(ctx);

//             for (int i = 0; i < meter_count; i++)
//             {
//                 const char *serial = meter_serials[i];
//                 cdf_result_t res = generate_instantaneous_cdf(ctx, serial);

//                 if (res.status == 0)
//                 {
//                     mqtt_send_file(current_active, res.filename, INST_DATA_TOPIC);
//                     // rithika 18Apr2026
//                     memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
//                     sprintf(file_rem_cmd, "rm %s", res.filename);
//                     system(file_rem_cmd);
//                     LOG_INFO("%s is deleted successfully", res.filename);
//                 }
//             }
//         }

//         if (check_redis_resp == 1)
//         {
//             read_redis_resp(current_active);
//         }

//         // interval_sec = current_active->cfg.dlms_data_pub_interval * 60;
//         // elapsed = now - last_publish_profile;
//         // remaining = interval_sec - elapsed;
//         // if (remaining > 0)
//         // {
//         //     LOG_INFO("Meter Profile Data will publish in %d minutes", remaining / 60);
//         // }
//         if (current_active && (now - last_publish_profile >= current_active->cfg.dlms_data_pub_interval * 60))
//         {
//             last_publish_profile = now;
//             time_t t = time(NULL);
//             struct tm *tm_det = localtime(&t);
//             char today_date[16];
//             strftime(today_date, sizeof(today_date), "%Y-%m-%d", tm_det);
//             for (int i = 0; i < meter_count; i++)
//             {
//                 const char *serial = meter_serials[i];
//                 cdf_result_t res = generate_profile_cdf(ctx, serial, today_date, "all");
//                 if (res.status == 0)
//                 {
//                     LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
//                     // LOG_INFO("Meter Profile Generated Successfully");
//                     mqtt_send_file(current_active, res.filename, METER_DATA_TOPIC);
//                     // rithika 18Apr2026
//                     memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
//                     sprintf(file_rem_cmd, "rm %s", res.filename);
//                     system(file_rem_cmd);
//                     LOG_INFO("%s is deleted successfully", res.filename);
//                 }
//             }
//         }

//         if (check_redis_resp == 1)
//         {
//             read_redis_resp(current_active);
//         }

//         // interval_sec = current_active->cfg.hc_pub_interval * 60;
//         // elapsed = now - last_publish_hc;
//         // remaining = interval_sec - elapsed;
//         // if (remaining > 0)
//         // {
//         //     LOG_INFO("Health check messages will publish in %d minutes", remaining / 60);
//         // }
//         // if (current_active && (time(NULL) - last_publish_hc >= current_active->cfg.hc_pub_interval * 60))
//         if (current_active && (monotonic_sec() - last_publish_hc >= current_active->cfg.hc_pub_interval * 60))
//         {
//             load_active_meters(ctx);

//             // last_publish_hc = time(NULL);
//             last_publish_hc = monotonic_sec();
//             char xml_buf[PAYLOAD_BUFFER_SIZE];
//             int file_Size;

//             build_health_status_xml(ctx, xml_buf, sizeof(xml_buf), &file_Size);

//             mqtt_send_msg(current_active, xml_buf, file_Size, HEALTH_DATA_TOPIC);
//         }

//         if (check_redis_resp == 1)
//         {
//             read_redis_resp(current_active);
//         }

//         // Publish modbus messaged ---> 08/04/2026
//         // LOG_INFO("Modbbus messages will Publish in %d minutes",current_active->cfg.modbus_data_pub_interval-(now - last_publish_modbus));
//         // interval_sec = current_active->cfg.modbus_data_pub_interval * 60;
//         // elapsed = now - last_publish_modbus;
//         // remaining = interval_sec - elapsed;
//         // if (remaining > 0)
//         // {
//         //     LOG_INFO("Modbus messages will publish in %d minutes", remaining / 60);
//         // }
//         if (current_active && (now - last_publish_modbus >= current_active->cfg.modbus_data_pub_interval * 60)) // every 60 sec
//         {
//             last_publish_modbus = now;

//             char *json = modbus_export_json(ctx, 2); //  set your serial ports

//             if (json)
//             {
//                 LOG_INFO("Modbus JSON generated");

//                 mqtt_send_msg(current_active, json, strlen(json), MODBUS_DATA_TOPIC);

//                 free(json); // VERY IMPORTANT
//             }
//             else
//             {
//                 LOG_ERROR("Failed to generate Modbus JSON");
//             }
//         }
//         // if (current_active)
//         // {
//         //     interval_sec = current_active->cfg.modbus_data_pub_interval * 60;
//         //     elapsed = now - last_publish_modbus;
//         //     remaining = interval_sec - elapsed;
//         //     if (remaining > 0)
//         //     {
//         //         LOG_INFO("Modbus messages will publish in %d minutes", remaining / 60);
//         //     }
//         //     interval_sec = current_active->cfg.hc_pub_interval * 60;
//         //     elapsed = now - last_publish_hc;
//         //     remaining = interval_sec - elapsed;
//         //     if (remaining > 0)
//         //     {
//         //         LOG_INFO("Health check messages will publish in %d minutes", remaining / 60);
//         //     }
//         //     interval_sec = current_active->cfg.dlms_data_pub_interval * 60;
//         //     elapsed = now - last_publish_profile;
//         //     remaining = interval_sec - elapsed;
//         //     if (remaining > 0)
//         //     {
//         //         LOG_INFO("Meter Profile Data will publish in %d minutes", remaining / 60);
//         //     }
//         //     interval_sec = current_active->cfg.dlms_inst_pub_interval * 60;
//         //     elapsed = now - last_publish_inst;
//         //     remaining = interval_sec - elapsed;
//         //     if (remaining > 0)
//         //     {
//         //         LOG_INFO("Instataneous Data will publish in %d minutes", remaining / 60);
//         //     }
//         // }

//         if (current_active)
//         {
//             interval_sec = current_active->cfg.modbus_data_pub_interval * 60;
//             elapsed = now - last_publish_modbus;
//             remaining = interval_sec - elapsed;
//             if (remaining > 0)
//             {
//                 int minutes = remaining / 60;
//                 int seconds = remaining % 60;
//                 LOG_INFO("Modbus messages will publish in %d min %02d sec",minutes, seconds);
//             }
//             interval_sec = current_active->cfg.hc_pub_interval * 60;
//             elapsed = now - last_publish_hc;
//             remaining = interval_sec - elapsed;
//             if (remaining > 0)
//             {
//                 int minutes = remaining / 60;
//                 int seconds = remaining % 60;
//                 LOG_INFO("Health check messages will publish in %d min %02d sec",minutes, seconds);
//             }
//             interval_sec = current_active->cfg.dlms_data_pub_interval * 60;
//             elapsed = now - last_publish_profile;
//             remaining = interval_sec - elapsed;
//             if (remaining > 0)
//             {
//                 int minutes = remaining / 60;
//                 int seconds = remaining % 60;
//                 LOG_INFO("Meter Profile Data will publish in %d min %02d sec",minutes, seconds);
//             }
//             interval_sec = current_active->cfg.dlms_inst_pub_interval * 60;
//             elapsed = now - last_publish_inst;
//             remaining = interval_sec - elapsed;
//             if (remaining > 0)
//             {
//                 int minutes = remaining / 60;
//                 int seconds = remaining % 60;
//                 LOG_INFO("Instantaneous Data will publish in %d min %02d sec",minutes, seconds);
//             }
//         }
//         /*If no active connections are there it should shows disconnected */
//         if (!mqtt1.connected && !mqtt2.connected)
//         {
//             update_mqtt_status("disconnected");
//         }
//         /*Redis updation added here by gokul --> 02/05/2026 */
//         if (mqtt_status_update_req)
//         {
//             mqtt_status_update_req = 0;

//             int active = get_active_broker();

//             redisReply *rly;

//             printf("MQTT status update: %s | active broker = %d\n",
//                    mqtt_status_value, active);

//             if (strcmp(mqtt_status_value, "connected") == 0)
//             {
//                 if (active == 0) // mqtt1
//                 {
//                     rly = redisCommand(ctx,"HSET %s connection_status connected",mqtt1_status_hash);
//                     if (rly)
//                         freeReplyObject(rly);

//                     rly = redisCommand(ctx,"HSET %s connection_status disconnected",mqtt2_status_hash);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//                 else if (active == 1) // mqtt2
//                 {
//                     rly = redisCommand(ctx,"HSET %s connection_status connected",mqtt2_status_hash);
//                     if (rly)
//                         freeReplyObject(rly);

//                     rly = redisCommand(ctx,"HSET %s connection_status disconnected",mqtt1_status_hash);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//                 else
//                 {
//                     // No broker active
//                     rly = redisCommand(ctx,"HSET %s connection_status disconnected",mqtt1_status_hash);
//                     if (rly)
//                         freeReplyObject(rly);

//                     rly = redisCommand(ctx,"HSET %s connection_status disconnected",mqtt2_status_hash);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//             }
//             else
//             {
//                 // DISCONNECTED / OTHER STATUS

//                 rly = redisCommand(ctx,"HSET %s connection_status %s",mqtt1_status_hash,mqtt_status_value);
//                 if (rly)
//                     freeReplyObject(rly);

//                 rly = redisCommand(ctx,"HSET %s connection_status %s",mqtt2_status_hash,mqtt_status_value);
//                 if (rly)
//                     freeReplyObject(rly);
//             }
//         }
//         // Uptime and last msg sent time has been updating here ---> 05/05/2026
//         if (mqtt_time_update_req)
//         {
//             mqtt_time_update_req = 0;

//             redisReply *rly;

//             if (mqtt_time_active == 0) // mqtt1
//             {
//                 if (mqtt_time_update_last)
//                 {
//                     rly = redisCommand(ctx,"HSET %s uptime %s last_message_time %s",mqtt1_status_hash,mqtt_time_uptime,mqtt_time_lastmsg);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//                 else
//                 {
//                     rly = redisCommand(ctx,"HSET %s uptime %s",mqtt1_status_hash,mqtt_time_uptime);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//                 rly = redisCommand(ctx,"HSET %s uptime 0s",mqtt2_status_hash);
//                 if (rly)
//                     freeReplyObject(rly);
//             }
//             else if (mqtt_time_active == 1) // mqtt2
//             {
//                 if (mqtt_time_update_last)
//                 {
//                     rly = redisCommand(ctx,"HSET %s uptime %s last_message_time %s",mqtt2_status_hash,mqtt_time_uptime,mqtt_time_lastmsg);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//                 else
//                 {
//                     rly = redisCommand(ctx,"HSET %s uptime %s",mqtt2_status_hash,mqtt_time_uptime);
//                     if (rly)
//                         freeReplyObject(rly);
//                 }
//                 rly = redisCommand(ctx,"HSET %s uptime 0s",mqtt1_status_hash);
//                 if (rly)
//                     freeReplyObject(rly);
//             }
//             else // NONE
//             {
//                 rly = redisCommand(ctx,"HSET %s uptime 0s",mqtt1_status_hash);
//                 if (rly)
//                     freeReplyObject(rly);

//                 rly = redisCommand(ctx,"HSET %s uptime 0s",mqtt2_status_hash);
//                 if (rly)
//                     freeReplyObject(rly);
//             }
//         }
//         //Subscribing topic command response function should be called from worker thread --> 29/05/2026 Gokul
//         if(mqtt_cmd_recv)
//         {
//             char local_cmd[4096];
//             pthread_mutex_lock(&cmd_mutex);
//             strcpy(local_cmd,mqtt_cmd_buffer);
//             mqtt_cmd_recv = 0;
//             pthread_mutex_unlock(&cmd_mutex);

//             processServerMsg(current_active, local_cmd);
//         }
//         send_hc_msg();
//         update_mqtt_time(0);
//         mqtt_loop_end:
//         sleep(3);
//     }
// }


static void mqtt_led_set(int value)
{
    FILE *fp = fopen("/sys/class/gpio/gpio87/value", "w");
    if (!fp)
        return;

    fprintf(fp, "%d", value);
    fclose(fp);
}

static int mqtt_led_init(void)
{
    FILE *fp;

    if (access("/sys/class/gpio/gpio87", F_OK) != 0)
    {
        fp = fopen("/sys/class/gpio/export", "w");
        if (!fp)
            return -1;

        fprintf(fp, "%d", MQTT_LED_GPIO);
        fclose(fp);
        usleep(100000);
    }

    fp = fopen("/sys/class/gpio/gpio87/direction", "w");
    if (!fp)
        return -1;

    fprintf(fp, "out");
    fclose(fp);

    mqtt_led_set(0);
    return 0;
}

// static void *mqtt_led_thread_func(void *arg)
// {
//     while (!mqtt_led_stop)
//     {
//         if (mqtt_led_connected)
//         {
//             mqtt_led_set(1);
//             sleep(1);
//         }
//         else
//         {
//             mqtt_led_set(1);
//             usleep(500000);

//             mqtt_led_set(0);
//             usleep(500000);
//         }
//     }

//     mqtt_led_set(0);
//     return NULL;
// }

static void *mqtt_led_thread_func(void *arg)
{
    while (!mqtt_led_stop)
    {
        if (mqtt_led_connected)
            mqtt_led_set(1);
        else
            mqtt_led_set(0);

        usleep(100000);
    }

    mqtt_led_set(0);
    return NULL;
}


// void *mqtt_worker_thread(void *arg)
// {
//     char file_rem_cmd[128];
//     time_t last_mqtt1_retry = 0;
//     time_t last_nw_logger_check = 0;
//     int interval_sec, elapsed, remaining;

//     while (stop_flag)
//     {
//         send_hc_msg();
//         time_t now = monotonic_sec();
//         /* =========================================================
//         * MQTT CONNECTION WATCHDOG
//         * ========================================================= */

//         if (mqtt1_connecting && mqtt1_connect_start > 0 && (now - mqtt1_connect_start >= MQTT_CONNECT_TIMEOUT))
//         {
//             LOG_ERROR("[WATCHDOG] mqtt1 connection timeout");
//             mqtt1_connecting = 0;
//             mqtt1_connect_start = 0;
//             if (mqtt1.client)
//             {
//                 mqtt1_need_destroy = 1;
//                 mqtt1_destroy_time = now;
//             }
//         }

//         if (mqtt2_connecting && mqtt2_connect_start > 0 && (now - mqtt2_connect_start >= MQTT_CONNECT_TIMEOUT))
//         {
//             LOG_ERROR("[WATCHDOG] mqtt2 connection timeout");
//             mqtt2_connecting = 0;
//             mqtt2_connect_start = 0;
//             if (mqtt2.client)
//             {
//                 mqtt2_need_destroy = 1;
//                 mqtt2_destroy_time = now;
//             }
//         }


//         if ((now - last_nw_logger_check) >= NW_LOGGER_CHECK)
//         {
//             last_nw_logger_check = now;
//             iec104_log_sink_poll_network();
//             send_hc_msg();
//         }

//         bool mqtt1_up = mqtt1.connected;
//         bool mqtt2_up = mqtt2.connected;

//         if (mqtt1_up)
//         {
//             mqtt1.connected = true;
//             mqtt2.connected = mqtt2_up;
//             current_active = &mqtt1;
//             cur_active_mqtt = (certificate_path_check_mqtt1 == 0) ? 1 : 2;

//             if (mqtt2_up)
//             {
//                 LOG_INFO("[SWITCHOVER] Disconnect mqtt2");
//                 pthread_mutex_lock(&mqtt_api_mutex);
//                 MQTTAsync_disconnect(mqtt2.client, NULL);
//                 pthread_mutex_unlock(&mqtt_api_mutex);
//                 mqtt2.connected = false;
//             }
//         }
//         else if (mqtt2_up)
//         {
//             mqtt2.connected = true;
//             mqtt1.connected = false;
//             current_active = &mqtt2;
//             cur_active_mqtt = (certificate_path_check_mqtt2 == 0) ? 1 : 2;
//         }
//         /*No Broker Connected - Trying both mqtt1 and mqtt2 broker with in the given amount of intervals*/
//         else
//         {
//             mqtt1.connected = false;
//             mqtt2.connected = false;

//             current_active = NULL;
//             cur_active_mqtt = -1;

//             printf("mqtt1 Connected = %d\n", mqtt1.connected);
//             printf("mqtt1 Connecting = %d\n", mqtt1_connecting);
//             printf("mqtt2 Connected = %d\n", mqtt2.connected);
//             printf("mqtt2 Connecting = %d\n", mqtt2_connecting);

//             /*
//             * mqtt1
//             */
//             if (mqtt1.cfg.enable_mqtt &&!mqtt1_connecting &&!mqtt2_connecting && (now - last_mqtt1_try >= mqtt1_RETRY_SEC))
//             {
//                 LOG_INFO("[RECONNECT] Trying mqtt1");

//                 mqtt1_connecting = 1;
//                 mqtt1_connect_start = now;
//                 last_mqtt1_try = now;
//                 int rc = mqtt_connect(&mqtt1);
//                 if (rc != MQTTASYNC_SUCCESS)
//                 {
//                     LOG_ERROR("[MQTT] mqtt1 connect start failed, rc=%d", rc);

//                     mqtt1_connecting = 0;
//                     mqtt1_connect_start = 0;

//                     mqtt1_need_destroy = 1;
//                     mqtt1_destroy_time = now;
//                 }
//             }

//             /*
//             * mqtt2
//             *
//             * Only try if mqtt1 is NOT connecting.
//             */
//             if (!mqtt1_connecting && !mqtt2_connecting && mqtt2.cfg.enable_mqtt && (now - last_mqtt2_try >= mqtt2_RETRY_SEC))
//             {
//                 LOG_INFO("[FAILOVER] Trying mqtt2");

//                 mqtt2_connecting = 1;
//                 mqtt2_connect_start = now;
//                 last_mqtt2_try = now;

//                 int rc = mqtt_connect(&mqtt2);

//                 if (rc != MQTTASYNC_SUCCESS)
//                 {
//                     LOG_ERROR("[MQTT] mqtt2 connect start failed, rc=%d", rc);

//                     mqtt2_connecting = 0;
//                     mqtt2_connect_start = 0;

//                     mqtt2_need_destroy = 1;
//                     mqtt2_destroy_time = now;
//                 }
//             }
//         }
//         if (mqtt1_need_destroy && (monotonic_sec() - mqtt1_destroy_time) >= 2)
//         {
//             if (mqtt1.client)
//             {
//                 LOG_INFO("[mqtt1] Destroying old client");
//                 pthread_mutex_lock(&mqtt_api_mutex);
//                 MQTTAsync_destroy(&mqtt1.client);
//                 pthread_mutex_unlock(&mqtt_api_mutex);
//                 mqtt1.client = NULL;
//             }
//             mqtt1_need_destroy = 0;
//             mqtt1_destroy_time = 0;
//         }

//         if (mqtt2_need_destroy && (monotonic_sec() - mqtt2_destroy_time) >= 2)
//         {
//             if (mqtt2.client)
//             {
//                 LOG_INFO("[mqtt2] Destroying old client");
//                 pthread_mutex_lock(&mqtt_api_mutex);
//                 MQTTAsync_destroy(&mqtt2.client);
//                 pthread_mutex_unlock(&mqtt_api_mutex);
//                 mqtt2.client = NULL;
//             }
//             mqtt2_need_destroy = 0;
//             mqtt2_destroy_time = 0;
//         }

//         printf("check_redis_resp %d\n\n", check_redis_resp);

//         if (check_redis_resp == 1)
//             read_redis_resp(current_active);

//         if (current_active && (now - last_publish_inst >= current_active->cfg.dlms_inst_pub_interval * 60))
//         {
//             last_publish_inst = now;
//             load_active_meters(ctx);

//             for (int i = 0; i < meter_count; i++)
//             {
//                 const char *serial = meter_serials[i];
//                 cdf_result_t res = generate_instantaneous_cdf(ctx, serial);

//                 if (res.status == 0)
//                 {
//                     mqtt_send_file(current_active, res.filename, INST_DATA_TOPIC);
//                     memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
//                     sprintf(file_rem_cmd, "rm %s", res.filename);
//                     system(file_rem_cmd);
//                     LOG_INFO("%s is deleted successfully", res.filename);
//                 }
//                 send_hc_msg();
//             }
//         }
//         send_hc_msg();
//         if (check_redis_resp == 1)
//             read_redis_resp(current_active);

//         if (current_active && (now - last_publish_profile >= current_active->cfg.dlms_data_pub_interval * 60))
//         {
//             last_publish_profile = now;

//             time_t t = time(NULL);
//             struct tm *tm_det = localtime(&t);
//             char today_date[16];
//             strftime(today_date, sizeof(today_date), "%Y-%m-%d", tm_det);

//             // for (int i = 0; i < meter_count; i++)
//             // {
//             //     const char *serial = meter_serials[i];
//             //     cdf_result_t res = generate_profile_cdf(ctx, serial, today_date, "all");

//             //     if (res.status == 0)
//             //     {
//             //         LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
//             //         mqtt_send_file(current_active, res.filename, METER_DATA_TOPIC);
//             //         memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
//             //         sprintf(file_rem_cmd, "rm %s", res.filename);
//             //         system(file_rem_cmd);
//             //         LOG_INFO("%s is deleted successfully", res.filename);
//             //     }
//             //     send_hc_msg();
//             // }

//             for (int i = 0; i < meter_count; i++)
//             {
//                 struct timespec start, mid_start , end,mid_end,dead_end;
//                 clock_gettime(CLOCK_MONOTONIC, &start);
//                 const char *serial = meter_serials[i];
//                 cdf_result_t res = generate_profile_cdf(ctx, serial, today_date, "all");
//                 clock_gettime(CLOCK_MONOTONIC, &end);
//                 long elapsed_ms =
//                     (end.tv_sec - start.tv_sec) * 1000L +
//                     (end.tv_nsec - start.tv_nsec) / 1000000L;

//                 LOG_INFO("Meter %s - Time taken for file generation alone : %ld ms (%.3f seconds)",
//                         serial, elapsed_ms, elapsed_ms / 1000.0);
//                 if (res.status == 0)
//                 {
//                     LOG_INFO("Meter Profile Generated Successfully: %s", res.filename);
//                     clock_gettime(CLOCK_MONOTONIC, &mid_start);
//                     mqtt_send_file(current_active, res.filename, METER_DATA_TOPIC);
//                     memset(file_rem_cmd, 0, sizeof(file_rem_cmd));
//                     sprintf(file_rem_cmd, "rm %s", res.filename);
//                     system(file_rem_cmd);

//                     LOG_INFO("%s is deleted successfully", res.filename);
//                 }
//                 send_hc_msg();
//                 clock_gettime(CLOCK_MONOTONIC, &dead_end);
//                 long elapsed_ms_pub =
//                     (dead_end.tv_sec - mid_start.tv_sec) * 1000L +
//                     (dead_end.tv_nsec - mid_start.tv_nsec) / 1000000L;

//                 LOG_INFO("Meter %s - Time taken after publishing and deletion: %ld ms (%.3f seconds)",
//                         serial, elapsed_ms_pub, elapsed_ms_pub / 1000.0);
//             }
//         }
//         send_hc_msg();
//         if (check_redis_resp == 1)
//             read_redis_resp(current_active);

//         if (current_active && (now - last_publish_hc >= current_active->cfg.hc_pub_interval * 60))
//         {
//             load_active_meters(ctx);
//             last_publish_hc = monotonic_sec();

//             char xml_buf[PAYLOAD_BUFFER_SIZE];
//             int file_Size;

//             build_health_status_xml(ctx, xml_buf, sizeof(xml_buf), &file_Size);
//             mqtt_send_msg(current_active, xml_buf, file_Size, HEALTH_DATA_TOPIC);
//         }
//         send_hc_msg();
//         if (check_redis_resp == 1)
//             read_redis_resp(current_active);

//         if (current_active &&
//             (now - last_publish_modbus >= current_active->cfg.modbus_data_pub_interval * 60))
//         {
//             last_publish_modbus = now;

//             char *json = modbus_export_json(ctx, 2);

//             if (json)
//             {
//                 LOG_INFO("Modbus JSON generated");
//                 mqtt_send_msg(current_active, json, strlen(json), MODBUS_DATA_TOPIC);
//                 free(json);
//             }
//             else
//             {
//                 LOG_ERROR("Failed to generate Modbus JSON");
//             }
//         }
//         send_hc_msg();

//         /* mqtt1 RETRY ONLY AFTER ALL PUBLISHING COMPLETES */
//         now = monotonic_sec();

//         if (mqtt2.connected &&
//             (now - last_mqtt1_retry >= PRI_BROKER_RECONNECT_PERIOD))
//         {
//             last_mqtt1_retry = now;
//             LOG_INFO("[FAILBACK] Checking mqtt1 broker");

//             if (mqtt1_connecting && !mqtt1.connected)
//             {
//                 LOG_INFO("[FAILBACK] Clearing stale mqtt1 state");
//                 mqtt1_connecting = 0;

//                 if (mqtt1.client)
//                 {
//                     mqtt1_need_destroy = 1;
//                     mqtt1_destroy_time = monotonic_sec();
//                 }
//             }

//             if (!mqtt1_connecting && mqtt1.cfg.enable_mqtt)
//             {
//                 LOG_INFO("[FAILBACK] Trying mqtt1 broker");
//                 mqtt1_connecting = 1;
//                 mqtt_connect(&mqtt1);
//             }
//         }

//         if (current_active)
//         {
//             interval_sec = current_active->cfg.modbus_data_pub_interval * 60;
//             elapsed = now - last_publish_modbus;
//             remaining = interval_sec - elapsed;

//             if (remaining > 0)
//                 LOG_INFO("Modbus messages will publish in %d min %02d sec",
//                          remaining / 60, remaining % 60);

//             interval_sec = current_active->cfg.hc_pub_interval * 60;
//             elapsed = now - last_publish_hc;
//             remaining = interval_sec - elapsed;

//             if (remaining > 0)
//                 LOG_INFO("Health check messages will publish in %d min %02d sec",
//                          remaining / 60, remaining % 60);

//             interval_sec = current_active->cfg.dlms_data_pub_interval * 60;
//             elapsed = now - last_publish_profile;
//             remaining = interval_sec - elapsed;

//             if (remaining > 0)
//                 LOG_INFO("Meter Profile Data will publish in %d min %02d sec",
//                          remaining / 60, remaining % 60);

//             interval_sec = current_active->cfg.dlms_inst_pub_interval * 60;
//             elapsed = now - last_publish_inst;
//             remaining = interval_sec - elapsed;

//             if (remaining > 0)
//                 LOG_INFO("Instantaneous Data will publish in %d min %02d sec",
//                          remaining / 60, remaining % 60);
//         }

//         if (!mqtt1.connected && !mqtt2.connected)
//             update_mqtt_status("disconnected");

//         if (mqtt_status_update_req)
//         {
//             mqtt_status_update_req = 0;
//             int active = get_active_broker();
//             redisReply *rly;

//             printf("MQTT status update: %s | active broker = %d\n",
//                    mqtt_status_value, active);

//             if (strcmp(mqtt_status_value, "connected") == 0)
//             {
//                 if (active == 0)
//                 {
//                     rly = redisCommand(ctx, "HSET %s connection_status connected", mqtt1_status_hash);
//                     if (rly) freeReplyObject(rly);
//                     rly = redisCommand(ctx, "HSET %s connection_status disconnected", mqtt2_status_hash);
//                     if (rly) freeReplyObject(rly);
//                 }
//                 else if (active == 1)
//                 {
//                     rly = redisCommand(ctx, "HSET %s connection_status connected", mqtt2_status_hash);
//                     if (rly) freeReplyObject(rly);
//                     rly = redisCommand(ctx, "HSET %s connection_status disconnected", mqtt1_status_hash);
//                     if (rly) freeReplyObject(rly);
//                 }
//                 else
//                 {
//                     rly = redisCommand(ctx, "HSET %s connection_status disconnected", mqtt1_status_hash);
//                     if (rly) freeReplyObject(rly);
//                     rly = redisCommand(ctx, "HSET %s connection_status disconnected", mqtt2_status_hash);
//                     if (rly) freeReplyObject(rly);
//                 }
//             }
//             else
//             {
//                 rly = redisCommand(ctx, "HSET %s connection_status %s", mqtt1_status_hash, mqtt_status_value);
//                 if (rly) freeReplyObject(rly);
//                 rly = redisCommand(ctx, "HSET %s connection_status %s", mqtt2_status_hash, mqtt_status_value);
//                 if (rly) freeReplyObject(rly);
//             }
//         }

//         if (mqtt_time_update_req)
//         {
//             mqtt_time_update_req = 0;
//             redisReply *rly;

//             if (mqtt_time_active == 0)
//             {
//                 if (mqtt_time_update_last)
//                 {
//                     rly = redisCommand(ctx, "HSET %s uptime %s last_message_time %s",
//                                        mqtt1_status_hash, mqtt_time_uptime, mqtt_time_lastmsg);
//                 }
//                 else
//                 {
//                     rly = redisCommand(ctx, "HSET %s uptime %s",
//                                        mqtt1_status_hash, mqtt_time_uptime);
//                 }

//                 if (rly) freeReplyObject(rly);

//                 rly = redisCommand(ctx, "HSET %s uptime 0s", mqtt2_status_hash);
//                 if (rly) freeReplyObject(rly);
//             }
//             else if (mqtt_time_active == 1)
//             {
//                 if (mqtt_time_update_last)
//                 {
//                     rly = redisCommand(ctx, "HSET %s uptime %s last_message_time %s",
//                                        mqtt2_status_hash, mqtt_time_uptime, mqtt_time_lastmsg);
//                 }
//                 else
//                 {
//                     rly = redisCommand(ctx, "HSET %s uptime %s",
//                                        mqtt2_status_hash, mqtt_time_uptime);
//                 }

//                 if (rly) freeReplyObject(rly);

//                 rly = redisCommand(ctx, "HSET %s uptime 0s", mqtt1_status_hash);
//                 if (rly) freeReplyObject(rly);
//             }
//             else
//             {
//                 rly = redisCommand(ctx, "HSET %s uptime 0s", mqtt1_status_hash);
//                 if (rly) freeReplyObject(rly);
//                 rly = redisCommand(ctx, "HSET %s uptime 0s", mqtt2_status_hash);
//                 if (rly) freeReplyObject(rly);
//             }
//         }

//         if (mqtt_cmd_recv)
//         {
//             char local_cmd[4096];

//             pthread_mutex_lock(&cmd_mutex);
//             strcpy(local_cmd, mqtt_cmd_buffer);
//             mqtt_cmd_recv = 0;
//             pthread_mutex_unlock(&cmd_mutex);

//             processServerMsg(current_active, local_cmd);
//         }

//         send_hc_msg();
//         update_mqtt_time(0);
//         mqtt_loop_end:
//         sleep(3);
//     }

//     return NULL;
// }



void *mqtt_worker_thread(void *arg)
{
    char file_rem_cmd[128];

    time_t timer_start = monotonic_sec();

    last_mqtt1_inst = timer_start;
    last_mqtt1_profile = timer_start;
    last_mqtt1_hc = timer_start;
    last_mqtt1_modbus = timer_start;

    last_mqtt2_inst = timer_start;
    last_mqtt2_profile = timer_start;
    last_mqtt2_hc = timer_start;
    last_mqtt2_modbus = timer_start;


    while (stop_flag)
    {
        send_hc_msg();
        time_t now = monotonic_sec();

        /* MQTT WATCHDOG */
        if (mqtt1_connecting &&
            mqtt1_connect_start > 0 &&
            now - mqtt1_connect_start >= MQTT_CONNECT_TIMEOUT)
        {
            LOG_ERROR("[WATCHDOG] mqtt1 connection timeout");
            mqtt1_connecting = 0;
            mqtt1_connect_start = 0;

            if (mqtt1.client)
            {
                mqtt1_need_destroy = 1;
                mqtt1_destroy_time = now;
            }
        }

        if (mqtt2_connecting &&
            mqtt2_connect_start > 0 &&
            now - mqtt2_connect_start >= MQTT_CONNECT_TIMEOUT)
        {
            LOG_ERROR("[WATCHDOG] mqtt2 connection timeout");
            mqtt2_connecting = 0;
            mqtt2_connect_start = 0;

            if (mqtt2.client)
            {
                mqtt2_need_destroy = 1;
                mqtt2_destroy_time = now;
            }
        }

        /* NETWORK LOGGER */
        static time_t last_nw_logger_check = 0;
        if (now - last_nw_logger_check >= NW_LOGGER_CHECK)
        {
            last_nw_logger_check = now;
            iec104_log_sink_poll_network();
            send_hc_msg();
        }

        /* STATE */
        LOG_INFO("[STATE] mqtt1: enabled=%d connected=%d connecting=%d | "
                 "mqtt2: enabled=%d connected=%d connecting=%d",
                 mqtt1.cfg.enable_mqtt,
                 mqtt1.connected,
                 mqtt1_connecting,
                 mqtt2.cfg.enable_mqtt,
                 mqtt2.connected,
                 mqtt2_connecting);

        /* mqtt1 RETRY */
        if (mqtt1.cfg.enable_mqtt && !mqtt1.connected && !mqtt1_connecting && now - last_mqtt1_try >= mqtt1_RETRY_SEC)
        {
            LOG_INFO("[RECONNECT] Trying mqtt1");

            mqtt1_connecting = 1;
            mqtt1_connect_start = now;
            last_mqtt1_try = now;

            int rc = mqtt_connect(&mqtt1);

            if (rc != MQTTASYNC_SUCCESS)
            {
                LOG_ERROR("[MQTT] mqtt1 connect start failed, rc=%d", rc);
                mqtt1_connecting = 0;
                mqtt1_connect_start = 0;
                mqtt1_need_destroy = 1;
                mqtt1_destroy_time = now;
            }
        }

        /* mqtt2 RETRY */
        if (mqtt2.cfg.enable_mqtt && !mqtt2.connected && !mqtt2_connecting && now - last_mqtt2_try >= mqtt2_RETRY_SEC)
        {
            LOG_INFO("[RECONNECT] Trying mqtt2");

            mqtt2_connecting = 1;
            mqtt2_connect_start = now;
            last_mqtt2_try = now;

            int rc = mqtt_connect(&mqtt2);

            if (rc != MQTTASYNC_SUCCESS)
            {
                LOG_ERROR("[MQTT] mqtt2 connect start failed, rc=%d", rc);
                mqtt2_connecting = 0;
                mqtt2_connect_start = 0;
                mqtt2_need_destroy = 1;
                mqtt2_destroy_time = now;
            }
        }

        /* DESTROY mqtt1 */
        if (mqtt1_need_destroy && now - mqtt1_destroy_time >= 2)
        {
            if (mqtt1.client)
            {
                LOG_INFO("[mqtt1] Destroying old client");

                pthread_mutex_lock(&mqtt_api_mutex);
                MQTTAsync_destroy(&mqtt1.client);
                pthread_mutex_unlock(&mqtt_api_mutex);

                mqtt1.client = NULL;
            }

            mqtt1_need_destroy = 0;
            mqtt1_destroy_time = 0;
        }

        /* DESTROY mqtt2 */
        if (mqtt2_need_destroy && now - mqtt2_destroy_time >= 2)
        {
            if (mqtt2.client)
            {
                LOG_INFO("[mqtt2] Destroying old client");

                pthread_mutex_lock(&mqtt_api_mutex);
                MQTTAsync_destroy(&mqtt2.client);
                pthread_mutex_unlock(&mqtt_api_mutex);

                mqtt2.client = NULL;
            }

            mqtt2_need_destroy = 0;
            mqtt2_destroy_time = 0;
        }

        /* LOAD METERS */
        if (mqtt1.connected || mqtt2.connected)
            load_active_meters(ctx);

        /* =========================================================
         * INSTANTANEOUS
         * ========================================================= */
        bool p_inst = mqtt1.connected && now - last_mqtt1_inst >= mqtt1.cfg.dlms_inst_pub_interval * 60;
        bool s_inst = mqtt2.connected && now - last_mqtt2_inst >= mqtt2.cfg.dlms_inst_pub_interval * 60;

        if (p_inst || s_inst)
        {
            for (int i = 0; i < meter_count; i++)
            {
                cdf_result_t res = generate_instantaneous_cdf(ctx, meter_serials[i]);

                if (res.status == 0)
                {
                    if (p_inst && mqtt1.connected)
                    {
                        LOG_INFO("[PUBLISH] Instantaneous -> mqtt1");
                        mqtt_send_file(&mqtt1, res.filename,INST_DATA_TOPIC);
                    }
                    if (s_inst && mqtt2.connected)
                    {
                        LOG_INFO("[PUBLISH] Instantaneous -> mqtt2");
                        mqtt_send_file(&mqtt2, res.filename,INST_DATA_TOPIC);
                    }
                    snprintf(file_rem_cmd, sizeof(file_rem_cmd),"rm -f %s", res.filename);
                    system(file_rem_cmd);
                }

                send_hc_msg();
            }

            if (p_inst)
                last_mqtt1_inst = now;

            if (s_inst)
                last_mqtt2_inst = now;
        }

        /* =========================================================
         * METER PROFILE
         * ========================================================= */
        bool p_profile = mqtt1.connected &&
            now - last_mqtt1_profile >=
            mqtt1.cfg.dlms_data_pub_interval * 60;

        bool s_profile = mqtt2.connected &&
            now - last_mqtt2_profile >=
            mqtt2.cfg.dlms_data_pub_interval * 60;

        if (p_profile || s_profile)
        {
            time_t t = time(NULL);
            struct tm *tm_det = localtime(&t);
            char today_date[16];

            strftime(today_date, sizeof(today_date),
                     "%Y-%m-%d", tm_det);

            for (int i = 0; i < meter_count; i++)
            {
                cdf_result_t res =
                    generate_profile_cdf(ctx,
                                         meter_serials[i],
                                         today_date,
                                         "all");

                if (res.status == 0)
                {
                    LOG_INFO("[PROFILE] Generated: %s", res.filename);

                    if (p_profile && mqtt1.connected)
                    {
                        LOG_INFO("[PUBLISH] Profile -> mqtt1");
                        mqtt_send_file(&mqtt1, res.filename,
                                       METER_DATA_TOPIC);
                    }

                    if (s_profile && mqtt2.connected)
                    {
                        LOG_INFO("[PUBLISH] Profile -> mqtt2");
                        mqtt_send_file(&mqtt2, res.filename,
                                       METER_DATA_TOPIC);
                    }

                    snprintf(file_rem_cmd, sizeof(file_rem_cmd),
                             "rm -f %s", res.filename);
                    system(file_rem_cmd);
                }

                send_hc_msg();
            }

            if (p_profile)
                last_mqtt1_profile = now;

            if (s_profile)
                last_mqtt2_profile = now;
        }

        /* =========================================================
         * HEALTH CHECK
         * ========================================================= */
        bool p_hc = mqtt1.connected &&
            now - last_mqtt1_hc >=
            mqtt1.cfg.hc_pub_interval * 60;

        bool s_hc = mqtt2.connected &&
            now - last_mqtt2_hc >=
            mqtt2.cfg.hc_pub_interval * 60;

        if (p_hc || s_hc)
        {
            char xml_buf[PAYLOAD_BUFFER_SIZE];
            int file_size = 0;

            build_health_status_xml(ctx,xml_buf,sizeof(xml_buf),&file_size);
            if (p_hc && mqtt1.connected)
            {
                LOG_INFO("[PUBLISH] Health -> mqtt1");
                mqtt_send_msg(&mqtt1,xml_buf,file_size,HEALTH_DATA_TOPIC);
            }

            if (s_hc && mqtt2.connected)
            {
                LOG_INFO("[PUBLISH] Health -> mqtt2");
                mqtt_send_msg(&mqtt2,xml_buf,file_size,HEALTH_DATA_TOPIC);
            }

            if (p_hc)
                last_mqtt1_hc = now;

            if (s_hc)
                last_mqtt2_hc = now;
        }

        /* =========================================================
         * MODBUS
         * ========================================================= */
        bool p_modbus = mqtt1.connected &&
            now - last_mqtt1_modbus >=
            mqtt1.cfg.modbus_data_pub_interval * 60;

        bool s_modbus = mqtt2.connected &&
            now - last_mqtt2_modbus >=
            mqtt2.cfg.modbus_data_pub_interval * 60;

        if (p_modbus || s_modbus)
        {
            char *json = modbus_export_json(ctx, 2);
            if (json)
            {
                if (p_modbus && mqtt1.connected)
                {
                    LOG_INFO("[PUBLISH] Modbus -> mqtt1");
                    mqtt_send_msg(&mqtt1,json,strlen(json),MODBUS_DATA_TOPIC);
                }

                if (s_modbus && mqtt2.connected)
                {
                    LOG_INFO("[PUBLISH] Modbus -> mqtt2");
                    mqtt_send_msg(&mqtt2,json,strlen(json),MODBUS_DATA_TOPIC);
                }

                free(json);
            }
            else
            {
                LOG_ERROR("Failed to generate Modbus JSON");
            }

            if (p_modbus)
                last_mqtt1_modbus = now;

            if (s_modbus)
                last_mqtt2_modbus = now;
        }

        /* =========================================================
         * PUBLISHING STATUS / REMAINING TIME
         * ========================================================= */
        now = monotonic_sec();

        if (mqtt1.connected)
        {
            long modbus_rem  = mqtt1.cfg.modbus_data_pub_interval * 60 - (now - last_mqtt1_modbus);
            long hc_rem      = mqtt1.cfg.hc_pub_interval * 60 - (now - last_mqtt1_hc);
            long profile_rem = mqtt1.cfg.dlms_data_pub_interval * 60 - (now - last_mqtt1_profile);
            long inst_rem    = mqtt1.cfg.dlms_inst_pub_interval * 60 - (now - last_mqtt1_inst);

            LOG_INFO("┌─────────────────────────────────────────────────────────┐");
            LOG_INFO("│ MQTT1                                                   │");
            LOG_INFO("│ Modbus-->%02ld min %02ld sec    HC-->%02ld min %02ld sec │",
                    modbus_rem / 60, modbus_rem % 60,
                    hc_rem / 60, hc_rem % 60);
            LOG_INFO("│ Profile-->%02ld min %02ld sec   Instant-->%02ld min %02ld sec │",
                    profile_rem / 60, profile_rem % 60,
                    inst_rem / 60, inst_rem % 60);
            LOG_INFO("└─────────────────────────────────────────────────────────┘");
        }

        if (mqtt2.connected)
        {
            long modbus_rem  = mqtt2.cfg.modbus_data_pub_interval * 60 - (now - last_mqtt2_modbus);
            long hc_rem      = mqtt2.cfg.hc_pub_interval * 60 - (now - last_mqtt2_hc);
            long profile_rem = mqtt2.cfg.dlms_data_pub_interval * 60 - (now - last_mqtt2_profile);
            long inst_rem    = mqtt2.cfg.dlms_inst_pub_interval * 60 - (now - last_mqtt2_inst);

            LOG_INFO("┌─────────────────────────────────────────────────────────┐");
            LOG_INFO("│ MQTT2                                                   │");
            LOG_INFO("│ Modbus-->%02ld min %02ld sec    HC-->%02ld min %02ld sec │",
                    modbus_rem / 60, modbus_rem % 60,
                    hc_rem / 60, hc_rem % 60);
            LOG_INFO("│ Profile-->%02ld min %02ld sec    Instant-->%02ld min %02ld sec │",
                    profile_rem / 60, profile_rem % 60,
                    inst_rem / 60, inst_rem % 60);
            LOG_INFO("└─────────────────────────────────────────────────────────┘");
        }

        /* =========================================================
         * MQTT STATUS
         * ========================================================= */
        if (mqtt_status_update_req)
        {
            mqtt_status_update_req = 0;

            redisReply *rly;

            rly = redisCommand(ctx,"HSET %s connection_status %s",mqtt1_status_hash,mqtt1.connected ? "connected" : "disconnected");
            if (rly)
                freeReplyObject(rly);

            rly = redisCommand(ctx,"HSET %s connection_status %s",mqtt2_status_hash,mqtt2.connected ? "connected" : "disconnected");
            if (rly)
                freeReplyObject(rly);

            LOG_INFO("[STATUS] mqtt1=%s mqtt2=%s",mqtt1.connected ? "connected" : "disconnected",mqtt2.connected ? "connected" : "disconnected");
        }

        /* =========================================================
         * MQTT TIME STATUS
         * ========================================================= */
        if (mqtt_time_update_req)
        {
            mqtt_time_update_req = 0;

            redisReply *rly;

            if (mqtt_time_active == 0)
            {
                if (mqtt_time_update_last)
                {
                    rly = redisCommand(ctx,"HSET %s uptime %s last_message_time %s",mqtt1_status_hash,mqtt_time_uptime,mqtt_time_lastmsg);
                }
                else
                {
                    rly = redisCommand(ctx,"HSET %s uptime %s",mqtt1_status_hash,mqtt_time_uptime);
                }

                if (rly)
                    freeReplyObject(rly);
            }
            else if (mqtt_time_active == 1)
            {
                if (mqtt_time_update_last)
                {
                    rly = redisCommand(ctx,"HSET %s uptime %s last_message_time %s",mqtt2_status_hash,mqtt_time_uptime,mqtt_time_lastmsg);
                }
                else
                {
                    rly = redisCommand(ctx,"HSET %s uptime %s",mqtt2_status_hash,mqtt_time_uptime);
                }

                if (rly)
                    freeReplyObject(rly);
            }
        }

        /* =========================================================
         * INCOMING MQTT COMMAND
         *
         * mqtt_cmd_broker is set by the MQTT callback.
         * ========================================================= */
        if (mqtt_cmd_recv)
        {
            char local_cmd[4096];
            int broker;

            pthread_mutex_lock(&cmd_mutex);
            strncpy(local_cmd,mqtt_cmd_buffer,sizeof(local_cmd) - 1);
            local_cmd[sizeof(local_cmd) - 1] = '\0';
            broker = mqtt_cmd_broker;
            mqtt_cmd_recv = 0;
            mqtt_cmd_broker = -1;
            pthread_mutex_unlock(&cmd_mutex);

            if (broker == 0)
            {
                LOG_INFO("[COMMAND] Processing command from mqtt1");
                processServerMsg(&mqtt1, local_cmd);
            }
            else if (broker == 1)
            {
                LOG_INFO("[COMMAND] Processing command from mqtt2");
                processServerMsg(&mqtt2, local_cmd);
            }
            else
            {
                LOG_ERROR("[COMMAND] Unknown broker source");
            }
        }

        send_hc_msg();
        update_mqtt_time(0);

        sleep(3);
    }

    return NULL;
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
    printf("mqtt1            : %d\n", cfg->mqtt1);
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
    cfg->mqtt1 = redis_get_int(ctx, hash, "mqtt1");
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
    int is_mqtt1 = redis_get_int(ctx, "mqtt_0_cfg", "mqtt1");

    if (is_mqtt1)
    {
        certificate_path_check_mqtt1 = 0;
        certificate_path_check_mqtt2 = 1;

        strcpy(mqtt1_status_hash, "mqtt_0_status");
        strcpy(mqtt2_status_hash, "mqtt_1_status");

        LOG_INFO("Mqtt-1 is configured as mqtt1!!!");
        load_mqtt_cfg("mqtt_0_cfg", &mqtt1.cfg);

        LOG_INFO("Mqtt-2 is configured as mqtt2!!!");
        load_mqtt_cfg("mqtt_1_cfg", &mqtt2.cfg);
    }
    else
    {
        certificate_path_check_mqtt1 = 1;
        certificate_path_check_mqtt2 = 0;

        strcpy(mqtt1_status_hash, "mqtt_1_status");
        strcpy(mqtt2_status_hash, "mqtt_0_status");

        LOG_INFO("Mqtt-2 is configured as mqtt1!!!");
        load_mqtt_cfg("mqtt_1_cfg", &mqtt1.cfg);

        LOG_INFO("Mqtt-1 is configured as mqtt2!!!");
        load_mqtt_cfg("mqtt_0_cfg", &mqtt2.cfg);
    }
    send_hc_msg();

    print_mqtt_full_cfg(&mqtt1.cfg);
    printf("mqtt1 cfg addr: %p\n", &mqtt1.cfg);
    printf("mqtt2 cfg addr: %p\n", &mqtt2.cfg);
    // load_mqtt_cfg("mqtt_1_cfg", &mqtt2.cfg);
    print_mqtt_full_cfg(&mqtt2.cfg);
    // load_mqtt_ssl_cfg("mqtt_ssl_cfg", &mqtt1.ssl);
    // load_mqtt_ssl_cfg("mqtt2_ssl_cfg", &mqtt2.ssl);

    printf("After Loading SSL config\n");

    /* Always try mqtt1 first */
    /* Gokul commented the below mqtt1 because it should only be handled in worker thread only --> 05/05/2026 */
    // if (mqtt1.cfg.enable_mqtt)
    // {
    //     LOG_INFO("[START] Trying mqtt1 broker");

    //     mqtt_connect(&mqtt1);
    // }

    pthread_t worker;
    pthread_create(&worker, NULL, mqtt_worker_thread, NULL);
}

/* This function will handle the closing broker connections of both mqtt1 and mqtt2 -- 27/04/2026 */
void mqtt_cleanup()
{
    LOG_INFO("Cleaning up MQTT connections...");

    if (mqtt1.client)
    {
        MQTTAsync_disconnectOptions disc_opts =
            MQTTAsync_disconnectOptions_initializer;

        MQTTAsync_disconnect(mqtt1.client, &disc_opts);
        MQTTAsync_destroy(&mqtt1.client);
        mqtt1.client = NULL;
    }

    if (mqtt2.client)
    {
        MQTTAsync_disconnectOptions disc_opts =
            MQTTAsync_disconnectOptions_initializer;

        MQTTAsync_disconnect(mqtt2.client, &disc_opts);
        MQTTAsync_destroy(&mqtt2.client);
        mqtt2.client = NULL;
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
    // Signal Handling for MQTT Process -- Gokul added this 27/04/2026
    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    // signal(SIGQUIT, handle_signal);
    printf("MQTT Process started\n"); // Gokul added
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

    //Gokul added this to delete the previously stored mqtt status --> 22/08/2026
    redisReply *rly = redisCommand(ctx, "DEL mqtt_0_status mqtt_1_status");
    if (rly)
    {
        if (rly->type == REDIS_REPLY_INTEGER)
            LOG_INFO("[INIT] Deleted MQTT status hashes: %lld", rly->integer);
        freeReplyObject(rly);
    }
    else
    {
        LOG_ERROR("[INIT] Failed to delete MQTT status hashes");
    }

    // rithika 16April2026
    char redis_key[64];
    snprintf(redis_key, sizeof(redis_key), "MQTT_PROC");
    dcu_netlog_init(redis_key);
    send_hc_msg();


    if (mqtt_led_init() != 0)
        LOG_ERROR("Failed to initialize MQTT LED GPIO");

    if (pthread_create(&mqtt_led_thread, NULL, mqtt_led_thread_func, NULL) != 0)
        LOG_ERROR("Failed to create MQTT LED thread");

    mqtt_module_start();

    while (stop_flag)
    {
        // rithika 16April2026
        iec104_log_sink_poll_network();
        sleep(1);
    }

    mqtt_led_stop = 1;
    pthread_join(mqtt_led_thread, NULL);

    mqtt_cleanup(); // Gokul added the mqtt cleanup function to shut down the process gracefully..!! 27/04/2026

    redisFree(ctx);

    log_close();
    // rithika 16April2026
    dcu_netlog_close();

    return 0;
}