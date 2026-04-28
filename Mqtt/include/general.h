
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <signal.h>
#include "/home/vishnu/Projects/REConnect/net_logger/dcu_netlog.h"
#include "/home/vishnu/Projects/REConnect/hc_file/hc_heartbeat.h"

/* Third-party headers */
#include <hiredis.h>
#include <cJSON.h>
#include <sqlite3.h>
#define MQTT_MODULE_H

#include <stdbool.h>
#include <MQTTAsync.h>
#include <string.h>
/* ============================================================
 *  Build-time configuration macros
 * ============================================================ */

#define PAYLOAD_BUFFER_SIZE 16384 // 16 KB --> File chunk size need to be adjusted .


/** Redis connection parameters */
#define REDIS_HOST              "127.0.0.1"
#define REDIS_PORT              6379
#define REDIS_TIMEOUT_SEC       2

/** Redis key prefixes / hash names */
// #define REDIS_KEY_INST_PREFIX   "meter_0_1_"          /**< Instantaneous data key prefix  */
#define REDIS_HASH_INST_INFO    "meter_inst_info"      /**< Instantaneous hash name         */
#define REDIS_HASH_OBIS_MAP     "inst_cdf_obis_param_map" /**< OBIS-to-parameter mapping hash */

/** Redis sub-fields inside REDIS_HASH_OBIS_MAP */
#define REDIS_FIELD_PARAM_NAME  "obis_paramname_map"
#define REDIS_FIELD_PARAM_CODE  "obis_paramcode_map"
#define REDIS_FIELD_PARAM_UNIT  "obis_unit_map"

/** LS (Load Survey) specific */
#define REDIS_HASH_LS_OBIS_MAP  "ls_cdf_obis_param_map"    /**< LS OBIS mapping hash */
#define REDIS_HASH_MN_OBIS_MAP  "mn_cdf_obis_param_map" 
#define REDIS_HASH_BILL_OBIS_MAP "bill_cdf_obis_param_map" /**< Billing OBIS mapping hash */
#define REDIS_HASH_EVENT_OBIS_MAP "event_cdf_obis_param_map" /**< Event OBIS mapping hash */
#define REDIS_HASH_METER_STATUS "meter_status"             /**< Meter status hash */
#define SQLITE_DB_PATH          "/usr/cms/data/dcu_dlms.db"  /**< SQLite DB path */

/** OBIS code that carries the meter timestamp */
#define OBIS_TIMESTAMP          "0_0_1_0_0_255"

/** CDF output directory (trailing slash required) */
#define CDF_OUTPUT_DIR          "/usr/cms/data/"

/** Log file configuration */
#define LOG_DIR                 "/usr/cms/log/"
#define LOG_FILE_BASE           "dlms_cdf_gen"
#define LOG_FILE_EXT            ".log"
#define LOG_MAX_SIZE_BYTES      (1024 * 1024)   /**< Rotate at 1 MB  */
#define LOG_MAX_FILES           5               /**< Keep 5 rotated files */

/* ---- DCU / Meter nameplate macros (D1 / GENERAL section) ---- */
#define DCU_HASH                "dcu_info"
#define DCU_ATTR1               "CHG_TEST"
#define DCU_ATTR2               "TEST"
#define DCU_ATTR3               "M1M2_1F"
#define DCU_ATTR4               "TEST"
#define DCU_ATTR5               "33KV_FR"
#define DCU_SERIAL              "1024"
#define METER_BAY               "METER_IC1"
#define METER_IP                "192.168.11.101"
#define UTILITY_CODE            "1"
#define UTILITY_DATA_TYPE       "CYCLIC"

/** OBIS codes for D1 (Nameplate) section */
#define OBIS_METER_SERIAL "00_00_60_01_00_ff"
#define OBIS_MANUFACTURER "00_00_60_01_01_ff"
#define OBIS_FW_VERSION "01_00_00_02_00_ff"
#define OBIS_METER_TYPE "00_00_5e_5b_09_ff"
#define OBIS_CT_RATIO "01_00_00_04_02_ff"
#define OBIS_VT_RATIO "01_00_00_04_03_ff"
#define OBIS_METER_CATEGORY "00_00_5e_5b_0b_ff"
#define OBIS_CURR_RATING "00_00_5e_5b_0c_ff"
#define OBIS_YR_OF_MANUF "00_00_60_01_04_ff"
#define EXTRA_OBIS_1 "01_00_00_03_00_ff"
#define IPV4_ADDRESS "00_00_19_01_00_ff"
#define EXTRA_OBIS_2 "00_00_19_01_00_ff"
#define HDLC_SETUP "00_00_16_00_00_ff"
#define TRANSFRMR_RATIO_VOLTAGE "01_00_00_04_06_ff"
#define EXTRA_OBIS_3 "01_00_00_03_80_ff"

/** Nameplate default values (replace with Redis lookup in future versions) */
#define NP_METER_SERIAL_VAL     "HVPN5016"
#define NP_MANUFACTURER_VAL     "GENUS POWER INFRASTRUCTURES LTD"
#define NP_FW_VERSION_VAL       "HB003.57345"
#define NP_METER_TYPE_VAL       "2"
#define NP_CT_RATIO_VAL         "1"
#define NP_VT_RATIO_VAL         "1000"
#define NUM_CONCATENATE_FILES       4

/** Convenience macros */
#define LOG_INFO(...) log_write("INFO", __VA_ARGS__)
#define LOG_WARN(...) log_write("WARN", __VA_ARGS__)
#define LOG_ERROR(...) log_write("ERROR", __VA_ARGS__)
#define LOG_DEBUG(...) log_write("DEBUG", __VA_ARGS__)


/* ================= Configuration Limits ================= */

#define MAX_STR_LEN             256
#define MIN_STR_LEN             64
#define MAX_TOPIC_LEN           256
#define MAX_SUB_TOPICS          1
#define MAX_SSL_FILES           4
#define SIZE_32                 32
#define SIZE_64                 64
#define SIZE_128                128
#define SIZE_256                256

/*Gokul added the below topic macros to publish the from a single worker thread --> 09/04/2026 */
#define INST_DATA_TOPIC         1
#define METER_DATA_TOPIC        2
#define MODBUS_DATA_TOPIC       3
#define HEALTH_DATA_TOPIC       4
#define CMD_RESP_TOPIC          5
/* ================= Timing Parameters ================= */

#define HEALTH_CHECK_INTERVAL   30    /* seconds */
#define MQTT_CONNECT_WAIT       5     /* seconds */

#define MAX_METERS              20
/* ================= MQTT Configuration ================= */


/* ============================================================
 *  Data structures
 * ============================================================ */
extern volatile sig_atomic_t stop_flag;

typedef struct
{
    char filename[256];
    long filesize;
    int status;
} cdf_result_t;

/** Supported CDF data types */
typedef enum {
    DATA_TYPE_INSTANTANEOUS = 1,
    DATA_TYPE_LOAD_PROFILE  = 2,
    DATA_TYPE_BILLING       = 3,
    DATA_TYPE_NAMEPLATE     = 4,
    DATA_TYPE_EVENT_LOG     = 5,
    DATA_TYPE_MIDNIGHT      = 6,
    CONCATENATE_FILE        = 7,
    GENERATE_ZIP_FILE       = 8,
    DATA_TYPE_MAX
} DataType;

/** One instantaneous parameter entry */
typedef struct {
    char obis_code[32];    /**< Original OBIS string, e.g. "1_0_31_7_0_255"    */
    char obis_hex[32];     /**< Hex OBIS for CDF, e.g. "01_00_1f_07_00_ff"     */
    char param_code[32];   /**< Parameter code from mapping hash                */
    char param_name[128];  /**< Parameter name from mapping hash                */
    char unit[32];         /**< Unit string from mapping hash                   */
    char value[64];        /**< Current reading value                           */
} InstParam;

/** Container for a full snapshot of instantaneous data */
typedef struct {
    char        meter_serial[32];
    char        snapshot_date[32];  /**< Timestamp from OBIS 0_0_1_0_0_255      */
    InstParam  *params;             /**< Dynamically allocated array             */
    int         param_count;        /**< Number of valid entries in params[]     */
} InstSnapshot;

/** One Load Survey parameter entry (within an interval block) */
typedef struct {
    char obis_code[32];    /**< Original OBIS string                        */
    char obis_hex[32];     /**< Hex OBIS for CDF                            */
    char param_code[32];   /**< Parameter code from mapping hash            */
    char param_name[128];  /**< Parameter name from mapping hash            */
    char unit[32];         /**< Unit string from mapping hash               */
    char value[64];        /**< Reading value for this interval             */
} LSParam;

/** One interval period (IP) within a day profile */
typedef struct {
    int       interval_num;   /**< 0..95 for 15-min blocks                  */
    char      timestamp[32];  /**< Timestamp for this interval              */
    LSParam  *params;         /**< Parameters in this interval              */
    int       param_count;    /**< Number of parameters                     */
} LSInterval;

/** LS data for one day */
typedef struct {
    char         meter_serial[32];
    char         date[16];                /**< YYYY-MM-DD                    */
    int          interval_period;         /**< block_int (e.g. 15 minutes)   */
    int          demand_integration_period; /**< demand_int_period           */
    LSInterval  *intervals;               /**< Array of intervals for the day*/
    int          interval_count;          /**< Number of valid intervals     */
} LSDayProfile;

/** Meter metadata from meter_status hash */
typedef struct {
    char manufacturer[64];
    char manuf_key[16];      /**< Mapped key: lnt, genus, secure, etc.   */
    char dcu_serial[32];
    char port[8];
    int  block_int;
    int  demand_int_period;
} MeterStatus;

/** One Midnight register parameter */
typedef struct {
    char obis_code[32];    /**< Original OBIS string                        */
    char obis_hex[32];     /**< Hex OBIS for CDF                            */
    char param_code[32];   /**< Parameter code from mapping hash            */
    char param_name[128];  /**< Parameter name from mapping hash            */
    char unit[32];         /**< Unit string from mapping hash               */
    char value[64];        /**< Reading value                               */
} MNParam;

/** Midnight data snapshot for one date */
typedef struct {
    char      meter_serial[32];
    char      snapshot_date[32];  /**< Timestamp from 0_0_1_0_0_255        */
    MNParam  *params;             /**< Dynamically allocated array         */
    int       param_count;        /**< Number of valid parameters          */
} MNSnapshot;

/** One Billing profile parameter */
typedef struct {
    char obis_code[32];    /**< Original OBIS string                        */
    char obis_hex[32];     /**< Hex OBIS for CDF                            */
    char param_code[32];   /**< Parameter code from mapping hash            */
    char param_name[128];  /**< Parameter name from mapping hash            */
    char unit[32];         /**< Unit string from mapping hash               */
    char value[64];        /**< Reading value                               */
} BillParam;

/** One billing entry (can have multiple per month) */
typedef struct {
    char        billing_date[32];  /**< Bill date timestamp                */
    BillParam  *params;            /**< Parameters for this billing entry  */
    int         param_count;       /**< Number of parameters               */
} BillingEntry;

/** Billing data for a month (up to 2 entries) */
typedef struct {
    char           meter_serial[32];
    char           year_month[8];     /**< YYYY-MM format                  */
    BillingEntry  *entries;           /**< Array of billing entries        */
    int            entry_count;       /**< Number of entries (max 2)       */
} BillingData;


/** Event type mapping */
typedef struct {
    int  event_type_num;              /**< 1-7 for different event types   */
    char event_type_desc[64];         /**< Description                     */
    char obis_code[32];               /**< OBIS to read event code from    */
    char profile_obis_code[32];       /**< EVT_CODE for D5 section         */
} EventTypeMap;

/** One Event snapshot parameter */
typedef struct {
    char obis_code[32];    /**< Original OBIS string                        */
    char obis_hex[32];     /**< Hex OBIS for CDF                            */
    char param_code[32];   /**< Parameter code from mapping hash            */
    char param_name[128];  /**< Parameter name from mapping hash            */
    char unit[32];         /**< Unit string from mapping hash               */
    char value[64];        /**< Reading value                               */
} EventParam;

/** One event entry */
typedef struct {
    char        event_date[32];       /**< Event occurrence timestamp      */
    char        evt_code_hex[32];     /**< Profile OBIS code in hex        */
    EventParam *params;               /**< Parameters for this event       */
    int         param_count;          /**< Number of parameters            */
} EventEntry;

/** Event data collection */
typedef struct {
    char         meter_serial[32];
    EventEntry  *entries;             /**< Array of event entries          */
    int          entry_count;         /**< Number of events                */
} EventData;

typedef struct
{
    char meter_sn[64];
    char meter_name[16];
    char cmd_type[64];
    int seqnum;

} resp_msg;

#define CMD_TYPE_MAX_LEN        64
#define CMD_TRANSACTION_MAX_LEN 16
#define CMD_MAX_ARGS            16
#define CMD_ARG_MAX_LEN         128

typedef struct {
    char     type[CMD_TYPE_MAX_LEN];               /* COMMAND_INFO TYPE attr    */
    char     transaction[CMD_TRANSACTION_MAX_LEN]; /* COMMAND_INFO TRANSACTION  */
    uint32_t arg_count;                            /* ARGUMENTS COUNT attr      */
    char     args[CMD_MAX_ARGS][CMD_ARG_MAX_LEN];  /* ARG_01 … ARG_N values     */
} cmd_request_t;

/**
 * mqtt_cfg_t
 * ----------
 * Holds all MQTT-related configuration read from Redis.
 * Stored statically to avoid heap usage.
 */
// typedef struct {
//     int  enable_mqtt;
//     char broker_ip[MAX_STR_LEN];
//     int  broker_port;
//     char client_id[MAX_STR_LEN];
//     char username[MAX_STR_LEN];
//     char password[MAX_STR_LEN];
//     int  keep_alive;
//     int  qos;

//     char per_data_topic[MAX_TOPIC_LEN];
//     char event_pub_topic[MAX_TOPIC_LEN];
//     char diag_pub_topic[MAX_TOPIC_LEN];

//     char device_reg_topic[MAX_TOPIC_LEN];

//     int  per_data_interval;
//     int  enable_ssl;

//     char subscribe_topics[MAX_SUB_TOPICS][MAX_TOPIC_LEN];
// } mqtt_cfg_t;


// typedef struct
// {
//     // Core MQTT
//     int  enable_mqtt;
//     char broker_ip[MAX_STR_LEN];
//     int  broker_port;
//     char client_id[MAX_STR_LEN];
//     char username[MAX_STR_LEN];
//     char password[MAX_STR_LEN];
//     int  keep_alive;
//     int  qos;
//     int  primary;

//     // Topics (publish)
//     char per_data_topic[MAX_TOPIC_LEN];
//     char event_pub_topic[MAX_TOPIC_LEN];
//     char diag_pub_topic[MAX_TOPIC_LEN];
//     char data_topic[MAX_TOPIC_LEN];
//     char config_topic[MAX_TOPIC_LEN];
//     char resp_topic[MAX_TOPIC_LEN];
//     char ack_topic[MAX_TOPIC_LEN];
//     char log_topic[MAX_TOPIC_LEN];
//     char diag_topic[MAX_TOPIC_LEN];
//     char dev_reg_topic[MAX_TOPIC_LEN];

//     // Subscribe topics
//     char subscribe_topics[MAX_SUB_TOPICS][MAX_TOPIC_LEN];

//     // Feature flags
//     int event_topic_enable;
//     int diag_topic_enable;

//     // Timing
//     int per_data_interval;

//     // SSL
//     int enable_ssl;
//     int insecure;

//     char ca_certificate[MAX_STR_LEN];
//     char client_certificate[MAX_STR_LEN];
//     char client_key[MAX_STR_LEN];
//     char key_password[MAX_STR_LEN];
//     int  encrypted_key;

// } mqtt_cfg_t;



typedef struct
{
    // Core MQTT
    int  enable_mqtt;
    char broker_ip[SIZE_128];
    int  broker_port;
    char client_id[MAX_STR_LEN];
    char username[MAX_STR_LEN];
    char password[MAX_STR_LEN];
    int  keep_alive;
    int  qos;
    int  primary;
    int  clean_session;
    int  insecure;

    // Topics (publish)
    char cyclic_modbus_data_topic[MAX_TOPIC_LEN];
	char cyclic_dlms_data_topic[MAX_TOPIC_LEN];
	char inst_data_topic[MAX_TOPIC_LEN];
	char health_check_data_topic[MAX_TOPIC_LEN];
	char cmd_response_topic[MAX_TOPIC_LEN];
    // Subscribe topics
    char subscribe_topics[MAX_SUB_TOPICS][MAX_TOPIC_LEN];

    int hc_pub_interval;
    int dlms_inst_pub_interval;
    int dlms_data_pub_interval;
    int modbus_data_pub_interval;
 
    // SSL
    int enable_ssl;
    char ca_certificate[MAX_STR_LEN];
    char client_certificate[MAX_STR_LEN];
    char client_key[MAX_STR_LEN];
    char key_password[MAX_STR_LEN];
    int  encrypted_key;

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


/*extern variables declaration*/
extern mqtt_conn_t primary;
extern mqtt_conn_t secondary;
extern mqtt_conn_t *current_active;
extern redisContext *ctx;
/*extern variable declaration is done here*/



/*++++++++++Public API Function Calls++++++++++++*/
void get_datetime_str(char *, size_t );
void get_date_str(char *, size_t);
void log_rotate(void);
int log_init(void);
void log_write(const char * , const char * , ...);
void log_close(void);
redisContext *redis_connect(void);
int obis_dec_to_hex(const char *, char *);
void cdf_write_header(FILE *, const char *);
int read_meter_status(redisContext *, const char *, MeterStatus *);
void cdf_write_general(redisContext *, FILE *, const char *, const char *);
void cdf_write_d1(FILE *, redisContext *, const char *);
void cdf_write_footer(FILE *);
int parse_obis_map(const char *, const char *, char *, size_t );
cJSON *get_param_name_json(redisContext *, char *);
int chk_param_name_hash_exists(redisContext *ctx, char *key);
cdf_result_t generate_instantaneous_cdf(redisContext *ctx, const char *serial);
int generate_event_log_cdf(redisContext *ctx, const char *serial,
                           const char *date, const char *event_type, char *output_file);
int generate_billing_cdf(redisContext *ctx, const char *serial, const char *year_month, char *output_file);
int generate_load_profile_cdf(redisContext *ctx, const char *serial, const char *date, char *output_file);
int generate_midnight_cdf(redisContext *ctx, const char *serial, const char *date, char *output_file);
cdf_result_t generate_profile_cdf(redisContext *ctx, const char *serial, const char *date, const char *event_type);
int build_health_status_xml(redisContext *ctx, char *out_buf, size_t out_sz, int *output_file_sz);

void mqtt_module_start();
int  mqtt_connect(mqtt_conn_t *conn);
bool try_primary_health_check(mqtt_conn_t *primary);
void *mqtt_worker_thread(void *arg);
void load_mqtt_cfg(const char *hash, mqtt_cfg_t *cfg);
void configure_tls(mqtt_conn_t *conn);
void load_mqtt_ssl_cfg(const char *section, mqtt_ssl_cfg_t *cfg);
void connectionLost(void *context, char *cause);
void mqtt_send_file(mqtt_conn_t *ctx, const char *filename, int topic_type);
// void mqtt_send_msg(mqtt_conn_t *ctx, const char *mqtt_msg, int msg_size);
void mqtt_send_msg(mqtt_conn_t *mqtt_cfg, const char *mqtt_msg, int msg_size, int topic_type);
void mqtt_subscribe_topic(mqtt_conn_t *mqtt_cfg);
int on_message_arrived(void *context,
                       char *topicName,
                       int topicLen,
                       MQTTAsync_message *message);
int success_resp_msg_cdf(redisContext *ctx, resp_msg resp, char *out_buf);


/* ================= Public APIs ================= */




