#ifndef MQTT_CMD_VALIDATE_H
#define MQTT_CMD_VALIDATE_H

/*
 * Validation of MDAS -> DCU command messages (topic cms/mqtt/dcu/cmdrequest/{serial})
 * per "DCU - MDAS MQTT Message Formats" v0.20 (25-Sep-2026):
 *   - set_cfg (section 4.5.1), all DATA_TYPE groups
 *   - general commands (section 4.4): Reset, START_TRANS_MODE, STOP_TRANS_MODE
 *
 * Value rules mirror what the DCU Web UI enforces today for the same fields
 * (src/components/DeviceConfiguration.tsx deviceConfigSchema, the config-section
 * dropdowns, and app.py for transparent mode). Rules that need the rest of the
 * stored configuration (duplicate names/IPs across devices, IEC IOA zone
 * overlaps, MQTT primary selection) are NOT done here; see mqtt_cmd_validate.c.
 *
 * Depends on cJSON (1.7.x, as vendored in dlms_104_data/).
 */

#include "cJSON.h"

/* CMD_STATUS codes, spec section 6.1 */
#define MQTT_CMD_ST_SUCCESS          0
#define MQTT_CMD_ST_INVALID_METER    3   /* "Invalid meter name" */
#define MQTT_CMD_ST_UNKNOWN_REQUEST  7   /* "Unknown request" */
#define MQTT_CMD_ST_INVALID_PARAM    8   /* "Invalid parameter" - missing/unknown/duplicate key */
#define MQTT_CMD_ST_INVALID_VALUE    9   /* "Invalid parameter value" - bad or out-of-range value */
#define MQTT_CMD_ST_FAILED          10   /* "FAILED" */

/* Returned for a recognised COMMAND_TYPE this module does not validate
 * (GetDay, FetchDay, get_cfg, ReadModbus, SET_METER_CFG, ...). Not a CMD_STATUS. */
#define MQTT_CMD_NOT_VALIDATED     (-1)

#define MQTT_VAL_FIELD_LEN  32
#define MQTT_VAL_MSG_LEN    192

typedef struct {
    int  status;                      /* one of MQTT_CMD_ST_* or MQTT_CMD_NOT_VALIDATED */
    char field[MQTT_VAL_FIELD_LEN];   /* offending DATA key, "" if not field-specific */
    char msg[MQTT_VAL_MSG_LEN];       /* human-readable reason (log it; CMD_MSG uses the spec text) */
} mqtt_val_result_t;

typedef enum {
    MQTT_METER_MODBUS_TCP,
    MQTT_METER_MODBUS_RTU,
    MQTT_METER_DLMS_SERIAL,
    MQTT_METER_DLMS_ETHERNET
} mqtt_meter_kind_t;

/* Current IEC104/IEC101 settings, needed because the ASDU/IOA limits depend on
 * the configured address sizes and the 1000-apart offset rule involves all
 * three offsets even when a message changes only one. Fill from iec10x_0_cfg. */
typedef struct {
    int  enable;              /* enable_104 / enable_101 (0/1) */
    int  asdu_addr_size;      /* 1 or 2 */
    int  ioa_addr_size;       /* 1, 2 or 3 */
    long ioa_offset;          /* DLMS */
    long ioa_offset_modbus;
    long ioa_offset_command;
} mqtt_val_iec_cfg_t;

typedef struct {
    /* Serial of this DCU; DATA.DCU must match (case-insensitive). NULL/"" = not checked. */
    const char *dcu_serial;

    /* features.enable_v11 === true: IPSEC RIGHT_IP may also be a domain name. */
    int v11_enabled;

    /* features.separate_common_addresses (with enable_v11): meters may carry
     * their own IEC104/101 Common Address (COMMON_ADDR in MODBUS_TCP,
     * MODBUS_RTU, DLMS_SERIAL, DLMS_ETHERNET). 0 = COMMON_ADDR is rejected
     * (status 8), as the UI does not offer the field then. */
    int separate_common_addresses;

    /* Also used for the meter COMMON_ADDR limit (enable + asdu_addr_size). */
    mqtt_val_iec_cfg_t iec104;
    mqtt_val_iec_cfg_t iec101;

    /* Optional. Return nonzero if a meter with this name is configured.
     * port is 1 or 2 for MODBUS_RTU / DLMS_SERIAL, 0 otherwise.
     * NULL = meter names are not looked up (no CMD_STATUS 3). */
    int  (*meter_exists)(mqtt_meter_kind_t kind, int port, const char *name, void *user);
    void  *user;
} mqtt_val_ctx_t;

/* Defaults: no DCU check, v1.1 off, separate common addresses off, IEC enabled with ASDU size 2, IOA size 3,
 * offsets 1000/10000/50000 (backend defaults), no meter lookup. */
void mqtt_val_ctx_init(mqtt_val_ctx_t *ctx);

/* Parse + validate a raw payload. Returns res->status. */
int mqtt_validate_command_json(const char *json, const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res);

/* Validate an already-parsed message: envelope (TYPE, SEQ_NUM, COMMAND_TYPE,
 * DATA, DATA.DCU), then dispatch on COMMAND_TYPE. Returns res->status. */
int mqtt_validate_command(const cJSON *root, const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res);

/* DATA of a set_cfg for the given DATA_TYPE. DCU itself is not checked here. */
int mqtt_validate_set_cfg(const char *data_type, const cJSON *data,
                          const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res);

/* DATA of Reset / START_TRANS_MODE / STOP_TRANS_MODE. data_type may be NULL for Reset. */
int mqtt_validate_general_cmd(const char *command_type, const char *data_type, const cJSON *data,
                              const mqtt_val_ctx_t *ctx, mqtt_val_result_t *res);

#endif /* MQTT_CMD_VALIDATE_H */