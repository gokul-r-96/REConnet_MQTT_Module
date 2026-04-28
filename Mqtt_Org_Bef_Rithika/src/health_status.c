/*
 * health_status.c
 *
 * Generates HEALTH_STATUS XML message from Redis hashes.
 *
 * Build (host):
 *   gcc -Wall -Wextra -Werror -O2 -std=c99 health_status.c logger.c -lhiredis -o health_status
 *
 * Cross-compile (iMX6):
 *   arm-linux-gnueabihf-gcc -march=armv7-a -mfpu=neon -mfloat-abi=hard \
 *       -Wall -Wextra -Werror -O2 -std=c99 health_status.c logger.c -lhiredis -o health_status
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <hiredis/hiredis.h>
#include "../include/general.h"

/* -------------------------------------------------------------------------
 * Constants
 * ---------------------------------------------------------------------- */
#define REDIS_HOST "127.0.0.1"
#define REDIS_PORT 6379

#define MAX_METERS 64
#define METER_FIELD_LEN 64
#define XML_BUF_SIZE (16 * 1024) /* 16 KB – static, not on stack */

/* Redis hash names */
#define HASH_DCU_INFO "dcu_info"
#define HASH_DCU_UPTIME "dcu_uptime"
#define HASH_GPRS_NW_STATUS "gprs_nw_status"
#define HASH_MODEM_STATUS "modem_status"
#define HASH_VPN_DIAG "vpn_diag"
#define HASH_ETH_METER_CFG "ethernet_meter_cfg"
#define HASH_SER0_CFG "serial_port_0_cfg"
#define HASH_SER1_CFG "serial_port_1_cfg"
#define HASH_METER_STATUS "meter_status"
#define HASH_METER_COMMN_STATUS "meter_commn_status"

/* -------------------------------------------------------------------------
 * Structures
 * ---------------------------------------------------------------------- */
typedef struct
{
    char dcu_name[METER_FIELD_LEN];
    char serial_num[METER_FIELD_LEN];
    char firmware[METER_FIELD_LEN];
    char attr1[METER_FIELD_LEN];
    char attr2[METER_FIELD_LEN];
    char attr3[METER_FIELD_LEN];
    char attr4[METER_FIELD_LEN];
    char attr5[METER_FIELD_LEN];
} dcu_info_t;

typedef struct
{
    char power[16];
    char time_now[32];
    char active_since[32];
    char time_synched[8];
    /* link */
    char connected_on[32];
    char active_sim[16];
    char isp[64];
    /* vpn */
    char vpn_ip[32];
    char remote_access_enabled[8];
} dcu_status_t;

typedef struct
{
    uint32_t slave_id;
    char bay[METER_FIELD_LEN];
    char ip_address[32];
    char comm_status[32]; /* "CONNECTED" or "NOT CONNECTED" */
    char comm_inhibit[8];
    char time_synched[8];
    char vpn_ip[32];
    char remote_access_enabled[8];
    char port[16]; /* port identifier (eth/serial0/serial1) */
    char serial_number[METER_FIELD_LEN];
} meter_entry_t;

/* -------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------- */

/**
 * @brief  Fetch a single field from a Redis HASH.
 *
 * @param  ctx    Open Redis context.
 * @param  hash   Hash key name.
 * @param  field  Field name within the hash.
 * @param  out    Destination buffer.
 * @param  out_sz Size of destination buffer.
 * @return        0 on success, -1 on error or missing field.
 */
static int redis_hget(redisContext *ctx,
                      const char *hash,
                      const char *field,
                      char *out,
                      size_t out_sz)
{
    redisReply *reply = NULL;

    if (!ctx || !hash || !field || !out || out_sz == 0)
        return -1;

    reply = redisCommand(ctx, "HGET %s %s", hash, field);
    if (!reply)
    {
        LOG_ERROR("redis_hget: command failed for %s.%s: %s", hash, field, ctx->errstr);
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR)
    {
        LOG_ERROR("redis_hget: redis error %s.%s: %s", hash, field, reply->str);
        freeReplyObject(reply);
        return -1;
    }
    if (reply->type == REDIS_REPLY_NIL || !reply->str)
    {
        LOG_DEBUG("redis_hget: field %s.%s is NIL", hash, field);
        out[0] = '\0';
        freeReplyObject(reply);
        return -1;
    }

    snprintf(out, out_sz, "%s", reply->str);
    freeReplyObject(reply);
    return 0;
}

/**
 * @brief  Populate dcu_info_t from Redis hashes dcu_info and dcu_uptime.
 */
static int fetch_dcu_info(redisContext *ctx, dcu_info_t *info)
{
    if (!ctx || !info)
        return -1;

    memset(info, 0, sizeof(*info));

    redis_hget(ctx, HASH_DCU_INFO, "device_name", info->dcu_name, sizeof(info->dcu_name));
    redis_hget(ctx, HASH_DCU_INFO, "serial_num", info->serial_num, sizeof(info->serial_num));
    redis_hget(ctx, HASH_DCU_INFO, "fw_ver", info->firmware, sizeof(info->firmware));
    redis_hget(ctx, HASH_DCU_INFO, "attribute_1", info->attr1, sizeof(info->attr1));
    redis_hget(ctx, HASH_DCU_INFO, "attribute_2", info->attr2, sizeof(info->attr2));
    redis_hget(ctx, HASH_DCU_INFO, "attribute_3", info->attr3, sizeof(info->attr3));
    redis_hget(ctx, HASH_DCU_INFO, "attribute_4", info->attr4, sizeof(info->attr4));
    redis_hget(ctx, HASH_DCU_INFO, "attribute_5", info->attr5, sizeof(info->attr5));

    LOG_INFO("fetch_dcu_info: DCU=%s SN=%s FW=%s", info->dcu_name, info->serial_num, info->firmware);
    return 0;
}

/**
 * @brief  Populate dcu_status_t from Redis (gprs_nw_status, modem_status,
 *         dcu_uptime, vpn_diag).
 */
static int fetch_dcu_status(redisContext *ctx, dcu_status_t *st)
{
    char ppp_status[16] = {0};
    char ipsec_status[16] = {0};
    time_t now;
    struct tm *tm_info;
    char ts[32];

    if (!ctx || !st)
        return -1;

    memset(st, 0, sizeof(*st));

    /* Power is always ON if we're running */
    snprintf(st->power, sizeof(st->power), "ON");

    /* Current time */
    now = time(NULL);
    tm_info = localtime(&now);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm_info);
    snprintf(st->time_now, sizeof(st->time_now), "%s", ts);

    /* TIME_SYNCHED from dcu_info */
    redis_hget(ctx, HASH_DCU_INFO, "updatetime", st->time_synched, sizeof(st->time_synched));

    /* ACTIVE_SINCE from dcu_uptime */
    redis_hget(ctx, HASH_DCU_UPTIME, "dcu_uptime", st->active_since, sizeof(st->active_since));

    /* Link status */
    redis_hget(ctx, HASH_GPRS_NW_STATUS, "ppp_status", ppp_status, sizeof(ppp_status));
    if (strcmp(ppp_status, "ON") == 0)
    {
        snprintf(st->connected_on, sizeof(st->connected_on), "MODEM");
        redis_hget(ctx, HASH_MODEM_STATUS, "sim_select", st->active_sim, sizeof(st->active_sim));
        redis_hget(ctx, HASH_MODEM_STATUS, "operator", st->isp, sizeof(st->isp));
        LOG_INFO("fetch_dcu_status: PPP ON, SIM=%s ISP=%s", st->active_sim, st->isp);
    }
    else
    {
        snprintf(st->connected_on, sizeof(st->connected_on), "NONE");
        LOG_INFO("fetch_dcu_status: PPP not ON (status='%s')", ppp_status);
    }

    /* VPN */
    redis_hget(ctx, HASH_VPN_DIAG, "ipsec_status", ipsec_status, sizeof(ipsec_status));
    if (strcmp(ipsec_status, "Up") == 0)
    {
        redis_hget(ctx, HASH_VPN_DIAG, "ipsec1_ip", st->vpn_ip, sizeof(st->vpn_ip));
        snprintf(st->remote_access_enabled, sizeof(st->remote_access_enabled), "YES");
        LOG_INFO("fetch_dcu_status: IPSec Up, VPN_IP=%s", st->vpn_ip);
    }
    else
    {
        snprintf(st->remote_access_enabled, sizeof(st->remote_access_enabled), "NO");
        LOG_INFO("fetch_dcu_status: IPSec not Up (status='%s')", ipsec_status);
    }

    return 0;
}

/**
 * @brief  Fetch meter entries from a given config hash (ethernet or serial).
 *
 * @param  ctx        Open Redis context.
 * @param  hash_name  Name of the config hash.
 * @param  port_label Label to tag meters with (e.g. "eth", "serial0", "serial1").
 * @param  meters     Output array; must have capacity for MAX_METERS entries.
 * @param  count      In/out: current meter count; incremented on each addition.
 * @return            0 on success.
 */
static int fetch_meters_from_cfg(redisContext *ctx,
                                 const char *hash_name,
                                 const char *port_label,
                                 meter_entry_t *meters,
                                 uint32_t *count)
{
    char field[METER_FIELD_LEN];
    char num_meters_str[16] = {0};
    uint32_t num_meters = 0;
    uint32_t i;

    if (!ctx || !hash_name || !port_label || !meters || !count)
        return -1;
    if (strcmp(port_label, "eth") == 0)
    {
        redis_hget(ctx, hash_name, "num_meters", num_meters_str, sizeof(num_meters_str));
    }
    else
    {
        redis_hget(ctx, hash_name, "num_dev", num_meters_str, sizeof(num_meters_str));
    }

    if (num_meters_str[0] == '\0')
    {
        LOG_WARN("fetch_meters_from_cfg: num_meters missing in %s", hash_name);
        return 0;
    }
    num_meters = (uint32_t)strtoul(num_meters_str, NULL, 10);
    LOG_INFO("fetch_meters_from_cfg: %s num_meters=%u", hash_name, num_meters);

    if (strcmp(port_label, "eth") == 0)
    {
        i = 0;
    }
    else
    {
        i = 1;
        num_meters += 1;
    }
    for (; i < num_meters && *count < MAX_METERS; i++)
    {

        meter_entry_t *m = &meters[*count];
        memset(m, 0, sizeof(*m));

        snprintf(field, sizeof(field), "meter_addr[%u]", i);
        redis_hget(ctx, hash_name, field, field, sizeof(field)); /* reuse field buf */
        /* meter_addr used as ID */
        {
            char addr_buf[METER_FIELD_LEN] = {0};
            snprintf(field, sizeof(field), "meter_addr[%u]", i);
            if (redis_hget(ctx, hash_name, field, addr_buf, sizeof(addr_buf)) == 0)
                m->slave_id = (uint32_t)strtoul(addr_buf, NULL, 10);
        }

        {
            char loc_buf[METER_FIELD_LEN] = {0};
            snprintf(field, sizeof(field), "meter_loc[%u]", i);
            redis_hget(ctx, hash_name, field, loc_buf, sizeof(loc_buf));
            snprintf(m->bay, sizeof(m->bay), "%s", loc_buf);
        }

        /* IP only for ethernet meters */
        if (strcmp(port_label, "eth") == 0)
        {
            char ip_buf[32] = {0};
            snprintf(field, sizeof(field), "ip_addr[%u]", i);
            redis_hget(ctx, hash_name, field, ip_buf, sizeof(ip_buf));
            snprintf(m->ip_address, sizeof(m->ip_address), "%s", ip_buf);
        }

        snprintf(m->port, sizeof(m->port), "%s", port_label);

        /* Defaults */
        snprintf(m->comm_status, sizeof(m->comm_status), "NOT CONNECTED");
        snprintf(m->comm_inhibit, sizeof(m->comm_inhibit), "NO");
        snprintf(m->time_synched, sizeof(m->time_synched), "NO");
        snprintf(m->remote_access_enabled, sizeof(m->remote_access_enabled), "NO");

        (*count)++;
        LOG_DEBUG("fetch_meters_from_cfg: [%s] idx=%u slave_id=%u bay=%s ip=%s",
                  hash_name, i, m->slave_id, m->bay, m->ip_address);
    }
    return 0;
}

/**
 * @brief  Enrich meter entries with runtime status from meter_status and
 *         meter_commn_status hashes.
 *
 * The meter_status hash stores values in the form:
 *   field = <meter_port>_<id>_<serial_num>
 * meter_commn_status uses the same field name; value is "Communicating" or
 * "Not Communicating".
 *
 * @param  ctx     Open Redis context.
 * @param  meters  Array of meter entries to update.
 * @param  count   Number of valid entries in meters[].
 * @return         0 on success.
 */
static int enrich_meter_status(redisContext *ctx,
                               meter_entry_t *meters,
                               uint32_t count)
{
    redisReply *reply = NULL;
    uint32_t i, j;

    if (!ctx || !meters || count == 0)
        return -1;

    /* HGETALL meter_status */
    reply = redisCommand(ctx, "HGETALL %s", HASH_METER_STATUS);
    if (!reply)
    {
        LOG_ERROR("enrich_meter_status: HGETALL meter_status failed: %s", ctx->errstr);
        return -1;
    }
    if (reply->type == REDIS_REPLY_ERROR)
    {
        LOG_ERROR("enrich_meter_status: redis error: %s", reply->str);
        freeReplyObject(reply);
        return -1;
    }

    /* reply->elements is field/value pairs */
    for (i = 0; i + 1 < (uint32_t)reply->elements; i += 2)
    {
        const char *field_str = reply->element[i]->str;
        const char *value_str = reply->element[i + 1]->str;
        char port_buf[16] = {0};
        char id_buf[16] = {0};
        char sn_buf[METER_FIELD_LEN] = {0};
        uint32_t meter_id;

        if (!field_str || !value_str)
            continue;

        /* Field format: <port>_<id>_<serial_num> */
        {
            char tmp[128];
            char *tok;
            snprintf(tmp, sizeof(tmp), "%s", field_str);
            tok = strtok(tmp, "_");

            if (tok)
            {
                tok = strtok(NULL, "_");
                snprintf(port_buf, sizeof(port_buf), "%s", tok);

                printf("****************\nport_bus %s\n", port_buf);
            }
            if (tok)
            {
                tok = strtok(NULL, "_");
                snprintf(id_buf, sizeof(id_buf), "%s", tok);

                printf("****************\n id_buf %s\n", id_buf);
            }
            if (tok)
            {
                tok = strtok(NULL, "_");
                snprintf(sn_buf, sizeof(sn_buf), "%s", tok);
                printf("****************\n sn_buf %s\n", sn_buf);
            }
        }

        meter_id = (uint32_t)strtoul(id_buf, NULL, 10);

        /* Match against our meter array */
        for (j = 0; j <= count; j++)
        {
            printf("meters[j].id %d meter_id %d\n", meters[j].slave_id, meter_id);
            if (j == meter_id)
            {
                snprintf(meters[j].port, sizeof(meters[j].port), "%s", port_buf);
                snprintf(meters[j].serial_number, sizeof(meters[j].serial_number), "%s", sn_buf);
                LOG_DEBUG("enrich_meter_status: matched id=%u port=%s sn=%s",
                          meter_id, port_buf, sn_buf);
                break;
            }
        }
    }
    freeReplyObject(reply);

    /* Now fetch communication status using composed key */
    for (i = 0; i < count; i++)
    {
        char commn_field[128] = {0};
        char commn_val[32] = {0};

        if (meters[i].port[0] == '\0' || meters[i].serial_number[0] == '\0')
        {
            LOG_WARN("enrich_meter_status: slave_id=%u missing port/sn, skipping commn lookup",
                     meters[i].slave_id);
            continue;
        }

        snprintf(commn_field, sizeof(commn_field), "meter_%s_%u_%s",
                 meters[i].port, i, meters[i].serial_number);

        if (redis_hget(ctx, HASH_METER_COMMN_STATUS, commn_field, commn_val, sizeof(commn_val)) == 0)
        {
            if (strcmp(commn_val, "Communicating") == 0)
                snprintf(meters[i].comm_status, sizeof(meters[i].comm_status), "CONNECTED");
            else
                snprintf(meters[i].comm_status, sizeof(meters[i].comm_status), "NOT CONNECTED");
            LOG_DEBUG("enrich_meter_status: id=%u commn='%s' -> STATUS=%s",
                      i, commn_val, meters[i].comm_status);
        }
    }

    return 0;
}

/* -------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------- */

/**
 * @brief  Build the HEALTH_STATUS XML message from Redis data.
 *
 * Reads DCU info, status, and all meter configurations from Redis hashes and
 * formats them as an XML string suitable for transmission.
 *
 * @param  redis_host  Hostname or IP of the Redis server.
 * @param  redis_port  Port of the Redis server.
 * @param  out_buf     Caller-supplied buffer to receive the XML string.
 * @param  out_sz      Size of out_buf in bytes.
 * @return             0 on success, -1 on failure (partial XML may be in buf).
 *
 * @note   out_buf must be at least 16 KB for a typical deployment.
 *         Not thread-safe; call from a single thread or protect externally.
 */
int build_health_status_xml(redisContext *ctx, char *out_buf, size_t out_sz, int *output_file_sz)
{

    dcu_info_t dcu_inf;
    dcu_status_t dcu_st;
    /* Static allocation – avoids 64 * sizeof(meter_entry_t) on stack */
    static meter_entry_t meters[MAX_METERS];
    uint32_t meter_count = 0;
    int rc = -1;
    int n = 0;
    size_t written = 0;

    /* ---- Fetch data ---- */
    if (fetch_dcu_info(ctx, &dcu_inf) < 0)
    {
        LOG_ERROR("Error fetching dcu_info ");
        return rc;
    }

    if (fetch_dcu_status(ctx, &dcu_st) < 0)
    {
        LOG_ERROR("Error fetching dcu status ");
        return rc;
    }

    fetch_meters_from_cfg(ctx, HASH_ETH_METER_CFG, "eth", meters, &meter_count);
    fetch_meters_from_cfg(ctx, HASH_SER0_CFG, "serial0", meters, &meter_count);
    fetch_meters_from_cfg(ctx, HASH_SER1_CFG, "serial1", meters, &meter_count);

    enrich_meter_status(ctx, meters, meter_count);

    /* ---- Build XML ---- */
#define APPEND(fmt, ...)                                                       \
    do                                                                         \
    {                                                                          \
        n = snprintf(out_buf + written, out_sz - written, fmt, ##__VA_ARGS__); \
        if (n < 0 || (size_t)n >= out_sz - written)                            \
        {                                                                      \
            LOG_ERROR("build_health_status_xml: output buffer overflow");      \
        }                                                                      \
        written += (size_t)n;                                                  \
    } while (0)

    APPEND("<HEALTH_STATUS DCU=\"%s\" SERIALNUM=\"%s\" FIRMWARE=\"%s\">\n",
           dcu_inf.dcu_name, dcu_inf.serial_num, dcu_inf.firmware);

    APPEND("    <DCU_DETAILS"
           " ATTRIBUTE_1=\"%s\""
           " ATTRIBUTE_2=\"%s\""
           " ATTRIBUTE_3=\"%s\""
           " ATTRIBUTE_4=\"%s\""
           " ATTRIBUTE_5=\"%s\"/>\n",
           dcu_inf.attr1, dcu_inf.attr2, dcu_inf.attr3,
           dcu_inf.attr4, dcu_inf.attr5);

    APPEND("    <DCU_STATUS"
           " POWER=\"%s\""
           " TIME=\"%s\""
           " ACTIVE_SINCE=\"%s\""
           " TIME_SYNCHED=\"%s\">\n",
           dcu_st.power, dcu_st.time_now,
           dcu_st.active_since, dcu_st.time_synched);

    APPEND("        <LINK_STATUS"
           " CONNECTED_ON=\"%s\""
           " ACTIVE_SIM=\"%s\""
           " ISP=\"%s\"/>\n",
           dcu_st.connected_on, dcu_st.active_sim, dcu_st.isp);

    APPEND("        <REMOTE_ACCESS"
           " VPN_IP=\"%s\""
           " REMOTE_ACCESS_ENABLED=\"%s\"/>\n",
           dcu_st.vpn_ip, dcu_st.remote_access_enabled);

    APPEND("    </DCU_STATUS>\n");
    APPEND("    <METERS>\n");

    {
        uint32_t i;
        for (i = 0; i < meter_count; i++)
        {
            const meter_entry_t *m = &meters[i];
            APPEND("        <METER"
                   " ID=\"%u\""
                   " BAY=\"%s\""
                   " IP_ADDRESS=\"%s\""
                   " COMMUNICATION_STATUS=\"%s\""
                   " COMM_INHIBIT=\"%s\""
                   " TIME_SYNCHED=\"%s\""
                   " VPN_IP=\"%s\""
                   " REMOTE_ACCESS_ENABLED=\"%s\"/>\n",
                   m->slave_id, m->bay, m->ip_address,
                   m->comm_status, m->comm_inhibit,
                   m->time_synched, m->vpn_ip,
                   m->remote_access_enabled);
        }
    }

    APPEND("    </METERS>\n");
    APPEND("</HEALTH_STATUS>\n");

#undef APPEND

    *output_file_sz = written;
    LOG_INFO("build_health_status_xml: XML built (%zu bytes, %u meters) output_file_sz %d", written, meter_count, *output_file_sz);
    printf("Output Buffer = %s\n",out_buf);

    rc = 0;

    return rc;
}

/* =========================================================================
 * Main – test harness
 * ====================================================================== */

/*
 * Seed Redis with representative test data:
 *
 *   redis-cli HSET dcu_info dcu_name DCU-E70-DVC serial_num 10131F25 \
 *       firmware "VER5.7 Jan 16 2025-11:53:40(R)" \
 *       attribute_1 490007 attribute_2 CLW_CHITHARANJAN \
 *       attribute_3 M1F1F2F3_M2F1F2F3_3F attribute_4 9010013700017 \
 *       attribute_5 "33 kV_ FEEDER" time_synched YES
 *
 *   redis-cli HSET dcu_uptime active_since "2025-09-20 16:35:29"
 *
 *   redis-cli HSET gprs_nw_status ppp_status ON
 *   redis-cli HSET modem_status sim_select SIM1 operator "IND airtel"
 *   redis-cli HSET vpn_diag ipsec_status Up ipsec1_ip 172.24.18.65
 *
 *   redis-cli HSET ethernet_meter_cfg num_meters 2 \
 *       meter_addr_0 21 meter_loc_0 M1F1_METER ip_addr_0 192.168.11.102 \
 *       meter_addr_1 22 meter_loc_1 M1F2_METER ip_addr_1 192.168.11.103
 *
 *   redis-cli HSET meter_status eth_21_SN001 "" eth_22_SN002 ""
 *   redis-cli HSET meter_commn_status \
 *       meter_eth_21_SN001 Communicating \
 *       meter_eth_22_SN002 "Not Communicating"
 */
// int send_hc_msg(void)
// {
//     /* Static buffer – 16 KB is safe on embedded target */
//     static char xml_buf[XML_BUF_SIZE];
//     int rc;

//     LOG_INFO("main: starting health_status XML test");

//     rc = build_health_status_xml(REDIS_HOST, REDIS_PORT, xml_buf, sizeof(xml_buf));
//     if (rc != 0)
//     {
//         LOG_ERROR("main: build_health_status_xml failed");
//         fprintf(stderr, "ERROR: build_health_status_xml failed. Check app.log for details.\n");
//         return EXIT_FAILURE;
//     }

//     printf("%s", xml_buf);

//     LOG_INFO("main: done");
//     return EXIT_SUCCESS;
// }
