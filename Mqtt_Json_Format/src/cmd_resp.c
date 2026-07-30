#include "../include/general.h"

// int success_resp_msg(cmd_request_t cmd, char *out_buf)
// {
//     int offset = 0;

//     char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
//     offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE SERIALNUM=\"%s\">\n", dcu_sn);
//     offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_MSG=\"SUCCESS\"/>\n");
//     offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
//     offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
//     if (cmd.arg_count > 0)
//     {
//         offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
//         offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
//     }
//     offset += sprintf(out_buf + offset, "</ARGUMENTS>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

//     return offset;
// }

// int invalid_metsn_resp_msg(cmd_request_t cmd, char *out_buf)
// {
//     int offset = 0;

//     char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
//     char *dcu_name = redis_hget(ctx, "dcu_info", "device");

//     offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE DCU=\"%s\" SERIALNUM=\"%s\">\n", dcu_name, dcu_sn);
//     offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_STATUS=\"3\" CMD_MSG=\"Invalid meter name\"/>\n");
//     offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
//     offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
//     if (cmd.arg_count > 0)
//     {
//         offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
//         offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
//     }
//     offset += sprintf(out_buf + offset, "</ARGUMENTS>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

//     return offset;
// }

// int unknown_req_resp_msg(cmd_request_t cmd, char *out_buf)
// {
//     int offset = 0;

//     char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
//     char *dcu_name = redis_hget(ctx, "dcu_info", "device");

//     offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE DCU=\"%s\" SERIALNUM=\"%s\">\n", dcu_name, dcu_sn);
//     offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_STATUS=\"7\" CMD_MSG=\"Unknown request\"/>\n");
//     offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
//     offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
//     if (cmd.arg_count > 0)
//     {
//         offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
//         offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
//     }
//     offset += sprintf(out_buf + offset, "</ARGUMENTS>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

//     return offset;
// }

// int reset_resp_msg(cmd_request_t cmd, char *out_buf)
// {
//     int offset = 0;

//     char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
//     char *dcu_name = redis_hget(ctx, "dcu_info", "device");

//     offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE DCU=\"%s\" SERIALNUM=\"%s\">\n", dcu_name, dcu_sn);
//     offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_STATUS=\"0\" CMD_MSG=\"SUCCESS\"/>\n");
//     offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
//     offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
//     if (cmd.arg_count > 0)
//     {
//         offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
//         offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
//     }
//     offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
//     offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

//     return offset;
// }

// static int build_cmd_reply(cmd_request_t cmd,int status,const char *msg,char *out_buf)
// {
//     cJSON *root = cJSON_CreateObject();
//     cJSON *data = cJSON_CreateObject();

//     cJSON_AddStringToObject(root, "TYPE", "command_reply");
//     cJSON_AddStringToObject(root, "SEQ_NUM", cmd.transaction);
//     cJSON_AddStringToObject(root, "COMMAND_TYPE", cmd.type);
//     cJSON_AddStringToObject(root, "DATA_TYPE", cmd.data_type_req);

//     if (cmd.arg_count > 0)
//         cJSON_AddStringToObject(data, "DCU", cmd.args[0]);

//     if (cmd.arg_count > 1)
//         cJSON_AddStringToObject(data, "METER", cmd.args[1]);

//     if (strcmp(cmd.type, "GetDay") == 0)
//     {
//         if (cmd.arg_count > 2)
//             cJSON_AddStringToObject(data, "DATE", cmd.args[2]);
//     }
//     else if (strcmp(cmd.type, "FetchDay") == 0)
//     {
//         if (cmd.arg_count > 2)
//             cJSON_AddStringToObject(data, "START_DATE", cmd.args[2]);

//         if (cmd.arg_count > 3)
//             cJSON_AddStringToObject(data, "END_DATE", cmd.args[3]);
//     }

//     cJSON_AddNumberToObject(data, "CMD_STATUS", status);
//     cJSON_AddStringToObject(data, "CMD_MSG", msg);
//     cJSON_AddItemToObject(root, "DATA", data);

//     char *json = cJSON_PrintUnformatted(root);
//     strcpy(out_buf, json);
//     int len = strlen(out_buf);
//     free(json);
//     cJSON_Delete(root);

//     return len;
// }


static int build_cmd_reply(cmd_request_t cmd,int status,const char *msg,char *out_buf)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *data = cJSON_CreateObject();

    cJSON_AddStringToObject(root, "TYPE", "command_reply");
    cJSON_AddStringToObject(root, "SEQ_NUM", cmd.transaction);
    cJSON_AddStringToObject(root, "COMMAND_TYPE", cmd.type);
    cJSON_AddStringToObject(root, "DATA_TYPE", cmd.data_type_req);

    char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");

    if (dcu_sn != NULL)
    {
        cJSON_AddStringToObject(data, "DCU", dcu_sn);
        free(dcu_sn);    // Only if redis_hget() returns allocated memory
    }
    else
    {
        cJSON_AddStringToObject(data, "DCU", "");
    }

    // if (strcmp(cmd.type, "ReadModbus") == 0)
    // {
    //     cJSON_AddNumberToObject(data, "SLAVE_ID", cmd.slave_id);
    // }
    // else
    // {
    //     if (cmd.arg_count > 1)
    //         cJSON_AddStringToObject(data, "METER", cmd.args[1]);

    //     if (strcmp(cmd.type, "GetDay") == 0)
    //     {
    //         if (cmd.arg_count > 2)
    //             cJSON_AddStringToObject(data, "DATE", cmd.args[2]);
    //     }
    //     else if (strcmp(cmd.type, "FetchDay") == 0)
    //     {
    //         if (cmd.arg_count > 2)
    //             cJSON_AddStringToObject(data, "START_DATE", cmd.args[2]);

    //         if (cmd.arg_count > 3)
    //             cJSON_AddStringToObject(data, "END_DATE", cmd.args[3]);
    //     }
    // }

    cJSON_AddNumberToObject(data, "CMD_STATUS", status);
    cJSON_AddStringToObject(data, "CMD_MSG", msg);

    cJSON_AddItemToObject(root, "DATA", data);

    char *json = cJSON_PrintUnformatted(root);

    strcpy(out_buf, json);

    int len = strlen(json);

    free(json);
    cJSON_Delete(root);

    return len;
}

int success_resp_msg(cmd_request_t cmd, char *out_buf)
{
    return build_cmd_reply(cmd, 0, "SUCCESS", out_buf);
}

int success_resp_msg_set_cfg(cmd_request_t cmd, char *out_buf)
{
    return build_cmd_reply(cmd, 0, "SUCCESS", out_buf);
}

int failure_resp_msg_set_cfg(cmd_request_t cmd, char *out_buf)
{
    return build_cmd_reply(cmd, 10, "FAILED", out_buf);
}

int invalid_metsn_resp_msg(cmd_request_t cmd, char *out_buf)
{
    return build_cmd_reply(cmd, 3, "Invalid meter name", out_buf);
}

int unknown_req_resp_msg(cmd_request_t cmd, char *out_buf)
{
    return build_cmd_reply(cmd, 7, "Unknown request", out_buf);
}

int ack_msg_reply(int seq_num, char *out_buf)
{

    /* Get DCU Serial Number */
    char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
    if (dcu_sn == NULL)
        dcu_sn = "UNKNOWN";

    /* Get Current Timestamp */
    char ts[32];
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", tm_info);

    /* Create JSON */
    cJSON *root = cJSON_CreateObject();

    cJSON_AddStringToObject(root, "TYPE", "ack");

    char seq_str[16];
    snprintf(seq_str, sizeof(seq_str), "%d", seq_num);
    cJSON_AddStringToObject(root, "SEQ_NUM", seq_str);

    cJSON_AddStringToObject(root, "ACK_BY", "DCU");
    cJSON_AddStringToObject(root, "DCU", dcu_sn);
    cJSON_AddStringToObject(root, "TS", ts);

    char *json = cJSON_PrintUnformatted(root);
    strcpy(out_buf, json);
    int len = strlen(json);
    free(json);
    cJSON_Delete(root);

    if (dcu_sn != NULL && strcmp(dcu_sn, "UNKNOWN") != 0)
        free(dcu_sn);   // Only if redis_hget() allocates memory

    // redisFree(ctx);

    return len;
}


int reset_resp_msg(cmd_request_t cmd, char *out_buf)
{
    cJSON *root = cJSON_CreateObject();
    cJSON *data = cJSON_CreateObject();

    cJSON_AddStringToObject(root, "TYPE", "response");
    cJSON_AddStringToObject(root, "SEQ_NUM", cmd.transaction);
    cJSON_AddStringToObject(root, "COMMAND_TYPE", cmd.type);
    cJSON_AddStringToObject(root, "STATUS", "SUCCESS");
    cJSON_AddStringToObject(root, "STATUS_CODE", "0");

    for (int i = 0; i < cmd.arg_count && i < CMD_MAX_ARGS; i++)
    {
        char key[16];
        sprintf(key, "ARG_%02d", i + 1);
        cJSON_AddStringToObject(data, key, cmd.args[i]);
    }

    cJSON_AddItemToObject(root, "DATA", data);

    char *json = cJSON_PrintUnformatted(root);
    strcpy(out_buf, json);

    int len = strlen(out_buf);

    free(json);
    cJSON_Delete(root);

    return len;
}