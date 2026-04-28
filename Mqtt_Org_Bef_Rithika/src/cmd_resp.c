#include "../include/general.h"

int success_resp_msg(cmd_request_t cmd, char *out_buf)
{
    int offset = 0;

    char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
    offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE SERIALNUM=\"%s\">\n", dcu_sn);
    offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_MSG=\"SUCCESS\"/>\n");
    offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
    offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
    if (cmd.arg_count > 0)
    {
        offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
        offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
    }
    offset += sprintf(out_buf + offset, "</ARGUMENTS>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

    return offset;
}

int invalid_metsn_resp_msg(cmd_request_t cmd, char *out_buf)
{
    int offset = 0;

    char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
    char *dcu_name = redis_hget(ctx, "dcu_info", "device_name");

    offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE DCU=\"%s\" SERIALNUM=\"%s\">\n", dcu_name, dcu_sn);
    offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_STATUS=\"3\" CMD_MSG=\"Invalid meter name\"/>\n");
    offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
    offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
    if (cmd.arg_count > 0)
    {
        offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
        offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
    }
    offset += sprintf(out_buf + offset, "</ARGUMENTS>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

    return offset;
}

int unknown_req_resp_msg(cmd_request_t cmd, char *out_buf)
{
    int offset = 0;

    char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
    char *dcu_name = redis_hget(ctx, "dcu_info", "device_name");

    offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE DCU=\"%s\" SERIALNUM=\"%s\">\n", dcu_name, dcu_sn);
    offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_STATUS=\"7\" CMD_MSG=\"Unknown request\"/>\n");
    offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
    offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
    if (cmd.arg_count > 0)
    {
        offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
        offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
    }
    offset += sprintf(out_buf + offset, "</ARGUMENTS>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

    return offset;
}

int reset_resp_msg(cmd_request_t cmd, char *out_buf)
{
    int offset = 0;

    char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
    char *dcu_name = redis_hget(ctx, "dcu_info", "device_name");

    offset += sprintf(out_buf + offset, "<COMMAND_RESPONSE DCU=\"%s\" SERIALNUM=\"%s\">\n", dcu_name, dcu_sn);
    offset += sprintf(out_buf + offset, "<COMMAND_RESULT CMD_STATUS=\"0\" CMD_MSG=\"SUCCESS\"/>\n");
    offset += sprintf(out_buf + offset, "<COMMAND_INFO TYPE=\"%s\" TRANSACTION=\"%s\">\n", cmd.type, cmd.transaction);
    offset += sprintf(out_buf + offset, "<ARGUMENTS COUNT=\"%d\">\n", cmd.arg_count);
    if (cmd.arg_count > 0)
    {
        offset += sprintf(out_buf + offset, "<ARG_01>%s</ARG_01>\n", cmd.args[0]);
        offset += sprintf(out_buf + offset, "<ARG_02>%s</ARG_02>\n", cmd.args[1]);
    }
    offset += sprintf(out_buf + offset, "</COMMAND_INFO>\n");
    offset += sprintf(out_buf + offset, "</COMMAND_RESPONSE>\n");

    return offset;
}