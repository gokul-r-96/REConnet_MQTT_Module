#include <unistd.h>
// #include "logger.h"
#include "../include/general.h"

extern redisContext *ctx;
/**
 * try_primary_health_check()
 * -------------------------
 * Attempts a short connection to primary broker.
 * Used ONLY when both configs are enabled.
 */
bool try_primary_health_check(mqtt_conn_t *primary)
{
    primary->connected = false;
    mqtt_connect(primary);

    sleep(1);

    if (primary->connected)
    {
        LOG_INFO("[HEALTH] Primary broker reachable");
        return true;
    }

    LOG_INFO("[HEALTH] Primary still unreachable");
    return false;
}

int send_hc_msg()
{

    pid_t pid = getpid();
    char path[64];
    char name[256];
    char temp_buff[256];

    sprintf(path, "/proc/%d/cmdline", pid);

    FILE *fp = fopen(path, "r");
    if (fp == NULL)
    {
        perror("Failed to open cmdline");
        return -1;
    }

    fgets(name, sizeof(name), fp);
    fclose(fp);

    char *base_name = basename(name);

    sprintf(temp_buff, "%s_hc_up_time ", base_name);
    hc_update(ctx, temp_buff);

    return 0;
}