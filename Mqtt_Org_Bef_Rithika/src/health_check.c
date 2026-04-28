#include <unistd.h>
// #include "logger.h"
#include "../include/general.h"

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

    if (primary->connected) {
        LOG_INFO("[HEALTH] Primary broker reachable");
        return true;
    }

    LOG_INFO("[HEALTH] Primary still unreachable");
    return false;
}
