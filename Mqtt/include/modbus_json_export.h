/**
 * @file  modbus_json_export.h
 * @brief Generate a JSON snapshot of all Modbus device data from Redis
 *
 * Reads modrtu and modtcp configuration, status, and value hashes from
 * Redis and produces a single JSON document containing all device and
 * register data.  Intended for export to upstream SCADA / HES systems.
 */
#ifndef MODBUS_JSON_EXPORT_H
#define MODBUS_JSON_EXPORT_H

#include <hiredis/hiredis.h>
#include <stdint.h>

/**
 * @brief Generate a JSON snapshot of all Modbus device data
 *
 * Scans Redis for modrtu (serial) and modtcp (TCP/IP) device configs,
 * reads their current register values and communication status, and
 * produces a JSON document.
 *
 * @param ctx               Active Redis context
 * @param num_serial_ports  Number of serial ports to scan for modrtu
 *                          devices (typically 2)
 * @return Dynamically allocated JSON string (caller must free),
 *         or NULL on error
 */
char *modbus_export_json(redisContext *ctx, uint16_t num_serial_ports);

#endif /* MODBUS_JSON_EXPORT_H */
