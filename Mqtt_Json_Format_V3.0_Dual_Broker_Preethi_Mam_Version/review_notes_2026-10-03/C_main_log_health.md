# Review C: main.c, dbg_log.c, health_check.c, health_status.c, cmd_resp.c, headers, Makefile

Module: `Mqtt_Json_Format_V3.0_Dual_Broker` (daemon `re_mqtt_proc`)
Branch: `autofix/mqtt-high-2026-10-03` (HEAD `e670bb3`). Compared against `main`.
Date: 03-Oct-2026. Reviewer stance: critical, line by line. No source files edited.

Method: every line of the listed files read. Calls followed into `mqtt_connect.c` (FetchDay state, ACK call sites, `read_redis_resp`, `fetchday_reset_state`), `file_gen_main.c` (`redis_hget`, `redis_connect`, SEQ_NUM header, fork/tar), the web UI producer `www/redis_worker.py` + `www/feature_config.json` (meter slot layout), `Modem_PPP_IPSEC/modem_monitor.sh` (IMEI/operator producer), `dcu_mon_proc/dcu_health.c` (dcu_uptime producer) and the message spec `DCU_MDAS_MQTT_Message_Formats_0.20_29_September_2026.docx`. All five C files were syntax-checked with `arm-linux-gnueabihf-gcc -std=gnu99 -Wall -Wextra` against the repo stub headers (`dcu_netlog.h`/`hc_heartbeat.h` on this machine are compile stubs; the real `dcu_netlog.c` is not available, so anything depending on it is marked Plausible).

Files changed on this branch vs `main` (in scope): `src/dbg_log.c` (F5), `src/main.c` (F7 timeout), `include/general.h` (one prototype). `health_status.c`, `health_check.c`, `cmd_resp.c`, `Makefile`, `mqtt_module.h`, `get_set_cfg.h`, `json_helper.h` are byte-identical to `main`, so line numbers for those are the same on both.

---

## 1. Status of prior findings (03-Oct review) in scope

| ID | Prior summary | Status on HEAD | Evidence |
|---|---|---|---|
| F5 | Logging from Paho threads used shared Redis ctx / log file without lock | **Partly fixed** | `dbg_log.c:18-40` recursive mutex taken at `log_write:361`, released at `:393` and `:408`; `log_close:416-427` locked. Diag now uses private `g_diag_ctx` (`diag_redis()`, `:44-72`), not the global `ctx`. `localtime_r` at `:87`. Recursion `log_write -> checkDbgStatus -> LOG_INFO -> log_write` is safe (recursive mutex; inner call sees `dbgcfgtime == st_mtime`, so it does not recurse again). Not called from signal handlers (`main.c:1618-1623` only sets `stop_flag`). Fork in `file_gen_main.c:1807` child only does `execlp`/`_exit`, so a held log mutex in the child is harmless. Remaining problems: lock held over blocking I/O, `dcu_netlog_poll()` still unsynchronised, time()-based throttles, rotation failure permanent. See C9, C17, C18, C20. |
| F7 (main.c part) | check_redis_resp had no timeout | **Partly fixed** | `main.c:1011-1022` adds a 600 s monotonic timeout and `fetchday_reset_state()` (`mqtt_connect.c:2629-2642`). But: no failure reply to the server, abandoned request not withdrawn, late replies are consumed by the next FetchDay, timer frozen while the FetchDay broker is down. See C5, C6. |
| F17 (health part) | Hand-built health JSON not escaped | **Open** - and it is the cause of the bench "invalid JSON" | `health_status.c:993-1049` still writes every Redis string with `"%s"`. Root cause traced in C1. |
| F21 | Meter status matching reads/writes outside valid entries | **Open** (now Confirmed with a concrete trace, previously Plausible) | Unchanged `health_status.c:441, 562, 592, 612, 617`. Trace: after the Ethernet pass (`count`=30), the serial pass sets `count`=5 but `meters[5]` still holds Ethernet slot 5 (port `"2"`, its serial). If serial meter id 5 has no `meter_status` entry yet, the loop `i <= count` at `:592` builds `meter_2_5_<eth serial>` and writes that Ethernet meter's status into `meters[4]` (serial meter 5) at `:612/617`. Same for serial1 picking up serial0 meter 5. With 64 Ethernet meters `j <= count` writes `meters[64]` (static array overflow). Under this review's severity rule (wrong data to server) this is High. |
| F28 | `diag_msgs:MQTT_PROC` grows without limit | **Open** | `dbg_log.c:157` `LPUSH` with no `LTRIM`. Worse: a NIL `diag_enable` keeps the old cached value (`:146`), see C17. |
| L1 | Redis connection never re-established | **Open** | `main.c:1637` single `redis_connect()`; no `redisReconnect`/`ctx->err` check anywhere in main loop. Only `dbg_log.c` reconnects its private diag ctx. Self-heals only because `hc_update` fails and dcu_mon kills the process after 180 s. |
| L2 | `clean` misses `src/*.o`; no header deps | **Open** | `Makefile:42-43` unchanged (`rm -f *.o`), no `-MMD -MP`. |
| L7 (cmd_resp part) | `strcpy` of `cJSON_PrintUnformatted` without NULL check | **Open** | `cmd_resp.c:189, 257, 292`. |
| L8 | APPEND overflow handling makes truncation worse | **Open** (latent) | `health_status.c:966-978`: after truncation `written += n` exceeds `out_sz`, next `out_sz - written` wraps to ~4 GB, so `snprintf` writes past the 128 KB worker-stack buffer `main.c:1126`; `*output_file_sz = written` (`:1127`) then makes `mqtt_send_msg` read past it. Worst case with current limits (3 passes x 64 meters x ~450 B) is ~90 KB, so still latent. |
| L9 | `redis_uptime` used uninitialised | **Open** | `health_status.c:193` `char redis_uptime[64];` not initialised; `redis_hget` leaves it untouched on NULL/ERROR replies (`:115-125`); `atoi` at `:198` reads stack garbage. |
| L10 | `send_hc_msg`: unchecked fgets, overflow, key from argv[0] | **Open** | `health_check.c:46` fgets unchecked (`name` uninitialised if it fails), `:51` `sprintf(temp_buff[256], "%s_hc_up_time")` can write 267 bytes, key still derived from `/proc/<pid>/cmdline`. Also now called 3-60x per loop (`worker_service()` per meter), each time `fopen` of `/proc`. |
| L13 (main.c part) | Unsynchronised cross-thread access | **Open** | `dcu_netlog_poll()` is called from the main thread (`main.c:1657`) and the worker (`main.c:992`) with no lock, while `dcu_netlog_send()` runs under `g_log_lock` from all threads. See C9. |
| L19 | Unused `redis_hash_generator.c` linked in | **Open** | `Makefile:21` still `$(wildcard $(SRCDIR)/*.c)`. |
| L21 | Dead code with bugs | **Open** | `main.c:1471` `LOG_INFO("r->type %d", r->type)` before the NULL check (function now has no caller); `include/mqtt_module.h` still declares a conflicting `mqtt_cfg_t` / `mqtt_send_file` signature (masked only because `general.h:19` pre-defines `MQTT_MODULE_H`). `health_check.c:12-27` `try_mqtt1_health_check()` and `cmd_resp.c:271-300` `reset_resp_msg()` have no callers. |

---

## 2. New findings

Severity rule used: **High** = crash, hang, root code exec, or wrong/invalid data to the server. Medium = degraded or wrong behaviour with a narrower trigger. Low = latent, hygiene with a concrete risk.

### Summary

| ID | Sev | Location | Issue | Verification |
|---|---|---|---|---|
| C1 | High | health_status.c:153, 221, 993-1049 | Health JSON invalid: raw CR/LF (and any `"` `\`) from Redis written unescaped | Confirmed |
| C2 | High | health_status.c:284-354, 1032-1117 | Health lists every config slot (30+5+5 = 40), unused slots as real meters; `ID` is the DLMS address, not unique | Confirmed |
| C3 | High | main.c:25, 148-191; general.h:142 | `MAX_METERS 20` truncates active meter list; meters 21+ get no cyclic data and GetDay/FetchDay are rejected "Invalid meter name" | Confirmed |
| C4 | High | cmd_resp.c:229-268; mqtt_connect.c:3822-3853 | ACK `SEQ_NUM` is a constant per command type, not the request's SEQ_NUM | Confirmed |
| C5 | Medium | main.c:1011-1022; mqtt_connect.c:2629-2642, 2644-2700 | FetchDay timeout: no failure reply, request not withdrawn, late reply answers the next FetchDay | Confirmed (code) / Plausible (timing) |
| C6 | Medium | main.c:1013-1015 | FetchDay timeout clock frozen while the FetchDay broker is down; stale SEQ_NUM on cyclic data via the other broker indefinitely | Confirmed |
| C7 | Medium | health_status.c:186-214, 191, 345-349, 986-988, 1042-1049 | Health fields do not match spec v0.20 (fixed SEQ_NUM, ACTIVE_SINCE, TIME_SYNCHED, hard-coded meter fields, missing fields) | Confirmed vs spec |
| C8 | Medium | cmd_resp.c:136-197 | command_reply does not echo METER / DATE / START_DATE / END_DATE | Confirmed vs spec |
| C9 | Medium | dbg_log.c:359-409, 290-293; main.c:992, 1657 | Log mutex held across blocking network/Redis I/O; netlog poll/send unsynchronised | Plausible |
| C10 | Medium | main.c:1003, 1196-1202, 1491-1492; dbg_log.c:266-270; health_status.c:421-434, 1129 | Log volume ~25-40 MB/day: ~3-4 h of history kept, every line to netlog, flash wear | Confirmed (rate) / Plausible (impact) |
| C11 | Medium | general.h:16; health_status.c:18; json_helper.h:3; Makefile:10 | Two different hiredis header trees can be mixed in one binary (redisReply ABI) | Plausible |
| C12 | Medium | main.c:1382-1438, 1487-1520 | MQTT config read once with no error check; a Redis error at startup disables both brokers forever while heartbeat stays green | Plausible |
| C13 | Medium | main.c:1079-1090 | Cyclic profile always for wall-clock "today": tail of previous day never sent cyclically | Confirmed (code) / Plausible (impact) |
| C14 | Low | main.c:152-155 | `load_active_meters` leaks the reply every 3 s when HGETALL returns an error | Confirmed |
| C15 | Low | main.c:926-1285; cmd_resp.c:146, 233; health_check.c:49; dbg_log.c:219; Makefile:12 | Implicit declarations returning pointers (works only because target is 32-bit); no `-Werror` | Confirmed (compiler) |
| C16 | Low | Makefile:6-21; general.h:11-13 | Hard-coded absolute paths; objects written into another user's tree; no hardening flags | Confirmed |
| C17 | Low | dbg_log.c:44-72, 128-164 | diag throttles use `time()`; NIL `diag_enable` keeps diag on | Confirmed |
| C18 | Low | dbg_log.c:171-204, 243-288 | Failed reopen after rotation disables file logging until restart; `perror` per line when cfg missing | Confirmed |
| C19 | Low | main.c:917-918 | 32-bit `time_t` overflow in `PUB_DUE` for large intervals -> publish every loop | Confirmed |
| C20 | Low | main.c:1632-1648 | `dcu_netlog_send()` used before `dcu_netlog_init()` | Plausible |
| C21 | Low | general.h (whole), health_status.c:24-27 | No include guard, conflicting `MAX_METERS`, static prototype in header, stub `hc_update` signature mismatch | Confirmed |

---

### C1. Health JSON is invalid: Redis strings written raw, including CR/LF [High]
**Location:** `health_status.c:153` (IMEI), `:221` (ISP), and the writers `:993-998`, `:1004-1008`, `:1018`, `:1021-1022`, `:1025`, `:1043-1048`.

**Problem:** Every string goes into the JSON through `APPEND("... \"%s\" ...", value)` with no escaping, e.g.
```c
APPEND("    \"IMEI\": \"%s\",\n", dcu_inf.modem_imei);   // :996
APPEND("        \"ISP\": \"%s\"\n",dcu_st.isp);          // :1022
```
The producer stores raw AT-command output. In `Modem_PPP_IPSEC/modem_monitor.sh:531-533`:
```sh
result=$(send_at_cmd "at+gsn")
imei_num=$(echo "$result" | awk '{print $1}')   # one output line per input line, \r kept
imei=${imei_num:0:15}
```
An AT reply `\r\n861234567890123\r\n\r\nOK\r\n` produces `\r\n861234567890` (CR and LF inside the first 15 chars). Operator: `sed -e 's/.*"\(.*\)".*/\1/' | tr -d ' '` (`:546`) only rewrites the line containing quotes; the other lines (`\r`, `OK\r`) stay, so `operator` contains CR/LF and `OK`. JSON forbids raw control characters inside strings, so the whole health message fails to parse. This matches the bench result exactly (raw CR/LF in IMEI/ISP).

Same path for `"`/`\` in DEVNAME, DEV_LOC, ATTRIBUTE_1..5, BAY (meter names typed in the web UI are stored verbatim by `www/redis_worker.py`), FW_VER.

**Failure scenario:** Any DCU with a modem: every health message is rejected by the server, every cycle, for as long as the modem is attached. A user naming a meter `Feeder "A"` breaks it the same way without a modem.

**Fix:** Build the message with cJSON (already linked; `cmd_resp.c` already does) or add a `json_escape()` that emits `\" \\ \n \r \t` and `\u00XX` for other bytes < 0x20, and use it for every `%s`. Additionally trim CR/LF/whitespace in the shell producer (`tr -d '\r\n'`, take the line matching `^[0-9]{15}$`), but the C side must not rely on that.

**Verification:** Confirmed (C code traced; producer traced; exact `send_at_cmd` output format inferred from standard AT framing).

### C2. Health lists all configuration slots, and ID is not a unique key [High]
**Location:** `health_status.c:284-354` (`fetch_meters_from_cfg`), `:1032-1117`.

**Problem:** `num_meters`/`num_dev` is not the number of configured meters. The web UI always writes it as the slot capacity from `feature_config.json` (`www/redis_worker.py:1774-1778, 1812-1816`: "num_meters is always fixed to max_devices"; `feature_config.json:19-20`: Ethernet 30, serial 5 per port). Unused slots are written with `enable_meter=0`, `meter_loc=""`, `meter_addr="1"`, `ip_addr="0.0.0.0"`. `fetch_meters_from_cfg` emits every index regardless of enable flag or name:
```c
for (; i < num_meters && *count < MAX_METERS; i++) { ... (*count)++; }   // :296-351
```
So the array has 30 + 5 + 5 = **40 entries**; with 13 configured meters, **27 are empty** (`ID 1, BAY "", IP "0.0.0.0", COMM_INHIBIT "YES"`). This is exactly the bench observation (40 slots / 27 empty).

`ID` is `meter_addr[i]` (`:308-309`), i.e. the DLMS server address. That is only unique per serial bus; Ethernet meters routinely share the same address (distinguished by IP), and every empty slot is `ID 1`. Hence the bench "duplicate ID 25": two configured meters with DLMS address 25. The server cannot tell which meter a COMM_STATUS belongs to.

**Failure scenario:** Server shows 27 phantom meters "NOT CONNECTED / inhibited", and with duplicate IDs maps statuses to the wrong meter.

**Fix:** Skip slots with `enable_meter != 1` (or empty `meter_loc`), or iterate only the meters present in `meter_status`. Emit a unique identifier agreed with the server (meter serial number is already parsed into `serial_number` but never emitted; or port+slot). Confirm with the spec owner; spec example IDs (21, 22, ...) look like DLMS addresses, so the spec itself needs a uniqueness rule.

**Verification:** Confirmed (producer and consumer both traced).

### C3. `MAX_METERS 20` silently drops meters 21+ from cyclic data and from command validation [High]
**Location:** `general.h:142` `#define MAX_METERS 20`; `main.c:25` `char meter_serials[MAX_METERS][32]`; `main.c:184-185`.

**Problem:** `load_active_meters()` stops after 20 communicating meters:
```c
meter_count++;
if (meter_count >= MAX_METERS) break;     // main.c:182-185
```
The product supports 30 Ethernet + 2x5 serial = 40 DLMS meters (`feature_config.json`). `meter_serials[]` drives the instantaneous loop (`main.c:1045`), the profile loop (`:1088`) and the "is this meter known" check in `processServerMsg` (`mqtt_connect.c:3782-3799`). Which 20 survive depends on Redis hash iteration order, which changes as fields are added.

**Failure scenario:** Site with 25 communicating meters: 5 meters never publish instantaneous or profile data; a GetDay/FetchDay for one of them returns CMD_STATUS 3 "Invalid meter name" although it is configured and communicating. The same check also rejects FetchDay for a meter that is temporarily "Not Communicating", which is when the server most needs a backfill.

**Fix:** Size the array from the feature limits (>= 40, or allocate dynamically from `reply->elements/2`), log when truncating, and validate command meter names against configuration, not against the communicating list.

**Verification:** Confirmed.

### C4. ACK SEQ_NUM is a fixed number per command type [High]
**Location:** `cmd_resp.c:229` `int ack_msg_reply(int seq_num, char *out_buf)`; call sites `mqtt_connect.c:3822-3853`.

**Problem:**
```c
if (!strcmp(cmd.type, "GetDay"))   msg_size = ack_msg_reply(1001, output_msg);
else if (!strcmp(cmd.type, "FetchDay")) msg_size = ack_msg_reply(1002, output_msg);
... Reset 1003, set_cfg 2004, get_cfg 2102, ReadModbus 3001, trans mode 1009
```
These are the example SEQ_NUMs from the spec, copied as constants. Spec 5.3: "SEQ_NUM - Echoes the SEQ_NUM of the acknowledged message - the correlation key." The API takes an `int`, so it cannot even carry `cmd.transaction` (a string).

Also the ACK is sent only after the meter-name check (`mqtt_connect.c:3782-3800` returns before the ACK), while spec 5.1 says "The ACK is always step 1".

**Failure scenario:** Server sends GetDay with SEQ_NUM 4711; DCU ACKs 1001. Server never sees an ACK for 4711, retries/timeouts; with two GetDays in flight both ACKs are indistinguishable.

**Fix:** `ack_msg_reply(const char *seq_num, ...)` and pass `cmd.transaction`; send the ACK before any validation.

**Verification:** Confirmed (spec section 5.3 and code).

### C5. FetchDay timeout leaves the server and the poller inconsistent [Medium]
**Location:** `main.c:1016-1021`; `mqtt_connect.c:2629-2642` (`fetchday_reset_state`), `:2644-2700` (`read_redis_resp`).

**Problem:**
1. On timeout only `LOG_ERROR` and state reset. No `failure_resp_msg` is sent, so the server got an ACK and a "SUCCESS"-less silence (extends F14). `cpy_cmd` may already be overwritten by later commands, so the original transaction is not even kept for a reply.
2. The OD request already queued to the poller (`generate_redis_list` -> `LPUSH web_od_command`) is not withdrawn.
3. `DEL mqtt_command_resp` only drops replies that already arrived. A reply that the poller pushes after the timeout stays in the list (`check_redis_resp` is 0, so nobody reads it). The next FetchDay sets `check_redis_resp = 1`, and `read_redis_resp` `LPOP`s the stale reply first; it uses the meter/dates from the reply (`:2700-2720`) with no check against the current request, and the generated files carry the new request's SEQ_NUM.

**Failure scenario:** FetchDay A (meter M1, 5 days) times out at 600 s while the poller is still reading; the poller finishes at 700 s. Next day FetchDay B (meter M2) is answered first with M1's data labelled with B's SEQ_NUM.

**Fix:** Keep the pending request (transaction, meter, dates, broker) in a struct; on timeout send failure_resp_msg with that transaction; put a request id into the OD request and drop replies whose id does not match; ask the poller to cancel (or clear `web_od_command`) on timeout.

**Verification:** Confirmed in code; the late-reply case is Plausible (depends on poller timing).

### C6. FetchDay timeout does not run while the FetchDay broker is down [Medium]
**Location:** `main.c:1013-1015`.

**Problem:**
```c
int fd_up = (Fetchday_cmd_broker == 0) ? up1 : up2;
if (!fd_up)
    check_redis_resp_since = monotonic_sec();
```
While the broker that sent the FetchDay is not ready, the timer is restarted every loop. If that broker stays down (failover, disabled, certificate expired) the state never clears. Meanwhile the other broker keeps publishing cyclic files, and `file_gen_main.c:905-908` still writes `SEQ_NUM = cpy_cmd.transaction` while `check_redis_resp` is set, which is exactly the F7 symptom the timeout was meant to stop. A broker that flaps at least once every 10 minutes has the same effect.

**Failure scenario:** FetchDay arrives via mqtt2; mqtt2 goes down for a day; all cyclic INST/profile messages via mqtt1 carry the old FetchDay SEQ_NUM for that day.

**Fix:** Time out on absolute elapsed time since the request (monotonic), independent of broker state; on timeout reply on whichever broker is up (or drop the reply), and stop using `check_redis_resp` to choose the cyclic SEQ_NUM at all (pass it explicitly).

**Verification:** Confirmed.

### C7. Health message fields do not match the spec v0.20 [Medium]
**Location:** `health_status.c:987`, `:988`, `:191`, `:193-214`, `:345-349`, `:1042-1049`.

**Problem (each traced to the producer or the spec):**
- `"SEQ_NUM": "0001"` hard-coded (`:987`). Spec 2.2: "16 bit unsigned running integer"; it is also the correlation key the server ACKs. All health messages look identical to a server that de-duplicates by SEQ_NUM.
- `ACTIVE_SINCE` is a duration `"3d 4h 5m 6s"` built from `dcu_info.dcu_uptime` seconds (`dcu_mon_proc/dcu_health.c:166` writes `%ld` seconds). Spec: timestamp `"2025-09-20 16:35:29"`. (`"%0dh"` has no width, so it is not zero-padded either.)
- `TIME_SYNCHED` is the raw `ntp_cfg.ntp_sync_time` (`:191`), which the web side sets to a timestamp or `"Not_set_yet"` (`www/redis_worker.py:1214`). Spec: `"YES"/"NO"`.
- Per-meter `TIME_SYNCHED` and `REMOTE_ACCESS_ENABLED` are always `"NO"`, `VPN_IP` always `""` (`:348-349`), not measured.
- Spec v0.20 per-meter fields `DRIFT`, `DEMAND_PERIOD`, `PROFILE_CAP_PERIOD` are missing; Modbus devices ("NA" values per spec 3.1) are not listed.
- Key is `"DATATYPE"` (`:988`); spec says `"DATA_TYPE"`. All other generators also write `DATATYPE` (`file_gen_main.c:916, 1602`, `modbus_json_export.c:980`), so confirm which one the server parses before changing.

**Failure scenario:** Server shows DCU as "time not synchronised"/garbage, never sees per-meter drift, and may drop repeated SEQ_NUM 0001 messages.

**Fix:** Running 16-bit counter shared with other cyclic messages; ACTIVE_SINCE = now - uptime formatted as timestamp; TIME_SYNCHED = YES/NO from `ntp_sync_status`; fill or omit unmeasured fields per spec; align DATA_TYPE key with the server.

**Verification:** Confirmed against spec text and producers.

### C8. command_reply does not echo the request DATA keys [Medium]
**Location:** `cmd_resp.c:136-197` (`build_cmd_reply`), echo code commented out at `:158-180`.

**Problem:** Spec 4.3.2: "The reply echoes SEQ_NUM, COMMAND_TYPE, DATA_TYPE, and the request's DATA keys" (METER, DATE / START_DATE / END_DATE). The reply contains only DCU, CMD_STATUS, CMD_MSG. For Reset, `DATA_TYPE` is emitted as `""` although the spec says Reset carries no DATA_TYPE.

**Failure scenario:** Server with several GetDay requests for different meters cannot match "Invalid meter name"/FAILED replies to the meter if it keys on METER/DATE.

**Fix:** Re-enable the echo (it was correct), add DATA_TYPE only when present.

**Verification:** Confirmed against spec.

### C9. Log mutex is held across blocking I/O; netlog poll/send not synchronised [Medium]
**Location:** `dbg_log.c:361-408`, `:44-72`, `:290-293`; `main.c:992`, `main.c:1657`.

**Problem:** The F5 fix serialises the whole of `log_write`, including:
- `diag_redis()` reconnect: `redisConnectWithTimeout` 200 ms + `HGET` 200 ms + `LPUSH` 200 ms per line when diag is on (timeouts set at `:62-70`);
- `dcu_netlog_send()` (network logger, implementation not in repo);
- `printf` to stdout (blocking if stdout is a serial console), `stat()` of the cfg file, `stat()`+`fflush` of the log file.

Paho's receive/send threads log (`mqtt_trace_callback` at `MQTTASYNC_TRACE_PROTOCOL`, `main.c:120-123, 1491-1492`) and therefore queue behind a worker that is waiting on Redis or the network. Paho invokes the trace callback with its own internal mutex held, so a stall in `log_write` stalls Paho's other threads too.

`dcu_netlog_poll()` (`iec104_log_sink_poll_network`, `dbg_log.c:290-293`) runs from the main thread and the worker without `g_log_lock`, concurrently with `dcu_netlog_send()` under the lock. If netlog has no internal lock this is a data race on its socket/state; if it does have one and logs through `log_write` while holding it, the order netlog-lock -> g_log_lock vs g_log_lock -> netlog-lock is a deadlock.

**Failure scenario:** Redis momentarily slow (RDB save on the i.MX6UL) with diag enabled: each log line costs up to ~600 ms, Paho threads block, keep-alive PINGs are late, the broker drops the connection.

**Fix:** Format under the lock but do I/O outside it: push lines into a bounded in-memory ring and let one logger thread (or the worker) write file / Redis / netlog. At minimum take `g_log_lock` inside `iec104_log_sink_poll_network()` and call it from one thread only, and set the Paho trace level to ERROR in production.

**Verification:** Plausible (blocking path confirmed; deadlock depends on `dcu_netlog.c`, not available).

### C10. Log volume: hours of history at most, every line to netlog, flash wear [Medium]
**Location:** `main.c:1003` (`[STATE]` every loop), `main.c:1196-1202` + `:947-957` (7-line timer box per ready broker every loop), `main.c:1491-1492` (Paho PROTOCOL trace), `dbg_log.c:266-270` (`dbgloglevel = 1` when `debuglog.cfg` is missing), `health_status.c:421-434, 542-555, 1129` (unconditional `printf` per meter_status field and the whole health JSON every cycle).

**Problem:** With one broker up the worker logs ~8 lines every 3 s, i.e. ~230 000 lines/day (~25 MB at ~110 B/line) before Paho traces and per-message logs. `LOG_MAX_SIZE_BYTES` 2 MB with `LOG_MAX_FILES 2` keeps one backup, so only ~3-4 hours of history survive. Every line is also sent through `dcu_netlog_send` (and to the diag list when enabled, F28). Logging is on by default because a missing cfg file forces level 1.

**Failure scenario:** Field fault at night is gone from the logs by morning; continuous small writes + `fflush` per line wear the flash over months; netlog traffic over GPRS if the sink is remote.

**Fix:** Log `[STATE]`/timers only on change or every N minutes; default `dbgloglevel` to 0 (or WARN) when the cfg is missing; Paho trace off/ERROR; remove the debug `printf`s; larger rotation size or more files on the actual storage.

**Verification:** Rate Confirmed by reading the loop; storage/wear impact Plausible (log directory medium not visible).

### C11. Two hiredis header trees can be mixed in one binary [Medium]
**Location:** `general.h:16` `#include <hiredis.h>`; `health_status.c:18`, `json_helper.h:3`, `get_set_cfg.h:3` `#include <hiredis/hiredis.h>`; `Makefile:10` `-I../Tools/redis-bin/include ... -Iinclude`.

**Problem:** `<hiredis.h>` can only resolve under `../Tools/redis-bin/include/` (the repo has no `include/hiredis.h`). `<hiredis/hiredis.h>` resolves to `../Tools/redis-bin/include/hiredis/` if that exists, otherwise to the repo copy `include/hiredis/hiredis.h` (v1.3.0). Both use the same include guard, so whichever is included first in a TU wins: `health_status.c` and the json_helper users pick `<hiredis/hiredis.h>` first, everything else `<hiredis.h>`. `redisReply` layout differs between hiredis 0.x (`type, integer, len, str, elements, element`) and 1.x (`type, integer, dval, len, str, vtype[4], elements, element`).

**Failure scenario:** If `../Tools/redis-bin` is an older hiredis and has no `hiredis/` subdir, `health_status.c`/`modbus_json_export.c`/`get_cfg_request.c` are compiled against 1.3 structs but linked to the 0.x library: `reply->str` reads the wrong field -> garbage or SIGSEGV in health and Modbus every cycle.

**Fix:** One include style (`<hiredis/hiredis.h>`), one `-I` that matches the linked library, delete `include/hiredis/` or make it the only copy; add a static assert on `HIREDIS_MAJOR`.

**Verification:** Plausible (`../Tools` is not in the repo).

### C12. MQTT config is read once and Redis errors read as "disabled" [Medium]
**Location:** `main.c:140-146` (`redis_get_int` returns 0 on any non-string reply), `main.c:1382-1438`, `main.c:1495-1520`.

**Problem:** `mqtt_module_start()` loads both broker configs once. A NULL reply or an error reply (e.g. `-LOADING Redis is loading the dataset in memory` right after Redis restarts, or a broken ctx) makes `enable_mqtt = 0`, intervals 0, topics empty, `primary = 0`. Nothing re-reads it. The worker keeps running and keeps feeding the heartbeat (`send_hc_msg`), so dcu_mon never restarts it.

**Failure scenario:** Power-up where re_mqtt_proc starts while Redis is still loading: both brokers "disabled", DCU never connects, health stays green locally, and no remote command can fix it (no broker). Stays offline until someone restarts the process.

**Fix:** Distinguish "missing" from "error"; on error retry with backoff before starting the worker (or exit non-zero so dcu_mon restarts). Re-read config on a `cfg_changed` flag.

**Verification:** Plausible (depends on start order relative to Redis readiness).

### C13. Cyclic profile always covers wall-clock "today" [Medium]
**Location:** `main.c:1081-1090`.

**Problem:** `today_date` comes from `time(NULL)` at the moment the profile cycle fires. The blocks recorded after the last cycle before midnight belong to "yesterday" and are never included in a later cyclic message; the first cycle after midnight sends an almost empty day. The boundary also moves with the DCU clock (seen 50 s off) rather than meter time.

**Failure scenario:** Interval 30 min: blocks 23:30-24:00 of every day are never published cyclically; the server has to notice and GetDay them.

**Fix:** Track the last published block timestamp per meter and send everything newer, or on the first cycle after midnight also send the previous day.

**Verification:** Confirmed in code; impact Plausible (depends on server backfill behaviour).

### C14. `load_active_meters()` leaks error replies [Low]
**Location:** `main.c:152-155`.
```c
if (!reply || reply->type != REDIS_REPLY_ARRAY)
    return;          // reply (ERROR/NIL) never freed
```
Called every 3 s loop while a broker is up. During any Redis error period (LOADING, OOM, WRONGTYPE) the process leaks a reply per loop.
**Fix:** `freeReplyObject(reply)` before return. **Verification:** Confirmed.

### C15. Implicit declarations of pointer-returning functions; no `-Werror` [Low]
**Location:** compiler output for `main.c:1028, 1129, 1165, 1285, 926, 992`, `cmd_resp.c:146, 233`, `health_check.c:49`, `dbg_log.c:219`; `Makefile:12`.

**Problem:** `modbus_export_json` (returns `char *`), `redis_hget` in cmd_resp.c (returns `char *`) and `basename` are called without prototypes, so the compiler assumes `int` and converts. Works only because ARM32 has 32-bit `int` and pointers; `build_health_status_json` has no prototype at all (general.h only declares the dead `build_health_status_xml`). This is the same bug class as F4.
**Fix:** Add the prototypes (`#include "modbus_json_export.h"`, `<libgen.h>`, declare `char *redis_hget(...)` in general.h), and build with `-Werror=implicit-function-declaration -Werror=int-conversion -Werror=incompatible-pointer-types`. **Verification:** Confirmed (gcc warnings).

Also seen while following calls (outside scope, same class): `file_gen_main.c:1803, 1840` `LOG_ERROR(stderr, "...")` passes a `FILE *` as the level string.

### C16. Makefile: hard-coded paths, objects in foreign trees, no hardening [Low]
**Location:** `Makefile:6, 8, 10, 21`; `general.h:11-13`.

**Problem:** CC is `/home/imx6ul/Toolchain/...`, base path and two source files are under `/home/vishnu/Projects/REConnect/...`; `OBJ = $(SRC:.c=.o)` writes `dcu_netlog.o` and `hc_heartbeat.o` into `/home/vishnu/...`, and without header dependencies a stale `.o` built by another module with different flags is reused. On this machine those headers are compile stubs (`dcu_netlog.h`: "STUB for compile-check only"; `hc_heartbeat.h` declares `void hc_update` while the real `dcu_mon_proc/hc_heartbeat.c:21` returns `int`), so what is reviewed is not what ships. No `-g` (unusable cores), no `-fstack-protector-strong`, no `-D_FORTIFY_SOURCE=2` (which would trap the `sprintf` overflows in L10/F3 at runtime). No `.PHONY`.
**Fix:** `CROSS ?=`, `BASEPATHDIR ?=` variables, vendor or submodule netlog/heartbeat sources, build into `build/`, add `-MMD -MP`, hardening flags, and `-g` with a stripped copy for the target. **Verification:** Confirmed.

### C17. Diag throttles use wall-clock time; NIL flag keeps diag on [Low]
**Location:** `dbg_log.c:46-60`, `:140-152`.

**Problem:** `diag_redis()` and the 5 s `diag_enable` refresh use `time(NULL)`. After a backwards NTP step (`now - last_try` negative) reconnects and refreshes stop until the clock catches up. `monotonic_sec()` already exists in general.h. If the `diag_enable` field is deleted (HGET returns NIL, `reply->str == NULL`) the cached value is kept, so diag stays enabled and `LPUSH` continues (combines with F28).
**Fix:** Use `monotonic_sec()`; treat NIL as 0. **Verification:** Confirmed.

### C18. Rotation failure disables file logging permanently; per-line `perror` [Low]
**Location:** `dbg_log.c:179-203`, `:391-395`, `:266-270`.

**Problem:** If `fopen` after rename fails (directory full, read-only remount), `g_log_fp` is NULL and `log_write` returns early forever; nothing retries. When `debuglog.cfg` is missing, `perror("stat")` prints to stderr on every log line and `fopen` is attempted on every line (`dbgcfgtime` stays 0).
**Fix:** Retry `fopen` every N seconds; only warn once. **Verification:** Confirmed.

### C19. 32-bit `time_t` overflow in `PUB_DUE` [Low]
**Location:** `main.c:917-918`.
`(time_t)(interval_min) * 60` is a signed 32-bit multiply on this target. Intervals come unvalidated from set_cfg (L5); e.g. `HEALTH_INTERVAL` 50000000 gives 3e9 -> negative -> "due" every 3 s loop, flooding the broker.
**Fix:** Clamp intervals at load (1..1440 min) and compute in `int64_t`. **Verification:** Confirmed (needs an absurd but accepted value).

### C20. `dcu_netlog_send()` called before `dcu_netlog_init()` [Low]
**Location:** `main.c:1632-1648`.
`redis_connect()` logs (`file_gen_main.c:59, 65`) and the first `log_write` triggers `checkDbgStatus()`'s `LOG_INFO`, both before `dcu_netlog_init(redis_key)` at `main.c:1648`; `log_write` calls `dcu_netlog_send` unconditionally (`dbg_log.c:388`). On the Redis-failure exit path `dcu_netlog_init` is never called at all.
**Fix:** Call `dcu_netlog_init` first (or guard sends with an initialised flag). **Verification:** Plausible (real netlog not available).

### C21. Header hygiene with real effect [Low]
**Location:** `general.h` (no include guard), `general.h:142` vs `health_status.c:27` (`MAX_METERS` 20 vs 64, compiler warns "redefined"), `general.h:554` `static const char *mqtt_client_error_string(int rc);` (warning in every TU), `general.h:19` `#define MQTT_MODULE_H` (silently disables `mqtt_module.h`).
The two `MAX_METERS` values are why C3 (main) and F21 (health) have different limits for the same meters. Any second include of `general.h` in one TU would redefine typedefs (error in gnu99).
**Fix:** Include guard; one `MAX_DLMS_METERS` derived from the feature limits; move the static prototype into mqtt_connect.c; delete `mqtt_module.h`. **Verification:** Confirmed.

---

## 3. Items checked and found OK (no finding)
- Signal handling: `handle_signal` only writes `volatile sig_atomic_t stop_flag`; no logging, malloc or locks in the handler. Worker sleeps in 1 s slices and checks `stop_flag` between meters; `mqtt_cleanup` joins the worker before destroying clients and before `log_close`.
- Init/shutdown order of the logger: `log_write` before `log_init` is safe (`g_log_fp` NULL path); nothing logs after `log_close` (worker joined, Paho clients destroyed first). `pthread_once` makes the mutex usable from the first call on any thread.
- Fork/exec: only `file_gen_main.c:1807` (`fork` + `execlp("tar")` + `_exit`) and `system("reboot")`; the child never touches the log mutex or Redis.
- Main loop timing uses `CLOCK_MONOTONIC` for publish intervals, connect timeouts and the FetchDay timeout; wall-clock jumps do not stall or burst publishing (except C13, C17).
- Health JSON structure (braces, brackets, comma placement between meters across the three passes) is balanced; the invalidity comes only from string content (C1).
- `cmd_resp.c` replies are built with cJSON, so they are escaped correctly; the problems there are content (C4, C8) and L7.

## 4. Suggested order
1. C1 (escape or cJSON for health) and C2 (skip empty slots, unique ID) - fixes the bench failure.
2. C4 (ACK echoes SEQ_NUM), C3 (meter limit), F21 (index fix).
3. C5/C6 (FetchDay timeout semantics, failure reply, request id).
4. C9/C10 (logging I/O outside the lock, cut volume), F28.
5. C11/C15/C16 build fixes, so the compiler catches the rest.
