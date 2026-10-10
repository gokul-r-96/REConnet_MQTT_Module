# Review B: mqtt_connect.c lines 2450 to end (FetchDay / GetDay / command dispatch)

- Branch: `autofix/mqtt-high-2026-10-03`, HEAD `e670bb3`
- File: `Mqtt_Json_Format_V3.0_Dual_Broker/src/mqtt_connect.c` (4793 lines). Reviewed 2450-4794 line by line.
- `#if 0` block 3047-3231 (old read_redis_resp) and the commented-out code 3233-3442, 4115-4163, 4197-4284, 4296-4633, 4645-4732 were skipped. No active code depends on them.
- Followed into: main.c (worker loop, F7 timeout), file_gen_main.c (json_write_header, generate_mqtt_*_json, redis_hget, hscan_first_match), dlms_cdf_ls.c / _event.c / _mn.c / _billing.c, cmd_resp.c, get_cfg_request.c, set_cfg_request.c (trans_mode_request), include/general.h.
- Nothing was compiled or run. "Confirmed" means the path was traced in the source.

---

## 1. Status of earlier findings in scope

| ID | Status on HEAD | Evidence |
|----|----------------|----------|
| F4 (caller side) | **Partly fixed** | The callee no longer returns `(char*)-1` (get_cfg_request.c:1628-1636 now replies with `NUM_METERS 0`), so the strlen/free on -1 crash is gone. The caller at mqtt_connect.c:3978-3992 is unchanged: `strcmp(cmd.args[0], dcu_sn)` with no NULL check, and `dcu_sn` is leaked. See B1. |
| F6 | **Open** | The generators still choose the `*_od_*` table from the globals (dlms_cdf_ls.c:243, _mn.c:36, _event.c:106 and 209, _billing.c:45). The MN/EVENT/BILLING flags are cleared only on success (2920, 3001, 3024). The new `fetchday_reset_state()` runs only from the timeout, and only while `check_redis_resp == 1`. On those failure paths, read_redis_resp sets `check_redis_resp = 0` at 3040, so the timeout never fires and the flag stays 1 until the next successful request of the same type. `ls_cmd_redis_resp` is still 1 between days of a multi-day LS (set at 2816, cleared only at 2861), so cyclic LS reads and drops the OD table (dlms_cdf_ls.c:373-376). GetDay LS run during a FetchDay LS also reads the OD table. |
| F7 | **Partly fixed** | Fixed: `generate_redis_list` now returns 0/-1 (2604), and a queue failure clears the flag and sends `failure_resp_msg` (3923-3929). A 600 s timeout was added (main.c:1011-1022). Still open: (a) OD replies and cyclic files still take SEQ_NUM from `cpy_cmd`, and every command overwrites it (3768), see B4. (b) The timeout stops counting while the FetchDay broker is down, so the flag can stay set forever, see B6. (c) Replies are not matched to requests, see B3. (d) The LS failure paths (2770-2773, 2786-2790, 2837-2847) leave `check_redis_resp = 1` for up to 600 s, and the SEQ_NUM leaks onto cyclic messages during that time. |
| F12 | **Open** | `on_message_arrived` (4165-4195) still queues every message without checking `message->retained` or `dup`. Commands are not de-duplicated by SEQ_NUM. Reset (4092-4111) still runs `system("reboot")` without conditions. |
| F14 | **Partly fixed** (one path) | Only the FetchDay queue failure is now answered (3927). Still silent: parse failure (3761-3765, stderr only); GetDay ignores the return value of `parse_getday_cmd` (3878) and never sends a success or failure reply; GetDay LS stops at the first failed day after some files are already published (3580-3586); all generation failures in read_redis_resp (2837-2847, 2910-2921, 2987-3002, 3013-3025); GetDay/FetchDay with an empty `args[0]` get an ACK and then nothing (3860, 3903). `mqtt_send_file` is still `void` (1718). It now aborts on a failed chunk, but the caller cannot tell, and chunks still carry no sequence information. |
| L7 | **Partly fixed** | Fixed: generate_redis_list now returns 0, frees root/json_str on every path and handles a NULL `cJSON_Print` result (2591-2604). Still open: `data` leaks on the invalid-date return (2531: `cJSON_Delete(root)` runs before `data` is attached at 2589). `return;` in an int function remains at 3868, 3914, 3950, 3991, and control can fall off the end at 4113 (GetDay/FetchDay paths). cmd_resp.c:189/257/292 still `strcpy(out_buf, json)` with no NULL check. `dcu_sn` was fixed in get_cfg only at 1610-1613; the other 10 sites (135, 282, 439, 493, 626, 764, 895, 1073, 1387, 1488) still leak it and write it unescaped. |
| L11 | **Open** | mqtt_connect.c:749-753 still drops the oldest command when the queue is full, and 740-744 still truncates payloads to 4095 bytes. The server gets no reply in either case. The only change is a LOG_WARN. |

No other finding from the 03-Oct review is located at mqtt_connect.c >= 2450. F24 (key order, line 2360) is outside this range, but B13 depends on it.

---

## 2. New findings

Ranked by severity.

### B1. NULL `dcu_sn` passed to strcmp crashes the daemon on GetDay / ReadModbus / get_cfg / set_cfg / Reset [High]
**Location:** mqtt_connect.c:3863-3864 (GetDay), 3935/3946 (ReadModbus), 3978/3987 (get_cfg), 4022-4023 (set_cfg), 4095-4096 (Reset)

**Problem:** `redis_hget()` returns NULL when the reply is NULL, an error, or nil (file_gen_main.c:123-142). These five handlers then call `strcmp(cmd.args[0], dcu_sn)` without checking for NULL. The branch fixed only the FetchDay copy (3907-3909: `sn_ok = (dcu_sn != NULL && ...)`), and START/STOP_TRANS_MODE checks for NULL at 4058. The other five copies were left as they were. Every one of them also leaks `dcu_sn`.

```c
char *dcu_sn = redis_hget(ctx, "dcu_info", "serial_num");
if (strcmp(cmd.args[0], dcu_sn) != 0)      // dcu_sn may be NULL
```

**Failure scenario:** `dcu_info.serial_num` is missing (fresh or factory-reset DCU, key not yet written), or the Redis link has dropped (L1: no reconnect, so every `redisCommand` returns NULL afterwards). The server sends any get_cfg, set_cfg, GetDay, ReadModbus or Reset. The ACK goes out first (ack_msg_reply falls back to "UNKNOWN"), then strcmp dereferences NULL and the process gets SIGSEGV. A periodic get_cfg poll from MDAS then crashes the process on every restart.

**Fix:** One helper, `static int dcu_serial_matches(const char *req)`, that does the NULL check, compares and frees. Use it in all seven handlers.

**Verification:** Confirmed.

### B2. GetDay date range has no upper bound; LS loop blocks the worker thread for hours [High]
**Location:** mqtt_connect.c:3452 and 3529-3599 (`parse_getday_cmd`), 2459-2489 (`calculate_num_days`); dlms_cdf_ls.c:637-700

**Problem:** `num_days` comes from two server-supplied dates with no cap. The LS loop runs synchronously on the worker thread. Each day does read_meter_status (an HSCAN loop of up to 10000 pages), opens SQLite, writes a file and does a blocking publish (up to 10 s per chunk). `generate_load_profile_json` does not fail when a day has no data ("Keep generating JSON with empty BLOCK_PROFILE"), so days with no data still produce and publish a file. During the loop the worker runs no `worker_service()`, no `send_hc_msg()`, no cyclic publishing and no command processing. For MIDNIGHT the same unbounded `num_days` reaches `calloc(num_days, sizeof(MNSnapshot))` (dlms_cdf_mn.c:551; same issue as F16).

**Failure scenario:** GetDay LS with `01-01-1902` to `31-12-2037` (valid for a 32-bit `time_t` mktime) gives about 49,700 days. The worker publishes about 49,700 empty LS messages, one at a time, while cyclic data and heartbeats stop. dcu_mon then sees the heartbeat go stale and restarts the process part-way through (per the L10 analysis, about 180 s). Even a "reasonable" mistake such as a full year (365 days) blocks the worker for many minutes.

**Fix:** Validate the dates strictly (see B13). Reject ranges larger than the spec limit (for example, 31 days for LS) with `failure_resp_msg`. Move the per-day loop into the main loop (one day per iteration), or at least call `worker_service()` and `send_hc_msg()` inside it.

**Verification:** Confirmed (blocking path and missing cap). The dcu_mon restart is Plausible and depends on dcu_mon's timeout.

### B3. OD replies are never matched to the request, so a late reply is sent as the answer to the next FetchDay [High]
**Location:** mqtt_connect.c:2661-2742 (read_redis_resp), 2629-2642 (fetchday_reset_state), 2506 (`seq_no` is sent but never checked); main.c:1025-1031

**Problem:** generate_redis_list puts `"seq_no": cmd.transaction` into the request. read_redis_resp pops whatever is at the head of `mqtt_command_resp` and never compares its `seq_no` (or meter, or msg type) with the pending request. The list is cleared only by the timeout (`DEL` in fetchday_reset_state). read_redis_resp runs only while `check_redis_resp == 1`, so anything pushed while no request is pending stays in the list.

**Failure scenario:** FetchDay A (MIDNIGHT, meter X) takes longer than 600 s at the poller (meter retries). The timeout fires and DELs the list. The poller then pushes A's reply, which sits in the list. Hours later FetchDay B arrives (EVENT, meter Y). The first read_redis_resp pops A's reply, generates a midnight file for meter X, and publishes it with B's SEQ_NUM (taken from `cpy_cmd`). It then sets `check_redis_resp = 0`, so B's real reply stays in the list and is sent as the answer to request C. The server receives wrong data for every request from then on.

**Fix:** Keep the pending request (transaction, type, meter, dates) in a dedicated struct, separate from `cpy_cmd`. In read_redis_resp, drop any reply whose `seq_no` or `msg_type` does not match. `DEL mqtt_command_resp` before queuing a new FetchDay.

**Verification:** Confirmed on the DCU side (no matching is done). Whether the poller pushes replies late is Plausible.

### B4. `cpy_cmd` is overwritten by every command: OD replies get another command's SEQ_NUM, and EVENT uses another command's dates [High]
**Location:** mqtt_connect.c:3768 (`cpy_cmd = cmd;` for every parsed command), 2938-2945 (OD_EVENT dates from `cpy_cmd.args[2..3]`); file_gen_main.c:905-908

**Problem:** This is the remaining part of F7 and was not addressed by the branch. processServerMsg copies every incoming command, including get_cfg, set_cfg, ReadModbus and unknown types, into `cpy_cmd` before dispatching. json_write_header writes `cpy_cmd.transaction` as SEQ_NUM while `check_redis_resp` or `get_day_cmd` is set. The OD_EVENT handler builds the SQL date range from `cpy_cmd.args[2]` and `[3]`.

**Failure scenario:** The server sends FetchDay LS for 7 days (SEQ 100). On day 3, MDAS sends its periodic get_cfg MQTT_BROKER (SEQ 101). Days 3-7 of the LS reply are published with SEQ_NUM 101, and cyclic messages during the wait also carry 101. For a FetchDay EVENT followed by any other command before the poller answers, `args[2]`/`[3]` of that command (for example a set_cfg value) fail sscanf, `start_date` becomes "", and the query becomes `LIKE '%'`. The server then receives every event of the meter, capped at 1000, instead of the requested range.

**Fix:** Do not set `cpy_cmd` in processServerMsg. Store the pending OD request (see B3) and pass the transaction and dates explicitly to the generators and json_write_header.

**Verification:** Confirmed.

### B5. Multi-day LS progress state is left over on several paths, and a second FetchDay is accepted while one is pending [Medium]
**Location:** mqtt_connect.c:2623-2624, 2695, 2750, 2757-2765, 2837-2847, 3038-3041, 3905, 3921-3929

**Problem:** `ls_total_days` and `ls_completed_days` are reset only on completion (2867-2868) or by the timeout. Three paths clear `check_redis_resp` but keep them: a reply without `data` (2695), `num_days <= 0` (2750), and a reply with an unknown or non-LS `msg_type` that arrives mid-LS (3040). Because `check_redis_resp` is then 0, the timeout never resets them. A new LS request is detected only by `ls_total_days == 0` (2757). processServerMsg also starts a new FetchDay without checking whether one is in progress. `Fetchday_cmd_broker` is overwritten (3905) even before the serial check, so a rejected FetchDay from the other broker redirects the pending request's replies and timeout handling. A failed day (2837-2847) never increments `ls_completed_days`, so completion is never reached and the request waits for the full 600 s.

**Failure scenario:** LS FetchDay for 5 days. After day 2, a malformed or foreign reply clears `check_redis_resp`, leaving total=5 and done=2. The next LS FetchDay asks for 1 day. After its reply, done=3, which is less than 5, so `check_redis_resp` stays 1 and `ls_cmd_redis_resp` stays 1 for 600 s. During that time cyclic LS for all meters reads non-existent OD tables (empty cyclic LS is published), and cyclic messages carry the FetchDay SEQ_NUM.

**Fix:** Reject or queue a FetchDay while one is pending (reply busy). Call `fetchday_reset_state()` (or its non-DEL part) on every path that ends a request. Initialise `ls_total_days` from the request at queue time, not from the first reply. Count a failed day as done and report it to the server.

**Verification:** Confirmed.

### B6. The F7 timeout is suspended while the FetchDay broker is down, so the SEQ_NUM leak can last forever [Medium]
**Location:** main.c:1011-1016

```c
int fd_up = (Fetchday_cmd_broker == 0) ? up1 : up2;
if (!fd_up)
    check_redis_resp_since = monotonic_sec();
```

**Problem:** The timer is restarted on every loop iteration while the broker that received the FetchDay is not ready. In a dual-broker setup, if that broker stays down (link lost, or disabled by a later set_cfg MQTT_BROKER_mqttX enable=0), `check_redis_resp` never clears. json_write_header keeps writing `cpy_cmd.transaction` as SEQ_NUM on every cyclic instantaneous and profile file published on the other, healthy broker. Any OD flag already set (for example, `ls_cmd_redis_resp` mid-LS) also stays set, so F6 effects continue.

**Failure scenario:** FetchDay arrives on mqtt2. mqtt2 then drops for a day while mqtt1 stays up. For that whole day every cyclic message on mqtt1 carries the FetchDay SEQ_NUM instead of the cyclic counter.

**Fix:** Use an absolute deadline (for example, 600 s from the last progress, whatever the broker state). If the broker is down when the reply arrives, drop the reply or send it on the other broker. Better still, remove the dependence of json_write_header on `check_redis_resp` (pass the SEQ_NUM explicitly).

**Verification:** Confirmed.

### B7. SQL injection through GetDay EVENT category (`args[4]`) [Medium]
**Location:** mqtt_connect.c:3666-3699, then dlms_cdf_event.c:178-200 and 216-225

**Problem:** GetDay copies `cmd.args[4]`, server text truncated to 31 characters, into `event_type` and passes it unchanged to `read_event_data`. That function formats it into SQL with `'%s'`:

```c
"WHERE ... AND event_type = '%s'", start_date, next_date, event_type
```

(The FetchDay path whitelists "1".."6" at 2544, but GetDay does no validation.) `sqlite3_prepare_v2` compiles only the first statement, so stacked `DROP`/`DELETE` statements are not executed. The WHERE clause can still be rewritten. The same string also goes into the output path (dlms_cdf_event.c:743-746): a `/` makes fopen fail, and the server gets no reply.

**Failure scenario:** `"ARG": "1' OR '1'='1"` returns every event of the meter (up to 1000) regardless of date or category. Any legitimate value containing `'` makes prepare fail, and the server gets no reply (F14).

**Fix:** Whitelist `args[4]` to "1".."7" or "all", as FetchDay does. Bind values with `sqlite3_bind_text` instead of `snprintf` in all four dlms_cdf readers.

**Verification:** Confirmed (path traced; impact limited to read-only query manipulation).

### B8. OD_EVENT reply: `event_category` is used uninitialised; category 6 maps to "all" [Medium]
**Location:** mqtt_connect.c:2936, 2947-2978, 2982-2985

**Problem:** `char event_category[32];` is assigned only inside `if (cJSON_IsString(event_cat) ...)`. If the poller reply has no string `event_type`, uninitialised stack bytes are passed to `generate_mqtt_event_json`. There they are used in `strcmp(event_type, "all")`, in SQL (`event_type = '%s'`) and in the output filename, and they may not be NUL-terminated within 32 bytes. Separately, generate_redis_list accepts categories 1-6 (2544), but the reply mapping knows only cat1-cat5, so `event_data_cat6` becomes "all".

**Failure scenario:** A poller reply variant without `event_type` gives undefined behaviour: garbage SQL (no reply), a stack over-read into the filename, or rarely a crash. A category-6 request is filtered as "all", so the date range is applied without a category filter.

**Fix:** Initialise it to "all". Map with `sscanf(event_type, "event_data_cat%d", &n)` and range-check against what the reader supports.

**Verification:** Code defect Confirmed; trigger (poller omitting `event_type`) Plausible.

### B9. The ACK's SEQ_NUM is a fixed command code, not the request's transaction [Medium]
**Location:** mqtt_connect.c:3820-3854; cmd_resp.c:226-247

**Problem:** `ack_msg_reply(1001 | 1002 | 3001 | 2102 | 2004 | 1003 | TRANS_MODE_ACK_CODE, ...)` puts that constant in `"SEQ_NUM"`. The ACK never contains `cmd.transaction`. It is also sent before the DCU serial is validated, so a command for another DCU (on a shared topic) is ACKed and then rejected.

**Failure scenario:** The server sends two FetchDay requests (SEQ 500 and 501). Both ACKs carry `"SEQ_NUM":"1002"`, so the server cannot tell which request was acknowledged. If MDAS matches ACKs by SEQ_NUM, every request appears unacknowledged, and the server may retry, which leads to duplicate execution (see F12).

**Fix:** Add the request transaction to the ACK (keep the code as a separate field if the spec wants it). Send the ACK after the serial check.

**Verification:** Confirmed in code. The impact depends on the MDAS protocol spec, which was not checked: Plausible.

### B10. calculate_num_days accepts invalid dates, and the poller receives the raw strings [Medium]
**Location:** mqtt_connect.c:2459-2489, 2522-2537, 2568-2575

**Problem:** `sscanf("%d-%d-%d")` has no range checks and no end-of-string check. mktime normalises month 0/13, day 0/32 and the like without complaint. `"30-03-2026xyz"` is accepted. Only `num_days < 0` is rejected (2527), so `end = start - 1` gives `num_days = 0`, which is sent to the poller. The raw `args[2]`/`args[3]` are forwarded (2522-2523), while `num_days` is computed from the normalised values, so the poller and the DCU can disagree about the range. For BILLING, a failed sscanf sends `"startdate": ""` with no error to the server. (No `months[m]` out-of-range index was found in this path; generate_billing_json validates 1..12 at dlms_cdf_billing.c:817.)

**Failure scenario:** FetchDay MIDNIGHT `"01-10-2026"` to `"30-09-2026"` gives num_days 0. The request is queued, and the reply goes to generate_midnight_json, which rejects `num_days <= 0`. `midnight_cmd_redis_resp` stays 1 (F6), and from then on cyclic midnight data reads the OD table. `"31-13-2026"` is sent to the poller as written, while the DCU computes it as 31-01-2027.

**Fix:** Parse strictly (`%2d-%2d-%4d%n` and check the end), check 1<=m<=12, 1<=d<=days_in_month, a sane year range, start <= end and the maximum span. Then forward the normalised dates, not the raw strings. Reply `failure_resp_msg` on any rejection.

**Verification:** Confirmed.

### B11. Memory leaks on every command [Low]
**Location:** mqtt_connect.c:2452 (`cJSON_Delete(root)` commented out), 3794-3800, 3860-3930, 3866-3868, 3912-3914, 3948-3950, 3989-3991, 4025-4027, 4053, 2505/2531

**Problem:** parse_cmd_request leaves the parsed tree in `cmd.root`. It is freed only on the unknown-type, ReadModbus, get_cfg, set_cfg and TRANS paths that reach the end. GetDay, FetchDay, invalid-meter and all unknown-serial early returns leak the whole tree. `dcu_sn` is leaked in GetDay, ReadModbus, get_cfg, set_cfg, START/STOP_TRANS_MODE and Reset. generate_redis_list leaks `data` on an invalid date.

**Failure scenario:** Each GetDay/FetchDay leaks its parsed tree (roughly 1-2 KB) plus `dcu_sn`. At 1,000 on-demand requests per day, that is about 1-2 MB per day of unrecoverable heap on a 256 MB i.MX6UL, until the OOM killer or a restart.

**Fix:** A single exit path in processServerMsg that does `cJSON_Delete(cmd.root)` and `free(dcu_sn)`.

**Verification:** Confirmed.

### B12. SEQ_NUM silently truncated to 15 characters and written unescaped into OD and cyclic JSON [Low]
**Location:** include/general.h:315 (`CMD_TRANSACTION_MAX_LEN 16`), mqtt_connect.c:2309-2310; file_gen_main.c:908

**Problem:** A longer SEQ_NUM is cut to 15 characters without an error, so every reply (build_cmd_reply) and every OD file carries a SEQ_NUM the server never sent. json_write_header prints `cpy_cmd.transaction` with `"%s"` and no escaping. A `"` or `\` in the SEQ_NUM breaks every OD file, and every cyclic file while `check_redis_resp` is set (up to 600 s, or forever per B6).

**Failure scenario:** The server uses 17-digit timestamp SEQ_NUMs (`20261003120000123`). The replies carry `202610031200001`, and the server cannot match them.

**Fix:** Reject an over-long SEQ_NUM with a failure reply, or enlarge the buffer to the spec maximum. Escape the value, or build the header with cJSON.

**Verification:** Truncation Confirmed. That the server sends SEQ_NUMs longer than 15 characters is Plausible (spec not checked).

---

## 3. Other notes (checked, no finding)

- `generate_redis_list` EVENT `sprintf(event_cat, "event_data_cat%s", cmd.args[4])` is safe, because `args[4]` is whitelisted to one digit just before (2544).
- GetDay/FetchDay meter serials are checked against `meter_serials[]` (3778-3800) when `arg_count > 1`. This also blocks glob characters in the HSCAN MATCH and SQL in the table names. With `arg_count <= 1`, `args[1]` is "" and the lookup simply fails.
- ReadModbus `regs[].count/type` from the server are not used for buffer sizes (modbus_json_export.c:1221-1278). `reg_count` is capped at 64.
- trans_mode_request (set_cfg_request.c:1571-1679) validates METER characters, DUR and DATA_TYPE before writing. Only the `dcu_sn` leak at mqtt_connect.c:4053 remains (B11).
- Stack: processServerMsg (128 KB `output_msg`) calls parse_getday_cmd, which calls mqtt_send_file (128 KB buffer). That is more than 256 KB on the worker stack. This is fine with the default 8 MB pthread stack (main.c:1535 passes NULL attr), but it would break if a stack size is ever set. `read_redis_resp` declares a 128 KB `output_msg` and `msg_size` that are never used.
- `update_mqtt_time` (4743) and `parse_getday_cmd` (3533) use non-reentrant `localtime()` on the worker while Paho threads log. This is covered by F5.
- `cpy_cmd = cmd` also copies `root`/`data` pointers that become dangling after `cJSON_Delete(cmd.root)`. Nothing reads `cpy_cmd.root` today; this is a hazard only.
