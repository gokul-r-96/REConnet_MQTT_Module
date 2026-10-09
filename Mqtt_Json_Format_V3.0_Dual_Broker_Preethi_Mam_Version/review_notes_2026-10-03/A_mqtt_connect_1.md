# Review A: mqtt_connect.c lines 1-2450 (HEAD of autofix/mqtt-high-2026-10-03)

Date: 03 Oct 2026. Reviewer: Claude (senior embedded C review, read-only).
Scope: `Mqtt_Json_Format_V3.0_Dual_Broker/src/mqtt_connect.c` lines 1-2450 (connection manager, Paho callbacks, failover, TLS, publish, subscribe, message arrival, command queue, `parse_cmd_request`). I followed calls into `main.c`, `dbg_log.c`, `file_gen_main.c`, `cmd_resp.c` and `processServerMsg`/`read_redis_resp` where a finding needed it. I also reviewed the branch diff (`git diff main..HEAD`) for `mqtt_connect.c`, `dbg_log.c`, `main.c` and `general.h`.

Line numbers are HEAD unless marked `main:`. Lines 1-2450 moved by only +1 relative to `main` (one new global at line 30); every branch code change in `mqtt_connect.c` is at line 2491 or later.

Nothing was compiled or run. Paho behaviour was checked against the Paho 1.3 `MQTTAsync.h` in `/usr/include`. The `dcu_netlog` header in the build tree is a stub, so anything that depends on netlog internals is marked Plausible.

Four of the new findings (A3, A4, A5 and part of A1) are in code past line 2450 (`processServerMsg`/`read_redis_resp`). I found them while tracing the command path and reviewing the branch fixes. They are marked **[outside 1-2450]** so they can be deduplicated against the second-half review.

---

## 1. Status of prior findings (03-Oct review) in scope

| ID | Status | Evidence (HEAD) |
|---|---|---|
| F5 | **Fixed** (residual noted) | `dbg_log.c:361/392/408`: a recursive mutex now covers all of `log_write` and `log_close`. `dbg_log.c:136-157`: diag `HGET`/`LPUSH` use the private `g_diag_ctx`, not the global `ctx`. `dbg_log.c:87`: `localtime_r`. No Paho callback in `mqtt_connect.c` touches `ctx` any more. Residual: (a) `dcu_netlog_send` runs under the log lock, but `dcu_netlog_poll` runs on the worker (`main.c:992`) and on the main thread (`main.c:1657`) without it, so netlog state is still unsynchronised (see L13). (b) The lock is held across up to 200 ms Redis connect plus 200 ms per HGET/LPUSH. Paho uses one shared receive thread for both clients, so every Paho log line (including PROTOCOL-level traces, `main.c:1492`) can wait behind it. The trace level is still PROTOCOL. (c) The throttles use the wall clock, see A8. |
| F6 | **Open** (mitigated only for the stuck-wait case) | The generators still choose OD tables from the globals (`dlms_cdf_mn.c:36`, `dlms_cdf_ls.c:243`, `dlms_cdf_event.c:106/209`, `dlms_cdf_billing.c:45`; header label at `file_gen_main.c:896`). The flags are still cleared only on success (`mqtt_connect.c:2906`→`2920`, `2980`→`3001`, `3009`→`3024`). On a generation failure the flag stays 1 after `check_redis_resp` returns to 0. The new `fetchday_reset_state()` (`2627-2640`) clears the flags only when a FetchDay times out. |
| F7 | **Partly** | Fixed: `generate_redis_list` now returns 0/-1 (`2603`). A queueing failure resets the flag and replies with failure (`3921-3929`). A 10-minute timeout was added (`main.c:1011-1021`). Still open: (1) `json_write_header` still writes `cpy_cmd.transaction` while `check_redis_resp==1` (`file_gen_main.c:905-908`), so cyclic files carry the FetchDay SEQ_NUM for the whole wait, which can be unbounded (A2). (2) `cpy_cmd` is still overwritten by every incoming command (`3768`, `2493`). (3) The timeout opens a new wrong-data path (A1). |
| F11 | **Open** | `configure_tls` (`1480-1524`) never sets `ssl->verify`. The Paho initializer value is 0 (`/usr/include/MQTTAsync.h:1179`), so the hostname is not checked. `insecure` still disables certificate auth (`1492`). Redis paths are still passed raw to Paho (`1496/1502/1508`); `MQTT_1_CERTS_LOC`/`MQTT_2_CERTS_LOC` (`6-7`) are unused. |
| F12 | **Open** | `on_message_arrived` (`4165-4194`) queues every message and never checks `message->retained` or `message->dup`. Nothing de-duplicates by SEQ_NUM. See also A7. |
| F14 | **Partly** | FetchDay queueing failure now replies (`3921-3929`). Still open: a parse failure only goes to stderr (`3761-3765`). `mqtt_send_file` is still `void` and still aborts mid-file with no status to the caller (`1718-1779`); callers still `remove()` the file and GetDay still reports success. Chunks are still unframed 128 KB slices (`1746-1758`). |
| F24 | **Open** | `2361-2376` unchanged: only string values are stored, in JSON key order, and `args[0..4]` are positional. |
| F25 | **Open** | `sub_on_failure` (`1906-1913`) and the immediate-error branch (`1955-1959`) only log. The broker stays CONNECTED with no command subscription. |
| L7 (mqtt_connect.c part) | **Partly** | `generate_redis_list` fixed: return value, NULL `json_str`, and `root` freed before the reply check. Still open: the `num_days < 0` path (`2527-2532`) deletes `root` but not the unattached `data` object (leak), and `processServerMsg` still has `return;` in an `int` function (`3868`, `3914`, `3950`, `3991`). |
| L11 | **Open** | `741-745`: a payload of 4096 bytes or more is truncated (invalid JSON, so parse fails with no reply). `749-754`: when the queue is full the oldest command is dropped with only a log line. |
| L12 | **Open** | `438-439`: `in_callback==0` is sampled under `g_brk_mutex`, the lock is released at `441`, and `MQTTAsync_destroy` runs at `361` with no `MQTTAsync_disconnect`. In the "repeated publish timeouts" and "watchdog" CLOSING cases (`712`, `427`) the TCP session may still be alive, so Paho's receive thread can enter `on_message_arrived` inside that window. Paho releases its own mutex around the user callback, so this race is more reachable than the earlier "dead link only" assumption. `mqtt_conn_manager_shutdown` (`498-537`) destroys without checking `in_callback` at all. |
| L13 | **Open** | Netlog dual-thread polling is unchanged (`main.c:992`, `main.c:1657`). `on_connect_success` writes `last_mqtt*`/`current_active_mqtt*` from the Paho thread with no lock (`943-966`). `current_active_mqtt1/2` are now write-only (dead). |

---

## 2. New findings

### A1. FetchDay replies are not matched to their request: after a timeout or an overlapping FetchDay, data for the wrong meter or dates is sent under the next request's SEQ_NUM [High]
**Location:** `mqtt_connect.c:2627-2640` (`fetchday_reset_state`, new on this branch), `2660-2700` (`read_redis_resp`: `LPOP mqtt_command_resp` with no correlation check), `3040`, `3905`, `3921-3923` **[outside 1-2450]**; `main.c:1011-1031`

**Problem:** `generate_redis_list` puts `"seq_no"` into the OD request, but `read_redis_resp` never checks which request a popped response belongs to. It takes the meter and dates from the response, and the SEQ_NUM from `cpy_cmd` (the latest command). The new timeout runs `DEL mqtt_command_resp` once and then forgets the request. A poller reply that arrives later stays in the list and is consumed by the *next* FetchDay. That request's own reply is then left behind for the one after it. A second FetchDay accepted while one is pending gives the same mix-up: there is no busy check, and `Fetchday_cmd_broker = broker` (`3905`) is set before even the serial check, so a wrong-serial FetchDay arriving on the other broker also moves where the pending request's replies go.

**Failure scenario:**
1. FetchDay LS for meter A arrives with SEQ 101. Meter A is offline, so after 600 s the worker logs the timeout, clears state and DELs the list.
2. 20 minutes later the poller reaches meter A and LPUSHes A's response.
3. Hours later the server sends FetchDay MIDNIGHT for meter B with SEQ 202.
4. The first `read_redis_resp` pops A's stale LS response, generates meter A's LS file and sends it with `SEQ_NUM 202` (`file_gen_main.c:905-908`), then sets `check_redis_resp=0` (`3040`).
5. B's real response stays in the list and is sent as the answer to the next FetchDay.

The off-by-one persists across every following FetchDay until the list happens to drain. The server stores meter A's data as meter B's (or for the wrong dates).

**Suggested fix:**
- Have the poller echo `seq_no` and the meter, and in `read_redis_resp` drop (and log) any response whose `seq_no` is not the pending request's.
- Keep the pending transaction in a dedicated struct, not in `cpy_cmd`.
- Reject a FetchDay with a "busy" reply while one is pending, and set `Fetchday_cmd_broker` only after validation.
- On timeout, remember the abandoned `seq_no` so a late reply for it is discarded.

**Verification:** Confirmed (path traced in code). How often it happens depends on poller latency for offline meters.

### A2. F7 timeout is suspended while the FetchDay broker is down, so the FetchDay SEQ_NUM stays on the other broker's cyclic data indefinitely [Medium]
**Location:** `main.c:1011-1021`; `file_gen_main.c:905-908`

**Problem:** `if (!fd_up) check_redis_resp_since = monotonic_sec();` restarts the 10-minute timer on every loop while the broker that sent the FetchDay is down. All that time `check_redis_resp==1`, and `json_write_header` writes `cpy_cmd.transaction` into every cyclic INST and profile file published on the *other*, healthy broker.

**Failure scenario:** A FetchDay arrives on mqtt2 with SEQ 555, and mqtt2 then drops for 6 hours (or an operator disables it). For those 6 hours every cyclic message on mqtt1 carries `"SEQ_NUM":"555"` instead of the running counter. The server can treat them as OD replies to request 555 or drop them as duplicates. This is exactly the F7 symptom the fix was meant to bound. Even with both brokers up, every cyclic file for up to 10 minutes per pending day (multi-day LS refreshes the timer each day, `2873-2874`) carries the FetchDay SEQ_NUM.

**Suggested fix:** Decouple SEQ_NUM selection from `check_redis_resp`: cyclic generators should always use the running counter, and only the OD response path should pass the request's transaction explicitly. Keep the timeout running while the broker is down; the reply cannot be delivered anyway, so fail the request and drop it.

**Verification:** Confirmed

### A3. ACKs carry a hard-coded SEQ_NUM per command type instead of echoing the command's SEQ_NUM [High] **[outside 1-2450]**
**Location:** `mqtt_connect.c:3822, 3827, 3832, 3837, 3842, 3847, 3852`; `cmd_resp.c:229-250`

**Problem:** The ACK is built as `ack_msg_reply(1001, …)` for GetDay, `1002` FetchDay, `3001` ReadModbus, `2102` get_cfg, `2004` set_cfg, `1003` Reset and `1009` (`TRANS_MODE_ACK_CODE`) trans mode. These are the example SEQ_NUMs from the spec. `ack_msg_reply` writes the integer as `"SEQ_NUM"`. Spec v0.20 section 5.3 says the ACK SEQ_NUM "Echoes the SEQ_NUM of the acknowledged message — the correlation key", and says ACKs are mandatory.

**Failure scenario:** The server sends `{"COMMAND_TYPE":"GetDay","SEQ_NUM":"4711",…}` and the DCU ACKs with `"SEQ_NUM":"1001"`. The server cannot match the ACK to request 4711. If it enforces mandatory ACKs it will time out and resend, which causes duplicate execution (and with A1, more interleaving). It only works today when the server happens to use those exact numbers.

**Suggested fix:** Change the signature to `ack_msg_reply(const char *seq, char *out)` and pass `cmd.transaction`. Send the ACK for every parsed command, before meter validation, as the spec requires.

**Verification:** Confirmed (code and spec text)

### A4. NULL dereference on the DCU serial check in GetDay, ReadModbus, get_cfg, set_cfg and Reset (the branch fixed only FetchDay) [High] **[outside 1-2450]**
**Location:** `mqtt_connect.c:3863-3864` (GetDay), `3946`, `3987`, `4023`, `4096` (`strcmp(cmd.args[0], dcu_sn)`); `file_gen_main.c:123-141` (`redis_hget` returns NULL)

**Problem:** `redis_hget` returns NULL on no reply or on a missing field. The branch replaced the FetchDay copy with `sn_ok = (dcu_sn != NULL && …)` (`3908`), but the five sibling copies still call `strcmp(cmd.args[0], dcu_sn)` unguarded. None of them frees `dcu_sn` either (leaked per command).

**Failure scenario:** Redis is restarted. `ctx` is never re-established (L1), so `redisCommand` returns NULL and `redis_hget` returns NULL. The next GetDay or get_cfg segfaults `re_mqtt_proc`. A fresh DCU with `dcu_info.serial_num` not yet set crashes the same way on any of these commands.

**Suggested fix:** Use one helper, e.g. `static int dcu_serial_matches(const char *s)`, that handles NULL and frees, and call it from every branch.

**Verification:** Confirmed

### A5. The cJSON tree from `parse_cmd_request` is leaked on most command paths; error paths leave `cmd->root` dangling [Medium]
**Location:** `mqtt_connect.c:2280-2283`, `2296/2306/2335/2351`, `2452-2454`; leak sites in `processServerMsg`: `3793-3799`, `3863-3868`, `3858-3897`, `3901-3930`, `3946-3950`, `3987-3991`, `4023-4027`, `4096-4100`, and the fall-through when `args[0]` is empty (`3858`/`3901` conditions false, reaching `4110`)

**Problem:**
- On success `parse_cmd_request` hands ownership of `root` to the caller; `cJSON_Delete` is commented out at `2452`. Only the unknown-command, ReadModbus-success, get_cfg-success, set_cfg-success and trans-mode branches free it.
- GetDay and FetchDay never free it, on any path, and neither do the invalid-meter return or any serial-mismatch return.
- On the parser's own error paths, `root` is deleted but `cmd->root`/`cmd->data` (set at `2282-2283`, before validation) still point at freed memory. It is harmless today only because the caller returns at once.
- `cpy_cmd = cmd` copies the dangling pointer as well.

**Failure scenario:** The MDAS polls FetchDay/GetDay for each meter. With 20 meters and 4 data types a day that is about 80 commands a day, each leaking a parsed tree of about 1-4 KB plus the `dcu_sn` strdup. Over a 6-12 month uptime this adds tens of MB of heap on a DCU with no swap. A malformed or oversized command adds a leaked partial tree every time.

**Suggested fix:**
- Make `parse_cmd_request` set `cmd->root` only on success.
- Give `processServerMsg` a single exit label that always runs `cJSON_Delete(cmd.root)` and `free(dcu_sn)`.
- Remove the per-branch deletes.

**Verification:** Confirmed

### A6. Publish timers are reset on every (re)connect, so cyclic data is starved on a flapping link [Medium]
**Location:** `mqtt_connect.c:943-966` (`on_connect_success`); `main.c:917-918` (`PUB_DUE`)

**Problem:** Every successful connect sets `last_mqttN_inst/profile/hc/modbus = now`, and `PUB_DUE` then requires a full interval of continuous uptime before the first publish. If the link drops more often than the interval, nothing is ever published on that broker.

Drops come from GPRS NAT expiry, a broker restart, the 3-publish-timeout recycle (`709-715`) or the 20 s watchdog. Each one costs a 60 s retry plus a fresh full interval.

**Failure scenario:** Profile interval 15 min, inst 15 min, and a cellular link that drops every 10-12 min. On each reconnect the timers restart, the 15-minute mark is never reached, and the broker receives health and Modbus (if shorter) but no meter data at all, indefinitely. The inst snapshots for those cycles are lost permanently.

**Suggested fix:** Keep a per-broker "last successful publish" time and do not touch it on connect. After a connect, publish anything already overdue (optionally after a few seconds of settling).

**Verification:** Confirmed (logic). How often it happens depends on link stability.

### A7. A persistent session plus no staleness or duplicate check replays queued or duplicated commands [Medium]
**Location:** `mqtt_connect.c:1588` (`opts.cleansession = conn->cfg.clean_session ? 1 : 0`), `1954` (subscribe on both brokers), `4165-4194`; `main.c:1394` (`redis_get_int` returns 0 when the field is absent)

**Problem:**
- `clean_session` defaults to 0 whenever `mqtt_N_cfg.clean_session` is missing or unreadable. The broker then keeps the subscription and queues QoS 1/2 commands while the DCU is offline: during the 60 s retry gaps, the reboot after Reset, a restart after set_cfg, or days of outage.
- On reconnect they are all delivered at once and executed, with no age check. The 8-slot queue silently drops the oldest of them (L11).
- Separately, the DCU is subscribed on mqtt1 and mqtt2 at the same time. A server that publishes each command to both brokers (or bridged brokers) gets every command executed twice.
- Nothing de-duplicates by SEQ_NUM across brokers or sessions (extends F12, which covers only retained messages).

**Failure scenario:**
- The DCU is offline for 2 days and the server retried set_cfg for MODEM every hour. On reconnect, 48 set_cfg commands arrive. 40 are dropped unseen and 8 are executed back to back, each triggering a restart request.
- Dual case: one FetchDay arrives on both brokers. Both are accepted, and with A1 the two replies cross over.

**Suggested fix:** Default `cleansession=1` unless the field is explicitly present. Drop `retained` messages. Keep a small ring of recently seen SEQ_NUMs shared by both brokers, ignore repeats within N minutes, and ignore commands older than a TS threshold if the spec carries one.

**Verification:** Plausible (code path confirmed; depends on the stored config and on server and broker behaviour)

### A8. The F5 fix's diag throttles use the wall clock and stall after a backward time step [Low]
**Location:** `dbg_log.c:46-56` (`diag_redis`: `static time_t last_try`, `now - last_try < 10`), `dbg_log.c:131-150` (`add_process_diag`: `last_check`)

**Problem:** Both use `time(NULL)`. If the RTC or NTP steps the clock backwards, `now - last_try` stays negative until wall time catches up. During that time the diag Redis connection is never re-established after a failure, and `diag_enable` is never re-read.

**Failure scenario:** The DCU boots with the RTC a year ahead. The first diag connect fails (Redis not up yet), then NTP corrects the clock. Diag logging is effectively disabled for a year, and turning `diag_enable` on or off has no effect.

**Suggested fix:** Use `monotonic_sec()` (already in `general.h`) for both throttles.

**Verification:** Confirmed

### A9. Each successful publish stamps `last_message_time` on both brokers' status hashes [Low]
**Location:** `mqtt_connect.c:1778`, `1903` → `update_mqtt_time(1)` (`4734-4790`)

**Problem:** `update_mqtt_time(1)` writes `last_message_time` to `mqtt1_status_hash` *and* `mqtt2_status_hash` for every broker whose `connected` is true, no matter which broker actually sent. It also runs `localtime()` (not thread-safe) and two to four Redis HSETs per chunk.

**Failure scenario:** mqtt2 is connected but every publish to it fails (for example ACL denial, with onFailure each time), while mqtt1 works. The web UI still shows mqtt2's `last_message_time` advancing, which hides the outage from the operator.

**Suggested fix:** Pass the broker index and update only that hash. Use `localtime_r`.

**Verification:** Confirmed

---

## 3. Outside scope, noted while tracing (not counted)
- `file_gen_main.c:1803`, `1838`: `LOG_ERROR(stderr, "...")` passes a `FILE*` as the format string to `log_write`, so `vsnprintf` parses the FILE struct bytes as a format string (possible crash). It is reached when the tar command string is truncated or `stat()` of the zip fails. Fix: drop the `stderr` argument and build with `-Werror=format`.
- `processServerMsg` ACKs are sent only after meter validation, and unknown commands get no ACK at all (spec 5.2: ACK first, always). Related to A3.

## 4. Checked and found OK (so nobody re-checks them)
- Publish path: `g_pub_serial` serialises publish against destroy. Paho copies the payload in `MQTTAsync_sendMessage`. Late completions are filtered by sequence number. Waits are bounded on the monotonic clock. Lock order is consistent: `g_pub_serial` → `mqtt_api_mutex`/`g_pub_mutex`/`g_brk_mutex`, and `g_brk_mutex` → log lock. No inverse order was found, including the Paho trace callback, which runs under Paho's log mutex and calls `log_write`.
- Paho callbacks no longer call Redis; `update_mqtt_status` only sets a flag.
- `on_message_arrived` frees the message and topic and always returns 1.
- Connection configuration is loaded once at startup and never rewritten at runtime, so Paho threads do not race on `conn->cfg`. Paho copies the SSL, username and password options at connect.
- Every callback path pairs `brk_cb_enter` with `brk_cb_exit`; no early return leaks `in_callback`.
- The watchdog and the 60 s retry cannot leave a broker stuck in CONNECTING or CLOSING. "Both brokers down" alternates cleanly between the two.
- No format-string bugs in lines 1-2450: every `LOG_*` call uses a literal format. No `sprintf`/`strcpy`/`strcat` in the range. Topic and URL building uses `snprintf` (silent truncation only if a configured topic exceeds about 220 characters).

## Counts (new findings)
High: 3 (A1, A3, A4) · Medium: 4 (A2, A5, A6, A7) · Low: 2 (A8, A9)
Prior in scope: Fixed 1 (F5) · Partly 3 (F7, F14, L7-part) · Open 8 (F6, F11, F12, F24, F25, L11, L12, L13) · Regressed 0
