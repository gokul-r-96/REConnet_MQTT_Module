# Review E: set_cfg / get_cfg / Modbus JSON (Mqtt_Json_Format_V3.0_Dual_Broker)

Branch `autofix/mqtt-high-2026-10-03`, HEAD `e670bb3` (main = `e7a65d7`). Line numbers are HEAD unless marked `main:`.
Files read in full: `src/set_cfg_request.c`, `src/get_cfg_request.c`, `src/modbus_json_export.c`, `src/redis_hash_generator.c` (helpers and entry points; the CDF tables were skimmed), `include/modbus_json_export.h`, `include/get_set_cfg.h`, `include/json_helper.h`, `mqtt_cmd_validation/mqtt_cmd_validate.{c,h}`. Followed into `src/mqtt_connect.c` (parse_cmd_request 2266-2440, processServerMsg 3750-4110), `src/main.c` (broker cfg load), `src/file_gen_main.c` (redis_hget). Cross-checked consumers in `Modem_PPP_IPSEC/`, `ipsec_module/`, `dcu_mon_proc/`, `modrtuMasterModule/`, `modtcpMasterModule/`, `iec104SlaveModule/`, `www/`.
I compiled the four .c files with host gcc (`-std=gnu99 -Wall -Wextra -fsyntax-only`) only to collect warnings. Nothing ran on target.

---

## 1. Status of prior findings in scope

| ID | Status | Evidence on HEAD |
|---|---|---|
| **F1** MODEM shell/pppd injection | **Fixed** (in this module, with caveats, see E14) | `set_cfg_request.c:260-305` whitelist; `:323-327` checks all four fields before any HSET. APN `[A-Za-z0-9._-]`, DIAL_NUM `[0-9*#+]`, credentials printable ASCII without ``'"`$\;&|<>(){}*?[]~`` or space, max 63 chars. Checked against `ppp_monitor.sh:531-651`: the characters still allowed (`! # % + , - . / : = @ ^ _`) cannot break out of `TELEPHONE=`/`ACCOUNT=`/`PASSWORD=` assignment words in the generated `/etc/ppp/ppp-on`, the chat `"AT+CGDCONT=..."` string, or the `printf` to the pap/chap secrets files. No other set_cfg handler writes `modem_cfg`. Not covered: `ppp_monitor.sh` still writes shell code built from Redis values, so any other writer of `modem_cfg` (Web UI, redis-cli, a future handler) brings the hole back. |
| **F3** METER_NAME stack overflow | **Fixed** | `:906-908` (MODBUS_TCP) and `:1002-1004` (MODBUS_RTU) reject names of 64 bytes or more, then use `snprintf`. |
| **F4** get_cfg DLMS_ETHERNET returns (char*)-1 | **Fixed** | `get_cfg_request.c:1632-1636` now replies with `NUM_METERS 0`. `dcu_sn` is freed on this path (`:1612`). The other 10 exporters still leak it (L7). The `-Werror=int-conversion` suggestion was not applied (Makefile unchanged), and host gcc still shows int-conversion warnings in this file (E11). |
| **F9** NULL deref and leak, Modbus nameplate / DCU details | **Fixed** | `modbus_json_export.c:138-139`: NULL is treated as "". `:353` and `:1218` free attr1-5. The unused `dcu_name`/`dcu_ser` reads were removed. `redis_hget` returns `strdup` memory (`file_gen_main.c:123-141`), so the frees are correct. |
| **F13** Secrets returned by get_cfg and printed | **Open** (and worse, see E4) | Clear-text secrets in get_cfg replies: MQTT `password`/`key_password` (`get_cfg_request.c:81,91`), modem `password1/2` (`:241,256`), IPsec `pre_shared_key` (`:385`), IEC104/101 `key_password` (`:575,709`), FTP `password_N` (`:851`), DLMS `password[n]` (`:1557,1681`). `set_mqtt_cfg` still prints the whole DATA object, PASSWORD included (`set_cfg_request.c:175-177`). The bench result "get_cfg returns broker passwords in clear" matches. |
| **F18** get_cfg invalid JSON | **Open** | DLMS_SERIAL appends `"SERIAL_PORT_%d":{` at `:1504` before the `rhash_exists` check, then `continue` at `:1509-1510` leaves the object open. ADD_STR in the DLMS exporters writes no comma, but `:1548,1553,1662,1667,1672,1677` always add one, so a missing field gives `,,` or `,}`. `enable_meter` defaults to -1 (`:1533,1647`), so a missing field reports YES. MODEM: the SIM1/SIM2 ADD_STR macro always appends `,` (`:186-192`), so a missing `phone_numN` gives `...,}` (`:243-249,258-264`). Typo `MTER_IP_ADDRESS` remains (`:1661`). |
| **F19** SERPORT never applied, half-applied on bad PARITY | **Open** | `set_cfg_request.c:842-888`: no `SADD proc_restart`. Baud rate, data bits and stop bits are written before PARITY is checked (`:861-880`). |
| **F20** Redis write failures reported as SUCCESS | **Open** | `hset_int_field` ignores the reply type and returns 1 (`:143-147`). Every UPDATE_STR macro does the same (`:212-216,336-342,395-401,544-549,624-629,702-707,790-795,938-942`). The exceptions are `set_dlms_*`, which check only for a NULL reply (`:1152,1227`); REDIS_REPLY_ERROR still counts as success. See E7 for the wider "success with nothing applied" problem. |
| **F22** ReadModbus `}{` when RTU and TCP share a slave ID | **Open** | `modbus_json_export.c:1115-1116`: no separator between the two exporters. |
| **F23** ReadModbus searches only serial port 1 | **Open** | Hard-coded `modrtu_serial1_%d_cfg` / `_status` / `_reg_` / `modrtu:1:` (`:1290,1304,1347,1349`). `num_points` is not clamped (`:1343,1468`). The device search is also capped at MAX_RTU_DEVICES = 2 (E3). |
| **L3** Restart names with spaces | **Open** | `"modrtu_master 0/1"` (`set_cfg_request.c:1038-1039`), `"SerDaProc 0/1"` (`:1160`), `"ethDaProc %d"` (`:1236`). dcu_mon_proc matches `modrtu_master_%d`, `SerDaProc_%d`, `ethDaProc_%d` (`dcu_mon_proc.c:309,321,333`), so MODBUS_RTU, DLMS_SERIAL and DLMS_ETHERNET set_cfg never restart the consumer. The change takes effect only after a reboot. |
| **L4** get/set key names differ | **Open** | Unchanged (e.g. get `IKE_LIFETIME` vs set `IKELIFETIME`, `RESPONSE_TIMEOUT` vs `RESP_TIMEOUT`, `MASTER_0_IP` vs `MASTER0_IP`, `mqtt1` vs `MQTT_BROKER_mqtt1`). |
| **L5** No range/format checks | **Open** | Every UPDATE_STR stores free text. `json_get_int_value` (`:24-92`) accepts negative values and "YES"/"NO" for any integer field (e.g. `"BROKER_PORT":"YES"` stores 1), and does not check `strtol` for ERANGE. |
| **L6** IEC IOA offsets with no reader | **Open** (Plausible) | `:640-641,718-719` still written. No C reader in iec104/iec101/dlms_10x; only `www/redis_worker.py:2096-2163` handles these offsets, and set_cfg bypasses that path. |
| **L7** Leaks, missing NULL checks | **Partly** | Fixed only in `export_dlms_ethernet_cfg`. Still leaked and unescaped at `get_cfg_request.c:135,282,439,493,626,764,895,1073,1387,1488`. In `mqtt_connect.c`, `dcu_sn` is leaked in ReadModbus/get_cfg/set_cfg/TRANS/Reset (`3935,3978,4022,4053,4095`). See also E5/E6. |
| **L17** Modbus JSON escaping and number formatting | **Partly** | Control characters below 0x20 are now `\u00XX` (`modbus_json_export.c:149-150`). Still open: when `jbuf_grow` fails, `jbuf_append` drops the fragment silently (`:73`) and returns truncated JSON. Bytes ≥0x80 are copied raw. Values without a '.' still go through `atol`: "1e-05" becomes 1 and "nan" becomes 0 (`:203-206`). |
| **L19** redis_hash_generator.c linked into the daemon | **Open** | The Makefile wildcard still builds it. `nm re_mqtt_proc` shows `T log_message`, `T get_timestamp`, `T init_logging`, `T main1`. Nothing calls it. It still contains unbounded `strcpy`/`strcat` (`:402,429-445,516-518,995-1018`). |
| **L20** modrtu values written raw; no SERPORT get_cfg | **Open** | `get_cfg_request.c:1250-1274` writes `scale_fact`/`tol_*` with `"%s"`. `get_cfg_export_json` (`:1701-1739`) has no SERPORT branch. |
| **N1** (02-Oct) IPsec shell injection | **Open** (Confirmed again) | `set_ipsec_cfg` writes 22 string fields unvalidated (`set_cfg_request.c:409-432`). Both copies of ipsec_monitor.sh load them with `eval "cfg_${_idx}_<field>=$(redis_hget ...)"` (`ipsec_module/ipsec_monitor.sh:293-323`, `Modem_PPP_IPSEC/ipsec_monitor.sh:336-368`), and the IPSEC handler requests a restart of ipsec_monitor.sh (`:450`). Example: `"TUNNEL_NAME":"t;reboot"` runs `reboot` as root. Even a space is enough: `"PRE_SHARED_KEY":"a b"` runs a command named `b`. `ENABLE_TUNNEL` does not matter, because `load_tunnel_config` reads and evals every field unconditionally. A second, separate root path through the same fields is described in E1. |
| **N2** (02-Oct) SQL injection | **N/A here** | None of the reviewed files run SQL (the only "SELECT" match is `SIM_SELECTION`). |
| 29-Sep **#1/#2/#3** (MQTT field / DATA_TYPE names) | **Open** | These live in these files, so they are covered here. `set_mqtt_cfg` selects the hash by field `mqtt1` (`set_cfg_request.c:196`). Nothing writes that field: `main.c:1495` and the Web UI (`www/redis_worker.py:1856,1864`) use `primary`. `rget_int(..., -1)` therefore never equals 1 or 0, so **every MQTT set_cfg returns failure** without writing anything. The dispatch expects `MQTT_BROKER_mqtt1/mqtt2` (`:1669,1672`), but the spec (`Json_Format_Set_Config.txt:6,25`, validator `:872-873`) says `MQTT_BROKER_PRIMARY/SECONDARY`. get_cfg emits `"mqtt1"` from that same missing field (`get_cfg_request.c:71`), so both brokers always show `"mqtt1":"NO"`. **This explains the bench failure "#3 get_cfg mqtt1 key".** |
| 29-Sep **#11** cyclic Modbus `}{` | **Open** | Details in E8. |

---

## 2. New findings

### Set_cfg fields reaching shell or system() (requested inventory)

All string fields are stored with `redisCommand("HSET %s %s %s")`. hiredis passes each argument binary-safe, so there is **no Redis-protocol injection**. Selectors are limited to 1 or 2, so no hash or field name is built from an out-of-range index. The danger is entirely in the consumers:

| DATA_TYPE / field | Redis key | Validation in C | Consumer and use | Risk |
|---|---|---|---|---|
| MODEM USERNAME/PASSWORD/APN/DIAL_NUM | modem_cfg usernameN… | whitelist (F1) | ppp_monitor.sh generates ppp-on, chat, options, secrets | closed by F1 |
| IPSEC RIGHT_IP, LEFT_ID, LEFT_SUBNET(_MASK), RIGHT_ID, RIGHT_SUBNET(_MASK), TUNNEL_NAME, LEFT, LEFT_SRC_IP, PRE_SHARED_KEY, KEYING_MODE, CONN_TYPE, AUTO_MODE, DPD_ACTION, CLOSEACTION, PHASE1/2_* | ipsec_N_cfg | none | ipsec_monitor.sh `eval` (N1); dcu_mon_proc `system()` for right_subnet / right_subnet_mask (E1) | **root RCE** (Confirmed) |
| DLMS_ETHERNET IP_ADDR | ethernet_meter_cfg ip_addr[n] | none (`snprintf` to 63 chars) | dcu_mon_proc `system("eth_nat.sh start … <ip>")` (E1); ethDaProc (not in repo) | **root RCE** (Confirmed) |
| NTP NTP_IP | ntp_cfg ntpN_server_ip | none | ntp_time_sync.sh (not in repo) | Plausible: shell script handling a host string |
| FTP IP_ADDRESS, USERNAME, PASSWORD, REMOTE_DIRECTORY | ftp_cfg *_N | none | ftp_pusher_1/2 (not in repo) | Plausible: typical ftp/curl command line |
| IEC104/101 MASTER0/1_IP, CA_CERTIFICATE, SERVER_CERTIFICATE, SERVER_KEY, KEY_PASSWORD | iec10x_0_cfg | none | iec104/101 C modules (strncpy into TLS paths; the only system() call there is a fixed `hwclock`) | no shell. The server can point TLS paths at any file. Low. |
| MODBUS_TCP IP_ADDRESS | modtcp_N_cfg dev_ip | none | modtcp master (C) | no shell. Low. |
| DLMS_SERIAL METER_ADDRESS | serial_port_N_cfg meter_addr[n] | none | SerDaProc (not in repo) | Low (Plausible atoi) |
| MQTT BROKER_IP, CLIENT_ID, USERNAME, PASSWORD | mqtt_N_cfg | none | re_mqtt_proc (C) | never written today (29-Sep #1). Once fixed, a free-text broker redirect with no validation. |

No set_cfg handler writes the DCU's own network config (eth IPs, DNS, routes), so `network_watchdog.sh` and the network scripts get no input from MQTT.

---

### E1 [High] Root command injection through dcu_mon_proc NAT setup (IPSEC RIGHT_SUBNET / RIGHT_SUBNET_MASK and DLMS_ETHERNET IP_ADDR)

- **Where:** `set_cfg_request.c:412,414-415` (RIGHT_SUBNET, RIGHT_SUBNET_MASK), `:1199-1225` (IP_ADDR → `ethernet_meter_cfg ip_addr[n]`). Consumer: `dcu_mon_proc/dcu_mon_proc.c:2531-2556` and `2583-2585`.
- **Problem:** dcu_mon_proc (root) builds `snprintf(nat_cmd, 256, "/usr/cms/bin/eth_nat.sh start %s %s %s", right_subnet, right_mask, meter_ip); system(nat_cmd);`. All three values come straight from the Redis fields set_cfg writes without validation. This is a second path, independent of the `eval` in ipsec_monitor.sh (N1). Fixing the shell script alone does not close it.
- **Scenario:** (1) set_cfg IPSEC `{"IPSEC_TUNNEL":1,"ENABLE_TUNNEL":1,"RIGHT_SUBNET":"10.0.0.0;reboot"}`, or set_cfg DLMS_ETHERNET `{"METER_NAME":"<existing>","IP_ADDR":"1.2.3.4;reboot"}`. (2) START_TRANS_MODE ETHERNET for that meter. `trans_mode_request` writes `trans_dcu_mode_info port=3 met_id=n mode=1` (`set_cfg_request.c:1644`). `mon_check_transparent_mode` → `enter_transparent_mode` (`dcu_mon_proc.c:2811-2838`) → `system()` runs `reboot` as root. Each step only needs the DCU serial.
- **Fix:** In set_cfg, accept only dotted-quad IPv4 for RIGHT_SUBNET, RIGHT_SUBNET_MASK and IP_ADDR (validate with `inet_pton`) and reject anything else. In dcu_mon_proc, validate with `inet_pton` again and call `execv("/usr/cms/bin/eth_nat.sh", argv)` without a shell.
- **Verification:** Confirmed (full path traced in source).

### E2 [High] Modbus RESP_TIMEOUT is written in seconds into a millisecond field

- **Where:** `set_cfg_request.c:955` (MODBUS_TCP) and `:1034` (MODBUS_RTU): `UPDATE_INT("RESP_TIMEOUT","resp_timeout")`.
- **Problem:** The masters read `resp_timeout` as milliseconds (`modrtu_redis_cfg.c:259-260`, `modtcp_redis_cfg.c:210-211`, default 1000). The Web UI shows seconds and stores `value*1000` (`www/redis_worker.py:2693-2694,2842-2843`). The spec example sends `"RESP_TIMEOUT" : 11`, and the validator's range is 1-30 seconds (`mqtt_cmd_validate.c` F_MODTCP). set_cfg stores the raw number. get_cfg returns raw milliseconds (`get_cfg_request.c:1174,1348`), so the round trip is inconsistent as well.
- **Scenario:** The server sends `RESP_TIMEOUT 11` for a device. resp_timeout becomes 11 ms. Every poll times out, the device goes to faulty after poll_faulty_cnt, and data stops. set_cfg replied SUCCESS.
- **Fix:** Multiply by 1000 with range check 1..30, and divide by 1000 in get_cfg. Or agree on milliseconds in the spec and validate 100..30000.
- **Verification:** Confirmed.

### E3 [Medium] MQTT-side Modbus device and register limits do not match the masters

- **Where:** `include/general.h:318-320` (MAX_TCP_DEVICES 10, MAX_RTU_DEVICES 2, MAX_REGS_PER_DEV 64). `modbus_json_export.c:27-30` redefines MAX_REGS_PER_DEV to 128 (gcc: "redefined") and adds MAX_MODRTU_DEVICES 5. `set_cfg_request.c:913` hard-codes 10.
- **Problem:** The masters scan 32 devices with 128 registers each (`modrtu_master_cfg.h:17-18`, `modtcp_master_cfg.h:17-18`, `modrtu_redis_cfg.c:208`, `modtcp_redis_cfg.c:159-161`). Each MQTT path uses a different smaller limit:
  - set_cfg MODBUS_RTU finds only devices 0-1 (`:1009`).
  - set_cfg MODBUS_TCP finds only devices 0-9.
  - get_cfg exports 2 RTU devices per port, 10 TCP devices and 64 registers.
  - ReadModbus searches only devices 0-1 on port 1.
  - The cyclic export loops over 5 RTU devices but counts 2 (E8).
- **Scenario:** An RTU meter configured from the Web UI at index 3 is polled by the master. set_cfg for it returns "failure", get_cfg omits it, ReadModbus returns no device, and it breaks the cyclic JSON (E8).
- **Fix:** Use a single shared header with the masters' limits (32 devices / 128 registers). Size `cmd_request_t.regs` to match, and remove the local redefinitions.
- **Verification:** Confirmed.

### E4 [Medium] Credentials from every set_cfg go to the log file and the diag Redis list

- **Where:** `mqtt_connect.c:3774-3775`: `for (i...) LOG_INFO("ARG_%02u : %s", i+1, cmd.args[i]);`. `LOG_INFO` → `log_write` → `add_process_diag` → `LPUSH diag_msgs:MQTT_PROC` (`dbg_log.c:128-163`, `:384`). Also `set_cfg_request.c:175-177`.
- **Problem:** `args[]` holds every string value of DATA, including MODEM PASSWORD, IPSEC PRE_SHARED_KEY, FTP PASSWORD and MQTT PASSWORD. They are written to the rotating log on flash. When diag is enabled they also go to `diag_msgs:MQTT_PROC`, which the Web UI reads (`www/redis_worker.py:376`).
- **Scenario:** The operator sets a new IPsec PSK over MQTT. The PSK sits in `/…/re_mqtt_proc.log*` and in the Web UI diagnostics view, readable by any web user who has diag access.
- **Fix:** Do not log argument values, or mask any key containing PASSWORD/KEY/PSK. Remove the `cJSON_Print(data)` printf.
- **Verification:** Confirmed for the log and the Redis list. Plausible for the Web UI display (the reader exists; the UI page was not traced).

### E5 [Medium] Remote unauthenticated memory leak: rejected commands never free the parsed JSON

- **Where:** `mqtt_connect.c:3946-3951` (ReadModbus), `:3987-3992` (get_cfg), `:4023-4028` (set_cfg), `:4096-4101` (Reset). These return on DCU-serial mismatch without `cJSON_Delete(cmd.root)`. `dcu_sn` (strdup) is also never freed on any of these paths, success or failure, and in TRANS (`:4053`).
- **Problem:** The serial check is the only gate, and the leak happens on the "wrong serial" branch. Any client that can publish to the command topic can leak a whole cJSON tree (payload up to 4 KB, tree several times larger) per message without knowing the serial.
- **Scenario:** A script publishes `{"TYPE":"command","SEQ_NUM":"1","COMMAND_TYPE":"get_cfg","DATA_TYPE":"MODEM","DATA":{"DCU":"x"}}` in a loop. RSS grows without bound until the OOM killer hits re_mqtt_proc or another process. Even legitimate traffic leaks one `dcu_sn` per command.
- **Fix:** Use a single exit path in processServerMsg that frees `cmd.root` and `dcu_sn`.
- **Verification:** Confirmed.

### E6 [Medium] Daemon crashes on any command when dcu_info.serial_num is missing

- **Where:** `mqtt_connect.c:3864` (GetDay), `:3946` (ReadModbus), `:3987` (get_cfg), `:4023` (set_cfg), `:4096` (Reset): `strcmp(cmd.args[0], dcu_sn)` with `dcu_sn` from `redis_hget`, which returns NULL when the field is missing (`file_gen_main.c:131-140`). FetchDay (`:3908`) and TRANS (`:4058`) were fixed; these five were not.
- **Scenario:** On a fresh or re-flashed DCU, or after a Redis flush before provisioning, any get_cfg/set_cfg/ReadModbus with any DCU value causes a SIGSEGV. dcu_mon_proc restarts the process, and the next command crashes it again.
- **Fix:** Use the same `dcu_sn != NULL &&` guard as FetchDay. Better, look up DATA.DCU by name instead of `args[0]` (F24).
- **Verification:** Confirmed. Medium rather than High because it needs the provisioning field to be missing.

### E7 [Medium] set_cfg replies SUCCESS when values were rejected, ignored or not written (half-applied config)

- **Where:** Every handler ignores the return of `hset_int_field` (-1 = invalid value, `set_cfg_request.c:137-141`) and of UPDATE_STR. For example, `:225,231-234,434-445,562-565,637-656,715-736,807-815,861-863,951-955,1031-1034`.
- **Problem:**
  - (a) An invalid integer is logged and skipped while the other fields are written, and the reply is SUCCESS.
  - (b) A string field sent as a JSON number (e.g. `"NTP_IP": 5`, `"IP_ADDR"` as a number) is skipped silently.
  - (c) Unknown keys, including the get_cfg spellings from L4, are ignored.
  - (d) A request with nothing but the selector (`{"DCU":…,"IPSEC_TUNNEL":1}`) writes nothing, still requests a restart, and returns SUCCESS.
  - (e) SERPORT `STOP_BITS "1.5"`, which the spec allows, is treated as invalid and skipped.
  - (f) DLMS_SERIAL/DLMS_ETHERNET apply only METER_ADDRESS or IP_ADDR. The spec and validator fields LLS_PASSWORD, HLS_PASSWORD, COMMON_ADDR, and METER_ADDRESS for Ethernet are ignored, with SUCCESS.
  - Together with F20 (Redis errors ignored), CMD_STATUS 0 says nothing about what was applied.
- **Scenario:** `set_cfg MODBUS_TCP {"METER_NAME":"M1","IP_ADDRESS":"10.0.0.9","PORT":"5O2"}` (letter O) changes the IP and keeps the old port. The reply is SUCCESS, the master restarts against the wrong port, and the server believes 502 was set.
- **Fix:** Validate the whole DATA object first (all-or-nothing) and reject unknown or ill-typed keys. Write with MULTI/EXEC and check each reply. Return failure if nothing was written.
- **Verification:** Confirmed.

### E8 [Medium] Cyclic Modbus message is invalid JSON when an RTU device with index 2-4 is enabled (29-Sep #11 still open)

- **Where:** `modbus_json_export.c:999-1010` counts RTU devices with `d < MAX_RTU_DEVICES` (2) across all ports, whatever their `device_type`. `:1049-1056` exports `d < MAX_MODRTU_DEVICES` (5).
- **Problem:** The device that reaches `dev_count == total_devices` is closed with no comma, and later devices are appended directly. `jbuf_trim_comma` only removes a trailing comma.
- **Scenario:** Port 0 is Modbus, devices 0 and 2 are enabled, and there are no TCP devices. total = 1. Device 0 gets is_last and is closed with `}`. Device 2 is then appended, giving `…}\n{…},`. The comma is trimmed, the `}{` stays, and the MODBUS_DATA_MESSAGE is unparseable every cycle. Devices with index ≥5 are never exported (E3).
- **Fix:** Write a comma before every device except the first (a `first` flag), and drop the separate count pass. Use the shared device limit.
- **Verification:** Confirmed.

### E9 [Medium] mqtt_cmd_validation is dead code, and wiring it in as-is would not stop the injections

- **Where:** `mqtt_cmd_validation/` (identical copy at the repo root). The Makefile builds only `src/*.c` plus two external files. No symbol `mqtt_validate_*` is referenced anywhere in the module.
- **Meaning:** None of its range or format rules run today. Every set_cfg value except MODEM (F1) and the selectors goes to Redis unchecked.
- **Gaps if it were wired in unchanged:**
  - (a) Injection fields are only length-checked: IPSEC LEFT_ID, LEFT_SUBNET, RIGHT_ID, RIGHT_SUBNET, TUNNEL_NAME (FSTR), DLMS_ETHERNET IP_ADDR (FSTR with no length or charset), MQTT strings, FTP USERNAME/PASSWORD. `"RIGHT_SUBNET":"1.0.0.0;reboot"` passes.
  - (b) With `"ENABLE_TUNNEL":"NO"` the validator skips every other IPSEC value (`validate_group` step 4), yet ipsec_monitor.sh still evals all of them (N1).
  - (c) It rejects keys the handler accepts and the server may send (PRE_SHARED_KEY, *_SUBNET_MASK, PHASE*, NTP_PORT is fine…), and it expects `"YES"/"NO"` for ENABLE_* while the spec examples send `1`. Spec-conformant messages would fail.
  - (d) Its DATA_TYPE names (`MQTT_BROKER_PRIMARY/SECONDARY`) do not match the handler (`MQTT_BROKER_mqtt1/2`).
- **Fix:** Add per-field charset rules (IPv4/CIDR for subnets and IPs, `[A-Za-z0-9._@-]` for IDs and names, a PSK charset without shell metacharacters or space) and validate disabled groups too. Reconcile the key sets and DATA_TYPE names with the handlers and the spec, then call it from processServerMsg before `set_cfg_export_json` and return its CMD_STATUS. Remove the duplicate copy.
- **Verification:** Confirmed (not compiled: Makefile and grep. Gaps: validator tables read).

### E10 [Low] get_cfg MODBUS_TCP truncates float register parameters to integers

- **Where:** `get_cfg_request.c:1029` (`scale_fact`), `:1042-1046` (`tol_min/max/per`) via `rget_int` (atoi).
- **Problem:** The master reads these as floats (`modtcp_redis_cfg.c:312-318`). The RTU exporter uses strings for the same fields (`:1250-1274`).
- **Scenario:** A register with scale_fact 0.1 is reported as `0`. A missing scale_fact is reported as 0, although the master uses 1.0. If the server copies the value back, the configuration becomes wrong.
- **Fix:** Read as a string and emit via `strtod` with a validated number format, the same way for RTU and TCP.
- **Verification:** Confirmed.

### E11 [Low] `redis_hget` is used without a prototype in get_cfg_request.c and modbus_json_export.c

- **Where:** Host gcc: "implicit declaration of function 'redis_hget'" plus int-conversion warnings at `get_cfg_request.c:135,282,439,493,626,764,895,1073,1387,1488,1610` and `modbus_json_export.c:285-289,1199-1203`.
- **Problem:** The returned pointer passes through `int`. This works on 32-bit ARM by accident, but truncates on any 64-bit build (host unit tests, a future target), and then `free()` and the escaping code crash. This is the class of bug behind F4.
- **Fix:** Declare `redis_hget` in a shared header and add `-Werror=implicit-function-declaration -Werror=int-conversion` to CFLAGS.
- **Verification:** Confirmed.

### E12 [Low] ReadModbus reports success with an empty result, and a string SLAVE_ID matches the wrong devices

- **Where:** `modbus_cmd_export_json` (`modbus_json_export.c:1083-1122`) returns NULL only when malloc fails, and the reply has no CMD_STATUS. `parse_cmd_request` takes SLAVE_ID only when it is a JSON number (`mqtt_connect.c:2389-2393`), otherwise 0. The exporters compare against `rget_int(cfg,"slave_id",0)` (`:1293,1385`).
- **Scenario:**
  - `"SLAVE_ID":"5"` (string, as in the other commands) gives slave_id 0. Any enabled device with no `slave_id` field matches, and its data is returned as if it were slave 5.
  - An unknown slave returns `"MODBUS_DEVICES":[]` with no failure, so the server cannot tell "no such device" from "no data".
- **Fix:** Accept number or string, reject when 1..247 is not satisfied, and send failure_resp_msg when nothing matched.
- **Verification:** Confirmed.

### E13 [Low] DLMS set_cfg truncates instead of rejecting

- **Where:** `set_cfg_request.c:1117,1128-1129,1197,1203-1204`: `snprintf(..., 64, "%s", ...)` with no length check (unlike the F3 fix).
- **Scenario:** A 70-character IP_ADDR or METER_ADDRESS is stored cut to 63 characters, and the reply is SUCCESS. A METER_NAME longer than 63 characters is matched on its truncated prefix.
- **Fix:** Reject values that do not fit, the same way as F3.
- **Verification:** Confirmed.

### E14 [Low] F1 whitelist side effects

- **Where:** `set_cfg_request.c:281-287`.
- **Problem:** `#` is allowed in USERNAME/PASSWORD. ppp_monitor.sh writes `name ${_username}` into `/etc/ppp/options` and `user<TAB>*<TAB>pass` lines into pap/chap-secrets (`ppp_monitor.sh:642-651`). pppd treats a `#` at the start of a word as a comment, so a username starting with `#` produces an empty `name` and a commented-out secrets line. The PPP link then fails authentication while set_cfg returned SUCCESS. Also, a field sent as a JSON number (e.g. `"DIAL_NUM": 99`) now makes the whole request fail, which is acceptable but is not in the spec.
- **Fix:** Disallow a leading `#`, or quote these values in ppp_monitor.sh. As defense in depth, quote every expansion in the script and stop generating shell code from Redis values (read them from a data file).
- **Verification:** Confirmed (pppd comment semantics are standard behaviour, not tested on target).

---

## 3. Notes (no finding)

- No `sprintf`/`strcpy` overflow remains in the three config/Modbus files. All hash and field buffers fit their formats with selectors limited to 1-2, `meter_id` comes from Redis, and the transparent-mode `met_id` is range-checked (0-29). The remaining unbounded copies are in redis_hash_generator.c (dead code, L19).
- `trans_mode_request` is well done: it validates before writing, uses a charset whitelist on METER for the SCAN pattern, and checks the Redis reply.
- Lines from `processServerMsg` onward: `cpy_cmd = cmd` copies `root`/`data` pointers that are freed later. No reader of `cpy_cmd.root/data` exists, so this is harmless today.

## 4. Counts

New: High 2 (E1, E2), Medium 7 (E3-E9), Low 5 (E10-E14).
Prior in scope: Fixed F1, F3, F4, F9. Partly L7, L17. Open F13, F18, F19, F20, F22, F23, L3, L4, L5, L6, L19, L20, N1, 29-Sep #1/#2/#3/#11. N2 is N/A in these files.
