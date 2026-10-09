# Review D: file generation and DLMS CDF/JSON generators

Module: `Mqtt_Json_Format_V3.0_Dual_Broker/src/`, files `file_gen_main.c`, `dlms_cdf_ls.c`, `dlms_cdf_mn.c`, `dlms_cdf_billing.c`, `dlms_cdf_event.c`. Calls were followed into `mqtt_connect.c`, `main.c` and `include/general.h`.
Branch `autofix/mqtt-high-2026-10-03`, HEAD `e670bb3` (main = `e7a65d7`). Date: 03 Oct 2026.
All line numbers refer to **HEAD** unless marked `main:`. I read the code only. Nothing was compiled or run.
Where it helped, behaviour was checked against `DCU_MDAS_MQTT_Message_Formats_0.20_29_September_2026.docx` (the spec).

Note on scope: in these files the whole CDF/XML path (`generate_profile_cdf`, `generate_*_cdf`, `cdf_write_*`, `generate_nameplate_cdf`, `generate_billing_json_single_month`) has no callers. It is dead code. Findings below are about the live JSON path unless stated otherwise.

---

## 1. Status of prior findings in scope

| ID | Status on HEAD | Evidence |
|---|---|---|
| **F2** Uninitialised file names to `system("rm")` / fopen | **Fixed (core), partly open (error reporting)** | The four name buffers are now `= ""` (file_gen_main.c:2153-2156, 2296, 2345, 2382, 2416). `system("rm ...")` is replaced by `remove_part_file()` → `unlink()`, and empty names are skipped (2027-2036, 2248-2251). With empty names the wrappers' `fopen("")` fails with ENOENT, so no stale file can be sent. Still open: `rc1..rc4` are still ignored (2170-2227), and a failed `concatenate_files` (2240) only shows up as `fopen` failing at 2270. The server gets no failure reply (F14). The dead `generate_profile_cdf` still calls `remove("")` (2107-2125); this is harmless. |
| **F6** Global OD flags redirect cyclic data to OD tables (file-gen side) | **Open** | The generators still choose the table from globals: ls.c:243, mn.c:36, billing.c:45-50, event.c:106 and 209-214. They also still drop it: ls.c:373, mn.c:172, event.c:376, billing.c:196-200. `od_table` in billing is still `static` (billing.c:42). The labels still come from globals too (`json_write_header` file_gen_main.c:896). The new F7 timeout (main.c:1011-1021 → `fetchday_reset_state`, mqtt_connect.c:2629) only runs while `check_redis_resp==1`. For MN/EVENT/BILL, `read_redis_resp` sets `check_redis_resp=0` unconditionally (mqtt_connect.c:3038-3041), but the per-type flags are reset only on success (2920, 3001, 3024). So one failed OD response still leaves the flag set until restart, and the timeout never fires for it. |
| **F7** FetchDay SEQ_NUM leaks into cyclic messages (file-gen side) | **Partly** | `check_redis_resp` is now bounded by `FETCHDAY_TIMEOUT_SEC`, which fixes the "permanent" case. `json_write_header` still writes `cpy_cmd.transaction` whenever `get_day_cmd || check_redis_resp` (file_gen_main.c:905-909). `cpy_cmd` is still overwritten by every incoming command (mqtt_connect.c:3768). During a FetchDay window, cyclic INST/LS/MN/... files therefore carry the FetchDay transaction, and an OD reply carries the SEQ_NUM of whatever command arrived last. |
| **F8** HSCAN first page only | **Fixed** | The new `hscan_first_match()` (file_gen_main.c:87-111) follows the cursor with `COUNT 200` and stops on the first page with a match, at cursor 0, or after 10 000 pages. All six call sites use it (202, 384, 534, 929, 1076, ls.c:119). Values go in as hiredis `%s` arguments, so there is no command injection. The "returned key never checked" part is still there in `read_meter_status` (`strstr(field_key, serial)`, ls.c:145, always true). It is now harmless, because MATCH only returns keys that end in `_<serial>_details`. |
| **F10** NULL deref on missing/non-string JSON field | **Fixed (crash)**, with a side effect (see D14) | Every `cJSON_GetObjectItem(...)->valuestring` in file_gen_main.c is replaced by `js_str()` (72-79), and none are left in the five files. `js_str` returns `""` for **numbers** as well, so numeric values in the details JSON are now dropped without any warning (D14). |
| **F15** Redis reply leaked; reply-owned string freed | **Open, severity should be raised to High** | Unchanged: ls.c:138-159. `json_str = value` points into the reply, `free(json_str)` (159) frees that inner buffer, and `freeReplyObject(r)` is never called on the success path. The `!json_str` path (152-156) also leaks. `read_meter_status` runs 4× per meter per cyclic profile run (LS/BILL/MN/EVENT) plus once per FetchDay. With 20 meters at the spec minimum of 5 min that is about 23 000 calls/day × ~1 KB (reply structs + key + ~600 B details JSON), so roughly **20-25 MB/day**. On an i.MX6UL running unattended for months, that ends in OOM. |
| **F16** No bound on billing month range / MN day count | **Open** | billing.c:805-830 checks the format and the order only. The loop at 879-993 opens SQLite once per month. mn.c:520 only rejects `num_days <= 0`, then does `calloc(num_days, 72)` (550) and opens SQLite once per day (577). |
| **F17** Hand-built JSON unescaped; early returns leave it unbalanced | **Open** | Unchanged: `json_write_general` returns at 931-979 after `json_write_header` has already written `{`. `json_write_d1` returns at 1078-1127. All values go out with `"%s"`: 996-1009, 1015, 1186-1258, 1287-1289, ls.c:524, mn.c:359/366, billing.c:385/394, event.c:569/589. `cpy_cmd.transaction` (from the server) is written raw at 908. A `NULL` from `redis_hget` prints as `(null)`. In the cyclic path any of these makes `concatenate_files` fail (1569-1584), and the **whole meter** (LS+MN+EVENT+BILL) is dropped from that cycle. |
| **F26** LS interval number = row index | **Open** | ls.c:305 `interval->interval_num = interval_idx;`. The day window is still `LIKE 'YYYY-MM-DD%'` (270-273), and rows beyond 288 are dropped silently (302). See also D11 (no `DATE` key). |
| **F27** 64-byte concatenated output path | **Partly** | It is now initialised to `""` (2229), so a `get_base_path` failure fails cleanly. It is still `char output_file_name[64]` filled with `"%s/data/%s_%s"` (2235): with a base path longer than about 33 characters the serial is cut off, and different meters end up sharing one file name. |
| **L14** OD table left behind on error returns | **Open** | prepare failure ls.c:278-283; billing.c:77-82; event.c:151-156, 185-190, 225-230; empty-result path billing.c:207-213 (that one does drop). |
| **L15** Paths used uninitialised when `get_base_path` fails | **Open (LS, EVENT)**; MN and multi-month billing already guarded | ls.c:643-648 and 670-677, event.c:715-723 and 742-749: `sqlite3_open()` and `fopen("w")` run on stack garbage. `sqlite3_open` (no `_v2`/READONLY) also **creates** a DB file at that path. |
| **L16** block_int / demand_int_period ignored when numeric | **Open** | ls.c:192-202. Only `cJSON_IsString` is accepted, so a numeric value gives `BLOCK_INTERVAL: 0`. |
| **L18** Missing error checks around file I/O | **Open** | `load_json_file` file_gen_main.c:1526-1538: `ftell` returning -1 leads to `malloc(0)` and a write to `buf[-1]`, and `fread` is unchecked. `cJSON_Print` returning NULL is passed to `fputs` at 1728/1747, which crashes. |
| L21 (billing part) dead `generate_billing_cdf` | **Open (dead code)** | billing.c:458 still tests `strstr(date, "Jun")` on an uninitialised `date`. There are no callers. |

---

## 2. New findings

Severity: High = crash, hang, root code exec, or wrong/lost data to the server.

### Summary

| ID | Sev | Location | Issue |
|---|---|---|---|
| D1 | High | billing.c:148-153, 160-164 | **Cause of the malformed billing date labels**: the month label column gets ".000" appended, and the real billing timestamp is discarded |
| D2 | High | file_gen_main.c:1205-1213 (and 638-639) | CT ratio and VT/PT ratio values are swapped under their OBIS codes |
| D3 | High | event.c:328-334, 515-539, 573-592 | Event values shift against PARAMS (FFFF filtering is done per record) |
| D4 | High | event.c:144-169, 385-391; file_gen_main.c:2216; mqtt_connect.c:2551/2557, 2940-2947 | **Causes of empty EVENT data** |
| D5 | High | billing.c:901-904 with 196-200 | A billing range that spans years drops the OD table part-way through, and later months are lost |
| D6 | High | mn.c:329-340, 597-604 | MN PARAMS come from the first day even when that day has no data |
| D7 | High | ls.c:278-302, mn.c:69-109, billing.c:77-101, event.c:225-248 | SQLite BUSY/errors are treated as "no more rows"; partial or empty data is sent as success |
| D8 | Medium | file_gen_main.c:1592-1602, 913 | Every cyclic meter-data message has SEQ_NUM "0003" |
| D9 | Medium | file_gen_main.c:916, 1602; ls.c:482-537; mn.c:131-132, 359; OD wrappers | JSON envelope and profile layout differ from spec v0.20 |
| D10 | Medium | ls.c:329-330; mn.c:123-124; billing.c:144-145; event.c:310-311 | Missing (NULL) DB values are reported as "0.0" |
| D11 | Medium | ls.c:694; mn.c:682; billing.c:1044; event.c:772 | `strcpy` of a 512-byte path into the caller's 128-byte buffer |
| D12 | Medium | file_gen_main.c:72-79 | `js_str` turns numeric JSON values into "" (port, PT/CT ratio) |
| D13 | Medium | event.c:212-219, 236, 248 | Event cap of 1000 keeps the oldest rows and drops the newest; large allocation |
| D14 | Low | file_gen_main.c:1156-1157, 1230-1233, 1250-1253; general.h IPV4_ADDRESS/EXTRA_OBIS_2 | NAMEPLATE carries a duplicate OBIS 00_00_19_01_00_ff; its IP comes from a removed key |
| D15 | Low | ls.c:373; mn.c:172; event.c:376 | DROP TABLE is decided by the substring "od_" in the table name |
| D16 | Low | event.c:176, 195, 203; ls.c:76-79 | SQL built with snprintf from server/Redis strings; `drop_table` uses multi-statement `sqlite3_exec` |
| D17 | Low | billing.c:70-71, 918 | The current-month "first billing entry" is an arbitrary row |
| D18 | Low | file_gen_main.c:1322-1323 | INST file is created with a relative path in the daemon's CWD |
| D19 | Low | file_gen_main.c:242, 616, 892-893, 2284 etc. | Full meter JSON is printed to stdout every cycle |

High: 7 · Medium: 6 · Low: 6

---

### D1. Billing date labels malformed [High]

**Location:** dlms_cdf_billing.c:148-153, 160-164 (query 69-72, labels 792-794/890)

**Problem:** Rows are selected with `WHERE bill_date like 'Oct 2026'` (or `'June 2026'`, or `'curr mon 2026'`), so the `bill_date` column holds a **month label**, not a timestamp. The reader then treats it as a timestamp:
```c
if (strcmp(col_name, "bill_date") == 0)
    snprintf(entry->billing_date, ..., "%s.000", val_str);
```
The real DLMS billing-date column `0_0_0_1_2_255` is thrown away ("another form of billing date, skip it", 160-164). `json_write_d3` uses `billing_date` as the record key (385).

**Failure scenario:** Every BILLING_PROFILE record is keyed `"Oct 2026.000"` or `"curr mon 2026.000"`. The spec expects a date (`YYYY-MM-DD`). This matches the bench observation. Two records of the same month also get identical keys.

**Fix:** Use the value of `0_0_0_1_2_255` (normalised to `YYYY-MM-DD`, or `YYYY-MM-DD HH:MM:SS` if the server wants it) as the record key. Keep `bill_date` only for filtering. Never append ".000" to it.

**Verification:** Confirmed (the label format follows from the query and the month table; the column content is not visible in this repo).

### D2. CT and VT (PT) ratio swapped in NAMEPLATE [High]

**Location:** file_gen_main.c:1205-1213 (live JSON); the same at 638-639 and 672-673 (dead XML)

**Problem:**
```c
fprintf(out, "[\"%s\",\"%s\"],\n", OBIS_CT_RATIO, pt_ratio);   // 01_00_00_04_02_ff
fprintf(out, "[\"%s\",\"%s\"],\n", OBIS_VT_RATIO, ct_ratio);   // 01_00_00_04_03_ff
```
`OBIS_CT_RATIO` is 1.0.0.4.2.255 (CT ratio) and `OBIS_VT_RATIO` is 1.0.0.4.3.255 (VT ratio). The values are cross-wired.

**Failure scenario:** For any meter, the server stores the PT ratio as the CT ratio and the CT ratio as the PT ratio. For the Genus meter on the bench, the server's "PT ratio" is the meter's `CT_ratio` value. If that field is empty or non-string it is `""` (D12), which the server will typically show as 0. This is the most direct code-level explanation for "Genus PT ratio 0" that is visible in this module. Separately, `www/redis_worker.py:get_ratio()` turns any non-numeric ratio string into 0 in the web UI.

**Fix:** Swap the two value arguments (and the `Internal_*_Ratio` names in the dead XML path). Confirm the units/convention with the poller (numerator only, or primary/secondary).

**Verification:** Confirmed (the swap). Whether it fully explains the bench value: Plausible.

### D3. Event values shift against PARAMS [High]

**Location:** dlms_cdf_event.c:328-334 (read), 515-539 (PARAMS), 573-592 (RECORDS)

**Problem:** While reading, any column whose value is `FFFF`/`FFFFFFFF` is left out of that entry's `params[]` array. PARAMS is then printed from `entries[0]` only, and each RECORD prints its own shortened list.

**Failure scenario:** Event 1 has all 12 values, so PARAMS lists 12 OBIS codes. Event 2 has `FFFF` in column 3, so its record has 11 values and every value from column 3 onwards is attributed to the next OBIS code. The server stores wrong values, for example a current value under a voltage OBIS. If `entries[0]` is the one with FFFF, every *other* record is shifted.

**Fix:** Keep one value per column for every record (send `""` or `null` for FFFF), and build PARAMS from the column list, not from a record.

**Verification:** Confirmed.

### D4. Causes of empty EVENT data [High]

**Location:** see each item

**Problem/scenarios:**
1. **Cyclic** (file_gen_main.c:2216) calls `generate_event_log_json(date, date, "all")` with today's date. That takes the `!is_date_all && is_event_type_all` branch, single date: `WHERE ts LIKE 'YYYY-MM-DD%'` (event.c:164-169). Only events time-stamped today are sent. On most days the cyclic EVENT_PROFILE is empty, and events from yesterday evening never reach the server if the meter-data publish did not run between the event and midnight.
2. **FetchDay EVENT** asks the poller for `"startdate":"all"` (latest events regardless of date, mqtt_connect.c:2551/2557). The reply handler then filters the OD table by the *request's* START/END dates taken from `cpy_cmd.args[2..3]` (mqtt_connect.c:2940-2947 → event.c:147-163). Fetched events outside that window are dropped, and the OD table is then deleted (event.c:376-379). `cpy_cmd` may also belong to a later command (F7).
3. No rows → `read_event_data` returns -1 (385-391). The caller ignores this and writes an empty `EVENT_PROFILE` with status 0 (728-737), so the server sees "success, no events" and cannot tell a failure from no data.
4. FetchDay: category "6" (spec: non roll-over) is not in the map and becomes `"all"` (mqtt_connect.c:2956-2977). If the poller reply has no `event_type` string, `event_category[32]` is used uninitialised (mqtt_connect.c:2936, 2985).
5. Rows whose `event_type` column is not numeric 1-7 are skipped silently (event.c:270-276). If the poller stores category names (it is *asked* for `event_data_catN`), every row is dropped. (Plausible; DB contents not in repo.)

**Fix:** Cyclic: send events newer than the last published event timestamp (persist the timestamp per meter), or at least a rolling 24 h window. FetchDay: do not re-filter the OD table by date; send what the poller fetched. Return a distinct "no data" status. Map category 6 and initialise `event_category`.

**Verification:** Items 1-4 Confirmed; item 5 Plausible.

### D5. A billing range spanning years drops the OD table part-way through [High]

**Location:** dlms_cdf_billing.c:901-904 together with 196-200

**Problem:**
```c
if (!is_current_month && month == end_month)   // year not compared
    multi_month_billing = 0;
```
`read_billing_data` drops `od_table` whenever `int_cur_month == 0 && multi_month_billing == 0`. `int_cur_month` is always 0 on this path, because it is only changed in the unused `generate_billing_json_single_month`.

**Failure scenario:** FetchDay BILLING 01-09-2025 → 31-10-2026 (end_month = 10). After Oct **2025** is read, the flag is 0 and the OD table is dropped. Nov 2025 … Oct 2026 then fail with "no such table". The server gets one month of data with CMD_STATUS success.

**Fix:** Compare `year == end_year && month == end_month`. Better: pass an explicit "drop after read" argument and drop once after the loop.

**Verification:** Confirmed.

### D6. MN PARAMS come from the first day even when that day has no data [High]

**Location:** dlms_cdf_mn.c:329-340 (PARAMS), 597-604 (empty snapshots kept)

**Problem:** Days with no row are kept as snapshots with `param_count = 0`. `json_write_d6_multi` prints PARAMS from `snapshots[0]` only, while RECORDS skip empty days.

**Failure scenario:** GetDay MIDNIGHT 20-09-2026 … 24-09-2026 where 20-09 has no midnight row (DCU was off, or the meter was not polled). The output is `"PARAMS": []` with four records of N values each, so the server cannot map any value to an OBIS code. The whole response is unusable.

**Fix:** Build PARAMS from the first snapshot with `param_count > 0`, or from `sqlite3_column_name` once per request.

**Verification:** Confirmed.

### D7. SQLite BUSY/errors treated as end of data [High]

**Location:** dlms_cdf_ls.c:257-302; mn.c:50-109; billing.c:59-101; event.c:120-248. No `sqlite3_busy_timeout` anywhere in the module.

**Problem:** `dcu_dlms.db` is written all the time by the DLMS poller. The readers open it with no busy handler. `while (sqlite3_step(stmt) == SQLITE_ROW)` ends on `SQLITE_BUSY`/`SQLITE_ERROR` exactly as it does on `SQLITE_DONE`, and the result is never checked. A failed prepare (BUSY while reading the schema, or "database is locked") returns -1, but every caller ignores it and still writes a file with an empty profile, status 0 (ls.c:653-659, event.c:728-732, billing.c:913/939). MN is the exception: a non-"no such table" prepare error fails the whole MN, and in the cyclic path that drops the whole meter through `concatenate_files`.

**Failure scenario:** In rollback-journal mode, while the poller commits a block (EXCLUSIVE lock), a cyclic or OD read gets BUSY. LS for that day goes out with 40 of 96 blocks, or none, as a successful message. The OD table is then dropped (ls.c:373), so a retry cannot recover it.

**Fix:** `sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL)` + `sqlite3_busy_timeout(db, 2000)`. After the loop, require `rc == SQLITE_DONE`, otherwise fail and do not drop the OD table. Propagate the failure to the caller and on to the server (F14). Make sure the poller uses WAL.

**Verification:** Plausible (the missing checks are confirmed; how often it triggers depends on the poller's journal mode and timing).

### D8. Hard-coded SEQ_NUM "0003" on every cyclic meter-data message [Medium]

**Location:** file_gen_main.c:1601 (also 1592-1599, 913)

**Problem:** `concatenate_files` builds the published envelope with `cJSON_AddStringToObject(out_root, "SEQ_NUM", "0003")`, copied from the spec's example. TYPE is chosen from `get_day_cmd` only. Each part file's `json_write_header` also uses `seq_num++` (913), so every meter's cyclic profile burns 4 sequence numbers that are thrown away. `seq_num` is an `int` and never wraps at 16 bits (the spec says it is a "16 bit unsigned running integer").

**Failure scenario:** Every METER_DATA message from every meter has SEQ_NUM "0003". The spec uses SEQ_NUM as the correlation and ACK key, so the server cannot ACK, de-duplicate or detect a lost meter-data message.

**Fix:** Take the next value of the shared 16-bit counter when the final envelope is built (one per published message), and do not call `seq_num++` for intermediate files.

**Verification:** Confirmed.

### D9. JSON envelope and profile layout differ from spec v0.20 [Medium]

**Location:** file_gen_main.c:916 and 1602 (`"DATATYPE"`); ls.c:482-537; mn.c:131-132, 359; billing.c:150; the OD wrappers file_gen_main.c:2289-2441

**Problem:** These were checked against the spec:
- The spec uses the key `DATA_TYPE`. The code writes `DATATYPE` in every message.
- `BLOCK_LOAD_PROFILE` must carry `DATE`. `json_write_d4` never writes `DATE`, so `copy_item(new_profile, profile, "DATE")` (1627) copies nothing.
- The spec says blocks have "no per-block timestamp". The code puts the timestamp column (`00_00_01_00_00_ff`) in PARAMS and as the first value of every block (ls.c:335-346).
- Daily/billing record keys should be a date. MN uses `"<timestamp>.000"`; billing uses D1.
- OD responses must be "PROFILES exactly as in the A.3 Meter Data message" with `DATA_TYPE: METER_DATA_MESSAGE`. GetDay/FetchDay instead send the per-type part file (`LS_DATA_MESSAGE` with `DATA.BLOCK_PROFILE.VALUES`, `MIDNIGHT_DATA_MESSAGE` with `DAILY_PROFILE`, …), which has a different shape from the cyclic message.

**Failure scenario:** A spec-conformant MDAS parser rejects or misfiles OD responses, and LS has no date except the embedded timestamps.

**Fix:** Generate one envelope builder (cJSON) used by both the cyclic and OD paths, following the spec's field names.

**Verification:** Confirmed against spec v0.20 (the spec is a draft; the server's actual tolerance is not known).

### D10. NULL DB values reported as "0.0" [Medium]

**Location:** ls.c:329-330; mn.c:123-124; billing.c:144-145; event.c:310-311

**Problem:** `if (!val_str) val_str = "0.0";`. A value the meter did not return, or a missing column, is sent as a real zero reading.

**Failure scenario:** After a partial LS read (a meter timeout on some OBIS), the server records 0 kWh / 0 V for those blocks. That is indistinguishable from a real zero and corrupts energy accounting.

**Fix:** Send `""` or `null` (as the spec allows) for NULL.

**Verification:** Confirmed.

### D11. `strcpy` of a 512-byte path into a 128-byte buffer [Medium]

**Location:** ls.c:694; mn.c:682; billing.c:1044; event.c:772 (dead copies at ls.c:624, mn.c:503, billing.c:563/707, event.c:695). Callers' buffers are `char x_file_name[128]` (file_gen_main.c:2153-2156, 2296, 2345, 2382, 2416).

**Problem:** `out_path[512]` = `base_path` (up to 255) + `/data/...`. The event name also contains the server-supplied EVENT_CATEGORY (`cmd.args[4]`, up to 31 chars, mqtt_connect.c:3667-3669): `"%s/data/EVENT_%s_%s_%s_%s.json"`.

**Failure scenario:** With a base path of about 48 characters or more, a GetDay EVENT with a 31-character EVENT_CATEGORY overflows `event_file_name[128]` on the worker stack (root process). The billing name with `_to_` overflows at about 70 characters of base path. The result is a crash or stack corruption.

**Fix:** Pass the output buffer size and use `snprintf`, failing on truncation. Validate EVENT_CATEGORY against `{"1".."7","all"}` before it is used.

**Verification:** Plausible (depends on the configured BASE_DIR in `/usr/cms/config/active_path.conf`, which is not in the repo).

### D12. `js_str` turns numeric values into "" [Medium]

**Location:** file_gen_main.c:72-79 (used at 436-449, 981-1019, 1129-1172)

**Problem:** The F10 helper returns `""` unless the item is a string. `read_meter_status` already accepts `port` as a number (ls.c:187-190), and `www/redis_worker.py:560-561` does `int(meter["port"])`. Both suggest the producer may emit numbers.

**Failure scenario:** If `port` is numeric, `strcmp(port,"2")` fails and the Ethernet meter's `IP_ADDRESS` is always sent as `""`. If `PT_ratio`, `CT_ratio`, `met_id` or `current_rating` are numeric, they are sent as `""` (see D2). Before F10 these crashed; now they are wrong without any error.

**Fix:** In `js_str`, format `cJSON_IsNumber` values (`%g` or `valueint`) into a caller buffer, and log at WARN when a key is missing.

**Verification:** Plausible (the producer's JSON types are not visible).

### D13. Event cap of 1000 keeps the oldest rows and drops the newest; large allocation [Medium]

**Location:** event.c:212-219 (`ORDER BY ts ASC`), 236 (`calloc(1000, …)`), 248, 286

**Problem:** For "all dates, one category" (event.c:175-176) there is no date filter. Rows come oldest first and the loop stops at 1000, so the newest events are dropped silently. Each entry allocates `col_count × 320 B`: with 40 columns that is about 12.8 MB for 1000 events, held while the file is written.

**Failure scenario:** A meter with more than 1000 stored events of one type: the server never receives the latest events.

**Fix:** `ORDER BY ts DESC LIMIT N`, then reverse (or emit in descending order), and stream rows straight to the file instead of buffering.

**Verification:** Confirmed.

### D14. NAMEPLATE duplicate OBIS; IP from a removed key [Low]

**Location:** file_gen_main.c:1156-1157, 1230-1233, 1250-1253; general.h `IPV4_ADDRESS` = `EXTRA_OBIS_2` = `"00_00_19_01_00_ff"`

**Problem:** The NAMEPLATE_PROFILE contains OBIS `00_00_19_01_00_ff` twice, once as the IP and once as extra_obis_2. The IP is read from `ipv4_address` in the details JSON, but a comment at 596-597 says that key was removed, so it is always `""`. Meanwhile `json_write_general` reads the IP from `ethernet_meter_cfg`.

**Failure scenario:** A server keyed on OBIS keeps the later entry (extra_obis_2), and the IP is lost or empty.

**Fix:** Correct the EXTRA_OBIS_2 OBIS code, and take the IP from the same source as `json_write_general`.

**Verification:** Confirmed.

### D15. DROP TABLE decided by the substring "od_" [Low]

**Location:** ls.c:373; mn.c:172; event.c:376

**Problem:** `if (strstr(table, "od_")) drop_table(table, db);`. A normal table is dropped if `dcu_serial` or the serial contains `od_`, for example a DCU serial ending in "od" (`ls_data_genus_XYZmod_1_123`).

**Failure scenario:** All stored LS/MN/event history for that meter is deleted after every cyclic read.

**Fix:** Drop only when the caller explicitly read the OD table (a boolean argument).

**Verification:** Confirmed code path; trigger Plausible.

### D16. SQL built with snprintf; multi-statement `sqlite3_exec` for DROP [Low]

**Location:** event.c:176, 195, 203 (`event_type`); ls.c:270-273, mn.c:61-64, billing.c:69-72 (dates); ls.c:76-79 (`drop_table`)

**Problem:** GetDay EVENT passes `cmd.args[4]` unvalidated into `WHERE event_type = '%s'`. `prepare_v2` compiles only the first statement, so this is limited to changing the result set or causing errors. Table names include `dcu_serial_number` from Redis, and `drop_table` runs them through `sqlite3_exec`, which executes **every** statement. Serials and dates coming from the server are validated or reformatted on the paths traced (meter list check mqtt_connect.c:3784-3800; `%04d-%02d-%02d` reformatting), so I did not find a remote injection.

**Failure scenario:** A malformed or hostile `meter_status` entry (local Redis) with `;` in `dcu_serial_number` runs arbitrary SQL, including DROP, on the next OD read.

**Fix:** Validate identifier parts against `[A-Za-z0-9]`, use `sqlite3_bind_text` for values, quote identifiers, and use prepare+step for DROP.

**Verification:** Confirmed (code); exploitability Low.

### D17. Current-month "first billing entry" is arbitrary [Low]

**Location:** billing.c:70-71, 918

**Problem:** `ORDER BY bill_date ASC` orders by the month label, which is identical for every row in the month. "First" therefore depends on SQLite's row order.

**Fix:** Order by `0_0_0_1_2_255` (or `id`).

**Verification:** Plausible.

### D18. INST file created with a relative path [Low]

**Location:** file_gen_main.c:1322-1323

**Problem:** `"INST_%s_%s.json"` is created in the daemon's current directory, unlike every other file (`<base>/data/`).

**Failure scenario:** If the daemon starts with CWD `/` on a read-only or small rootfs, instantaneous data is never published (fopen fails) or rootfs flash is written every cycle.

**Fix:** Use `<base>/data/`.

**Verification:** Plausible.

### D19. Full meter JSON printed to stdout every cycle [Low]

**Location:** file_gen_main.c:242 (`printf("!!!! json str %s\n")`), 616, 892-893, 2284, and similar printf calls in all generators

**Failure scenario:** If the init script redirects stdout to a file without rotation, it grows by MBs per day until flash is full.

**Fix:** Remove these calls or move them to `LOG_DEBUG`.

**Verification:** Plausible.

---

## 3. Items checked and found OK

- **Handle leaks:** every `sqlite3_prepare_v2` is finalized and every `sqlite3_open` closed on every return path in the four readers. Every `fopen` in the live generators is closed. No fd leak over months was found.
- **Date arithmetic:** `increment_date` (mn.c:281-313) handles month/year rollover and leap years correctly. `get_next_date` (event.c:42-75) and GetDay LS (`mktime` + 86400, mqtt_connect.c:3508-3592) are correct for IST (no DST). `calculate_num_days` uses noon to avoid edge cases.
- **`hscan_first_match`** checks the reply shape before dereferencing, is bounded, and frees intermediate pages.
- **Remote injection:** server-supplied serials are checked against `meter_serials[]` before reaching SQL, Redis patterns or file paths, and server dates are re-printed as integers. No remote SQL/path injection was found except D11/D16 (EVENT_CATEGORY).

## 4. Suggested order of work

1. Data correctness the server sees today: D1, D2, D3, D6, D5, D4.
2. Long-run stability: F15 (raise to High), D7, D11, F16.
3. Remove the global OD state (F6/F7), and finish F2 by propagating generator failures.
4. Bring the format in line with the spec (D8, D9, F26, D10) and build the JSON with cJSON (F17).
