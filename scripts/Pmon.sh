#!/bin/sh

set -m
PMON_PID=$$  # PID of this script (used for process group killing)
trap cleanup SIGTERM SIGINT

if [ -f /usr/cms/bin/pin_details.sh ]
then
	source /usr/cms/bin/pin_details.sh
	echo "Pin source file Imported!!!"
else
	exit 1
fi

HC_TIMEOUT_SEC=180
maxMemory=80000
#maxMemory=3500

#rithika 30Dec2025
wdt_pulse=1

#Service array contains all processes need to be run
# ServiceArray='/usr/cms/bin/ntp_time_sync.sh' 


#------------------- sanjay added apr4-2k24 begin -----------------
Log_file_path=/usr/cms/log/pmon_proc.log
Log_file_bckup_path=/usr/cms/log/pmon_proc_bak.log
Max_log_size=500000  #in bytes
Max_relaunch_count=20
Relaunch_time_window=300 #5minutes

last_eth_ping_time=$(date +%s)

ethernet_proc_bin="/usr/cms/bin/ethDaProc" 
ethernet_cfg_hash="ethernet_meter_cfg"

#rithika 12March2026
iec104_proc_bin="/proc1"
transDCU_proc_bin="/usr/cms/bin/transDCU"

mode_key="mode"
trans_pid=0
current_mode=-1

#rithika 12Dec2025
strt_flag=1

declare -A service_pids
declare -A relaunch_counters
declare -A first_relaunch_time
declare -A first_launch_flag

Pmon_run_cnt=0

Dev_Type_Dlms=1
Dev_Type_Modbus=2
Dev_Type_103=3


echo "before bgsave"
/usr/cms/bin/save_redis_config_bgsave.sh #rithika 25Nov2025
echo "after bgsave"

update_device_uptime(){
uptime=$(uptime)
echo "uptime $uptime"

IFS=',' read -r uptime_str b c <<< "$uptime"

 up=$(echo "$uptime" | awk -F'up ' '{print $2}' | awk -F',' '{print $1}')

echo "uptime_str $up"
redis-cli hset dcu_info dcu_uptime "$up"

}
update_device_uptime

update_memory_det()
{
    memory=$(top -n 1 | grep "Mem")
    echo "mem == $memory"

    IFS=',' read -r used free others <<< "$memory"

    # echo "used field: $used"
    # echo "free field: $free"

    # Extract numeric only (remove K)
    used_kb=$(echo "$used" | awk '{print $2}' | sed 's/K//')
    free_kb=$(echo "$free" | awk '{print $1}' | sed 's/K//')

    # echo "Used Memory: ${used_kb} KB"
    # echo "Free Memory: ${free_kb} KB"
	redis-cli hset dcu_info mem_used "$used_kb kb" mem_avail "$free_kb kb"
}

update_memory_det

#++++++++++++++++++++++++++ sanjay +++++++++++++++++++++++
Proc_log()
{
	dbgloglevel=$(redis-cli hget logs_rel_cfg enable_dbg_log)
	
	 # Only log if dbgloglevel is enabled (for example, "1" means enabled)
    if [ "$dbgloglevel" != "1" ]; then
        echo "Debug logging is disabled."
        return 0  # Exit the function if debug logging is not enabled
    fi
	
	# $1 $2
	log_type=$1
	log_str=$2

	# Check if the log file size exceeds the maximum size
    if [ -f "$Log_file_path" ] && [ $(stat -c%s "$Log_file_path") -ge $Max_log_size ]; then
        mv "$Log_file_path" "$Log_file_bckup_path"
    fi

	echo "$(date +"%d-%m-%Y %H:%M:%S") : [$log_type] : $log_str" >> "$Log_file_path"

}


check_debug_log_enbl() {

    debug_log_level=$(cat /usr/cms/config/debuglog.cfg)
    echo "=========== loglevel $debug_log_level"

    if [ "$debug_log_level" -eq 1 ] && [ "$strt_flag" -eq 1 ]; then
		Proc_log "INFORM" "Dbglog level is enabled."
        dbglog_start_time=$(date +%s)
        strt_flag=0

    elif [ "$debug_log_level" -eq 1 ]; then
        current_time=$(date +%s)
        elapsed=$((current_time - dbglog_start_time))

        dbg_interval=$(redis-cli hget features_cfg dbg_log_interval)

        if [ "$elapsed" -ge "$dbg_interval" ]; then
            echo "0" > /usr/cms/config/debuglog.cfg
			Proc_log "INFORM" "Dbglog level is disabled."
            strt_flag=1
        fi
    fi
}


check_dcu_serial_num() {
	dcu_ser_num_redis=$(redis-cli hget dcu_info serial_num)
	dcu_ser_num_file=$(cat /srv/www/htdocs/info/serial.txt) 

	if [ "$dcu_ser_num_redis" = "DCU-DEFAULT-SERNO" ] && [ "$dcu_ser_num_file" = "DCU-DEFAULT-SERNO" ]; then
		last_log_time=0
		update_log_interval=180

		while true; do
			wdt_pulse=$((!wdt_pulse))
			echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value

			
			current_time=$(date +%s)

            if [ $((current_time - last_log_time)) -ge "$update_log_interval" ]; then
                Proc_log "SEVERE" "DCU serial number is in Default. Reset it with the actual DCU serial number."
                last_log_time=$current_time
            fi
			
			dcu_ser_num_redis=$(redis-cli hget dcu_info serial_num)
            dcu_ser_num_file=$(cat /srv/www/htdocs/info/serial.txt)

			if [ "$dcu_ser_num_redis" != "DCU-DEFAULT-SERNO" ] && [ "$dcu_ser_num_file" != "DCU-DEFAULT-SERNO" ] && [ "$dcu_ser_num_file" == "$dcu_ser_num_redis" ]; then
				break
			elif [ "$dcu_ser_num_redis" != "DCU-DEFAULT-SERNO" ] && [ "$dcu_ser_num_file" = "DCU-DEFAULT-SERNO" ]; then
				Proc_log "Serial number is changed in redis, updating serial number in the file"
				echo "$dcu_ser_num_redis" > /srv/www/htdocs/info/serial.txt
				break
			elif [ "$dcu_ser_num_redis" = "DCU-DEFAULT-SERNO" ] && [ "$dcu_ser_num_file" != "DCU-DEFAULT-SERNO" ]; then
				Proc_log "Serial number is changed in file, updating serial number in redis database"
    			redis-cli hset dcu_info serial_num "$dcu_ser_num_file"
				break
			fi
			sleep 5

		done
	fi

}

check_dcu_serial_num

#rithika 22Dec2025
check_eth_ping() {
    current_time=$(date +%s)

    eth_ping_interval=$(redis-cli hget features_cfg eth_ping_chk_interval)
	
    # fallback to 1 hour if Redis value missing
    [ -z "$eth_ping_interval" ] && eth_ping_interval=3600

    if [ $((current_time - last_eth_ping_time)) -ge "$eth_ping_interval" ]; then
        Proc_log "INFORM" "Running Ethernet ping check script."

        /etc/add_def_route.sh
        ret=$?
		echo "ret is $ret"
        if [ "$ret" -ne 0 ]; then
            Proc_log "SEVERE" "Ping failed. The system is going to Power down mode."
            /usr/cms/bin/killall_proc.sh
			exit 0
        fi

        last_eth_ping_time=$current_time
    fi
}


del_redis_hash()
{
	hash_name="$1"
	hash_exists=$(redis-cli exists $hash_name)
	if [ "$hash_exists" -eq 1 ]; then
		redis-cli del $hash_name
		Proc_log "INFORM" "Deleted existing Redis hash: $hash_name"
	else
        echo "Hash '$hash_name' does not exist. Skipping deletion."
    fi
	
}

delete_multiple_hashes_if_exist() {
    for hash_name in "$@"; do
        del_redis_hash "$hash_name"
    done
}

delete_multiple_hashes_if_exist  "meter_status" "meter_commn_status" "mn_data_timing" "event_data_timing" "inst_data_timing" "ls_scalar_info" "ls_poll_time" "bill_data_timing" "event_scalar_info" "event_poll_time" "mn_scalar_info" "meter_inst_info" "mn_poll_time" "bill_scalar_info" "ls_data_timing" "inst_scalar_info" "billing_poll_time" "hc_msgs"



# init_hc_keys() 
# {      

# 		# echo "hc_service_name : $hc_service_name"
# 		hc_current_time=$(date +%s)
#         redis-cli hset hc_msgs checkpppip.sh_hc_up_time {$hc_current_time} rms_newdcuProc_hc_up_time $hc_current_time dcuDaModule_0_hc_up_time {$hc_current_time} ntp_time_sync.sh_hc_up_time {$hc_current_time} connMonProc_0_hc_up_time {$hc_current_time} modem_status.sh_hc_up_time {$hc_current_time}
        
# }


#  rithika 04/09
# kill_all_normal_services() {
#     echo "Killing all normal services..."
#     service_array='SerDaProc ethDaProc inst_data_update ntp_time_sync.sh file_gen_proc ftp_pusher'

# 	for service in $service_array ; do
#         echo "$service"
#         service_name=$(basename $service)
#         echo "$service_name"
#         killall $service
# 	done
# }



#rithika 30/10/2025
kill_all_normal_services() {
    echo "Killing all normal services..."
    service_array=(
        "SerDaProc"
        "ethDaProc"
        "inst_data_update"
        "ntp_time_sync.sh"
        "file_gen_proc"
        "ftp_pusher"
    )

    for service in "${service_array[@]}"; do
        echo "Stopping $service"
        # Find matching processes and kill them
        for pid in $(ps | grep "$service" | grep -v grep | awk '{print $1}'); do
            echo "Killing PID $pid ($service)"
            kill -9 "$pid" 2>/dev/null
        done
    done

    echo "All normal services killed."
}



kill_transparent_process() {
    if [ "$trans_pid" -ne 0 ]; then
        echo "Killing transparent process with PID $trans_pid"
        kill -9 "$trans_pid" 2>/dev/null
        trans_pid=0
    fi
}


cleanup() {
    echo "SIGTERM received. Cleaning up before exit..."

    # Kill transparent process if running
    kill_transparent_process

    # Kill all normal services
    kill_all_normal_services

    # Kill any other launched services stored in service_pids[]
    for key in "${!service_pids[@]}"; do
        pid=${service_pids[$key]}
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "Stopping managed process [$key] with PID $pid"
            kill -TERM "$pid"
        fi
    done

    # Log shutdown
    Proc_log "INFORM" "pmon_proc.sh shutting down due to SIGTERM"

	redis-cli hset dcu_info last_exit_reason "Terminated by sigterm signal"

    # Reset watchdog pulse to safe state
    # if [ -n "$wdt_pin" ]; then
    #     echo 0 > /sys/class/gpio/gpio$wdt_pin/value 2>/dev/null
    # fi

    exit 0
}

# Handle termination signals
# trap cleanup SIGTERM SIGINT


init_hc_keys() 
{      
	hc_current_time=$(date +%s)

	# ================= Ethernet Meters =================
	ethernet_num_meters=$(redis-cli hget ethernet_meter_cfg num_meters)
	for ((i=0; i<ethernet_num_meters; i++)); do
		enabled=$(redis-cli hget ethernet_meter_cfg enable_meter[$i])
		if [ "$enabled" == "1" ]; then
			redis-cli hset hc_msgs ethDaProc_${i}_hc_up_time "$hc_current_time"
		fi
	done

	# ================= Serial Ports =================
	num_of_ser_ports=$(redis-cli hget features_cfg num_ser_ports)
	for ((i=0; i<num_of_ser_ports; i++)); do
		hash_name="serial_port_${i}_cfg"
		dev_type=$(redis-cli hget "$hash_name" device_type)

		case "$dev_type" in
			"1") # DLMS
				process_name="SerDaProc"
				;;
			"2") # MODBUS
				process_name="mod_rtu_proc"
				;;
			"3") # IEC 103
				process_name="103sProc"
				;;
			*)
				echo "Unknown or unsupported device type $dev_type for port $i"
				continue
				;;
		esac

		# redis-cli hset hc_msgs ${process_name}_$((i-1))_hc_up_time "$hc_current_time"
		redis-cli hset hc_msgs ${process_name}_${i}_hc_up_time "$hc_current_time"
	done

	# ================= GPRS =================
	enb_gprs=$(redis-cli hget gprs_cfg enable_gprs)
	if [ "$enb_gprs" -eq "1" ]; then
		redis-cli hset hc_msgs \
			checkpppip.sh_hc_up_time "$hc_current_time" \
			connMonProc_0_hc_up_time "$hc_current_time" \
			modem_status.sh_hc_up_time "$hc_current_time"
	fi

	# ================= MQTT =================
	mqtt_enbl=$(redis-cli hget mqtt_cfg enable_mqtt)
	if [ "$mqtt_enbl" -eq "1" ]; then
		redis-cli hset hc_msgs re_mqtt_proc_hc_up_time "$hc_current_time"
	fi

	# ================= FTP =================
	ftp_server1_enb=$(redis-cli hget ftp_cfg server_enable_1)
	ftp_server2_enb=$(redis-cli hget ftp_cfg server_enable_2)
	if [ $ftp_server1_enb -eq "1" ] || [ $ftp_server2_enb -eq "1" ]; then
		redis-cli hset hc_msgs \
			file_gen_proc_hc_up_time "$hc_current_time"\
			inst_data_update_hc_up_time "$hc_current_time"\
			ftp_pusher_hc_up_time "$hc_current_time"
	fi

	# ================= NTP =================
	ntp_enb1=$(redis-cli hget ntp_cfg enable_ntp1)
	ntp_enbl2=$(redis-cli hget ntp_cfg enable_ntp2)
	if [ "$ntp_enb1" -eq "1" ] || [ "$ntp_enbl2" -eq "1" ]; then
		redis-cli hset hc_msgs ntp_time_sync.sh_hc_up_time "$hc_current_time"
	fi

		# ================= IEC104 =================
	iec104_enbl=$(redis-cli hget mqtt_cfg enable_iec104)
	if [ "$iec104_enbl" = "1" ]; then
		redis-cli hset hc_msgs ${iec104_proc_bin}_hc_up_time "$hc_current_time"
	fi
}


init_hc_keys


num_of_ser_ports=$(redis-cli hget features_cfg num_ser_ports)
for ((i=0; i<num_of_ser_ports; i++)); do
    eval "Ser_Port_${i}_Process=\"\""
done

for ((i=0; i<num_of_ser_ports; i++)); do
    hash_name="serial_port_${i}_cfg"
    dev_type=$(redis-cli hget "$hash_name" device_type)
    # serport_id=$(redis-cli hget "$hash_name" serial_port_id) #rithika 8Sep
	task_name=""

	if [ "$dev_type" -eq "$Dev_Type_Dlms" ]
	then
		task_name="/usr/cms/bin/SerDaProc" 
	elif [ "$dev_type" -eq "$Dev_Type_Modbus" ]
	then
		task_name="/usr/cms/bin/mod_rtu_proc"
	elif [ "$dev_type" -eq "$Dev_Type_103" ]
	then
		task_name="/usr/cms/bin/103sProc"
	fi

#rithik 8Sep
	# if [ $serport_id -eq "0" ]
	# then
	# Ser_Port_1_Process=$task_name
	# elif [ $serport_id -eq "2" ]
	# then
	# Ser_Port_2_Process=$task_name
	# elif [ $serport_id -eq "3" ]
	# then
	# Ser_Port_3_Process=$task_name
	# fi

	eval "Ser_Port_${i}_Process=$task_name"
done

for ((i=0; i<num_of_ser_ports; i++)); do
	eval "process=\$Ser_Port_${i}_Process"
    printf "SERPORT %d PROCESS : %s\n" "$i" "$process"
done


ethernet_num_meters=$(redis-cli hget $ethernet_cfg_hash num_meters)

if [ -z "$ethernet_num_meters" ] || [ "$ethernet_num_meters" -eq 0 ]; then
    echo "No Ethernet devices configured"
    ethernet_num_meters=0
fi

for ((i=0; i<ethernet_num_meters; i++)); do
    enable_key="enable_meter[$i]"
    enabled=$(redis-cli hget $ethernet_cfg_hash $enable_key)

    if [ "$enabled" == "1" ]; then
        eval "Ethernet_Process_$i=\"$ethernet_proc_bin\""
    else
        echo "Ethernet device $i is disabled. Skipping..."
    fi
done

# logging: show what will be launched
for ((i=0; i<ethernet_num_meters; i++)); do
    eval "process=\$Ethernet_Process_${i}"
    if [ -n "$process" ]; then
        echo "ETHERNET DEVICE $i PROCESS : $process"
    fi
done


# redis-cli del ioa_val 
#------------------------- sanjay added apr4-2k24 end --------------------

#++++++++++++++++++++++++++ modem integration added by sanjay ++++++++++++++++++
# /////////// rithika ////////
# gprs_feature_en=$(redis-cli hget RTU2000_FEATURES GPRS)
# /////////////////
en_gprs=$(redis-cli hget gprs_cfg enable_gprs)
if [ $en_gprs -eq "1" ]
then
	echo "GPRS Enabled...."
	conn_mon_Process="/usr/cms/bin/connMonProc"
	ServiceArray="$ServiceArray
				/usr/cms/bin/checkpppip.sh
				/usr/cms/bin/modem_status.sh"
fi

ntp_enb1=$(redis-cli hget ntp_cfg enable_ntp1)
ntp_enbl2=$(redis-cli hget ntp_cfg enable_ntp2)
if [ "$ntp_enb1" -eq "1" ] || [ "$ntp_enbl2" -eq "1" ]
then
	echo "NTP enabled...."
	ServiceArray="$ServiceArray
				  /usr/cms/bin/ntp_time_sync.sh"
	
fi

# vpn_feature_en=$(redis-cli hget RTU2000_FEATURES VPN)
en_vpn=$(redis-cli hget vpn_cfg enable_vpn)
if [ $en_vpn -eq "1" ] && [ $vpn_feature_en -eq "1" ]
then
	echo "VPN Enabled...."
	ServiceArray="$ServiceArray
				  /usr/cms/bin/ipSecMonProc"
fi
# rithika 26/08/2025
mqtt_enb=$(redis-cli hget mqtt_cfg enable_mqtt)
if [ $mqtt_enb -eq "1" ]
then
    echo "Mqtt Enabled...."
	ServiceArray="$ServiceArray
				  /usr/cms/bin/re_mqtt_proc"
fi

ftp_server1_enb=$(redis-cli hget ftp_cfg server_enable_1)
ftp_server2_enb=$(redis-cli hget ftp_cfg server_enable_2)

if [ "$ftp_server1_enb" = "1" ] || [ "$ftp_server2_enb" = "1" ]
then
	echo "FTP Enabled..."
	ServiceArray="$ServiceArray
				  /usr/cms/bin/file_gen_proc
				  /usr/cms/bin/inst_data_update
				  /usr/cms/bin/ftp_pusher"
fi

#rithika 12March2026
iec104_enb=$(redis-cli hget iec104_cfg enable_iec104)
if [ "$mqtt_enb" = "1" ]
then
    echo "Mqtt Enabled...."
	ServiceArray="$ServiceArray
				  $iec104_proc_bin"
fi
	

#-------------------------- modem integration added by sanjay ------------------

#echo ""
#echo ""

############################## rithika 06Oct2025 ##########

check_health_msgs() 
{
    if [ "$Pmon_run_cnt" -gt "0" ]; then
        hc_service=$1
        hc_arg=$2

        if [ -z "$hc_arg" ]; then
            hc_key="${hc_service}"
            hc_service_name=$(basename "$hc_service")
        else
            hc_key="${hc_service}_${hc_arg}"
            hc_service_name="$(basename "$hc_service")_${hc_arg}"
        fi

        hc_last_update_time=$(redis-cli hget hc_msgs "${hc_service_name}_hc_up_time")
        hc_current_time=$(date +%s)

        if [ -z "$hc_last_update_time" ]; then
            echo "No last update time found for $hc_service_name, will attempt to restart (if needed)."
            Proc_log "SEVERE" "${hc_service_name}_hc_up_time NOT found in Redis."
            # Try to safely kill the stored PID for this service key if it exists and is the same binary
            pid="${service_pids[$hc_key]}"
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo "Killing PID $pid for key $hc_key because HC key missing."
                Proc_log "SEVERE" "Killing PID $pid for key $hc_key because HC key missing."
                kill -9 "$pid"
                sleep 2
            fi
            return
        fi

        hc_time_diff=$((hc_current_time - hc_last_update_time))
        echo "Time diff ${hc_time_diff} timeout $HC_TIMEOUT_SEC"

        if [ -z "$hc_arg" ]; then
            echo "killing service [$hc_service] if it not sent HC msg in $((HC_TIMEOUT_SEC - hc_time_diff)) seconds"
        else
            echo "killing service [$hc_service] with argument [$hc_arg] if it not sent HC msg in $((HC_TIMEOUT_SEC - hc_time_diff)) seconds"
        fi

        if [ "$hc_time_diff" -gt "$HC_TIMEOUT_SEC" ]; then
            echo "!!!!! $hc_service_name has not updated HC time in $HC_TIMEOUT_SEC seconds, Relaunching it... !!!!!"
            Proc_log "SEVERE" "$hc_service_name has not updated HC time in $HC_TIMEOUT_SEC seconds"
            pid="${service_pids[$hc_key]}"
            if [ -n "$pid" ]; then
                # verify process is alive before killing
                if kill -0 "$pid" 2>/dev/null; then
                    echo "Killing PID $pid for key $hc_key"
                    Proc_log "INFORM" "Killing PID $pid for key $hc_key"
                    kill -9 "$pid"
                    sleep 2
                else
                    Proc_log "INFORM" "Stored PID $pid for key $hc_key is not running."
                fi
            else
                Proc_log "INFORM" "No stored PID for key $hc_key to kill."
            fi
        fi
    fi
}


# Function to check health messages
# check_health_msgs() 
# {
#     if [ "$Pmon_run_cnt" -gt "0" ]; then
#         hc_service=$1
#         hc_arg=$2

#        	if [ -z "$hc_arg" ]; then
# 			hc_key="${hc_service}"
# 			hc_service_name=$(basename "$hc_service")
# 		else
# 			hc_key="${hc_service}_${hc_arg}"
# 			hc_service_name=$(basename "$hc_service")_${hc_arg}
# 		fi

        
# 		# echo "hc_service_name : $hc_service_name"

#         hc_last_update_time=$(redis-cli hget hc_msgs "${hc_service_name}_hc_up_time")
#         hc_current_time=$(date +%s)

		
#         # Check if last_update_time is available
#         if [ -z "$hc_last_update_time" ]; then
#             echo "!!!!!!!!!!!!!!!!!!!!! No last update time found for $hc_service_name, Relaunching it... !!!!!!!!!!!!!!!!!!!"
# 			Proc_log "SEVERE" "${hc_service_name}_hc_up_time NOT found in Redis. So, Killing the process"
#             kill -9 "${service_pids[$key]}"
# 			sleep 2
#             return
#         fi

#         # Calculate time since last update
#         hc_time_diff=$((hc_current_time - hc_last_update_time))
# 		#Proc_log "INFORM" "Time diff ${hc_time_diff} timeout $HC_TIMEOUT_SEC"
# 		echo "Time diff ${hc_time_diff} timeout $HC_TIMEOUT_SEC"

# 		if [ -z "$hc_arg" ]; then
# 			echo "killing service [$hc_service] if it not sent HC msg in $((HC_TIMEOUT_SEC - hc_time_diff)) seconds"
# 		else
# 			echo "killing service [$hc_service] with argument [$hc_arg] if it not sent HC msg in $((HC_TIMEOUT_SEC - hc_time_diff)) seconds"
# 		fi

#         # If the time difference exceeds the timeout threshold, restart the service
#         if [ "$hc_time_diff" -gt "$HC_TIMEOUT_SEC" ]; then
#             echo "!!!!!!!!!!!!!!!!!!!!!$hc_service_name has not updated HC time in $HC_TIMEOUT_SEC seconds, Relaunching it... !!!!!!!!!!!!!!!!!!!!!"
# 			Proc_log "SEVERE" "$hc_service_name has not updated HC time in $HC_TIMEOUT_SEC seconds, So, Killing the process"
#             kill -9 "${service_pids[$key]}"
# 			sleep 2
#         fi
#     fi
# }


#+++++++++++++++++++++++++++ sanjay +++++++++++++++++++++++

monitor_arg_proc()
{
	service=$1
	arg=$2
	
	echo "Monitoring service [$service] with arg $arg"

	key="${service}_${arg}"

	#+++++++++++++++++++++++++++ HC +++++++++++++++++++++++
	if [ "$Pmon_run_cnt" != "0" ]; then
		check_health_msgs $service $arg
	fi
	#+++++++++++++++++++++++++++ HC +++++++++++++++++++++++

	# Monitor process
	if kill -0 ${service_pids[$key]} 2>/dev/null && [ "$Pmon_run_cnt" != "0" ]; then

		echo "Service : [$service] is Running with PID : ${service_pids[$key]}"

		size=$(cat /proc/${service_pids[$key]}/status | grep VmSize | awk '{print $2}')

		echo "Memory Occupied: $size  MaxMemory limit: $maxMemory"	

		if [ "$service" != "/usr/cms/bin/re_mqtt_proc" ]; then
			# If memory exceeds kill the process 
			if [ $size -gt $maxMemory ]; then
				echo "Proc memory exceeeds for service : $service with arg : $arg"
				#++++++++++++++++++++++ sanjay log +++++++++++++++++++
				Proc_log "SEVERE" "$service in port : $((arg+1)) exceeds Proc Maxmemory ($maxMemory Bytes) So, killing service $service in port : $((arg+1))with pid number : ${service_pids[$key]}" #priya pid changed to service_pid
				#++++++++++++++++++++++ sanjay log +++++++++++++++++++
				#kill -9 $pid #priya 07042025
				
				kill -9 ${service_pids[$key]}
				sleep 2
			fi	
		fi
	else
		echo "$service with arg : $arg is not running, Relaunching $service with arg : $arg"
		Proc_log "INFORM" "${service} in port : $((arg+1)) is not running, Relaunching ${service} in port : $((arg+1))"

		$service $arg > /dev/null 2>&1 &
		sleep 1
		service_pids[$key]=$!

		echo "$service Relaunched with PID : ${service_pids[$key]}"
		Proc_log "INFORM" "$service Relaunched with PID : ${service_pids[$key]}"


		#++++++++++++++++++++++ sanjay relaunch cnt +++++++++++++++++++
		# echo "################################## first_relaunch_time[service] : ${first_relaunch_time[$service]}"
		key="${service}_${arg}"
		current_time=$(date +%s)
		relaunch_counters[$key]=$((relaunch_counters[$key] + 1))
		if [ ${relaunch_counters[$key]} -eq "2" ]; then
			first_relaunch_time[$key]=$current_time
			echo " first_relaunch_time[$key] : ${first_relaunch_time[$key]}"
		fi
		# echo "################################## first_relaunch_time[key] : ${first_relaunch_time[$key]}"

		# Check if the relaunch counter exceeds the threshold within the time window
		if [ ${relaunch_counters[$key]} -ge $((Max_relaunch_count+1)) ]; then
			time_diff=$((current_time - first_relaunch_time[$key]))
			echo "################################## time_diff : ${time_diff}    Relaunch_time_window : ${Relaunch_time_window}"
			if [ $time_diff -le $Relaunch_time_window ]; then
				Proc_log "CRITICAL" "Service ${key} has been relaunched $((relaunch_counters[$key]-1)) times in port : $((arg+1)) within ${Relaunch_time_window} seconds, rebooting system Gracefully"
				echo "key ${key} has been relaunched ${relaunch_counters[$key]} times within ${Relaunch_time_window} seconds, rebooting system Gracefully"
				redis-cli hset dcu_info last_restart_reason "Service ${key} has been relaunched $((relaunch_counters[$key]-1)) times in port : $((arg+1)) within ${Relaunch_time_window} seconds" #sanjay_01Aug2k24 ++
				restart_time=$(date +"%d-%B-%Y  %H:%M:%S") #sanjay_01Aug2k24 ++
				redis-cli hset dcu_info last_restart_time "$restart_time"  #sanjay_01Aug2k24 ++
				/usr/cms/bin/save_reboot_log.sh "Service ${key} has been relaunched $((relaunch_counters[$key]-1)) times in port : $((arg+1)) within ${Relaunch_time_window} seconds, rebooting system Gracefully"
				# rithika 19Jan026
				# /sbin/reboot
				/usr/cms/bin/killall_proc.sh
			else
				echo "Service ${key} has been relaunched $((relaunch_counters[$key]-1)) times in port : $((arg+1)) but NOT within ${Relaunch_time_window} seconds .So, NOT rebooting the system Gracefully"
				relaunch_counters[$key]=0
			fi
		fi
		#++++++++++++++++++++++ sanjay relaunch cnt +++++++++++++++++++
	fi 
	echo "--------------------"	
	echo ""
	return
}

while [ 1 ]; do

	check_debug_log_enbl

# rithika 22dec2025
	check_eth_ping

	#rithika 29oct2025
	wdt_pulse=$((!wdt_pulse))
	echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value	

	mode=$(redis-cli hget trans_dcu_mode_info "$mode_key")
	
    # If mode is 1 => Transparent Mode
    if [ "$mode" = "1" ]; then
        if [ "$current_mode" != "1" ]; then
            echo "Switching to Transparent Mode"

            # Kill all running services first
            kill_all_normal_services

            # Start transparent process
			trans_arg=$(redis-cli hget trans_dcu_mode_info port)

			# Launch transparent mode with the argument
			$transDCU_proc_bin "$trans_arg" &

            trans_pid=$!
            echo "Transparent Process started with PID $trans_pid"
			Proc_log "Transparent Process started with PID $trans_pid"

            current_mode=1
            trans_start_time=$(date +%s)
			redis-cli hset trans_dcu_mode_info strt_time "$trans_start_time"
        else
            # Already in Transparent mode, check interval
            interval=$(redis-cli hget features_cfg transparent_mode_dur)
            current_time=$(date +%s)
			trans_start_time=$(redis-cli hget trans_dcu_mode_info strt_time)

            elapsed=$(($current_time - $trans_start_time))
			redis-cli hset trans_dcu_mode_info elapsed_time $elapsed

            if [ "$elapsed" -ge "$interval" ]; then
                echo "Transparent mode interval exceeded"
				Proc_log "Transparent mode interval exceeded"
                
				echo "Switching back to Normal Mode..."
				Proc_log "Switching back to Normal Mode..."
				kill_transparent_process

				redis-cli hset trans_dcu_mode_info strt_time 0
				redis-cli hset trans_dcu_mode_info mode 0

				current_mode=0
				Pmon_run_cnt=0  # reset monitoring count

				#rithika 29oct2025
				wdt_pulse=$((!wdt_pulse))
				echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value	
				continue  # Go to next loop to re-launch services
            
            fi
        fi

        sleep 5  # Short sleep to avoid CPU churn
		#rithika 29oct2025
		wdt_pulse=$((!wdt_pulse))
		echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value	
        continue  # Skip normal service monitoring
    fi

    # ------------------ NORMAL MODE SECTION ------------------

    if [ "$current_mode" != "0" ]; then
        echo "Switching to Normal Mode"

        kill_transparent_process

		redis-cli hset trans_dcu_mode_info strt_time 0
		redis-cli hset trans_dcu_mode_info mode 0
        current_mode=0
        Pmon_run_cnt=0  # Reset monitoring counter
        # Let next loop handle launching services

		#rithika 29oct2025
		wdt_pulse=$((!wdt_pulse))
		echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value	
        continue
    fi

#/////////////////////////////////////////
	# monitor_arg_proc $Ser_Port_1_Process "0"
	# sleep 1
	# monitor_arg_proc $Ser_Port_2_Process "1" 
	# sleep 1
	for ((i=0; i<num_of_ser_ports; i++)); do
    eval "process=\$Ser_Port_${i}_Process"
    if [ -n "$process" ]; then
        monitor_arg_proc "$process" "$i"
        sleep 1
    fi
	done

	#26/08/2025 rithika for ethernet process
	for ((i=0; i<ethernet_num_meters; i++)); do
    eval "process=\$Ethernet_Process_${i}"
    if [ -n "$process" ]; then
        monitor_arg_proc "$process" "$i"
        sleep 1
    fi
	done


	#++++++++++++++++++++++++++ modem integration added by sanjay ++++++++++++++++++

	if [ $en_gprs -eq "1" ] 
	then
		monitor_arg_proc $conn_mon_Process "0" 
	fi
	sleep 1
	#-------------------------- modem integration added by sanjay ------------------
	
	for service in $ServiceArray; do
		# # Feeding WDT Pulse
		# wdt_pulse=$((!wdt_pulse))
		# echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value		
		
		echo "Monitoring service [$service]"

		key=$service #rithika 10Feb2026
		#+++++++++++++++++++++++++++ HC +++++++++++++++++++++++
		if [ "$Pmon_run_cnt" != "0" ]; then
			check_health_msgs $service
		fi
		#+++++++++++++++++++++++++++ HC +++++++++++++++++++++++
		

		# If process id is not empty monitor the process	
		if kill -0 ${service_pids[$key]} 2>/dev/null && [ "$Pmon_run_cnt" != "0" ]; then

			echo "Service : [$service] is Running with PID : ${service_pids[$key]}"

			# Get process running memory
			size=$(cat /proc/${service_pids[$key]}/status | grep VmSize | awk '{print $2}')
	
			echo "PID : ${service_pids[$key]} Memory Occupied: $size  MaxMemory limit: $maxMemory"	

			# If memory exceeds kill the process 
			if [ "$size" -gt "$maxMemory" ]; then
				echo "Proc memory exceeeds for service : $service"
				Proc_log "SEVERE" "$service exceeds Proc Maxmemory ($maxMemory Bytes) So, killing service $service with pid number : ${service_pids[$key]}"
				kill -9 ${service_pids[$key]}
			fi	
			
		# Else Launch the process
		else
			echo "$service is not running,Relaunching $service"
			Proc_log "INFORM" "${service} is not running, Relaunching ${service}"

			$service > /dev/null 2>&1 &
			service_pids[$key]=$!

			echo "$service Relaunched with PID : ${service_pids[$key]}"
			Proc_log "INFORM" "$service Relaunched with PID : ${service_pids[$key]}"

			#++++++++++++++++++++++ sanjay relaunch cnt +++++++++++++++++++
			# echo "################################## first_relaunch_time[key] : ${first_relaunch_time[$key]}"
			current_time=$(date +%s)
			relaunch_counters[$key]=$((relaunch_counters[$key] + 1))

			if [ ${relaunch_counters[$key]} -eq "2" ]; then #skipping first launching
				first_relaunch_time[$key]=$current_time
				echo " first_relaunch_time[$key] : ${first_relaunch_time[$key]}"
			fi
            # echo "################################## first_relaunch_time[key] : ${first_relaunch_time[$key]}"

            # Check if the relaunch counter exceeds the threshold within the time window
            if [ ${relaunch_counters[$key]} -ge $((Max_relaunch_count+1)) ]; then #adding first launching by +1
                time_diff=$((current_time - first_relaunch_time[$key]))
				# echo "################################## time_diff : ${time_diff}    Relaunch_time_window : ${Relaunch_time_window}"
                if [ $time_diff -le $Relaunch_time_window ]; then
                    Proc_log "CRITICAL" "Service ${key} has been relaunched $((relaunch_counters[$key] -1)) times within ${Relaunch_time_window} seconds. So, rebooting system Gracefully"
                    echo "Service ${key} has been relaunched $((relaunch_counters[$key]-1)) times within ${Relaunch_time_window} seconds. So, rebooting system Gracefully"
                    #++++++++++++++++++++++ sanjay reboot log +++++++++++++++++++
					redis-cli hset dcu_info last_restart_reason "Service ${key} has been relaunched $((relaunch_counters[$key] -1)) times within ${Relaunch_time_window} seconds"
					restart_time=$(date +"%d-%B-%Y  %H:%M:%S")
					redis-cli hset dcu_info last_restart_time "$restart_time" 
					/usr/cms/bin/save_reboot_log.sh "Service ${key} has been relaunched $((relaunch_counters[$key] -1)) times within ${Relaunch_time_window} seconds. So, Rebooting system Gracefully"
					#rithika 19Jan2026
					# /sbin/reboot
					/usr/cms/bin/killall_proc.sh
					#++++++++++++++++++++++ sanjay reboot log +++++++++++++++++++
				else
					echo "Service ${key} has been relaunched $((relaunch_counters[$key]-1)) times but NOT within ${Relaunch_time_window} seconds .So, NOT rebooting the system Gracefully"
					relaunch_counters[$key]=0
                fi
            fi
			#++++++++++++++++++++++ sanjay relaunch cnt +++++++++++++++++++
		fi
		
		# Feeding WDT pulse
		wdt_pulse=$((!wdt_pulse))
		echo $wdt_pulse > /sys/class/gpio/gpio$wdt_pin/value	
		sleep 1
		echo "---------------------"	
		echo ""
	done
	
	
	#+++++++++++++++++++++++++++ HC +++++++++++++++++++++++
	Pmon_run_cnt=$((Pmon_run_cnt + 1))
	#+++++++++++++++++++++++++++ HC +++++++++++++++++++++++
done
