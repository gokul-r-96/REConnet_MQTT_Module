#!/bin/bash

# Redis configuration
REDIS_CLI="redis-cli"
REDIS_KEY="ntp_cfg"
DIAG_KEY="ntp_diag_msg"
REDIS_FIELD_INTERVAL="interval"
REDIS_FIELD_SYNC_NOW="ntp_sync_time"

# File paths
NTP_sync_log_file_path="/usr/cms/log/NTP_sync.log"
NTP_sync_bckup_log_file_path="/usr/cms/log/NTP_sync_bckup.log"
Max_log_size=15360  # 15 KB in bytes

# Function to store diag message in redis list
store_in_redis(){
    CURRENT_DATE=$(date +"%Y_%m_%d %H_%M_%S")
    DATE_WITH_VALUE="$CURRENT_DATE : $1"
    $REDIS_CLI LPUSH $DIAG_KEY "$DATE_WITH_VALUE"
}

# Function to manage the log file size
manage_log_file() 
{
    if [ -f "$NTP_sync_log_file_path" ]; then
        file_size=$(stat -c%s "$NTP_sync_log_file_path")
        if [ "$file_size" -ge "$Max_log_size" ]; then
            # Remove the first line from the log file
            sed -i '1d' "$NTP_sync_log_file_path"
            mv "$NTP_sync_log_file_path" "$NTP_sync_bckup_log_file_path"
        fi
    fi
}


# Function to log messages and manage log file
log_and_manage() 
{
    dbgloglevel=$($REDIS_CLI hget logs_rel_cfg enable_dbg_log)
	
	 # Only log if dbgloglevel is enabled (for example, "1" means enabled)
    if [ "$dbgloglevel" != "1" ]; then
        echo "Debug logging is disabled."
        return 0  # Exit the function if debug logging is not enabled
    fi
    local log_message="$1"
    echo "$(date +"%d-%m-%Y %H:%M:%S") : ${log_message}" >> "$NTP_sync_log_file_path"
    manage_log_file
}

# Function to get the interval from Redis
get_interval_from_redis() {
    $REDIS_CLI hget $REDIS_KEY $REDIS_FIELD_INTERVAL
}

# Function to check if sync_time_now is set to 1
check_sync_time_now() {
    $REDIS_CLI hget $REDIS_KEY $REDIS_FIELD_SYNC_NOW
}

# Sync time using NTP client
sync_time() 
{
    /usr/cms/bin/ntp_client
}

####################### CMS ##################
send_hc_msg() 
{
    local path="/proc/$$/cmdline"  # $$ gives the current script's PID
    local name
    local cur_time
    local base_name

    # Read the process name
    if [ -f "$path" ]; then
        name=$(tr '\0' ' ' < "$path" | xargs)  # Remove leading/trailing whitespace
    else
        echo "Failed to open cmdline"
        return 1
    fi

    # Extract the basename
    base_name=$(basename "$name")

    # Get the current time
    # cur_time=$(date +%s)
    # rithika 7March2026
    # Write monotonic time (seconds since boot) 
    cur_time=$(awk '{print int($1)}' /proc/uptime)

    # Create the Redis key without spaces
    local redis_key="${base_name}_hc_up_time"

    # Execute the Redis command
    if redis-cli hset hc_msgs "$redis_key" "$cur_time" > /dev/null 2>&1; then
        # Uncomment the next line if you want to log successful updates
        # echo "Updated Redis with: $redis_key = $cur_time"
        return 0
    else
        echo "Failed to update Redis"
        return 1
    fi
}



####################### CMS ##################

# Function to handle SIGINT (Ctrl+C)
handle_sigint() {
    echo "Caught KILL signal! stopping time sync proc..."
    log_and_manage "Caught KILL signal! stopping time sync proc..."
    store_in_redis "[Inf] : Caught KILL signal! stopping time sync proc..."
    # Perform cleanup tasks here if needed
    exit 1
}

# Trap SIGINT and call handle_sigint function
trap 'handle_sigint' SIGINT SIGTERM

send_hc_msg
FIRST_TIME=1
# Main loop
while true; do
    log_and_manage ""
    log_and_manage "Ntp time sync SH started running..."

    INTERVAL=$(get_interval_from_redis)

    # Check if the interval is valid
    if [[ "$INTERVAL" =~ ^(6|12|24|48|72|96)$ ]]; then
        while true; do
            CURRENT_TIME_EPOCH=$(date +%s)  # Update current time epoch

            if [ $FIRST_TIME -eq 1 ]; then
                CURRENT_DATE=$(date "+%Y-%m-%d")

                if [ "$INTERVAL" -ge 12 ]; then
                    # For intervals >= 12 hours, sync at 12:00 or 00:00
                    if [ "$(date +%H)" -lt 12 ]; then
                        NEXT_SYNC_DATE=$(date -d "$CURRENT_DATE 12:00:00" "+%Y-%m-%d %H:%M:%S")
                    else
                        NEXT_DAY_EPOCH=$((CURRENT_TIME_EPOCH + 86400))
                        NEXT_SYNC_DATE=$(date -d "@$NEXT_DAY_EPOCH" "+%Y-%m-%d 00:00:00")
                    fi
                else
                    # For intervals less than 12 hours, sync at the beginning of the day
                    CURRENT_HOUR=$(date +%H)
                    
                    # Determine the next sync hour based on current time
                    if [ "$CURRENT_HOUR" -lt 6 ]; then
                        NEXT_SYNC_DATE=$(date -d "$CURRENT_DATE 06:00:00" "+%Y-%m-%d %H:%M:%S")
                    elif [ "$CURRENT_HOUR" -lt 12 ]; then
                        NEXT_SYNC_DATE=$(date -d "$CURRENT_DATE 12:00:00" "+%Y-%m-%d %H:%M:%S")
                    elif [ "$CURRENT_HOUR" -lt 18 ]; then
                        NEXT_SYNC_DATE=$(date -d "$CURRENT_DATE 18:00:00" "+%Y-%m-%d %H:%M:%S")
                    else
                        NEXT_DAY_EPOCH=$((CURRENT_TIME_EPOCH + 86400))
                        NEXT_SYNC_DATE=$(date -d "@$NEXT_DAY_EPOCH" "+%Y-%m-%d 00:00:00")
                    fi
                fi
                NEXT_SYNC_TIME_EPOCH=$(date -d "$NEXT_SYNC_DATE" +%s)
            else
                # Calculate the next sync time by adding the interval to the last sync time
                NEXT_SYNC_TIME_EPOCH=$((NEXT_SYNC_TIME_EPOCH + (INTERVAL * 60 * 60)))
                NEXT_SYNC_DATE=$(date -d "@$NEXT_SYNC_TIME_EPOCH" "+%Y-%m-%d %H:%M:%S")
            fi

            FIRST_TIME=0
            SLEEP_DURATION=$((NEXT_SYNC_TIME_EPOCH - CURRENT_TIME_EPOCH))

            CURRENT_TIME=$(date -d "@$CURRENT_TIME_EPOCH" "+%Y-%m-%d 00:00:00")
            # Debugging information
            echo "Current Time: $(date '+%Y-%m-%d %H:%M:%S')"
            echo "Next Sync Time: $NEXT_SYNC_DATE"
            echo "Synching in $SLEEP_DURATION Seconds"
            echo "Sync Interval : $INTERVAL"   

            log_and_manage "Current Time: $(date '+%Y-%m-%d %H:%M:%S')"
            log_and_manage "Next Sync Time: $NEXT_SYNC_DATE"
            log_and_manage "Synching in $SLEEP_DURATION Seconds"
            store_in_redis "[Inf] : Next Sync Time: $NEXT_SYNC_DATE"

            if [ "$SLEEP_DURATION" -lt 0 ]; then
                echo -e "\nError: Calculated sleep duration is negative. Sync time might be in the past."
                sleep 10
                send_hc_msg
                continue
            fi

            while [ $SLEEP_DURATION -gt 0 ]; do
                # Print the next sync time and countdown
                echo -ne "Next sync time: $NEXT_SYNC_DATE   Next sync in $SLEEP_DURATION seconds.\r"

                # Sleep in 1-second intervals
                sleep 1
                SLEEP_DURATION=$((SLEEP_DURATION - 1))

                # Check if sync_time_now is 1
                if [ "$(check_sync_time_now)" == "1" ]; then
                    echo -ne "\nImmediate sync requested via Redis. Syncing now.\n"
                    log_and_manage "=====> Immediate sync requested via Redis. Syncing now <====="
                    store_in_redis "[Inf] : Immediate sync requested via Redis. Syncing now"
                    $REDIS_CLI hset $REDIS_KEY $REDIS_FIELD_SYNC_NOW 0
                    FIRST_TIME=1
                    send_hc_msg
                    sleep 1  # Short delay after immediate sync
                    break
                fi
                send_hc_msg
            done

            if [ $FIRST_TIME -eq 0 ]; then
                # Perform NTP sync at the scheduled time
                echo -ne "\nScheduled sync time reached. Syncing now.\n"
                log_and_manage "=====> Scheduled sync time reached. Syncing now <====="
                store_in_redis "[Inf] : Scheduled sync time reached. Syncing now"
            fi
            send_hc_msg
            sync_time
        done
    else
        echo "Invalid interval: $INTERVAL. Retrying in 10 minutes. Allowed intervals in hours (6|12|24|48|72|96)"
        log_and_manage  "Invalid interval: $INTERVAL. Retrying in 10 minutes. Allowed intervals in hours (6|12|24|48|72|96)"
        send_hc_msg
        sleep 10  # Retry after 10 seconds
    fi
    send_hc_msg
done
