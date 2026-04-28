#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <hiredis.h>
#include <jansson.h>

#define MAX_OBIS_CODES 100
#define MAX_PARAM_LENGTH 256
#define MAX_CODE_LENGTH 64
#define LOG_FILE "redis_hash_generator.log"

// Log levels
typedef enum {
    LOG_DEBUG,
    LOG_INFO,
    LOG_WARN,
    LOG_ERROR
} LogLevel;

static FILE *log_file = NULL;
static LogLevel current_log_level = LOG_INFO;

// Function to get current timestamp
void get_timestamp(char *buffer, size_t size) {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);
    strftime(buffer, size, "%Y-%m-%d %H:%M:%S", tm_info);
}

// Function to get log level string
const char* log_level_string(LogLevel level) {
    switch(level) {
        case LOG_DEBUG: return "DEBUG";
        case LOG_INFO:  return "INFO";
        case LOG_WARN:  return "WARN";
        case LOG_ERROR: return "ERROR";
        default:        return "UNKNOWN";
    }
}

// Initialize logging
void init_logging(const char *filename, LogLevel level) {
    current_log_level = level;
    log_file = fopen(filename, "a");
    if (!log_file) {
        fprintf(stderr, "Warning: Could not open log file %s\n", filename);
    }
}

// Close logging
void close_logging() {
    if (log_file) {
        fclose(log_file);
        log_file = NULL;
    }
}

// Log message function
void log_message(LogLevel level, const char *format, ...) {
    if (level < current_log_level) {
        return;
    }
    
    char timestamp[64];
    get_timestamp(timestamp, sizeof(timestamp));
    
    va_list args;
    
    // Print to console
    printf("[%s] [%s] ", timestamp, log_level_string(level));
    va_start(args, format);
    vprintf(format, args);
    va_end(args);
    printf("\n");
    fflush(stdout);
    
    // Write to log file
    if (log_file) {
        fprintf(log_file, "[%s] [%s] ", timestamp, log_level_string(level));
        va_start(args, format);
        vfprintf(log_file, format, args);
        va_end(args);
        fprintf(log_file, "\n");
        fflush(log_file);
    }
}

typedef struct {
    char obis_code[MAX_CODE_LENGTH];
    char param_code[MAX_CODE_LENGTH];
    char param_name[MAX_PARAM_LENGTH];
    char unit[MAX_CODE_LENGTH];
} ObisMapping;

typedef enum {
    DATA_TYPE_INST,
    DATA_TYPE_LS,
    DATA_TYPE_MN,
    DATA_TYPE_BILL,
    DATA_TYPE_EVENT
} DataType;

// CDF format instantaneous data mappings
typedef struct {
    const char *code;
    const char *obis_code;
    const char *name;
    const char *unit;
} CDFInstMapping;

CDFInstMapping cdf_inst_mappings[] = {
    {"P2-1-1-1-0", "01_00_1f_07_00_ff", "Current_IR", "A"},
    {"P2-2-1-1-0", "01_00_33_07_00_ff", "Current_IY", "A"},
    {"P2-3-1-1-0", "01_00_47_07_00_ff", "Current_IB", "A"},
    {"P1-1-4-1-0", "01_00_20_07_00_ff", "Voltage_VRY", "V"},
    {"P1-1-5-1-0", "01_00_34_07_00_ff", "Voltage_VBY", "V"},
    {"P1-2-3-1-0", "01_00_48_07_00_ff", "Voltage_VBN", "V"},
    {"P4-1-1-0-0", "01_00_21_07_00_ff", "Power_Factor_R_Phase", "count"},
    {"P4-2-1-0-0", "01_00_35_07_00_ff", "Power_Factor_Y_Phase", "count"},
    {"P4-3-1-0-0", "01_00_49_07_00_ff", "Power_Factor_B_Phase", "count"},
    {"P4-4-1-0-0", "01_00_0d_07_00_ff", "Three_Phase_Power_Factor_PF", "count"},
    {"P9-1-0-0-0", "01_00_0e_07_00_ff", "Frequency", "Hz"},
    {"P3-4-4-1-0", "01_00_09_07_00_ff", "Apparant_Power_kVA", "VA"},
    {"P3-1-4-1-0", "01_00_01_07_00_ff", "Signed_Active_Power_kW", "W"},
    {"P3-3-4-1-0", "01_00_03_07_00_ff", "Signed_Reactive_Power_kvar", "var"},
    {"P7-1-5-0-0", "01_00_01_08_00_ff", "Energy_kWh_Import", "Wh"},
    {"P7-1-6-0-0", "01_00_02_08_00_ff", "Energy_kWh_Export", "Wh"},
    {"P7-3-5-0-0", "01_00_09_08_00_ff", "Energy_kVAh_Import", "VAh"},
    {"P7-3-6-0-0", "01_00_0a_08_00_ff", "Energy_kVAh_Export", "VAh"},
    {"", "00_00_60_07_00_ff", "Number_of_Power_Failures", "count"},
    {"", "00_00_5e_5b_08_ff", "Cumulative_Power_Failure_Duration", "min."},
    {"", "00_00_5e_5b_00_ff", "Cumulative_Temper_Count", "count"},
    {"", "00_00_00_01_00_ff", "Cumulative_Billing_Count", "count"},
    {"", "00_00_60_02_00_ff", "Cumulative_Programing_Count", "count"},
    {"", "00_00_00_01_02_ff", "Billing_Date", "count"},
    {"P7-2-1-0-0", "01_00_05_08_00_ff", "Energy_kvarh_1Q", "varh"},
    {"P7-2-2-0-0", "01_00_06_08_00_ff", "Energy_kvarh_2Q", "varh"},
    {"P7-2-3-0-0", "01_00_07_08_00_ff", "Energy_kvarh_3Q", "varh"},
    {"P7-2-4-0-0", "01_00_08_08_00_ff", "Energy_kvarh_4Q", "varh"},
    {"", "01_00_10_08_00_ff", "", "Wh"},
    {"", "01_00_c3_08_00_ff", "", "varh"},
    {"", "01_80_82_08_00_ff", "", "Wh"},
    {"", "01_80_83_08_00_ff", "", "Wh"},
    {"", "01_80_84_08_00_ff", "", "VAh"},
    {"", "01_80_85_08_00_ff", "", "VAh"},
    {"", "01_00_15_07_00_ff", "", "W"},
    {"", "01_00_29_07_00_ff", "", "W"},
    {"", "01_00_3d_07_00_ff", "", "W"},
    {"", "01_00_20_07_7c_ff", "", "%"},
    {"", "01_00_1f_07_7c_ff", "", "%"},
    {"", "01_00_34_07_7c_ff", "", "%"},
    {"", "01_00_33_07_7c_ff", "", "%"},
    {"", "01_00_48_07_7c_ff", "", "%"},
    {"", "01_00_47_07_7c_ff", "", "%"},
    {"", "01_00_0c_80_00_ff", "", "%"},
    {"", "01_00_01_06_00_ff", "MD_KW_DATE", "W"},
    {"", "01_00_01_06_00_ff_DT", "MD_KW_DATE", "W"}, //rithika 17Feb2026
    {"", "01_00_02_06_00_ff", "", "W"},
    {"", "01_00_09_06_00_ff", "MD_kVA_DATE", "VA"},
    {"", "01_00_09_06_00_ff_DT", "MD_kVA_DATE", "VA"},//rithika 17Feb2026
    {"", "01_00_0a_06_00_ff", "", "VA"},
    {"", "01_00_01_02_00_ff", "", "W"},
    {"", "01_00_02_02_00_ff", "", "W"},
    {"", "01_00_09_02_00_ff", "", "VA"},
    {"", "01_00_0a_02_00_ff", "", "VA"},
    {"P7-2-16-2-2", "01_00_5e_5b_01_ff", "Reactive_Energy_High", "varh"},
    {"P7-2-16-2-3", "01_00_5e_5b_02_ff", "Reactive_Energy_Low", "varh"},
    {"", "01_00_c4_08_00_ff", "", "varh"},
    {"", "01_00_c5_08_00_ff", "", "varh"},
    {"", "01_00_0f_08_00_ff", "", "Wh"}
};

const int cdf_inst_mappings_count = sizeof(cdf_inst_mappings) / sizeof(CDFInstMapping);

// CDF format load survey data mappings
CDFInstMapping cdf_ls_mappings[] = {
    {"", "00_00_01_00_00_ff", "Date_Time", "count"},
    {"P9-1-0-0-0", "01_00_0e_1b_00_ff", "Frequency", "Hz"},
    {"P1-1-4-1-0", "01_00_20_1b_00_ff", "Voltage_VRY", "V"},
    {"P1-1-5-1-0", "01_00_34_1b_00_ff", "Voltage_VBY", "V"},
    {"P1-2-3-1-0", "01_00_48_1b_00_ff", "Voltage_VBN", "V"},
    {"P7-1-0-1-0", "01_00_01_1d_00_ff", "Block_Energy_kWh", "Wh"},
    {"P7-2-0-0-0", "01_00_10_1d_00_ff", "Net_Active_Energy", "Wh"},
    {"P7-1-6-0-0", "01_00_02_1d_00_ff", "Energy_Active_Export", "Wh"},
    {"P7-2-0-1-0", "01_00_05_1d_00_ff", "Block_Energy_Lag_kvarh", "varh"},
    {"P7-2-2-0-0", "01_00_06_1d_00_ff", "Energy_kvarh_2Q", "varh"},
    {"P7-2-3-0-0", "01_00_07_1d_00_ff", "Energy_kvarh_3Q", "varh"},
    {"P7-2-0-1-0", "01_00_08_1d_00_ff", "Block_Energy_Lead_kvarh", "varh"},
    {"", "01_00_0c_1b_00_ff", "", "V"},
    {"", "00_00_60_0a_80_ff", "", "count"},
    {"", "01_00_c3_1d_00_ff", "", "varh"},
    {"P2-1-1-1-0", "01_00_1f_1b_00_ff", "Current_IR", "A"},
    {"P2-2-1-1-0", "01_00_33_1b_00_ff", "Current_IY", "A"},
    {"P2-3-1-1-0", "01_00_47_1b_00_ff", "Current_IB", "A"},
    {"", "01_00_c4_1d_00_ff", "", "varh"},
    {"", "01_00_c5_1d_00_ff", "", "varh"},
    {"", "01_00_0d_1b_00_ff", "", "count"},
    {"", "01_00_54_1b_00_ff", "", "count"},
    {"P7-3-0-1-0", "01_00_09_1d_00_ff", "Block_Energy_kVAh", "VAh"},
    {"", "01_00_0a_1d_00_ff", "", "VAh"},
    {"", "01_00_cf_1d_00_ff", "", "count"},
    {"", "00_00_60_0e_83_ff", "", "count"}
};

const int cdf_ls_mappings_count = sizeof(cdf_ls_mappings) / sizeof(CDFInstMapping);

// CDF format midnight data mappings
CDFInstMapping cdf_mn_mappings[] = {
    {"P7-1-5-0-0", "01_00_01_08_00_ff", "Energy_kWh_Import", "Wh"},
    {"P7-1-6-0-0", "01_00_02_08_00_ff", "Energy_kWh_Export", "Wh"},
    {"P7-3-5-0-0", "01_00_09_08_00_ff", "Energy_kVAh_Import", "VAh"},
    {"P7-3-6-0-0", "01_00_0a_08_00_ff", "Energy_kVAh_Export", "VAh"},
    {"P7-2-16-2-2", "01_00_5e_5b_01_ff", "Reactive_Energy_High", "varh"},
    {"P7-2-16-2-3", "01_00_5e_5b_02_ff", "Reactive_Energy_Low", "varh"},
    {"P7-2-1-0-0", "01_00_05_08_00_ff", "Energy_kvarh_1Q", "varh"},
    {"P7-2-2-0-0", "01_00_06_08_00_ff", "Energy_kvarh_2Q", "varh"},
    {"P7-2-3-0-0", "01_00_07_08_00_ff", "Energy_kvarh_3Q", "varh"},
    {"P7-2-4-0-0", "01_00_08_08_00_ff", "Energy_kvarh_4Q", "varh"},
    {"", "01_00_10_08_00_ff", "", "Wh"},
    {"", "01_00_c3_08_00_ff", "", "varh"},
    {"", "01_00_c4_08_00_ff", "", "varh"},
    {"", "01_00_c5_08_00_ff", "", "varh"}
};

const int cdf_mn_mappings_count = sizeof(cdf_mn_mappings) / sizeof(CDFInstMapping);

// CDF format billing data mappings
CDFInstMapping cdf_bill_mappings[] = {
    {"P7-1-5-0-0", "01_00_01_08_00_ff", "Energy_kWh_Import", "Wh"},
    {"P7-1-6-0-0", "01_00_02_08_00_ff", "Energy_kWh_Export", "Wh"},
    {"P7-3-5-0-0", "01_00_09_08_00_ff", "Energy_kVAh_Import", "VAh"},
    {"P7-3-6-0-0", "01_00_0a_08_00_ff", "Energy_kVAh_Export", "VAh"},
    {"P7-2-1-0-0", "01_00_05_08_00_ff", "Energy_kvarh_1Q", "varh"},
    {"P7-2-2-0-0", "01_00_06_08_00_ff", "Energy_kvarh_2Q", "varh"},
    {"P7-2-3-0-0", "01_00_07_08_00_ff", "Energy_kvarh_3Q", "varh"},
    {"P7-2-4-0-0", "01_00_08_08_00_ff", "Energy_kvarh_4Q", "varh"},
    {"", "00_00_5e_5b_0d_ff", "", "min."},
    {"P4-4-1-0-0", "01_00_0d_00_00_ff", "Power_Factor_PF", "count"},
    {"", "01_00_54_00_00_ff", "", "count"},
    {"", "01_00_01_06_00_ff", "MD_KW_DATE", "W"},
    {"", "01_00_02_06_00_ff", "", "W"},
    {"", "01_00_09_06_00_ff", "MD_kVA_DATE", "VA"},
    {"", "01_00_0a_06_00_ff", "", "VA"},
    {"", "01_00_01_08_01_ff", "Cumulative_Energy_kWh_TZ1", "Wh"},
    {"", "01_00_01_08_02_ff", "Cumulative_Energy_kWh_TZ2", "Wh"},
    {"", "01_00_01_08_03_ff", "Cumulative_Energy_kWh_TZ3", "Wh"},
    {"", "01_00_01_08_04_ff", "Cumulative_Energy_kWh_TZ4", "Wh"},
    {"", "01_00_01_08_05_ff", "Cumulative_Energy_kWh_TZ5", "Wh"},
    {"", "01_00_01_08_06_ff", "Cumulative_Energy_kWh_TZ6", "Wh"},
    {"", "01_00_01_08_07_ff", "Cumulative_Energy_kWh_TZ7", "Wh"},
    {"", "01_00_01_08_08_ff", "Cumulative_Energy_kWh_TZ8", "Wh"},
    {"", "01_00_02_08_01_ff", "", "Wh"},
    {"", "01_00_02_08_02_ff", "", "Wh"},
    {"", "01_00_02_08_03_ff", "", "Wh"},
    {"", "01_00_02_08_04_ff", "", "Wh"},
    {"", "01_00_02_08_05_ff", "", "Wh"},
    {"", "01_00_02_08_06_ff", "", "Wh"},
    {"", "01_00_02_08_07_ff", "", "Wh"},
    {"", "01_00_02_08_08_ff", "", "Wh"},
    {"", "01_00_09_08_01_ff", "", "VAh"},
    {"", "01_00_09_08_02_ff", "", "VAh"},
    {"", "01_00_09_08_03_ff", "", "VAh"},
    {"", "01_00_09_08_04_ff", "", "VAh"},
    {"", "01_00_09_08_05_ff", "", "VAh"},
    {"", "01_00_09_08_06_ff", "", "VAh"},
    {"", "01_00_09_08_07_ff", "", "VAh"},
    {"", "01_00_09_08_08_ff", "", "VAh"},
    {"", "01_00_0a_08_01_ff", "", "VAh"},
    {"", "01_00_0a_08_02_ff", "", "VAh"},
    {"", "01_00_0a_08_03_ff", "", "VAh"},
    {"", "01_00_0a_08_04_ff", "", "VAh"},
    {"", "01_00_0a_08_05_ff", "", "VAh"},
    {"", "01_00_0a_08_06_ff", "", "VAh"},
    {"", "01_00_0a_08_07_ff", "", "VAh"},
    {"", "01_00_0a_08_08_ff", "", "VAh"},
    {"", "01_00_01_06_01_ff", "MD_kW_TZ1_DATE", "W"},
    {"", "01_00_01_06_02_ff", "MD_kW_TZ2_DATE", "W"},
    {"", "01_00_01_06_03_ff", "MD_kW_TZ3_DATE", "W"},
    {"", "01_00_01_06_04_ff", "MD_kW_TZ4_DATE", "W"},
    {"", "01_00_01_06_05_ff", "MD_kW_TZ5_DATE", "W"},
    {"", "01_00_01_06_06_ff", "MD_kW_TZ6_DATE", "W"},
    {"", "01_00_01_06_07_ff", "MD_kW_TZ7_DATE", "W"},
    {"", "01_00_01_06_08_ff", "MD_kW_TZ8_DATE", "W"},
    {"", "01_00_02_06_01_ff", "", "W"},
    {"", "01_00_02_06_02_ff", "", "W"},
    {"", "01_00_02_06_03_ff", "", "W"},
    {"", "01_00_02_06_04_ff", "", "W"},
    {"", "01_00_02_06_05_ff", "", "W"},
    {"", "01_00_02_06_06_ff", "", "W"},
    {"", "01_00_02_06_07_ff", "", "W"},
    {"", "01_00_02_06_08_ff", "", "W"},
    {"", "01_00_09_06_01_ff", "MD_kVAh_TZ1_DATE", "VA"},
    {"", "01_00_09_06_02_ff", "MD_kVAh_TZ2_DATE", "VA"},
    {"", "01_00_09_06_03_ff", "MD_kVAh_TZ3_DATE", "VA"},
    {"", "01_00_09_06_04_ff", "MD_kVAh_TZ4_DATE", "VA"},
    {"", "01_00_09_06_05_ff", "MD_kVAh_TZ5_DATE", "VA"},
    {"", "01_00_09_06_06_ff", "MD_kVAh_TZ6_DATE", "VA"},
    {"", "01_00_09_06_07_ff", "MD_kVAh_TZ7_DATE", "VA"},
    {"", "01_00_09_06_08_ff", "MD_kVAh_TZ8_DATE", "VA"},
    {"", "01_00_0a_06_01_ff", "", "VA"},
    {"", "01_00_0a_06_02_ff", "", "VA"},
    {"", "01_00_0a_06_03_ff", "", "VA"},
    {"", "01_00_0a_06_04_ff", "", "VA"},
    {"", "01_00_0a_06_05_ff", "", "VA"},
    {"", "01_00_0a_06_06_ff", "", "VA"},
    {"", "01_00_0a_06_07_ff", "", "VA"},
    {"", "01_00_0a_06_08_ff", "", "VA"},
    {"", "01_00_05_08_01_ff", "", "varh"},
    {"", "01_00_05_08_02_ff", "", "varh"},
    {"", "01_00_05_08_03_ff", "", "varh"},
    {"", "01_00_05_08_04_ff", "", "varh"},
    {"", "01_00_05_08_05_ff", "", "varh"},
    {"", "01_00_05_08_06_ff", "", "varh"},
    {"", "01_00_05_08_07_ff", "", "varh"},
    {"", "01_00_05_08_08_ff", "", "varh"},
    {"", "01_00_06_08_01_ff", "", "varh"},
    {"", "01_00_06_08_02_ff", "", "varh"},
    {"", "01_00_06_08_03_ff", "", "varh"},
    {"", "01_00_06_08_04_ff", "", "varh"},
    {"", "01_00_06_08_05_ff", "", "varh"},
    {"", "01_00_06_08_06_ff", "", "varh"},
    {"", "01_00_06_08_07_ff", "", "varh"},
    {"", "01_00_06_08_08_ff", "", "varh"},
    {"", "01_00_07_08_01_ff", "", "varh"},
    {"", "01_00_07_08_02_ff", "", "varh"},
    {"", "01_00_07_08_03_ff", "", "varh"},
    {"", "01_00_07_08_04_ff", "", "varh"},
    {"", "01_00_07_08_05_ff", "", "varh"},
    {"", "01_00_07_08_06_ff", "", "varh"},
    {"", "01_00_07_08_07_ff", "", "varh"},
    {"", "01_00_07_08_08_ff", "", "varh"},
    {"", "01_00_08_08_01_ff", "", "varh"},
    {"", "01_00_08_08_02_ff", "", "varh"},
    {"", "01_00_08_08_03_ff", "", "varh"},
    {"", "01_00_08_08_04_ff", "", "varh"},
    {"", "01_00_08_08_05_ff", "", "varh"},
    {"", "01_00_08_08_06_ff", "", "varh"},
    {"", "01_00_08_08_07_ff", "", "varh"},
    {"", "01_00_08_08_08_ff", "", "varh"},
    {"", "01_00_92_08_00_ff", "", "varh"},
    {"", "01_00_93_08_00_ff", "", "varh"},
    {"", "01_00_94_08_00_ff", "", "varh"},
    {"", "01_00_95_08_00_ff", "", "varh"},
    {"", "01_00_10_08_00_ff", "", "Wh"},
    {"", "01_00_20_80_00_ff", "", "V"},
    {"", "01_00_34_80_00_ff", "", "V"},
    {"", "01_00_48_80_00_ff", "", "V"},
    {"", "01_00_15_80_00_ff", "", "W"},
    {"", "01_00_29_80_00_ff", "", "W"},
    {"", "01_00_3d_80_00_ff", "", "W"},
    {"", "01_00_17_80_00_ff", "", "var"},
    {"", "01_00_2b_80_00_ff", "", "var"},
    {"", "01_00_3f_80_00_ff", "", "var"},
    {"", "01_00_05_06_00_ff", "", "var"},
    {"", "01_00_06_06_00_ff", "", "var"},
    {"", "01_00_07_06_00_ff", "", "var"},
    {"", "01_00_08_06_00_ff", "", "var"}
};

const int cdf_bill_mappings_count = sizeof(cdf_bill_mappings) / sizeof(CDFInstMapping);

CDFInstMapping cdf_event_mapping[] =     {
      {  "P1-2-1-1-0", "01_00_20_07_00_ff", "volt_r", "V" },
      { "P1-2-2-1-0", "01_00_34_07_00_ff",  "volt_y",  "V" },
      {  "P1-2-3-1-0", "01_00_48_07_00_ff", "volt_b", "V" },
      {  "P1-1-4-1-0", "" ,"volt_ry" ,  "V"},
      { "P1-1-5-1-0", "",  "volt_yb",  "V"},
      { "P1-1-6-1-0", "" , "volt_rb",  "V"},
      {  "P2-1-1-1-0", "01_00_1f_07_00_ff", "cur_ir",  "A" },
      {  "P2-1-2-1-0", "01_00_33_07_00_ff", "cur_iy",  "A" },
      {  "P2-1-3-1-0", "01_00_47_07_00_ff", "cur_ib",  "A" },
      {  "P4-1-1-0-0", "01_00_21_07_00_ff", "pf_r",  "NA" },
      {  "P4-2-1-0-0", "01_00_35_07_00_ff", "pf_y", "NA" },
      {  "P4-3-1-0-0", "01_00_49_07_00_ff", "pf_b",  "NA" },
      {  "P7-1-5-2-0", "01_00_01_08_00_ff", "kwh",   "K" }
    
  };

    
const int cdf_event_mappings_count = sizeof(cdf_event_mapping) / sizeof(CDFInstMapping);

// Function to convert OBIS code from decimal to hex for CDF lookup only
// Input: "1_0_31_7_0_255" Output: "01_00_1f_07_00_ff"
// Input: "1_0_1_8_0_255_DT" Output: "01_00_01_08_00_ff_DT" (preserve _DT)
void convert_decimal_to_hex_for_cdf(const char *decimal_obis, char *hex_obis) {
    log_message(LOG_DEBUG, "Converting OBIS for CDF lookup: %s", decimal_obis);
    
    char temp[MAX_CODE_LENGTH];
    
    strcpy(temp, decimal_obis);
    if (temp[0] == '"') {
    memmove(temp, temp + 1, strlen(temp));
}
 

    // Check if the OBIS code ends with _DT
    int has_dt_suffix = 0;
    int len = strlen(temp);
  
    if (len > 3 && strcmp(&temp[len - 3], "_DT") == 0) {
        has_dt_suffix = 1;
        temp[len - 3] = '\0';  // Temporarily remove _DT for processing
        log_message(LOG_DEBUG, "  OBIS code has _DT suffix, will preserve it");
    }
    
    char *token = strtok(temp, "_");
    int first = 1;
    hex_obis[0] = '\0';
    
    int octet_count = 0;
    while (token != NULL) {
        log_message(LOG_DEBUG, "  ***********????????? token %s *****", token);
        int value = atoi(token);
        char hex_part[8];
        
        if (!first) {
            strcat(hex_obis, "_");
        }
        first = 0;
        
        // Convert to hexadecimal with zero padding (2 digits)
        sprintf(hex_part, "%02x", value);
        
        log_message(LOG_DEBUG, "  Octet %d: %d -> %s", octet_count + 1, value, hex_part);
        
        strcat(hex_obis, hex_part);
        token = strtok(NULL, "_");
        octet_count++;
    }
    
    // Append _DT suffix if it was present
    if (has_dt_suffix) {
        strcat(hex_obis, "_DT");
        log_message(LOG_DEBUG, "  Appended _DT suffix");
    }
    
    log_message(LOG_DEBUG, "Converted for CDF lookup: %s -> %s", decimal_obis, hex_obis);
}

// Function to keep OBIS code in decimal format (no conversion to hex)
// Input: "0_0_98_0_1_255" Output: "0_0_98_0_1_255" (unchanged)
// Input: "1_0_1_8_0_255_DT" Output: "1_0_1_8_0_255_DT" (unchanged)
// void convert_obis_decimal_to_hex(const char *decimal_obis, char *hex_obis) {
//     log_message(LOG_DEBUG, "Keeping OBIS code in decimal format: %s", decimal_obis);
    
//     // Simply copy the input to output without any conversion
//     strcpy(hex_obis, decimal_obis);
    
//     log_message(LOG_DEBUG, "Result (unchanged): %s", hex_obis);
// }

// Function to get mapping info from CDF format
int get_cdf_mapping(const char *obis_code, char *param_code, char *param_name, char *unit, DataType data_type) {
    log_message(LOG_DEBUG, "Looking up CDF mapping for OBIS (decimal): %s", obis_code);
    
    // Convert decimal OBIS to hex for CDF lookup
    char hex_obis[MAX_CODE_LENGTH];
    convert_decimal_to_hex_for_cdf(obis_code, hex_obis);
    
    log_message(LOG_DEBUG, "Searching CDF with hex format: %s", hex_obis);
    
    // Select appropriate CDF table based on data type
    CDFInstMapping *cdf_table = NULL;
    int cdf_count = 0;
    const char *data_type_name = "";
    
    switch(data_type) {
        case DATA_TYPE_INST:
            cdf_table = cdf_inst_mappings;
            cdf_count = cdf_inst_mappings_count;
            data_type_name = "INST";
            break;
        case DATA_TYPE_LS:
            cdf_table = cdf_ls_mappings;
            cdf_count = cdf_ls_mappings_count;
            data_type_name = "LS";
            break;
        case DATA_TYPE_MN:
            cdf_table = cdf_mn_mappings;
            cdf_count = cdf_mn_mappings_count;
            data_type_name = "MN";
            break;
        case DATA_TYPE_BILL:
            cdf_table = cdf_bill_mappings;
            cdf_count = cdf_bill_mappings_count;
            data_type_name = "BILL";
            break;
        case DATA_TYPE_EVENT:
            cdf_table = cdf_event_mapping;
            cdf_count = cdf_event_mappings_count;
            data_type_name = "EVENT";
            break;
            // log_message(LOG_WARN, "No CDF mappings defined for EVENT data type");
            // return 0;
        default:
            log_message(LOG_ERROR, "Invalid data type for CDF mapping");
            return 0;
    }
    
    log_message(LOG_DEBUG, "Using %s CDF table with %d entries", data_type_name, cdf_count);
    
    for (int i = 0; i < cdf_count; i++) {
        if (strcmp(cdf_table[i].obis_code, hex_obis) == 0) {
            strcpy(param_code, cdf_table[i].code);
            strcpy(param_name, cdf_table[i].name);
            strcpy(unit, cdf_table[i].unit);
            
            log_message(LOG_DEBUG, "  Found: code='%s', name='%s', unit='%s'", 
                       param_code, param_name, unit);
            return 1;
        }
    }
    
    log_message(LOG_WARN, "  No CDF mapping found for OBIS: %s (searched as %s in %s table)", 
               obis_code, hex_obis, data_type_name);
    return 0;
}

// Function to get obis list from Redis based on data type
// char** get_obis_list_from_redis(redisContext *c, DataType data_type, int *count) {
//     const char *field_name = NULL;
    
//     switch(data_type) {
//         case DATA_TYPE_INST:
//             field_name = "inst_obis_list";
//             break;
//         case DATA_TYPE_LS:
//             field_name = "ls_obis_list";
//             break;
//         case DATA_TYPE_MN:
//             field_name = "mn_obis_list";
//             break;
//         case DATA_TYPE_BILL:
//             field_name = "bill_obis_list";
//             break;
//         case DATA_TYPE_EVENT:
//             field_name = "event_obis_list";
//             break;
//         default:
//             log_message(LOG_ERROR, "Invalid data type: %d", data_type);
//             return NULL;
//     }
    
//     log_message(LOG_INFO, "Fetching OBIS codes from Redis field: %s", field_name);
    
//     redisReply *reply = redisCommand(c, "HGET master_obis_codes %s", field_name);
//     if (reply == NULL) {
//         log_message(LOG_ERROR, "Redis command failed: reply is NULL");
//         return NULL;
//     }
    
//     if (reply->type != REDIS_REPLY_STRING) {
//         log_message(LOG_ERROR, "Redis reply type mismatch. Expected STRING, got type: %d", reply->type);
//         freeReplyObject(reply);
//         return NULL;
//     }
    
//     log_message(LOG_DEBUG, "Retrieved data from Redis: %s", reply->str);
    
//     // Parse the comma-separated obis codes
//     char *obis_str = strdup(reply->str);
//     freeReplyObject(reply);
    
//     // Count the number of codes
//     *count = 1;
//     for (char *p = obis_str; *p; p++) {
//         if (*p == ',') (*count)++;
//     }
    
//     log_message(LOG_INFO, "Found %d OBIS codes in the list", *count);
    
//     char **obis_codes = malloc(sizeof(char*) * (*count));
//     char *token = strtok(obis_str, ",");
//     int i = 0;
//     while (token != NULL) {
//         // Trim whitespace
//         while (*token == ' ') token++;
        
//         log_message(LOG_DEBUG, "Processing OBIS code %d (decimal): %s", i + 1, token);
        
//         // Keep in decimal format (no conversion to hex)
//         char decimal_obis[MAX_CODE_LENGTH];
//         convert_obis_decimal_to_hex(token, decimal_obis);
//         obis_codes[i] = strdup(decimal_obis);
        
//         log_message(LOG_INFO, "OBIS code %d: %s", i + 1, decimal_obis);
        
//         token = strtok(NULL, ",");
//         i++;
//     }
    
//     free(obis_str);
//     log_message(LOG_INFO, "Successfully processed %d OBIS codes", i);
//     return obis_codes;
// }


// rithika 17Feb2026
char **get_obis_list_from_redis(redisContext *c, DataType data_type, int *count) {
     const char *field_name = NULL;
    
    switch(data_type) {
        case DATA_TYPE_INST:
            field_name = "inst_obis_list";
            break;
        case DATA_TYPE_LS:
            field_name = "ls_obis_list";
            break;
        case DATA_TYPE_MN:
            field_name = "mn_obis_list";
            break;
        case DATA_TYPE_BILL:
            field_name = "bill_obis_list";
            break;
        case DATA_TYPE_EVENT:
            field_name = "event_obis_list";
            break;
        default:
            log_message(LOG_ERROR, "Invalid data type: %d", data_type);
            return NULL;
    }
    redisReply *reply = redisCommand(c, "HGET master_obis_codes %s", field_name);
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        return NULL;
    }

    log_message(LOG_DEBUG, "Retrieved data from Redis: %s", reply->str);

    json_error_t error;
    json_t *array = json_loads(reply->str, 0, &error);
    freeReplyObject(reply);
    if (!array || !json_is_array(array)) {
        log_message(LOG_ERROR, "Failed to parse OBIS JSON array from Redis: %s", error.text);
        if (array) json_decref(array);
        return NULL;
    }

    *count = json_array_size(array);
    char **obis_codes = malloc(sizeof(char*) * (*count));

    for (int i = 0; i < *count; i++) {
        const char *obis = json_string_value(json_array_get(array, i));
        if (!obis) obis = "";  // fallback empty
        obis_codes[i] = strdup(obis);
        log_message(LOG_INFO, "OBIS code %d: %s", i + 1, obis_codes[i]);
    }

    json_decref(array);
    return obis_codes;
}


// Function to get parameter names from Redis
// char** get_param_names_from_redis(redisContext *c, DataType data_type, int *count) {
//     const char *field_name = NULL;
    
//     switch(data_type) {
//         case DATA_TYPE_INST:
//             field_name = "inst_param_name_list";
//             break;
//         case DATA_TYPE_LS:
//             field_name = "ls_param_name_list";
//             break;
//         case DATA_TYPE_MN:
//             field_name = "mn_param_name_list";
//             break;
//         case DATA_TYPE_BILL:
//             field_name = "bill_param_name_list";
//             break;
//         case DATA_TYPE_EVENT:
//             field_name = "event_param_name_list";
//             break;
//         default:
//             log_message(LOG_ERROR, "Invalid data type: %d", data_type);
//             return NULL;
//     }
    
//     log_message(LOG_INFO, "Fetching parameter names from Redis field: %s", field_name);
    
//     redisReply *reply = redisCommand(c, "HGET master_obis_codes %s", field_name);
//     if (reply == NULL) {
//         log_message(LOG_ERROR, "Redis command failed: reply is NULL");
//         return NULL;
//     }
    
//     if (reply->type != REDIS_REPLY_STRING) {
//         log_message(LOG_ERROR, "Redis reply type mismatch. Expected STRING, got type: %d", reply->type);
//         freeReplyObject(reply);
//         return NULL;
//     }
    
//     log_message(LOG_DEBUG, "Retrieved parameter names from Redis: %s", reply->str);
    
//     // Parse the comma-separated parameter names
//     char *param_str = strdup(reply->str);
//     freeReplyObject(reply);
    
//     // Count the number of names
//     *count = 1;
//     for (char *p = param_str; *p; p++) {
//         if (*p == ',') (*count)++;
//     }
    
//     log_message(LOG_INFO, "Found %d parameter names in the list", *count);
    
//     char **param_names = malloc(sizeof(char*) * (*count));
//     char *token = strtok(param_str, ",");
//     int i = 0;
//     while (token != NULL) {
//         // Trim whitespace
//         while (*token == ' ') token++;
//         param_names[i] = strdup(token);
        
//         log_message(LOG_DEBUG, "Parameter name %d: %s", i + 1, param_names[i]);
        
//         token = strtok(NULL, ",");
//         i++;
//     }
    
//     free(param_str);
//     log_message(LOG_INFO, "Successfully processed %d parameter names", i);
//     return param_names;
// }


// rithika 17Feb2026
char** get_param_names_from_redis(redisContext *c, DataType data_type, int *count) {
    const char *field_name = NULL;
    switch(data_type) {
        case DATA_TYPE_INST: field_name = "inst_param_name_list"; break;
        case DATA_TYPE_LS:   field_name = "ls_param_name_list"; break;
        case DATA_TYPE_MN:   field_name = "mn_param_name_list"; break;
        case DATA_TYPE_BILL: field_name = "bill_param_name_list"; break;
        case DATA_TYPE_EVENT: field_name = "event_param_name_list"; break;
        default:
            log_message(LOG_ERROR, "Invalid data type: %d", data_type);
            return NULL;
    }

    redisReply *reply = redisCommand(c, "HGET master_obis_codes %s", field_name);
    if (!reply || reply->type != REDIS_REPLY_STRING) {
        if (reply) freeReplyObject(reply);
        log_message(LOG_ERROR, "Redis command failed or wrong type");
        return NULL;
    }

    json_error_t error;
    json_t *array = json_loads(reply->str, 0, &error);
    freeReplyObject(reply);

    if (!array || !json_is_array(array)) {
        log_message(LOG_ERROR, "Failed to parse JSON array of parameter names: %s", error.text);
        if (array) json_decref(array);
        return NULL;
    }

    *count = json_array_size(array);
    char **param_names = malloc(sizeof(char*) * (*count));

    for (int i = 0; i < *count; i++) {
        const char *name = json_string_value(json_array_get(array, i));
        param_names[i] = strdup(name ? name : "");
        log_message(LOG_DEBUG, "Parameter name %d: %s", i + 1, param_names[i]);
    }

    json_decref(array);
    return param_names;
}


// Function to create JSON mapping file
int create_json_mapping(ObisMapping *mappings, int count, const char *filename) {
    log_message(LOG_INFO, "Creating JSON mapping file: %s", filename);
    log_message(LOG_DEBUG, "Number of mappings to write: %d", count);
    
    json_t *root = json_object();
    json_t *obis_paramcode_map = json_object();
    json_t *obis_paramname_map = json_object();
    json_t *obis_unit_map = json_object();
    
    for (int i = 0; i < count; i++) {
        log_message(LOG_DEBUG, "Adding mapping %d: OBIS=%s, Code=%s, Name=%s, Unit=%s",
                   i + 1, mappings[i].obis_code, mappings[i].param_code,
                   mappings[i].param_name, mappings[i].unit);
        
        json_object_set_new(obis_paramcode_map, mappings[i].obis_code, 
                           json_string(mappings[i].param_code));
        json_object_set_new(obis_paramname_map, mappings[i].obis_code, 
                           json_string(mappings[i].param_name));
        json_object_set_new(obis_unit_map, mappings[i].obis_code, 
                           json_string(mappings[i].unit));
    }
    
    json_object_set_new(root, "obis_paramcode_map", obis_paramcode_map);
    json_object_set_new(root, "obis_paramname_map", obis_paramname_map);
    json_object_set_new(root, "obis_unit_map", obis_unit_map);
    
    // Write to file
    log_message(LOG_INFO, "Writing JSON to file: %s", filename);
    if (json_dump_file(root, filename, JSON_INDENT(2)) != 0) {
        log_message(LOG_ERROR, "Failed to write JSON file: %s", filename);
        json_decref(root);
        return 0;
    }
    
    json_decref(root);
    log_message(LOG_INFO, "Successfully created JSON file: %s", filename);
    return 1;
}

// Function to create Redis hash from mappings
int create_redis_hash(redisContext *c, const char *hash_name, ObisMapping *mappings, int count) {
    log_message(LOG_INFO, "Creating Redis hash: %s", hash_name);
    
    // First, delete the hash if it exists
    log_message(LOG_DEBUG, "Checking if hash exists and deleting if present");
    redisReply *reply = redisCommand(c, "DEL %s", hash_name);
    if (reply) {
        log_message(LOG_DEBUG, "DEL command result: %lld", reply->integer);
        freeReplyObject(reply);
    } else {
        log_message(LOG_WARN, "DEL command failed or returned NULL");
    }
    
    // Create the hash with three sub-hashes stored as JSON strings
    log_message(LOG_DEBUG, "Building JSON objects for Redis hash fields");
    json_t *obis_paramcode_map = json_object();
    json_t *obis_paramname_map = json_object();
    json_t *obis_unit_map = json_object();
    
    for (int i = 0; i < count; i++) {
        log_message(LOG_DEBUG, "Adding to Redis hash - Mapping %d: OBIS=%s", 
                   i + 1, mappings[i].obis_code);
        
        json_object_set_new(obis_paramcode_map, mappings[i].obis_code, 
                           json_string(mappings[i].param_code));
        json_object_set_new(obis_paramname_map, mappings[i].obis_code, 
                           json_string(mappings[i].param_name));
        json_object_set_new(obis_unit_map, mappings[i].obis_code, 
                           json_string(mappings[i].unit));
    }
    
    char *paramcode_str = json_dumps(obis_paramcode_map, JSON_COMPACT);
    char *paramname_str = json_dumps(obis_paramname_map, JSON_COMPACT);
    char *unit_str = json_dumps(obis_unit_map, JSON_COMPACT);
    
    log_message(LOG_DEBUG, "Paramcode map size: %zu bytes", strlen(paramcode_str));
    log_message(LOG_DEBUG, "Paramname map size: %zu bytes", strlen(paramname_str));
    log_message(LOG_DEBUG, "Unit map size: %zu bytes", strlen(unit_str));
    
    log_message(LOG_INFO, "Setting obis_paramcode_map field in Redis");
    printf("paramcode_str %s\n", paramcode_str);
    printf("paramname_str %s\n", paramname_str);
    printf("unit_str %s\n", unit_str);

    reply = redisCommand(c, "HSET %s obis_paramcode_map %s", hash_name, paramcode_str);
    if (reply) {
        log_message(LOG_DEBUG, "HSET obis_paramcode_map result: %lld", reply->integer);
        freeReplyObject(reply);
    } else {
        log_message(LOG_ERROR, "HSET obis_paramcode_map failed");
    }
    
    log_message(LOG_INFO, "Setting obis_paramname_map field in Redis");
    reply = redisCommand(c, "HSET %s obis_paramname_map %s", hash_name, paramname_str);
    if (reply) {
        log_message(LOG_DEBUG, "HSET obis_paramname_map result: %lld", reply->integer);
        freeReplyObject(reply);
    } else {
        log_message(LOG_ERROR, "HSET obis_paramname_map failed");
    }
    
    log_message(LOG_INFO, "Setting obis_unit_map field in Redis");
    reply = redisCommand(c, "HSET %s obis_unit_map %s", hash_name, unit_str);
    if (reply) {
        log_message(LOG_DEBUG, "HSET obis_unit_map result: %lld", reply->integer);
        freeReplyObject(reply);
    } else {
        log_message(LOG_ERROR, "HSET obis_unit_map failed");
    }
    
    free(paramcode_str);
    free(paramname_str);
    free(unit_str);
    json_decref(obis_paramcode_map);
    json_decref(obis_paramname_map);
    json_decref(obis_unit_map);
    
    log_message(LOG_INFO, "Successfully created Redis hash: %s", hash_name);
    return 1;
}




// Function to process data based on type
int process_data_type(redisContext *c, DataType data_type, int use_cdf_format) {
    const char *hash_name = NULL;
    const char *json_filename = NULL;
    
    log_message(LOG_INFO, "========================================");
    log_message(LOG_INFO, "Processing data type: %d", data_type);
    log_message(LOG_INFO, "Use CDF format: %s", use_cdf_format ? "YES" : "NO");
    log_message(LOG_INFO, "========================================");
    
    switch(data_type) {
        case DATA_TYPE_INST:
            hash_name = "inst_cdf_obis_param_map";
            json_filename = "inst_obis_mapping.json";
            log_message(LOG_INFO, "Data type: INSTANTANEOUS");
            break;
        case DATA_TYPE_LS:
            hash_name = "ls_cdf_obis_param_map";
            json_filename = "ls_obis_mapping.json";
            log_message(LOG_INFO, "Data type: LOAD SURVEY");
            break;
        case DATA_TYPE_MN:
            hash_name = "mn_cdf_obis_param_map";
            json_filename = "mn_obis_mapping.json";
            log_message(LOG_INFO, "Data type: MIDNIGHT");
            break;
        case DATA_TYPE_BILL:
            hash_name = "bill_cdf_obis_param_map";
            json_filename = "bill_obis_mapping.json";
            log_message(LOG_INFO, "Data type: BILLING");
            break;
        case DATA_TYPE_EVENT:
            hash_name = "event_cdf_obis_param_map";
            json_filename = "event_obis_mapping.json";
            log_message(LOG_INFO, "Data type: EVENT");
            break;
        default:
            log_message(LOG_ERROR, "Invalid data type: %d", data_type);
            return 0;
    }
    
    log_message(LOG_INFO, "Target hash name: %s", hash_name);
    log_message(LOG_INFO, "Target JSON file: %s", json_filename);
    
    // Get OBIS codes from Redis
    log_message(LOG_INFO, "Step 1: Retrieving OBIS codes from Redis");
    int obis_count = 0;
    char **obis_codes = get_obis_list_from_redis(c, data_type, &obis_count);
    if (!obis_codes) {
        log_message(LOG_ERROR, "Failed to get OBIS codes from Redis");
        return 0;
    }
    log_message(LOG_INFO, "Successfully retrieved %d OBIS codes", obis_count);
    
    // Get parameter names from Redis (for non-CDF format)
    int param_count = 0;
    char **param_names = NULL;
    if (!use_cdf_format) {
        log_message(LOG_INFO, "Step 2: Retrieving parameter names from Redis (non-CDF mode)");
        param_names = get_param_names_from_redis(c, data_type, &param_count);
        if (!param_names || param_count != obis_count) {
            log_message(LOG_ERROR, "Parameter name count mismatch: expected %d, got %d", 
                       obis_count, param_count);
            // Free resources
            for (int i = 0; i < obis_count; i++) free(obis_codes[i]);
            free(obis_codes);
            if (param_names) {
                for (int i = 0; i < param_count; i++) free(param_names[i]);
                free(param_names);
            }
            return 0;
        }
        log_message(LOG_INFO, "Successfully retrieved %d parameter names", param_count);
    } else {
        log_message(LOG_INFO, "Step 2: Skipping parameter name retrieval (CDF mode)");
    }
    
    // Create mappings
    log_message(LOG_INFO, "Step 3: Creating OBIS mappings");
    ObisMapping *mappings = malloc(sizeof(ObisMapping) * obis_count);
    
    int cdf_found = 0, cdf_not_found = 0;
    
    for (int i = 0; i < obis_count; i++) {
        strcpy(mappings[i].obis_code, obis_codes[i]);
        
        if (use_cdf_format) {
            // Use CDF format (available for INST, LS, MN)
            log_message(LOG_DEBUG, "Looking up CDF mapping for OBIS %d: %s", i + 1, obis_codes[i]);
            if (!get_cdf_mapping(obis_codes[i], mappings[i].param_code, 
                                mappings[i].param_name, mappings[i].unit, data_type)) {
                // If not found in CDF, use empty strings
                log_message(LOG_WARN, "CDF mapping not found for OBIS: %s - using empty values", 
                           obis_codes[i]);
                strcpy(mappings[i].param_code, "");
                strcpy(mappings[i].param_name, "");
                strcpy(mappings[i].unit, "");
                cdf_not_found++;
            } else {
                cdf_found++;
            }
        } else {
            // Use parameter names from Redis
            log_message(LOG_DEBUG, "Using Redis parameter name for OBIS %d: %s -> %s", 
                       i + 1, obis_codes[i], param_names[i]);
            strcpy(mappings[i].param_code, "");  // Empty for non-CDF
            strcpy(mappings[i].param_name, param_names[i]);
            strcpy(mappings[i].unit, "");  // Would need to be extended for other types
        }
    }
    
    if (use_cdf_format) {
        log_message(LOG_INFO, "CDF mapping summary: Found=%d, Not found=%d", cdf_found, cdf_not_found);
    }
    
    // Create JSON file
    log_message(LOG_INFO, "Step 4: Creating JSON mapping file");
    if (!create_json_mapping(mappings, obis_count, json_filename)) {
        log_message(LOG_ERROR, "Failed to create JSON mapping file");
        free(mappings);
        for (int i = 0; i < obis_count; i++) free(obis_codes[i]);
        free(obis_codes);
        if (param_names) {
            for (int i = 0; i < param_count; i++) free(param_names[i]);
            free(param_names);
        }
        return 0;
    }
    
    // Create Redis hash
    log_message(LOG_INFO, "Step 5: Creating Redis hash");
    if (!create_redis_hash(c, hash_name, mappings, obis_count)) {
        log_message(LOG_ERROR, "Failed to create Redis hash");
        free(mappings);
        for (int i = 0; i < obis_count; i++) free(obis_codes[i]);
        free(obis_codes);
        if (param_names) {
            for (int i = 0; i < param_count; i++) free(param_names[i]);
            free(param_names);
        }
        return 0;
    }
    
    log_message(LOG_INFO, "========================================");
    log_message(LOG_INFO, "SUCCESS: Created hash %s with %d OBIS codes", hash_name, obis_count);
    log_message(LOG_INFO, "========================================");
    
    // Cleanup
    free(mappings);
    for (int i = 0; i < obis_count; i++) free(obis_codes[i]);
    free(obis_codes);
    if (param_names) {
        for (int i = 0; i < param_count; i++) free(param_names[i]);
        free(param_names);
    }
    
    return 1;
}

void print_usage(const char *prog_name) {
    printf("Usage: %s <data_type> [options]\n", prog_name);
    printf("Data types:\n");
    printf("  inst      - Instantaneous data\n");
    printf("  ls        - Load Survey data\n");
    printf("  mn        - Midnight data\n");
    printf("  bill      - Billing data\n");
    printf("  event     - Event data\n");
    printf("\nOptions:\n");
    printf("  --cdf     - Use CDF format for parameter names and units (default for inst)\n");
    printf("  --redis   - Use parameter names from Redis master_obis_codes hash\n");
    printf("  -v, --verbose - Enable verbose debug logging\n");
    printf("\nExamples:\n");
    printf("  %s inst --cdf\n", prog_name);
    printf("  %s ls --redis\n", prog_name);
    printf("  %s inst --cdf --verbose\n", prog_name);
    printf("\nLog file: %s\n", LOG_FILE);
}

int main1(int argc, char **argv) {
    // Initialize logging
    init_logging(LOG_FILE, LOG_DEBUG);  // Use LOG_INFO for less verbose output
    
    log_message(LOG_INFO, "========================================");
    log_message(LOG_INFO, "Redis Hash Generator - Starting");
    log_message(LOG_INFO, "Version: 1.0");
    log_message(LOG_INFO, "========================================");
    
    if (argc < 2) {
        log_message(LOG_ERROR, "No arguments provided");
        print_usage(argv[0]);
        close_logging();
        return 1;
    }
    
    log_message(LOG_INFO, "Command line arguments:");
    for (int i = 0; i < argc; i++) {
        log_message(LOG_INFO, "  argv[%d]: %s", i, argv[i]);
    }
    
    // Parse data type
    DataType data_type;
    if (strcmp(argv[1], "inst") == 0) {
        data_type = DATA_TYPE_INST;
        log_message(LOG_INFO, "Selected data type: INSTANTANEOUS");
    } else if (strcmp(argv[1], "ls") == 0) {
        data_type = DATA_TYPE_LS;
        log_message(LOG_INFO, "Selected data type: LOAD SURVEY");
    } else if (strcmp(argv[1], "mn") == 0) {
        data_type = DATA_TYPE_MN;
        log_message(LOG_INFO, "Selected data type: MIDNIGHT");
    } else if (strcmp(argv[1], "bill") == 0) {
        data_type = DATA_TYPE_BILL;
        log_message(LOG_INFO, "Selected data type: BILLING");
    } else if (strcmp(argv[1], "event") == 0) {
        data_type = DATA_TYPE_EVENT;
        log_message(LOG_INFO, "Selected data type: EVENT");
    } else {
        log_message(LOG_ERROR, "Invalid data type: %s", argv[1]);
        print_usage(argv[0]);
        close_logging();
        return 1;
    }
    
    // Parse options
    int use_cdf_format = (data_type == DATA_TYPE_INST) ? 1 : 0;  // Default to CDF for inst
    log_message(LOG_DEBUG, "Default format mode: %s", use_cdf_format ? "CDF" : "Redis");
    
    for (int i = 2; i < argc; i++) {
        if (strcmp(argv[i], "--cdf") == 0) {
            use_cdf_format = 1;
            log_message(LOG_INFO, "Option: Using CDF format (from command line)");
        } else if (strcmp(argv[i], "--redis") == 0) {
            use_cdf_format = 0;
            log_message(LOG_INFO, "Option: Using Redis format (from command line)");
        } else if (strcmp(argv[i], "--verbose") == 0 || strcmp(argv[i], "-v") == 0) {
            current_log_level = LOG_DEBUG;
            log_message(LOG_INFO, "Verbose logging enabled");
        } else {
            log_message(LOG_WARN, "Unknown option: %s", argv[i]);
        }
    }
    
    // Connect to Redis
    log_message(LOG_INFO, "Connecting to Redis at 127.0.0.1:6379");
    redisContext *c = redisConnect("127.0.0.1", 6379);
    if (c == NULL || c->err) {
        if (c) {
            log_message(LOG_ERROR, "Redis connection error: %s", c->errstr);
            redisFree(c);
        } else {
            log_message(LOG_ERROR, "Cannot allocate redis context");
        }
        close_logging();
        return 1;
    }
    
    log_message(LOG_INFO, "Successfully connected to Redis");
    
    // Test Redis connection
    redisReply *reply = redisCommand(c, "PING");
    if (reply && reply->type == REDIS_REPLY_STATUS) {
        log_message(LOG_DEBUG, "Redis PING response: %s", reply->str);
        freeReplyObject(reply);
    } else {
        log_message(LOG_WARN, "Redis PING failed or returned unexpected response");
        if (reply) freeReplyObject(reply);
    }
    
    log_message(LOG_INFO, "Processing configuration:");
    log_message(LOG_INFO, "  Data Type: %s", argv[1]);
    log_message(LOG_INFO, "  Format: %s", use_cdf_format ? "CDF" : "Redis");
    
    // Process the data type
    int result = process_data_type(c, data_type, use_cdf_format);
    
    // Cleanup
    log_message(LOG_INFO, "Closing Redis connection");
    redisFree(c);
    
    if (result) {
        log_message(LOG_INFO, "========================================");
        log_message(LOG_INFO, "Program completed successfully");
        log_message(LOG_INFO, "========================================");
    } else {
        log_message(LOG_ERROR, "========================================");
        log_message(LOG_ERROR, "Program completed with errors");
        log_message(LOG_ERROR, "========================================");
    }
    
    close_logging();
    return result ? 0 : 1;
}
