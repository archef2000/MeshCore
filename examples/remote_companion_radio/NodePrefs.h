#pragma once
#include <cstdint> // For uint8_t, uint32_t

#define TELEM_MODE_DENY            0
#define TELEM_MODE_ALLOW_FLAGS     1     // use contact.flags
#define TELEM_MODE_ALLOW_ALL       2

#define ADVERT_LOC_NONE       0
#define ADVERT_LOC_SHARE      1

struct ClientNodePrefs {  // persisted to file
  float airtime_factor;
  char node_name[32];
  float freq;
  uint8_t sf;
  uint8_t cr;
  uint8_t multi_acks;
  uint8_t manual_add_contacts;
  float bw;
  int8_t tx_power_dbm;
  uint8_t telemetry_mode_base;
  uint8_t telemetry_mode_loc;
  uint8_t telemetry_mode_env;
  float rx_delay_base;
  uint32_t ble_pin;
  uint8_t  advert_loc_policy;
  uint8_t  buzzer_quiet;
  uint8_t  gps_enabled;      // GPS enabled flag (0=disabled, 1=enabled)
  uint32_t gps_interval;     // GPS read interval in seconds
  uint8_t autoadd_config;    // bitmask for auto-add contacts config
  uint8_t rx_boosted_gain; // SX126x RX boosted gain mode (0=power saving, 1=boosted)
  uint8_t client_repeat;
  uint8_t path_hash_mode;    // which path mode to use when sending
  uint8_t autoadd_max_hops;  // 0 = no limit, 1 = direct (0 hops), N = up to N-1 hops (max 64)
};

// from <helpers/CommonCLI.h>
struct RemotePrefs { // persisted to file
  float airtime_factor;
  char node_name[32];
  double node_lat, node_lon;
  char password[16];
  float freq;
  int8_t tx_power_dbm;
  uint8_t disable_fwd;
  uint8_t advert_interval;       // minutes / 2
  uint8_t flood_advert_interval; // hours
  float rx_delay_base;
  float tx_delay_factor;
  char guest_password[16];
  float direct_tx_delay_factor;
  uint32_t guard;
  uint8_t sf;
  uint8_t cr;
  uint8_t allow_read_only;
  uint8_t multi_acks;
  float bw;
  uint8_t flood_max;
  uint8_t interference_threshold;
  uint8_t agc_reset_interval; // secs / 4
  // Bridge settings
  uint8_t bridge_enabled; // boolean
  uint16_t bridge_delay;  // milliseconds (default 500 ms)
  uint8_t bridge_pkt_src; // 0 = logTx, 1 = logRx (default logTx)
  uint32_t bridge_baud;   // 9600, 19200, 38400, 57600, 115200 (default 115200)
  uint8_t bridge_channel; // 1-14 (ESP-NOW only)
  char bridge_secret[16]; // for XOR encryption of bridge packets (ESP-NOW only)
  // Power setting
  uint8_t powersaving_enabled; // boolean
  // Gps settings
  uint8_t gps_enabled;
  uint32_t gps_interval; // in seconds
  uint8_t advert_loc_policy;
  uint32_t discovery_mod_timestamp;
  float adc_multiplier;
  char owner_info[120];
  uint8_t rx_boosted_gain; // power settings
  uint8_t path_hash_mode;   // which path mode to use when sending
  uint8_t loop_detect;
};
