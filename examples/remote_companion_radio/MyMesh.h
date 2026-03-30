#pragma once

#include "AbstractUITask.h"
#include "RemoteCliCallbacks.h"

#include <Arduino.h>
#include <Mesh.h>

/*------------ Frame Protocol --------------*/
#define FIRMWARE_VER_CODE 10

#ifndef FIRMWARE_BUILD_DATE
#define FIRMWARE_BUILD_DATE "20 Mar 2026"
#endif

#ifndef FIRMWARE_VERSION
#define FIRMWARE_VERSION "v1.14.1"
#endif

#if defined(NRF52_PLATFORM) || defined(STM32_PLATFORM)
#include <InternalFileSystem.h>
#elif defined(RP2040_PLATFORM)
#include <LittleFS.h>
#elif defined(ESP32)
#include <SPIFFS.h>
#endif

#include "DataStore.h"
#include "NodePrefs.h"

#include <RTClib.h>
#include <helpers/AdvertDataHelpers.h>
#include <helpers/ArduinoHelpers.h>
#include <helpers/BaseSerialInterface.h>
#include <helpers/ChannelDetails.h>
#include <helpers/ClientACL.h>
#include <helpers/CommonCLI.h>
#include <helpers/ContactInfo.h>
#include <helpers/IdentityStore.h>
#include <helpers/SimpleMeshTables.h>
#include <helpers/StaticPoolPacketManager.h>
#include <helpers/TxtDataHelpers.h>
#include <target.h>

/* ---------------------------------- CONFIGURATION ------------------------------------- */

#ifndef LORA_FREQ
#define LORA_FREQ 915.0
#endif
#ifndef LORA_BW
#define LORA_BW 250
#endif
#ifndef LORA_SF
#define LORA_SF 10
#endif
#ifndef LORA_CR
#define LORA_CR 5
#endif
#ifndef LORA_TX_POWER
#define LORA_TX_POWER 20
#endif
#ifndef MAX_LORA_TX_POWER
#define MAX_LORA_TX_POWER LORA_TX_POWER
#endif

#ifndef MAX_CONTACTS
#define MAX_CONTACTS 100
#endif

#ifndef MAX_GROUP_CHANNELS
#define MAX_GROUP_CHANNELS 8
#endif

#ifndef OFFLINE_QUEUE_SIZE
#define OFFLINE_QUEUE_SIZE 16
#endif

#ifndef LOOPBACK_QUEUE_SIZE
#define LOOPBACK_QUEUE_SIZE 16
#endif

#ifndef BLE_NAME_PREFIX
#define BLE_NAME_PREFIX "MeshCore-"
#endif

#include <helpers/TransportKeyStore.h>

#define BYTE_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c"
#define BYTE_TO_BINARY(byte)                                                           \
  (byte & 0x80 ? '1' : '0'), (byte & 0x40 ? '1' : '0'), (byte & 0x20 ? '1' : '0'),     \
      (byte & 0x10 ? '1' : '0'), (byte & 0x08 ? '1' : '0'), (byte & 0x04 ? '1' : '0'), \
      (byte & 0x02 ? '1' : '0'), (byte & 0x01 ? '1' : '0')

/* -------------------------------------------------------------------------------------- */

#define REQ_TYPE_GET_STATUS         0x01 // same as _GET_STATS
#define REQ_TYPE_KEEP_ALIVE         0x02
#define REQ_TYPE_GET_TELEMETRY_DATA 0x03
#define REQ_TYPE_GET_AVG_MIN_MAX    0x04
#define REQ_TYPE_GET_ACCESS_LIST    0x05

#define MAX_TEXT_LEN                (10 * CIPHER_BLOCK_SIZE)

#define MAX_SEARCH_RESULTS          8

#define MSG_SEND_FAILED             0
#define MSG_SEND_SENT_FLOOD         1
#define MSG_SEND_SENT_DIRECT        2

#define RESP_SERVER_LOGIN_OK        0

#ifndef MAX_CONNECTIONS
#define MAX_CONNECTIONS 16
#endif

struct ConnectionInfo {
  mesh::Identity server_id;
  unsigned long next_ping;
  uint32_t last_activity;
  uint32_t keep_alive_millis;
  uint32_t expected_ack;
};

class ContactVisitor {
public:
  virtual void onContactVisit(const ContactInfo &contact) = 0;
};

class MyMesh;

class ContactsIterator {
  int next_idx = 0;

public:
  bool hasNext(const MyMesh *mesh, ContactInfo &dest);
};

struct AdvertPath {
  uint8_t pubkey_prefix[7];
  uint8_t path_len;
  char name[32];
  uint32_t recv_timestamp;
  uint8_t path[MAX_PATH_SIZE];
};

class MyMesh : public mesh::Mesh, public DataStoreHost {
  friend class RemoteCliCallbacks;
  friend class ContactsIterator;

public:
  MyMesh(mesh::Radio &radio, mesh::RNG &rng, mesh::RTCClock &rtc, SimpleMeshTables &tables, DataStore &store,
         AbstractUITask *ui = NULL);

  void begin(bool has_display);
  void startInterface(BaseSerialInterface &serial);

  const char *getNodeName();
  ClientNodePrefs *getNodePrefs();
  uint32_t getBLEPin();

  void loop();
  void handleCmdFrame(size_t len);
  bool advert();
  void enterCLIRescue();

  int getRecentlyHeard(AdvertPath dest[], int max_num);

  struct LocalIdentityEntry {
    mesh::LocalIdentity id;
    uint8_t adv_type;
    uint8_t prefs_kind;
  };

  static const uint8_t PREFS_KIND_MAIN = 0;
  static const uint8_t PREFS_KIND_SENSOR = 1;

  int addLocalIdentity(const mesh::LocalIdentity &id, uint8_t adv_type, uint8_t prefs_kind);
  int getNumLocalIdentities() const { return num_local_identities; }
  const LocalIdentityEntry *getLocalIdentity(int idx) const;
  const mesh::LocalIdentity &getMainIdentity() const { return local_identities[0].id; }
  mesh::LocalIdentity &getMainIdentity() { return local_identities[0].id; }
  mesh::LocalIdentity &getSensorIdentity() { return local_identities[sensor_identity_idx].id; }

  mesh::Packet *createSelfAdvert(uint8_t identity_idx, const char *name);
  mesh::Packet *createSelfAdvert(uint8_t identity_idx, const char *name, double lat, double lon);
  mesh::Packet *createSensorAdvert(const char *name) { return createSelfAdvert(sensor_identity_idx, name); }
  mesh::Packet *createSensorAdvert(const char *name, double lat, double lon) {
    return createSelfAdvert(sensor_identity_idx, name, lat, lon);
  }

  int sendMessage(const ContactInfo &recipient, uint32_t timestamp, uint8_t attempt, const char *text,
                  uint32_t &expected_ack, uint32_t &est_timeout, uint8_t sender_identity_idx = 0);
  int sendCommandData(const ContactInfo &recipient, uint32_t timestamp, uint8_t attempt, const char *text,
                      uint32_t &est_timeout, uint8_t sender_identity_idx = 0);
  bool sendGroupMessage(uint32_t timestamp, mesh::GroupChannel &channel, const char *sender_name,
                        const char *text, int text_len);
  int sendLogin(const ContactInfo &recipient, const char *password, uint32_t &est_timeout,
                uint8_t sender_identity_idx = 0);
  int sendAnonReq(const ContactInfo &recipient, const uint8_t *data, uint8_t len, uint32_t &tag,
                  uint32_t &est_timeout, uint8_t sender_identity_idx = 0);
  int sendRequest(const ContactInfo &recipient, uint8_t req_type, uint32_t &tag, uint32_t &est_timeout,
                  uint8_t sender_identity_idx = 0);
  int sendRequest(const ContactInfo &recipient, const uint8_t *req_data, uint8_t data_len, uint32_t &tag,
                  uint32_t &est_timeout, uint8_t sender_identity_idx = 0);
  bool shareContactZeroHop(const ContactInfo &contact);
  uint8_t exportContact(const ContactInfo &contact, uint8_t dest_buf[]);
  bool importContact(const uint8_t src_buf[], uint8_t len);
  void resetPathTo(ContactInfo &recipient);
  void scanRecentContacts(int last_n, ContactVisitor *visitor);
  ContactInfo *searchContactsByPrefix(const char *name_prefix);
  ContactInfo *lookupContactByPubKey(const uint8_t *pub_key, int prefix_len);
  bool removeContact(ContactInfo &contact);
  bool addContact(const ContactInfo &contact);
  int getNumContacts() const { return num_contacts; }
  bool getContactByIdx(uint32_t idx, ContactInfo &contact);
  ContactsIterator startContactsIterator();
  bool routePacketInternallyIfLocal(mesh::Packet *packet);
  ChannelDetails *addChannel(const char *name, const char *psk_base64);
  bool getChannel(int idx, ChannelDetails &dest);
  bool setChannel(int idx, const ChannelDetails &src);
  int findChannelIdx(const mesh::GroupChannel &ch);

protected:
  mesh::DispatcherAction onRecvPacket(mesh::Packet *pkt) override;

  float getAirtimeBudgetFactor() const override;
  int getInterferenceThreshold() const override;
  int calcRxDelay(float score, uint32_t air_time) const override;
  uint32_t getRetransmitDelay(const mesh::Packet *packet) override;
  uint32_t getDirectRetransmitDelay(const mesh::Packet *packet) override;
  uint8_t getExtraAckTransmitCount() const override;
  bool filterRecvFloodPacket(mesh::Packet *packet) override;
  // void onPeerDataRecv(mesh::Packet* packet, uint8_t type, int sender_idx, const uint8_t* secret, uint8_t*
  // data, size_t len) override;
  bool allowPacketForward(const mesh::Packet *packet) override;

  void sendFloodScoped(const ContactInfo &recipient, mesh::Packet *pkt, uint32_t delay_millis = 0);
  void sendFloodScoped(const mesh::GroupChannel &channel, mesh::Packet *pkt, uint32_t delay_millis = 0);

  void logRxRaw(float snr, float rssi, const uint8_t raw[], int len) override;
  bool isAutoAddEnabled() const;
  bool shouldAutoAddContactType(uint8_t type) const;
  bool shouldOverwriteWhenFull() const;
  uint8_t getAutoAddMaxHops() const;
  void onContactsFull();
  void onContactOverwrite(const uint8_t *pub_key);
  bool onContactPathRecv(ContactInfo &from, uint8_t *in_path, uint8_t in_path_len, uint8_t *out_path,
                         uint8_t out_path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len);
  bool onContactPathRecv(ClientInfo &from, uint8_t *in_path, uint8_t in_path_len, uint8_t *out_path,
                         uint8_t out_path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len);
  void onDiscoveredContact(ContactInfo &contact, bool is_new, uint8_t path_len, const uint8_t *path);
  void onContactPathUpdated(const ContactInfo &contact);
  ContactInfo *processAck(const uint8_t *data);
  void queueMessage(const ContactInfo &from, uint8_t txt_type, mesh::Packet *pkt, uint32_t sender_timestamp,
                    const uint8_t *extra, int extra_len, const char *text);

  void onMessageRecv(const ContactInfo &from, mesh::Packet *pkt, uint32_t sender_timestamp, const char *text);
  void onCommandDataRecv(const ContactInfo &from, mesh::Packet *pkt, uint32_t sender_timestamp,
                         const char *command);
  void onSignedMessageRecv(const ContactInfo &from, mesh::Packet *pkt, uint32_t sender_timestamp,
                           const uint8_t *sender_prefix, const char *text);
  void onChannelMessageRecv(const mesh::GroupChannel &channel, mesh::Packet *pkt, uint32_t timestamp,
                            const char *text);

  uint8_t onContactRequest(const ContactInfo &contact, uint32_t sender_timestamp, const uint8_t *data,
                           uint8_t len, uint8_t *reply);
  void onContactResponse(const ContactInfo &contact, const uint8_t *data, uint8_t len);
  void onControlDataRecv(mesh::Packet *packet) override;
  void onRawDataRecv(mesh::Packet *packet) override;
  void onTraceRecv(mesh::Packet *packet, uint32_t tag, uint32_t auth_code, uint8_t flags,
                   const uint8_t *path_snrs, const uint8_t *path_hashes, uint8_t path_len) override;

  void onAdvertRecv(mesh::Packet *packet, const mesh::Identity &id, uint32_t timestamp,
                    const uint8_t *app_data, size_t app_data_len) override;
  int searchPeersByHash(const uint8_t *src_hash, const uint8_t *dst_hash);
  void getPeerSharedSecret(uint8_t *dest_secret, int peer_idx) override;
  bool onPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                      uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len) override;
  bool clientOnPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                            uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len);
  bool sensorOnPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                            uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len);
  void onAckRecv(mesh::Packet *packet, uint32_t ack_crc) override;
  int searchChannelsByHash(const uint8_t *hash, mesh::GroupChannel channels[], int max_matches) override;
  void onGroupDataRecv(mesh::Packet *packet, uint8_t type, const mesh::GroupChannel &channel, uint8_t *data,
                       size_t len) override;

  uint32_t calcFloodTimeoutMillisFor(uint32_t pkt_airtime_millis) const;
  uint32_t calcDirectTimeoutMillisFor(uint32_t pkt_airtime_millis, uint8_t path_len) const;
  void onSendTimeout();

  // DataStoreHost methods
  bool onContactLoaded(const ContactInfo &contact) override { return addContact(contact); }
  bool getContactForSave(uint32_t idx, ContactInfo &contact) override {
    return getContactByIdx(idx, contact);
  }
  bool onChannelLoaded(uint8_t channel_idx, const ChannelDetails &ch) override {
    return setChannel(channel_idx, ch);
  }
  bool getChannelForSave(uint8_t channel_idx, ChannelDetails &ch) override {
    return getChannel(channel_idx, ch);
  }

  void clearPendingReqs() {
    pending_login = pending_status = pending_telemetry = pending_discovery = pending_req = 0;
  }

  void sendSensorAdvertToSelf();

  void bootstrapRTCfromContacts();
  void resetContacts() { num_contacts = 0; }
  void populateContactFromAdvert(ContactInfo &ci, const mesh::Identity &id, const AdvertDataParser &parser,
                                 uint32_t timestamp);
  ContactInfo *allocateContactSlot();
  ContactInfo *ensureContactForIdentity(const mesh::Identity &id, uint8_t type = ADV_TYPE_CHAT,
                                        const char *name = "Client");
  mesh::Packet *composeMsgPacket(const ContactInfo &recipient, uint32_t timestamp, uint8_t attempt,
                                 const char *text, uint32_t &expected_ack, uint8_t sender_identity_idx);
  // only for finding function that use the old one
  mesh::Packet *createDatagram(uint8_t type, const mesh::Identity &dest, const uint8_t *secret,
                               const uint8_t *data, size_t len);
  mesh::Packet *createDatagramFromIdentity(uint8_t type, const mesh::LocalIdentity &sender,
                                           const mesh::Identity &dest, const uint8_t *secret,
                                           const uint8_t *data, size_t data_len);
  mesh::Packet *createPathReturnFromIdentity(const mesh::LocalIdentity &sender, const mesh::Identity &dest,
                                             const uint8_t *secret, const uint8_t *path, uint8_t path_len,
                                             uint8_t extra_type, const uint8_t *extra, size_t extra_len);
  mesh::Packet *createPathReturnFromIdentity(const mesh::LocalIdentity &sender, const uint8_t *dest_hash,
                                             const uint8_t *secret, const uint8_t *path, uint8_t path_len,
                                             uint8_t extra_type, const uint8_t *extra, size_t extra_len);
  void sendAckTo(const ClientInfo &dest, uint32_t ack_hash, uint8_t path_hash_size);
  void sendAckTo(const ContactInfo &dest, uint32_t ack_hash, uint8_t path_hash_size);
  void handleReturnPathRetry(const ContactInfo &contact, const uint8_t *path, uint8_t path_len,
                             uint8_t sender_identity_idx = 0);
  void checkConnections();
  bool startConnection(const ContactInfo &contact, uint16_t keep_alive_secs);
  void stopConnection(const uint8_t *pub_key);
  bool hasConnectionTo(const uint8_t *pub_key);
  void markConnectionActive(const ContactInfo &contact);
  ContactInfo *checkConnectionsAck(const uint8_t *data);

  int getMatchingPeerIdentityIdx(int sender_idx) const;
  int findLocalIdentityIdx(const mesh::Identity &id) const;
  bool isPacketForLocalIdentity(const mesh::Packet *pkt) const;
  bool queueInternalPacket(mesh::Packet *pkt);
  const mesh::LocalIdentity &identityByIndex(uint8_t identity_idx) const;
  uint8_t handleLoginReq(const mesh::Identity &sender, const uint8_t *secret, uint32_t sender_timestamp,
                         const uint8_t *data, bool is_flood);

  void onAnonDataRecv(mesh::Packet *packet, const uint8_t *secret, const mesh::Identity &sender,
                      uint8_t *data, size_t len) override;
  void onPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                      uint8_t *data, size_t len) override;
  void sensorOnPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                            uint8_t *data, size_t len);
  void clientOnPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                            uint8_t *data, size_t len);
  bool handleIncomingMsg(ClientInfo &from, uint32_t timestamp, uint8_t *data, uint8_t flags, size_t len);

public:
  void savePrefs() { _store->savePrefs(_prefs, _cli, sensors.node_lat, sensors.node_lon); }

private:
  void writeOKFrame();
  void writeErrFrame(uint8_t err_code);
  void writeDisabledFrame();
  void writeContactRespFrame(uint8_t code, const ContactInfo &contact);
  void updateContactFromFrame(ContactInfo &contact, uint32_t &last_mod, const uint8_t *frame, int len);
  void addToOfflineQueue(const uint8_t frame[], int len);
  int getFromOfflineQueue(uint8_t frame[]);
  int getBlobByKey(const uint8_t key[], int key_len, uint8_t dest_buf[]) {
    return _store->getBlobByKey(key, key_len, dest_buf);
  }
  bool putBlobByKey(const uint8_t key[], int key_len, const uint8_t src_buf[], int len) {
    return _store->putBlobByKey(key, key_len, src_buf, len);
  }

  void checkCLIRescueCmd();
  void checkSerialInterface();
  bool isValidClientRepeatFreq(uint32_t f) const;
  uint8_t handleRequest(uint8_t perms, uint32_t sender_timestamp, uint8_t req_type, uint8_t *payload,
                        size_t payload_len);

  // helpers, short-cuts
  void saveChannels() { _store->saveChannels(this); }
  void saveContacts() { _store->saveContacts(this); }

  DataStore *_store;
  ContactInfo contacts[MAX_CONTACTS];
  int num_contacts;
  int sort_array[MAX_CONTACTS];
  struct MatchingPeer {
    int contact_idx;
    uint8_t identity_idx;
    int acl_idx;
  };
  MatchingPeer matching_peers[MAX_SEARCH_RESULTS];
  unsigned long txt_send_timeout;
#ifdef MAX_GROUP_CHANNELS
  ChannelDetails channels[MAX_GROUP_CHANNELS];
  int num_channels;
#endif
  mesh::Packet *loopback_queue[LOOPBACK_QUEUE_SIZE];
  uint8_t loopback_queue_head;
  uint8_t loopback_queue_len;
  uint8_t temp_buf[MAX_TRANS_UNIT];
  ConnectionInfo connections[MAX_CONNECTIONS];

  static const uint8_t MAX_LOCAL_IDENTITIES = 8;
  LocalIdentityEntry local_identities[MAX_LOCAL_IDENTITIES];
  uint8_t num_local_identities;
  uint8_t sensor_identity_idx;

  ClientNodePrefs _prefs;
  uint32_t pending_login;
  uint32_t pending_status;
  uint32_t pending_telemetry, pending_discovery; // pending _TELEMETRY_REQ
  uint32_t pending_req;                          // pending _BINARY_REQ
  BaseSerialInterface *_serial;
  AbstractUITask *_ui;

  ContactsIterator _iter;
  uint32_t _iter_filter_since;
  uint32_t _most_recent_lastmod;
  uint32_t _active_ble_pin;
  bool _iter_started;
  bool _cli_rescue;
  char cli_command[80];
  uint8_t app_target_ver;
  uint8_t *sign_data;
  uint32_t sign_data_len;
  unsigned long dirty_contacts_expiry;

  TransportKey send_scope;

  uint8_t cmd_frame[MAX_FRAME_SIZE + 1];
  uint8_t out_frame[MAX_FRAME_SIZE + 1];
  CayenneLPP telemetry;
  NodePrefs _sensor_prefs;
  ClientACL _acl;
  RemoteCliCallbacks _callbacks;
  CommonCLI _cli;

  struct Frame {
    uint8_t len;
    uint8_t buf[MAX_FRAME_SIZE];

    bool isChannelMsg() const;
  };
  int offline_queue_len;
  Frame offline_queue[OFFLINE_QUEUE_SIZE];

  struct AckTableEntry {
    unsigned long msg_sent;
    uint32_t ack;
    ContactInfo *contact;
  };
#define EXPECTED_ACK_TABLE_SIZE 8
  AckTableEntry expected_ack_table[EXPECTED_ACK_TABLE_SIZE]; // circular table
  int next_ack_idx;

#define ADVERT_PATH_TABLE_SIZE 16
  AdvertPath advert_paths[ADVERT_PATH_TABLE_SIZE]; // circular table

  uint8_t reply_data[MAX_PACKET_PAYLOAD];
};

extern MyMesh the_mesh;
