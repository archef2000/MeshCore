#include "MyMesh.h"

#include "Identity.h"
#include "LocalAwarePacketManager.h"
#include "MeshCore.h"
#include "helpers/ContactInfo.h"

#include <Arduino.h> // needed for PlatformIO
#include <Mesh.h>
#include <Utils.h>
#include <cstdint>

#define CLI_REPLY_DELAY_MILLIS 1000

#ifndef SERVER_RESPONSE_DELAY
#define SERVER_RESPONSE_DELAY 300
#endif

#ifndef TXT_ACK_DELAY
#define TXT_ACK_DELAY 200
#endif

#define CMD_APP_START                     1
#define CMD_SEND_TXT_MSG                  2
#define CMD_SEND_CHANNEL_TXT_MSG          3
#define CMD_GET_CONTACTS                  4 // with optional 'since' (for efficient sync)
#define CMD_GET_DEVICE_TIME               5
#define CMD_SET_DEVICE_TIME               6
#define CMD_SEND_SELF_ADVERT              7
#define CMD_SET_ADVERT_NAME               8
#define CMD_ADD_UPDATE_CONTACT            9
#define CMD_SYNC_NEXT_MESSAGE             10
#define CMD_SET_RADIO_PARAMS              11
#define CMD_SET_RADIO_TX_POWER            12
#define CMD_RESET_PATH                    13
#define CMD_SET_ADVERT_LATLON             14
#define CMD_REMOVE_CONTACT                15
#define CMD_SHARE_CONTACT                 16
#define CMD_EXPORT_CONTACT                17
#define CMD_IMPORT_CONTACT                18
#define CMD_REBOOT                        19
#define CMD_GET_BATT_AND_STORAGE          20 // was CMD_GET_BATTERY_VOLTAGE
#define CMD_SET_TUNING_PARAMS             21
#define CMD_DEVICE_QEURY                  22
#define CMD_EXPORT_PRIVATE_KEY            23
#define CMD_IMPORT_PRIVATE_KEY            24
#define CMD_SEND_RAW_DATA                 25
#define CMD_SEND_LOGIN                    26
#define CMD_SEND_STATUS_REQ               27
#define CMD_HAS_CONNECTION                28
#define CMD_LOGOUT                        29 // 'Disconnect'
#define CMD_GET_CONTACT_BY_KEY            30
#define CMD_GET_CHANNEL                   31
#define CMD_SET_CHANNEL                   32
#define CMD_SIGN_START                    33
#define CMD_SIGN_DATA                     34
#define CMD_SIGN_FINISH                   35
#define CMD_SEND_TRACE_PATH               36
#define CMD_SET_DEVICE_PIN                37
#define CMD_SET_OTHER_PARAMS              38
#define CMD_SEND_TELEMETRY_REQ            39 // can deprecate this
#define CMD_GET_CUSTOM_VARS               40
#define CMD_SET_CUSTOM_VAR                41
#define CMD_GET_ADVERT_PATH               42
#define CMD_GET_TUNING_PARAMS             43
// NOTE: CMD range 44..49 parked, potentially for WiFi operations
#define CMD_SEND_BINARY_REQ               50
#define CMD_FACTORY_RESET                 51
#define CMD_SEND_PATH_DISCOVERY_REQ       52
#define CMD_SET_FLOOD_SCOPE               54 // v8+
#define CMD_SEND_CONTROL_DATA             55 // v8+
#define CMD_GET_STATS                     56 // v8+, second byte is stats type
#define CMD_SEND_ANON_REQ                 57
#define CMD_SET_AUTOADD_CONFIG            58
#define CMD_GET_AUTOADD_CONFIG            59
#define CMD_GET_ALLOWED_REPEAT_FREQ       60
#define CMD_SET_PATH_HASH_MODE            61

// Stats sub-types for CMD_GET_STATS
#define STATS_TYPE_CORE                   0
#define STATS_TYPE_RADIO                  1
#define STATS_TYPE_PACKETS                2

#define RESP_CODE_OK                      0
#define RESP_CODE_ERR                     1
#define RESP_CODE_CONTACTS_START          2  // first reply to CMD_GET_CONTACTS
#define RESP_CODE_CONTACT                 3  // multiple of these (after CMD_GET_CONTACTS)
#define RESP_CODE_END_OF_CONTACTS         4  // last reply to CMD_GET_CONTACTS
#define RESP_CODE_SELF_INFO               5  // reply to CMD_APP_START
#define RESP_CODE_SENT                    6  // reply to CMD_SEND_TXT_MSG
#define RESP_CODE_CONTACT_MSG_RECV        7  // a reply to CMD_SYNC_NEXT_MESSAGE (ver < 3)
#define RESP_CODE_CHANNEL_MSG_RECV        8  // a reply to CMD_SYNC_NEXT_MESSAGE (ver < 3)
#define RESP_CODE_CURR_TIME               9  // a reply to CMD_GET_DEVICE_TIME
#define RESP_CODE_NO_MORE_MESSAGES        10 // a reply to CMD_SYNC_NEXT_MESSAGE
#define RESP_CODE_EXPORT_CONTACT          11
#define RESP_CODE_BATT_AND_STORAGE        12 // a reply to a CMD_GET_BATT_AND_STORAGE
#define RESP_CODE_DEVICE_INFO             13 // a reply to CMD_DEVICE_QEURY
#define RESP_CODE_PRIVATE_KEY             14 // a reply to CMD_EXPORT_PRIVATE_KEY
#define RESP_CODE_DISABLED                15
#define RESP_CODE_CONTACT_MSG_RECV_V3     16 // a reply to CMD_SYNC_NEXT_MESSAGE (ver >= 3)
#define RESP_CODE_CHANNEL_MSG_RECV_V3     17 // a reply to CMD_SYNC_NEXT_MESSAGE (ver >= 3)
#define RESP_CODE_CHANNEL_INFO            18 // a reply to CMD_GET_CHANNEL
#define RESP_CODE_SIGN_START              19
#define RESP_CODE_SIGNATURE               20
#define RESP_CODE_CUSTOM_VARS             21
#define RESP_CODE_ADVERT_PATH             22
#define RESP_CODE_TUNING_PARAMS           23
#define RESP_CODE_STATS                   24 // v8+, second byte is stats type
#define RESP_CODE_AUTOADD_CONFIG          25
#define RESP_ALLOWED_REPEAT_FREQ          26

#define SEND_TIMEOUT_BASE_MILLIS          500
#define FLOOD_SEND_TIMEOUT_FACTOR         16.0f
#define DIRECT_SEND_PERHOP_FACTOR         6.0f
#define DIRECT_SEND_PERHOP_EXTRA_MILLIS   250
#define LAZY_CONTACTS_WRITE_DELAY         5000

#define PUBLIC_GROUP_PSK                  "izOH6cXN6mrJ5e26oRXNcg=="

// these are _pushed_ to client app at any time
#define PUSH_CODE_ADVERT                  0x80
#define PUSH_CODE_PATH_UPDATED            0x81
#define PUSH_CODE_SEND_CONFIRMED          0x82
#define PUSH_CODE_MSG_WAITING             0x83
#define PUSH_CODE_RAW_DATA                0x84
#define PUSH_CODE_LOGIN_SUCCESS           0x85
#define PUSH_CODE_LOGIN_FAIL              0x86
#define PUSH_CODE_STATUS_RESPONSE         0x87
#define PUSH_CODE_LOG_RX_DATA             0x88
#define PUSH_CODE_TRACE_DATA              0x89
#define PUSH_CODE_NEW_ADVERT              0x8A
#define PUSH_CODE_TELEMETRY_RESPONSE      0x8B
#define PUSH_CODE_BINARY_RESPONSE         0x8C
#define PUSH_CODE_PATH_DISCOVERY_RESPONSE 0x8D
#define PUSH_CODE_CONTROL_DATA            0x8E // v8+
#define PUSH_CODE_CONTACT_DELETED         0x8F // used to notify client app of deleted contact when overwriting oldest
#define PUSH_CODE_CONTACTS_FULL           0x90 // used to notify client app that contacts storage is full

#define ERR_CODE_UNSUPPORTED_CMD          1
#define ERR_CODE_NOT_FOUND                2
#define ERR_CODE_TABLE_FULL               3
#define ERR_CODE_BAD_STATE                4
#define ERR_CODE_FILE_IO_ERROR            5
#define ERR_CODE_ILLEGAL_ARG              6

#define MAX_SIGN_DATA_LEN                 (8 * 1024) // 8K

// Auto-add config bitmask
// Bit 0: If set, overwrite oldest non-favourite contact when contacts file is full
// Bits 1-4: these indicate which contact types to auto-add when manual_contact_mode = 0x01
#define AUTO_ADD_OVERWRITE_OLDEST         (1 << 0) // 0x01 - overwrite oldest non-favourite when full
#define AUTO_ADD_CHAT                     (1 << 1) // 0x02 - auto-add Chat (Companion) (ADV_TYPE_CHAT)
#define AUTO_ADD_REPEATER                 (1 << 2) // 0x04 - auto-add Repeater (ADV_TYPE_REPEATER)
#define AUTO_ADD_ROOM_SERVER              (1 << 3) // 0x08 - auto-add Room Server (ADV_TYPE_ROOM)
#define AUTO_ADD_SENSOR                   (1 << 4) // 0x10 - auto-add Sensor (ADV_TYPE_SENSOR)

static void getContactSharedSecret(const mesh::LocalIdentity &local_id, const ContactInfo &contact,
                                   uint8_t *dest_secret) {
  const uint8_t *secret = contact.getSharedSecret(local_id);
  memcpy(dest_secret, secret, PUB_KEY_SIZE);
}

const mesh::LocalIdentity &MyMesh::identityByIndex(uint8_t identity_idx) const {
  if (identity_idx >= num_local_identities) {
    return local_identities[0].id;
  }
  return local_identities[identity_idx].id;
}

const MyMesh::LocalIdentityEntry *MyMesh::getLocalIdentity(int idx) const {
  if (idx < 0 || idx >= num_local_identities) {
    return NULL;
  }
  return &local_identities[idx];
}

int MyMesh::addLocalIdentity(const mesh::LocalIdentity &id, uint8_t adv_type, uint8_t prefs_kind) {
  if (num_local_identities >= MAX_LOCAL_IDENTITIES) {
    return -1;
  }
  uint8_t idx = num_local_identities++;
  local_identities[idx].id = id;
  local_identities[idx].adv_type = adv_type;
  local_identities[idx].prefs_kind = prefs_kind;

  if (idx == 0) {
    self_id = id;
  }
  return idx;
}

mesh::Packet *MyMesh::createSelfAdvert(uint8_t identity_idx, const char *name) {
  uint8_t app_data[MAX_ADVERT_DATA_SIZE];
  uint8_t app_data_len;
  {
    uint8_t adv_type = local_identities[identity_idx].adv_type;
    AdvertDataBuilder builder(adv_type, name);
    app_data_len = builder.encodeTo(app_data);
  }

  return createAdvert(identityByIndex(identity_idx), app_data, app_data_len);
}

mesh::Packet *MyMesh::createSelfAdvert(uint8_t identity_idx, const char *name, double lat, double lon) {
  uint8_t app_data[MAX_ADVERT_DATA_SIZE];
  uint8_t app_data_len;
  {
    uint8_t adv_type = local_identities[identity_idx].adv_type;
    AdvertDataBuilder builder(adv_type, name, lat, lon);
    app_data_len = builder.encodeTo(app_data);
  }

  return createAdvert(identityByIndex(identity_idx), app_data, app_data_len);
}

void MyMesh::sendAckTo(const ClientInfo &dest, uint32_t ack_hash, uint8_t path_hash_size) {
  if (dest.out_path_len == OUT_PATH_UNKNOWN) {
    mesh::Packet *ack = createAck(ack_hash);
    if (ack) sendFlood(ack, TXT_ACK_DELAY, path_hash_size);
  } else {
    uint32_t d = TXT_ACK_DELAY;
    if (getExtraAckTransmitCount() > 0) {
      mesh::Packet *a1 = createMultiAck(ack_hash, 1);
      if (a1) sendDirect(a1, dest.out_path, dest.out_path_len, d);
      d += 300;
    }

    mesh::Packet *a2 = createAck(ack_hash);
    if (a2) sendDirect(a2, dest.out_path, dest.out_path_len, d);
  }
}

void MyMesh::sendAckTo(const ContactInfo &dest, uint32_t ack_hash, uint8_t path_hash_size) {
  if (dest.out_path_len == OUT_PATH_UNKNOWN) {
    mesh::Packet *ack = createAck(ack_hash);
    if (ack) sendFlood(ack, TXT_ACK_DELAY, path_hash_size);
  } else {
    uint32_t d = TXT_ACK_DELAY;
    if (getExtraAckTransmitCount() > 0) {
      mesh::Packet *a1 = createMultiAck(ack_hash, 1);
      if (a1) sendDirect(a1, dest.out_path, dest.out_path_len, d);
      d += 300;
    }

    mesh::Packet *a2 = createAck(ack_hash);
    if (a2) sendDirect(a2, dest.out_path, dest.out_path_len, d);
  }
}

void MyMesh::bootstrapRTCfromContacts() {
  uint32_t latest = 0;
  for (int i = 0; i < num_contacts; i++) {
    if (contacts[i].lastmod > latest) {
      latest = contacts[i].lastmod;
    }
  }
  if (latest != 0) {
    getRTCClock()->setCurrentTime(latest + 1);
  }
}

ContactInfo *MyMesh::allocateContactSlot() {
  if (num_contacts < MAX_CONTACTS) {
    return &contacts[num_contacts++];
  } else if (shouldOverwriteWhenFull()) {
    int oldest_idx = -1;
    uint32_t oldest_lastmod = 0xFFFFFFFF;
    for (int i = 0; i < num_contacts; i++) {
      bool is_favourite = (contacts[i].flags & 0x01) != 0;
      if (!is_favourite && contacts[i].lastmod < oldest_lastmod) {
        oldest_lastmod = contacts[i].lastmod;
        oldest_idx = i;
      }
    }
    if (oldest_idx >= 0) {
      onContactOverwrite(contacts[oldest_idx].id.pub_key);
      return &contacts[oldest_idx];
    }
  }
  return NULL;
}

void MyMesh::populateContactFromAdvert(ContactInfo &ci, const mesh::Identity &id,
                                       const AdvertDataParser &parser, uint32_t timestamp) {
  memset(&ci, 0, sizeof(ci));
  ci.id = id;
  ci.out_path_len = OUT_PATH_UNKNOWN;
  StrHelper::strncpy(ci.name, parser.getName(), sizeof(ci.name));
  ci.type = parser.getType();
  if (parser.hasLatLon()) {
    ci.gps_lat = parser.getIntLat();
    ci.gps_lon = parser.getIntLon();
  }
  ci.last_advert_timestamp = timestamp;
  ci.lastmod = getRTCClock()->getCurrentTime();
}

void MyMesh::onAdvertRecv(mesh::Packet *packet, const mesh::Identity &id, uint32_t timestamp,
                          const uint8_t *app_data, size_t app_data_len) {
  AdvertDataParser parser(app_data, app_data_len);
  if (!(parser.isValid() && parser.hasName())) {
    MESH_DEBUG_PRINTLN("onAdvertRecv: invalid app_data, or name is missing: len=%d", app_data_len);
    return;
  }

  ContactInfo *from = NULL;
  for (int i = 0; i < num_contacts; i++) {
    if (id.matches(contacts[i].id)) {
      from = &contacts[i];
      if (timestamp <= from->last_advert_timestamp) {
        MESH_DEBUG_PRINTLN("onAdvertRecv: Possible replay attack, name: %s", from->name);
        return;
      }
      break;
    }
  }

  int plen;
  {
    uint8_t save = packet->header;
    packet->header &= ~PH_ROUTE_MASK;
    packet->header |= ROUTE_TYPE_FLOOD;
    plen = packet->writeTo(temp_buf);
    packet->header = save;
  }

  bool is_new = false;
  if (from == NULL) {
    if (!shouldAutoAddContactType(parser.getType())) {
      ContactInfo ci;
      populateContactFromAdvert(ci, id, parser, timestamp);
      onDiscoveredContact(ci, true, packet->path_len, packet->path);
      return;
    }

    uint8_t max_hops = getAutoAddMaxHops();
    if (max_hops > 0 && packet->getPathHashCount() >= max_hops) {
      ContactInfo ci;
      populateContactFromAdvert(ci, id, parser, timestamp);
      onDiscoveredContact(ci, true, packet->path_len, packet->path);
      return;
    }

    from = allocateContactSlot();
    if (from == NULL) {
      ContactInfo ci;
      populateContactFromAdvert(ci, id, parser, timestamp);
      onDiscoveredContact(ci, true, packet->path_len, packet->path);
      onContactsFull();
      MESH_DEBUG_PRINTLN("onAdvertRecv: unable to allocate contact slot for new contact");
      return;
    }

    is_new = true;
    populateContactFromAdvert(*from, id, parser, timestamp);
    from->sync_since = 0;
    from->shared_secret_valid = false;
  }

  putBlobByKey(id.pub_key, PUB_KEY_SIZE, temp_buf, plen);
  StrHelper::strncpy(from->name, parser.getName(), sizeof(from->name));
  from->type = parser.getType();
  if (parser.hasLatLon()) {
    from->gps_lat = parser.getIntLat();
    from->gps_lon = parser.getIntLon();
  }
  from->last_advert_timestamp = timestamp;
  from->lastmod = getRTCClock()->getCurrentTime();

  onDiscoveredContact(*from, is_new, packet->path_len, packet->path);
}

int MyMesh::searchPeersByHash(const uint8_t *src_hash, const uint8_t *dst_hash) {
  int n = 0;
  for (int i = 0; i < num_local_identities; i++) {
    if (local_identities[i].id.isHashMatch(dst_hash)) {
      uint8_t identity_adv_type = local_identities[i].adv_type;
      if (identity_adv_type == ADV_TYPE_CHAT) {
        for (int c = 0; c < num_contacts && n < MAX_SEARCH_RESULTS; c++) {
          if (contacts[c].id.isHashMatch(src_hash)) {
            matching_peers[n].acl_idx = -1;
            matching_peers[n].contact_idx = c;
            matching_peers[n].identity_idx = i;
            n++;
          }
        }
      } else {
        for (int a = 0; a < _acl.getNumClients() && n < MAX_SEARCH_RESULTS; a++) {
          if (_acl.getClientByIdx(a)->id.isHashMatch(src_hash)) {
            matching_peers[n].acl_idx = a;
            matching_peers[n].contact_idx = -1;
            matching_peers[n].identity_idx = i;
            n++;
          }
        }
      }
    }
  }
  return n;
}

void MyMesh::getPeerSharedSecret(uint8_t *dest_secret, int peer_idx) {
  int c = matching_peers[peer_idx].contact_idx;
  int a = matching_peers[peer_idx].acl_idx;
  uint8_t id_idx = matching_peers[peer_idx].identity_idx;
  if (c >= 0 && c < num_contacts && id_idx < num_local_identities) {
    getContactSharedSecret(identityByIndex(id_idx), contacts[c], dest_secret);
  } else if (a >= 0 && a < _acl.getNumClients() && id_idx < num_local_identities) {
    const mesh::LocalIdentity &local_id = identityByIndex(id_idx);
    const ClientInfo *client_info = _acl.getClientByIdx(a);
    local_id.calcSharedSecret(dest_secret, client_info->id);
  } else {
    MESH_DEBUG_PRINTLN("getPeerSharedSecret: Invalid mapping for peer idx: %d", peer_idx);
  }
}

int MyMesh::getMatchingPeerIdentityIdx(int sender_idx) const {
  if (sender_idx < 0 || sender_idx >= MAX_SEARCH_RESULTS) {
    return 0;
  }
  uint8_t id_idx = matching_peers[sender_idx].identity_idx;
  if (id_idx >= num_local_identities) {
    return 0;
  }
  return id_idx;
}

int MyMesh::findLocalIdentityIdx(const mesh::Identity &id) const {
  for (int i = 0; i < num_local_identities; i++) {
    if (id.matches(local_identities[i].id)) {
      return i;
    }
  }
  return -1;
}

bool MyMesh::isPacketForLocalIdentity(const mesh::Packet *pkt) const {
  if (pkt == NULL || pkt->payload_len < 1) {
    return false;
  }

  uint8_t type = pkt->getPayloadType();
  if (type != PAYLOAD_TYPE_PATH && type != PAYLOAD_TYPE_REQ && type != PAYLOAD_TYPE_RESPONSE &&
      type != PAYLOAD_TYPE_TXT_MSG && type != PAYLOAD_TYPE_ANON_REQ) {
    return false;
  }

  uint8_t dest_hash = pkt->payload[0];
  for (uint8_t i = 0; i < num_local_identities; i++) {
    if (local_identities[i].id.isHashMatch(&dest_hash)) {
      return true;
    }
  }
  return false;
}

bool MyMesh::queueInternalPacket(mesh::Packet *pkt) {
  if (pkt == NULL) {
    return false;
  }

  if (loopback_queue_len >= LOOPBACK_QUEUE_SIZE) {
    MESH_DEBUG_PRINTLN("queueInternalPacket: loopback queue full, dropping packet");
    return false;
  }

  uint8_t idx = (loopback_queue_head + loopback_queue_len) % LOOPBACK_QUEUE_SIZE;
  loopback_queue[idx] = pkt;
  loopback_queue_len++;
  return true;
}

bool MyMesh::clientOnPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                                  uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len) {
  int c = matching_peers[sender_idx].contact_idx;
  if (c < 0 || c >= num_contacts) {
    MESH_DEBUG_PRINTLN("clientOnPeerPathRecv: Invalid sender idx: %d", c);
    return false;
  }

  ContactInfo &from = contacts[c];
  return onContactPathRecv(from, packet->path, packet->path_len, path, path_len, extra_type, extra,
                           extra_len);
}

bool MyMesh::sensorOnPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                                  uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len) {
  int a = matching_peers[sender_idx].acl_idx;
  if (a < 0 || a >= _acl.getNumClients()) {
    MESH_DEBUG_PRINTLN("sensorOnPeerPathRecv: Invalid sender idx: %d", a);
    return false;
  }

  ClientInfo *from = _acl.getClientByIdx(a);

  MESH_DEBUG_PRINTLN("PATH to contact, path_len=%d", (uint32_t)path_len);
  // NOTE: for this impl, we just replace the current 'out_path' regardless, whenever sender sends us a new
  // out_path. FUTURE: could store multiple out_paths per contact, and try to find which is the 'best'(?)
  from->out_path_len =
      mesh::Packet::copyPath(from->out_path, path, path_len); // store a copy of path, for sendDirect()
  from->last_activity = getRTCClock()->getCurrentTime();

  // REVISIT: maybe make ALL out_paths non-persisted to minimise flash writes??
  if (from->isAdmin()) {
    // only do saveContacts() (of this out_path change) if this is an admin
    dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
  }

  // NOTE: no reciprocal path send!!
  return false;
}

bool MyMesh::onPeerPathRecv(mesh::Packet *packet, int sender_idx, const uint8_t *secret, uint8_t *path,
                            uint8_t path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len) {
  auto peer = matching_peers[sender_idx];
  if (peer.contact_idx >= 0) {
    MESH_DEBUG_PRINTLN("MyMesh::onPeerPathRecv: request to chat");
    return clientOnPeerPathRecv(packet, sender_idx, secret, path, path_len, extra_type, extra, extra_len);
  } else if (peer.acl_idx >= 0) {
    MESH_DEBUG_PRINTLN("MyMesh::onPeerPathRecv: request to sensor");
    return sensorOnPeerPathRecv(packet, sender_idx, secret, path, path_len, extra_type, extra, extra_len);
  } else {
    MESH_DEBUG_PRINTLN("onPeerPathRecv: Invalid sender idx: %d", sender_idx);
    return false;
  }
}

void MyMesh::onAckRecv(mesh::Packet *packet, uint32_t ack_crc) {
  ContactInfo *from;
  if ((from = processAck((uint8_t *)&ack_crc)) != NULL) {
    txt_send_timeout = 0;
    packet->markDoNotRetransmit();

    if (packet->isRouteFlood() && from->out_path_len != OUT_PATH_UNKNOWN) {
      handleReturnPathRetry(*from, packet->path, packet->path_len);
    }
  }
}

void MyMesh::handleReturnPathRetry(const ContactInfo &contact, const uint8_t *path, uint8_t path_len,
                                   uint8_t sender_identity_idx) {
  uint8_t secret[PUB_KEY_SIZE];
  if (sender_identity_idx >= num_local_identities) {
    sender_identity_idx = 0;
  }
  const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
  getContactSharedSecret(sender, contact, secret);
  mesh::Packet *rpath = createPathReturnFromIdentity(sender, contact.id, secret, path, path_len, 0, NULL, 0);
  if (rpath) sendDirect(rpath, contact.out_path, contact.out_path_len, 3000);
}

int MyMesh::searchChannelsByHash(const uint8_t *hash, mesh::GroupChannel dest[], int max_matches) {
  int n = 0;
#ifdef MAX_GROUP_CHANNELS
  for (int i = 0; i < MAX_GROUP_CHANNELS && n < max_matches; i++) {
    if (channels[i].channel.hash[0] == hash[0]) {
      dest[n++] = channels[i].channel;
    }
  }
#endif
  return n;
}

mesh::DispatcherAction MyMesh::onRecvPacket(mesh::Packet *pkt) {
  uint8_t dest_hash = pkt->payload[0];
  uint8_t local_identity_idx = 0xFF;
  for (uint8_t i = 0; i < num_local_identities; i++) {
    if (local_identities[i].id.isHashMatch(&dest_hash)) {
      local_identity_idx = i;
      break;
    }
  }
  MESH_DEBUG_PRINTLN("(MyMesh::onRecvPacket(): dest_hash=%02X, local_identity_idx=%u", (uint32_t)dest_hash,
                     (uint32_t)local_identity_idx);
  auto anyLocalHashMatch = [&](const uint8_t *hash, uint8_t hash_sz) -> bool {
    for (int i = 0; i < num_local_identities; i++) {
      if (local_identities[i].id.isHashMatch(hash, hash_sz)) return true;
    }
    return false;
  };

  auto anyLocalIdentityMatch = [&](const uint8_t *pub_key) -> bool {
    for (int i = 0; i < num_local_identities; i++) {
      if (local_identities[i].id.matches(pub_key)) return true;
    }
    return false;
  };

  auto findIdentityByDestHash = [&](uint8_t dest_hash) -> const mesh::LocalIdentity * {
    for (int i = 0; i < num_local_identities; i++) {
      if (local_identities[i].id.isHashMatch(&dest_hash)) {
        return &local_identities[i].id;
      }
    }
    return NULL;
  };

  auto removeFirstPathEntry = [&](mesh::Packet *p) {
    p->setPathHashCount(p->getPathHashCount() - 1);
    uint8_t sz = p->getPathHashSize();
    for (int k = 0; k < p->getPathHashCount() * sz; k += sz) {
      memcpy(&p->path[k], &p->path[k + sz], sz);
    }
  };

  auto routeDirectRecvAcksLocal = [&](mesh::Packet *p, uint32_t delay_millis) {
    if (!p->isMarkedDoNotRetransmit()) {
      uint32_t crc;
      memcpy(&crc, p->payload, 4);

      uint8_t extra = getExtraAckTransmitCount();
      while (extra > 0) {
        delay_millis += getDirectRetransmitDelay(p) + 300;
        auto a1 = createMultiAck(crc, extra);
        if (a1) {
          a1->path_len = mesh::Packet::copyPath(a1->path, p->path, p->path_len);
          a1->header &= ~PH_ROUTE_MASK;
          a1->header |= ROUTE_TYPE_DIRECT;
          sendPacket(a1, 0, delay_millis);
        }
        extra--;
      }

      auto a2 = createAck(crc);
      if (a2) {
        a2->path_len = mesh::Packet::copyPath(a2->path, p->path, p->path_len);
        a2->header &= ~PH_ROUTE_MASK;
        a2->header |= ROUTE_TYPE_DIRECT;
        sendPacket(a2, 0, delay_millis);
      }
    }
  };

  auto forwardMultipartDirectLocal = [&](mesh::Packet *p) -> mesh::DispatcherAction {
    uint8_t remaining = p->payload[0] >> 4;
    uint8_t type = p->payload[0] & 0x0F;

    if (type == PAYLOAD_TYPE_ACK && p->payload_len >= 5) {
      mesh::Packet tmp;
      tmp.header = p->header;
      tmp.path_len = mesh::Packet::copyPath(tmp.path, p->path, p->path_len);
      tmp.payload_len = p->payload_len - 1;
      memcpy(tmp.payload, &p->payload[1], tmp.payload_len);

      if (!getTables()->hasSeen(&tmp)) {
        removeFirstPathEntry(&tmp);
        routeDirectRecvAcksLocal(&tmp, ((uint32_t)remaining + 1) * 300);
      }
    }
    return ACTION_RELEASE;
  };

  if (pkt->isRouteDirect() && pkt->getPayloadType() == PAYLOAD_TYPE_TRACE) {
    if (pkt->path_len < MAX_PATH_SIZE) {
      uint8_t i = 0;
      uint32_t trace_tag;
      memcpy(&trace_tag, &pkt->payload[i], 4);
      i += 4;
      uint32_t auth_code;
      memcpy(&auth_code, &pkt->payload[i], 4);
      i += 4;
      uint8_t flags = pkt->payload[i++];
      uint8_t path_sz = flags & 0x03;

      uint8_t len = pkt->payload_len - i;
      uint8_t offset = pkt->path_len << path_sz;
      if (offset >= len) {
        onTraceRecv(pkt, trace_tag, auth_code, flags, pkt->path, &pkt->payload[i], len);
      } else if (anyLocalHashMatch(&pkt->payload[i + offset], 1 << path_sz) && allowPacketForward(pkt) &&
                 !getTables()->hasSeen(pkt)) {
        pkt->path[pkt->path_len++] = (int8_t)(pkt->getSNR() * 4);

        uint32_t d = getDirectRetransmitDelay(pkt);
        return ACTION_RETRANSMIT_DELAYED(5, d);
      }
    }
    return ACTION_RELEASE;
  }

  if (pkt->isRouteDirect() && pkt->getPayloadType() == PAYLOAD_TYPE_CONTROL &&
      (pkt->payload[0] & 0x80) != 0) {
    if (pkt->getPathHashCount() == 0) {
      onControlDataRecv(pkt);
    }
    return ACTION_RELEASE;
  }

  if (pkt->isRouteDirect() && pkt->getPathHashCount() > 0) {
    if (pkt->getPayloadType() == PAYLOAD_TYPE_ACK) {
      int i = 0;
      uint32_t ack_crc;
      memcpy(&ack_crc, &pkt->payload[i], 4);
      i += 4;
      if (i <= pkt->payload_len) {
        onAckRecv(pkt, ack_crc);
      }
    }

    if (anyLocalHashMatch(pkt->path, pkt->getPathHashSize()) && allowPacketForward(pkt)) {
      if (pkt->getPayloadType() == PAYLOAD_TYPE_MULTIPART) {
        return forwardMultipartDirectLocal(pkt);
      } else if (pkt->getPayloadType() == PAYLOAD_TYPE_ACK) {
        if (!getTables()->hasSeen(pkt)) {
          removeFirstPathEntry(pkt);
          routeDirectRecvAcksLocal(pkt, 0);
        }
        return ACTION_RELEASE;
      }

      if (!getTables()->hasSeen(pkt)) {
        removeFirstPathEntry(pkt);

        uint32_t d = getDirectRetransmitDelay(pkt);
        return ACTION_RETRANSMIT_DELAYED(0, d);
      }
    }
    return ACTION_RELEASE;
  }

  if (pkt->isRouteFlood() && filterRecvFloodPacket(pkt)) return ACTION_RELEASE;

  mesh::DispatcherAction action = ACTION_RELEASE;

  switch (pkt->getPayloadType()) {
  case PAYLOAD_TYPE_ACK: {
    int i = 0;
    uint32_t ack_crc;
    memcpy(&ack_crc, &pkt->payload[i], 4);
    i += 4;
    if (i > pkt->payload_len) {
      MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): incomplete ACK packet", getLogDateTime());
    } else if (!getTables()->hasSeen(pkt)) {
      onAckRecv(pkt, ack_crc);
      action = routeRecvPacket(pkt);
    }
    break;
  }
  case PAYLOAD_TYPE_PATH:
  case PAYLOAD_TYPE_REQ:
  case PAYLOAD_TYPE_RESPONSE:
  case PAYLOAD_TYPE_TXT_MSG: {
    int i = 0;
    uint8_t dest_hash = pkt->payload[i++];
    uint8_t src_hash = pkt->payload[i++];

    uint8_t *macAndData = &pkt->payload[i];
    if (i + CIPHER_MAC_SIZE >= pkt->payload_len) {
      MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): incomplete data packet", getLogDateTime());
    } else if (!getTables()->hasSeen(pkt)) {
      int num = searchPeersByHash(&src_hash, &dest_hash);
      MESH_DEBUG_PRINTLN("MyMesh::onRecvPacket(): found %d matching peers for src_hash=%02X", num,
                         (uint32_t)src_hash);
      bool found = false;
      for (int j = 0; j < num; j++) {
        uint8_t secret[PUB_KEY_SIZE];
        getPeerSharedSecret(secret, j);
        mesh::LocalIdentity recv_id = identityByIndex(matching_peers[j].identity_idx);

        uint8_t data[MAX_PACKET_PAYLOAD];
        int len = mesh::Utils::MACThenDecrypt(secret, data, macAndData, pkt->payload_len - i);
        MESH_DEBUG_PRINTLN("MyMesh::onRecvPacket(): decrypted data len=%d for peer idx=%d", len, j);
        if (len > 0) {
          if (pkt->getPayloadType() == PAYLOAD_TYPE_PATH) {
            int k = 0;
            uint8_t path_len = data[k++];
            uint8_t hash_size = (path_len >> 6) + 1;
            uint8_t hash_count = path_len & 63;
            uint8_t *path = &data[k];
            k += hash_size * hash_count;
            uint8_t extra_type = data[k++] & 0x0F;
            uint8_t *extra = &data[k];
            uint8_t extra_len = len - k;
            if (onPeerPathRecv(pkt, j, secret, path, path_len, extra_type, extra, extra_len)) {
              if (pkt->isRouteFlood()) {
                mesh::Packet *rpath = createPathReturnFromIdentity(recv_id, &src_hash, secret, pkt->path,
                                                                   pkt->path_len, 0, NULL, 0);
                if (rpath) sendDirect(rpath, path, path_len, 500);
              }
            }
          } else {
            onPeerDataRecv(pkt, pkt->getPayloadType(), j, secret, data, len);
          }
          found = true;
          break;
        }
      }
      if (found) {
        pkt->markDoNotRetransmit();
      } else {
        MESH_DEBUG_PRINTLN("%s recv matches no peers, src_hash=%02X", getLogDateTime(), (uint32_t)src_hash);
      }
      action = routeRecvPacket(pkt);
    }
    break;
  }
  case PAYLOAD_TYPE_ANON_REQ: {
    int i = 0;
    uint8_t dest_hash = pkt->payload[i++];
    uint8_t *sender_pub_key = &pkt->payload[i];
    i += PUB_KEY_SIZE;

    uint8_t *macAndData = &pkt->payload[i];
    if (i + 2 >= pkt->payload_len) {
      MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): incomplete data packet", getLogDateTime());
    } else if (!getTables()->hasSeen(pkt)) {
      const mesh::LocalIdentity *recv_id = findIdentityByDestHash(dest_hash);
      MESH_DEBUG_PRINTLN("MyMesh::onRecvPacket(): anon req dest_hash=%02X, recv_id %s", (uint32_t)dest_hash,
                         recv_id ? "found" : "NOT FOUND");
      if (recv_id) {
        mesh::Identity sender(sender_pub_key);

        uint8_t secret[PUB_KEY_SIZE];
        recv_id->calcSharedSecret(secret, sender);

        uint8_t data[MAX_PACKET_PAYLOAD];
        int len = mesh::Utils::MACThenDecrypt(secret, data, macAndData, pkt->payload_len - i);
        if (len > 0) {
          onAnonDataRecv(pkt, secret, sender, data, len);
          pkt->markDoNotRetransmit();
        }
      }
      action = routeRecvPacket(pkt);
    } else {
      MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): seen packet before, src_pub_key=%02X%02X%02X...",
                         getLogDateTime(), sender_pub_key[0], sender_pub_key[1], sender_pub_key[2]);
    }
    break;
  }
  case PAYLOAD_TYPE_GRP_DATA:
  case PAYLOAD_TYPE_GRP_TXT: {
    int i = 0;
    uint8_t channel_hash = pkt->payload[i++];

    uint8_t *macAndData = &pkt->payload[i];
    if (i + 2 >= pkt->payload_len) {
      MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): incomplete data packet", getLogDateTime());
    } else if (!getTables()->hasSeen(pkt)) {
      mesh::GroupChannel channels[4];
      int num = searchChannelsByHash(&channel_hash, channels, 4);
      for (int j = 0; j < num; j++) {
        uint8_t data[MAX_PACKET_PAYLOAD];
        int len = mesh::Utils::MACThenDecrypt(channels[j].secret, data, macAndData, pkt->payload_len - i);
        if (len > 0) {
          onGroupDataRecv(pkt, pkt->getPayloadType(), channels[j], data, len);
          break;
        }
      }
      action = routeRecvPacket(pkt);
    }
    break;
  }
  case PAYLOAD_TYPE_ADVERT: {
    int i = 0;
    mesh::Identity id;
    memcpy(id.pub_key, &pkt->payload[i], PUB_KEY_SIZE);
    i += PUB_KEY_SIZE;

    uint32_t timestamp;
    memcpy(&timestamp, &pkt->payload[i], 4);
    i += 4;
    const uint8_t *signature = &pkt->payload[i];
    i += SIGNATURE_SIZE;

    if (i > pkt->payload_len) {
      MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): incomplete advertisement packet", getLogDateTime());
    } else if (!getTables()->hasSeen(pkt)) {
      if (anyLocalIdentityMatch(id.pub_key)) {
        MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): receiving SELF advert packet (processing)",
                           getLogDateTime());
      }

      uint8_t *app_data = &pkt->payload[i];
      int app_data_len = pkt->payload_len - i;
      if (app_data_len > MAX_ADVERT_DATA_SIZE) {
        app_data_len = MAX_ADVERT_DATA_SIZE;
      }

      bool is_ok;
      {
        uint8_t message[PUB_KEY_SIZE + 4 + MAX_ADVERT_DATA_SIZE];
        int msg_len = 0;
        memcpy(&message[msg_len], id.pub_key, PUB_KEY_SIZE);
        msg_len += PUB_KEY_SIZE;
        memcpy(&message[msg_len], &timestamp, 4);
        msg_len += 4;
        memcpy(&message[msg_len], app_data, app_data_len);
        msg_len += app_data_len;

        is_ok = id.verify(signature, message, msg_len);
      }
      if (is_ok) {
        MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): valid advertisement received!", getLogDateTime());
        onAdvertRecv(pkt, id, timestamp, app_data, app_data_len);
        action = routeRecvPacket(pkt);
      } else {
        MESH_DEBUG_PRINTLN(
            "%s MyMesh::onRecvPacket(): received advertisement with forged signature! (app_data_len=%d)",
            getLogDateTime(), app_data_len);
      }
    }
    break;
  }
  case PAYLOAD_TYPE_RAW_CUSTOM: {
    if (pkt->isRouteDirect() && !getTables()->hasSeen(pkt)) {
      onRawDataRecv(pkt);
    }
    break;
  }
  case PAYLOAD_TYPE_MULTIPART:
    if (pkt->payload_len > 2) {
      uint8_t type = pkt->payload[0] & 0x0F;
      if (type == PAYLOAD_TYPE_ACK && pkt->payload_len >= 5) {
        mesh::Packet tmp;
        tmp.header = pkt->header;
        tmp.path_len = mesh::Packet::copyPath(tmp.path, pkt->path, pkt->path_len);
        tmp.payload_len = pkt->payload_len - 1;
        memcpy(tmp.payload, &pkt->payload[1], tmp.payload_len);

        if (!getTables()->hasSeen(&tmp)) {
          uint32_t ack_crc;
          memcpy(&ack_crc, tmp.payload, 4);

          onAckRecv(&tmp, ack_crc);
        }
      }
    }
    break;

  default:
    MESH_DEBUG_PRINTLN("%s MyMesh::onRecvPacket(): unknown payload type, header: %d", getLogDateTime(),
                       (int)pkt->header);
    break;
  }
  return action;
}

void MyMesh::onGroupDataRecv(mesh::Packet *packet, uint8_t type, const mesh::GroupChannel &channel,
                             uint8_t *data, size_t len) {
  uint8_t txt_type = data[4];
  if (type == PAYLOAD_TYPE_GRP_TXT && len > 5 && (txt_type >> 2) == 0) {
    uint32_t timestamp;
    memcpy(&timestamp, data, 4);
    data[len] = 0;
    onChannelMessageRecv(channel, packet, timestamp, (const char *)&data[5]);
  }
}

mesh::Packet *MyMesh::createDatagramFromIdentity(uint8_t type, const mesh::LocalIdentity &sender,
                                                 const mesh::Identity &dest, const uint8_t *secret,
                                                 const uint8_t *data, size_t data_len) {
  if (type == PAYLOAD_TYPE_TXT_MSG || type == PAYLOAD_TYPE_REQ || type == PAYLOAD_TYPE_RESPONSE) {
    if (data_len + CIPHER_MAC_SIZE + CIPHER_BLOCK_SIZE - 1 > MAX_PACKET_PAYLOAD) return NULL;
  } else {
    return NULL; // invalid type
  }

  mesh::Packet *packet = obtainNewPacket();
  if (packet == NULL) {
    MESH_DEBUG_PRINTLN("%s MyMesh::createDatagramFromIdentity(): error, packet pool empty", getLogDateTime());
    return NULL;
  }

  packet->header = (type << PH_TYPE_SHIFT);

  int len = 0;
  len += dest.copyHashTo(&packet->payload[len]);
  len += sender.copyHashTo(&packet->payload[len]);
  len += mesh::Utils::encryptThenMAC(secret, &packet->payload[len], data, data_len);

  packet->payload_len = len;

  return packet;
}

mesh::Packet *MyMesh::createPathReturnFromIdentity(const mesh::LocalIdentity &sender,
                                                   const mesh::Identity &dest, const uint8_t *secret,
                                                   const uint8_t *path, uint8_t path_len, uint8_t extra_type,
                                                   const uint8_t *extra, size_t extra_len) {
  uint8_t dest_hash[PATH_HASH_SIZE];
  dest.copyHashTo(dest_hash);
  return createPathReturnFromIdentity(sender, dest_hash, secret, path, path_len, extra_type, extra,
                                      extra_len);
}

mesh::Packet *MyMesh::createPathReturnFromIdentity(const mesh::LocalIdentity &sender,
                                                   const uint8_t *dest_hash, const uint8_t *secret,
                                                   const uint8_t *path, uint8_t path_len, uint8_t extra_type,
                                                   const uint8_t *extra, size_t extra_len) {
  uint8_t path_hash_size = (path_len >> 6) + 1;
  uint8_t path_hash_count = path_len & 63;

  const size_t max_combined_path = MAX_PACKET_PAYLOAD - 2 - CIPHER_BLOCK_SIZE;
  if (path_hash_count * path_hash_size + extra_len + 5 > max_combined_path) {
    return NULL;
  }

  mesh::Packet *packet = obtainNewPacket();
  if (packet == NULL) {
    MESH_DEBUG_PRINTLN("%s MyMesh::createPathReturnFromIdentity(): error, packet pool empty",
                       getLogDateTime());
    return NULL;
  }

  packet->header = (PAYLOAD_TYPE_PATH << PH_TYPE_SHIFT);

  int len = 0;
  memcpy(&packet->payload[len], dest_hash, PATH_HASH_SIZE);
  len += PATH_HASH_SIZE;                           // dest hash
  len += sender.copyHashTo(&packet->payload[len]); // src hash

  {
    int data_len = 0;
    uint8_t enc_data[MAX_PACKET_PAYLOAD];
    enc_data[data_len++] = path_len;
    memcpy(&enc_data[data_len], path, path_hash_count * path_hash_size);
    data_len += path_hash_count * path_hash_size;

    if (extra_len > 0) {
      enc_data[data_len++] = extra_type;
      memcpy(&enc_data[data_len], extra, extra_len);
      data_len += extra_len;
    } else {
      enc_data[data_len++] = 0xFF;
      getRNG()->random(&enc_data[data_len], 4);
      data_len += 4;
    }

    len += mesh::Utils::encryptThenMAC(secret, &packet->payload[len], enc_data, data_len);
  }
  packet->payload_len = len;

  return packet;
}

mesh::Packet *MyMesh::createDatagram(uint8_t type, const mesh::Identity &dest, const uint8_t *secret,
                                     const uint8_t *data, size_t data_len) {
  MESH_DEBUG_PRINTLN("Mesh::createDatagram:\n\n\n\n\n\n\n\n\n\n\nERROR\n\n\n\n\n\n\n\n\n\n\n\n\n");
}

mesh::Packet *MyMesh::composeMsgPacket(const ContactInfo &recipient, uint32_t timestamp, uint8_t attempt,
                                       const char *text, uint32_t &expected_ack,
                                       uint8_t sender_identity_idx) {
  int text_len = strlen(text);
  if (text_len > MAX_TEXT_LEN) return NULL;
  if (attempt > 3 && text_len > MAX_TEXT_LEN - 2) return NULL;

  uint8_t temp[5 + MAX_TEXT_LEN + 1];
  memcpy(temp, &timestamp, 4);
  temp[4] = (attempt & 3);
  memcpy(&temp[5], text, text_len + 1);

  const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
  mesh::Utils::sha256((uint8_t *)&expected_ack, 4, temp, 5 + text_len, sender.pub_key, PUB_KEY_SIZE);

  int len = 5 + text_len;
  if (attempt > 3) {
    temp[len++] = 0;
    temp[len++] = attempt;
  }
  uint8_t secret[PUB_KEY_SIZE];
  getContactSharedSecret(sender, recipient, secret);
  return createDatagramFromIdentity(PAYLOAD_TYPE_TXT_MSG, sender, recipient.id, secret, temp, len);
}

int MyMesh::sendMessage(const ContactInfo &recipient, uint32_t timestamp, uint8_t attempt, const char *text,
                        uint32_t &expected_ack, uint32_t &est_timeout, uint8_t sender_identity_idx) {
  mesh::Packet *pkt =
      composeMsgPacket(recipient, timestamp, attempt, text, expected_ack, sender_identity_idx);
  if (pkt == NULL) return MSG_SEND_FAILED;

  uint32_t t = _radio->getEstAirtimeFor(pkt->getRawLength());
  if (recipient.out_path_len == OUT_PATH_UNKNOWN) {
    sendFloodScoped(recipient, pkt);
    txt_send_timeout = futureMillis(est_timeout = calcFloodTimeoutMillisFor(t));
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  txt_send_timeout = futureMillis(est_timeout = calcDirectTimeoutMillisFor(t, recipient.out_path_len));
  return MSG_SEND_SENT_DIRECT;
}

int MyMesh::sendCommandData(const ContactInfo &recipient, uint32_t timestamp, uint8_t attempt,
                            const char *text, uint32_t &est_timeout, uint8_t sender_identity_idx) {
  MESH_DEBUG_PRINTLN("MyMesh::sendCommandData(): sender_idx=%d sending command=%s", sender_identity_idx,
                     text);
  int text_len = strlen(text);
  if (text_len > MAX_TEXT_LEN) return MSG_SEND_FAILED;

  uint8_t temp[5 + MAX_TEXT_LEN + 1];
  memcpy(temp, &timestamp, 4);
  temp[4] = (attempt & 3) | (TXT_TYPE_CLI_DATA << 2);
  memcpy(&temp[5], text, text_len + 1);

  const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
  uint8_t secret[PUB_KEY_SIZE];
  getContactSharedSecret(sender, recipient, secret);
  auto pkt =
      createDatagramFromIdentity(PAYLOAD_TYPE_TXT_MSG, sender, recipient.id, secret, temp, 5 + text_len);
  if (pkt == NULL) return MSG_SEND_FAILED;

  uint32_t t = _radio->getEstAirtimeFor(pkt->getRawLength());
  int rc;
  if (recipient.out_path_len == OUT_PATH_UNKNOWN) {
    sendFloodScoped(recipient, pkt);
    txt_send_timeout = futureMillis(est_timeout = calcFloodTimeoutMillisFor(t));
    rc = MSG_SEND_SENT_FLOOD;
  } else {
    sendDirect(pkt, recipient.out_path, recipient.out_path_len);
    txt_send_timeout = futureMillis(est_timeout = calcDirectTimeoutMillisFor(t, recipient.out_path_len));
    rc = MSG_SEND_SENT_DIRECT;
  }
  return rc;
}

bool MyMesh::sendGroupMessage(uint32_t timestamp, mesh::GroupChannel &channel, const char *sender_name,
                              const char *text, int text_len) {
  uint8_t temp[5 + MAX_TEXT_LEN + 32];
  memcpy(temp, &timestamp, 4);
  temp[4] = 0;

  sprintf((char *)&temp[5], "%s: ", sender_name);
  char *ep = strchr((char *)&temp[5], 0);
  int prefix_len = ep - (char *)&temp[5];

  if (text_len + prefix_len > MAX_TEXT_LEN) text_len = MAX_TEXT_LEN - prefix_len;
  memcpy(ep, text, text_len);
  ep[text_len] = 0;

  auto pkt = createGroupDatagram(PAYLOAD_TYPE_GRP_TXT, channel, temp, 5 + prefix_len + text_len);
  if (pkt) {
    sendFloodScoped(channel, pkt);
    return true;
  }
  return false;
}

int MyMesh::sendLogin(const ContactInfo &recipient, const char *password, uint32_t &est_timeout,
                      uint8_t sender_identity_idx) {
  MESH_DEBUG_PRINTLN("sendLogin: recipient=%s, password_len=%d", recipient.name, strlen(password));

  mesh::Packet *pkt;
  {
    int tlen;
    uint8_t temp[24];
    uint32_t now = getRTCClock()->getCurrentTimeUnique();
    memcpy(temp, &now, 4);
    if (recipient.type == ADV_TYPE_ROOM) {
      memcpy(&temp[4], &recipient.sync_since, 4);
      int len = strlen(password);
      if (len > 15) len = 15;
      memcpy(&temp[8], password, len);
      tlen = 8 + len;
    } else {
      int len = strlen(password);
      if (len > 15) len = 15;
      memcpy(&temp[4], password, len);
      tlen = 4 + len;
    }
    MESH_DEBUG_PRINTLN("sendLogin: temp_len=%d; password_len=%d", tlen, tlen - 4);

    const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
    uint8_t secret[PUB_KEY_SIZE];
    getContactSharedSecret(sender, recipient, secret);
    pkt = createAnonDatagram(PAYLOAD_TYPE_ANON_REQ, sender, recipient.id, secret, temp, tlen);
  }
  if (!pkt) return MSG_SEND_FAILED;

  uint32_t t = _radio->getEstAirtimeFor(pkt->getRawLength());
  MESH_DEBUG_PRINTLN("sendLogin: estimated airtime: %u; out_path_len: %u %s", t, recipient.out_path_len,
                     recipient.out_path_len == OUT_PATH_UNKNOWN ? "(flood)" : "(direct)");
  if (recipient.out_path_len == OUT_PATH_UNKNOWN) {
    sendFloodScoped(recipient, pkt);
    est_timeout = calcFloodTimeoutMillisFor(t);
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  est_timeout = calcDirectTimeoutMillisFor(t, recipient.out_path_len);
  return MSG_SEND_SENT_DIRECT;
}

int MyMesh::sendAnonReq(const ContactInfo &recipient, const uint8_t *data, uint8_t len, uint32_t &tag,
                        uint32_t &est_timeout, uint8_t sender_identity_idx) {
  MESH_DEBUG_PRINTLN("sendAnonReq: recipient=%s, data_len=%d", recipient.name, len);

  mesh::Packet *pkt;
  {
    uint8_t temp[MAX_PACKET_PAYLOAD];
    tag = getRTCClock()->getCurrentTimeUnique();
    memcpy(temp, &tag, 4);
    memcpy(&temp[4], data, len);

    const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
    uint8_t secret[PUB_KEY_SIZE];
    getContactSharedSecret(sender, recipient, secret);
    pkt = createAnonDatagram(PAYLOAD_TYPE_ANON_REQ, sender, recipient.id, secret, temp, 4 + len);
  }
  if (!pkt) return MSG_SEND_FAILED;

  uint32_t t = _radio->getEstAirtimeFor(pkt->getRawLength());
  if (recipient.out_path_len == OUT_PATH_UNKNOWN) {
    sendFloodScoped(recipient, pkt);
    est_timeout = calcFloodTimeoutMillisFor(t);
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  est_timeout = calcDirectTimeoutMillisFor(t, recipient.out_path_len);
  return MSG_SEND_SENT_DIRECT;
}

int MyMesh::sendRequest(const ContactInfo &recipient, const uint8_t *req_data, uint8_t data_len,
                        uint32_t &tag, uint32_t &est_timeout, uint8_t sender_identity_idx) {
  MESH_DEBUG_PRINTLN("sendRequest: recipient=%s, data_len=%d", recipient.name, data_len);
  if (data_len > MAX_PACKET_PAYLOAD - 16) return MSG_SEND_FAILED;
  for (int i = 0; i < data_len; i++) {
    MESH_DEBUG_PRINTLN("byte[%d] = %02X = %d = " BYTE_TO_BINARY_PATTERN, i, req_data[i], req_data[i],
                       BYTE_TO_BINARY(req_data[i]));
  }
  MESH_DEBUG_PRINTLN("sendRequest: dump finshed");

  mesh::Packet *pkt;
  {
    uint8_t temp[MAX_PACKET_PAYLOAD];
    tag = getRTCClock()->getCurrentTimeUnique();
    memcpy(temp, &tag, 4); // mostly an extra blob to help make packet_hash unique
    memcpy(&temp[4], req_data, data_len);

    const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
    uint8_t secret[PUB_KEY_SIZE];
    getContactSharedSecret(sender, recipient, secret);
    pkt = createDatagramFromIdentity(PAYLOAD_TYPE_REQ, sender, recipient.id, secret, temp, 4 + data_len);
  }
  if (!pkt) return MSG_SEND_FAILED;

  uint32_t t = _radio->getEstAirtimeFor(pkt->getRawLength());
  if (recipient.out_path_len == OUT_PATH_UNKNOWN) {
    sendFloodScoped(recipient, pkt);
    est_timeout = calcFloodTimeoutMillisFor(t);
    return MSG_SEND_SENT_FLOOD;
  }

  sendDirect(pkt, recipient.out_path, recipient.out_path_len);
  est_timeout = calcDirectTimeoutMillisFor(t, recipient.out_path_len);
  return MSG_SEND_SENT_DIRECT;
}

int MyMesh::sendRequest(const ContactInfo &recipient, uint8_t req_type, uint32_t &tag, uint32_t &est_timeout,
                        uint8_t sender_identity_idx) {
  mesh::Packet *pkt;
  {
    uint8_t temp[13];
    tag = getRTCClock()->getCurrentTimeUnique();
    memcpy(temp, &tag, 4); // mostly an extra blob to help make packet_hash unique
    temp[4] = req_type;
    memset(&temp[5], 0, 4);        // reserved (possibly for 'since' param)
    getRNG()->random(&temp[9], 4); // random blob to help make packet-hash unique

    const mesh::LocalIdentity &sender = identityByIndex(sender_identity_idx);
    uint8_t secret[PUB_KEY_SIZE];
    getContactSharedSecret(sender, recipient, secret);
    pkt = createDatagramFromIdentity(PAYLOAD_TYPE_REQ, sender, recipient.id, secret, temp, sizeof(temp));
  }
  if (pkt) {
    uint32_t t = _radio->getEstAirtimeFor(pkt->getRawLength());
    if (recipient.out_path_len == OUT_PATH_UNKNOWN) {
      sendFloodScoped(recipient, pkt);
      est_timeout = calcFloodTimeoutMillisFor(t);
      return MSG_SEND_SENT_FLOOD;
    } else {
      sendDirect(pkt, recipient.out_path, recipient.out_path_len);
      est_timeout = calcDirectTimeoutMillisFor(t, recipient.out_path_len);
      return MSG_SEND_SENT_DIRECT;
    }
  }
  return MSG_SEND_FAILED;
}

bool MyMesh::shareContactZeroHop(const ContactInfo &contact) {
  int plen = getBlobByKey(contact.id.pub_key, PUB_KEY_SIZE, temp_buf);
  if (plen == 0) return false;

  auto packet = obtainNewPacket();
  if (packet == NULL) return false;

  packet->readFrom(temp_buf, plen);
  uint16_t codes[2];
  codes[0] = codes[1] = 0;
  sendZeroHop(packet, codes);
  return true;
}

uint8_t MyMesh::exportContact(const ContactInfo &contact, uint8_t dest_buf[]) {
  return getBlobByKey(contact.id.pub_key, PUB_KEY_SIZE, dest_buf);
}

bool MyMesh::importContact(const uint8_t src_buf[], uint8_t len) {
  auto pkt = obtainNewPacket();
  if (pkt) {
    if (pkt->readFrom(src_buf, len) && pkt->getPayloadType() == PAYLOAD_TYPE_ADVERT) {
      pkt->header |= ROUTE_TYPE_FLOOD;
      getTables()->clear(pkt);
      if (queueInternalPacket(pkt)) {
        return true;
      }
    }
    releasePacket(pkt);
  }
  return false;
}

bool MyMesh::startConnection(const ContactInfo &contact, uint16_t keep_alive_secs) {
  int use_idx = -1;
  for (int i = 0; i < MAX_CONNECTIONS; i++) {
    if (connections[i].keep_alive_millis == 0) {
      use_idx = i;
    } else if (connections[i].server_id.matches(contact.id)) {
      use_idx = i;
      break;
    }
  }
  if (use_idx < 0) return false;

  connections[use_idx].server_id = contact.id;
  uint32_t interval = connections[use_idx].keep_alive_millis = ((uint32_t)keep_alive_secs) * 1000;
  connections[use_idx].next_ping = futureMillis(interval);
  connections[use_idx].expected_ack = 0;
  connections[use_idx].last_activity = getRTCClock()->getCurrentTime();
  return true;
}

void MyMesh::stopConnection(const uint8_t *pub_key) {
  for (int i = 0; i < MAX_CONNECTIONS; i++) {
    if (connections[i].server_id.matches(pub_key)) {
      connections[i].keep_alive_millis = 0;
      connections[i].next_ping = 0;
      connections[i].expected_ack = 0;
      connections[i].last_activity = 0;
      break;
    }
  }
}

bool MyMesh::hasConnectionTo(const uint8_t *pub_key) {
  for (int i = 0; i < MAX_CONNECTIONS; i++) {
    if (connections[i].keep_alive_millis > 0 && connections[i].server_id.matches(pub_key)) return true;
  }
  return false;
}

void MyMesh::markConnectionActive(const ContactInfo &contact) {
  for (int i = 0; i < MAX_CONNECTIONS; i++) {
    if (connections[i].keep_alive_millis > 0 && connections[i].server_id.matches(contact.id)) {
      connections[i].last_activity = getRTCClock()->getCurrentTime();
      connections[i].next_ping = futureMillis(connections[i].keep_alive_millis);
      break;
    }
  }
}

ContactInfo *MyMesh::checkConnectionsAck(const uint8_t *data) {
  for (int i = 0; i < MAX_CONNECTIONS; i++) {
    if (connections[i].keep_alive_millis > 0 && memcmp(&connections[i].expected_ack, data, 4) == 0) {
      connections[i].expected_ack = 0;
      connections[i].last_activity = getRTCClock()->getCurrentTime();
      connections[i].next_ping = futureMillis(connections[i].keep_alive_millis);

      auto id = &connections[i].server_id;
      return lookupContactByPubKey(id->pub_key, PUB_KEY_SIZE);
    }
  }
  return NULL;
}

void MyMesh::checkConnections() {
  for (int i = 0; i < MAX_CONNECTIONS; i++) {
    if (connections[i].keep_alive_millis == 0) continue;

    uint32_t now = getRTCClock()->getCurrentTime();
    uint32_t expire_secs = (connections[i].keep_alive_millis / 1000) * 5 / 2;
    if (now >= connections[i].last_activity + expire_secs) {
      connections[i].keep_alive_millis = 0;
      connections[i].next_ping = 0;
      connections[i].expected_ack = 0;
      connections[i].last_activity = 0;
      continue;
    }

    if (millisHasNowPassed(connections[i].next_ping)) {
      auto contact = lookupContactByPubKey(connections[i].server_id.pub_key, PUB_KEY_SIZE);
      if (contact == NULL || contact->out_path_len == OUT_PATH_UNKNOWN) {
        continue;
      }

      uint8_t data[9];
      uint32_t unique_now = getRTCClock()->getCurrentTimeUnique();
      memcpy(data, &unique_now, 4);
      data[4] = REQ_TYPE_KEEP_ALIVE;
      memcpy(&data[5], &contact->sync_since, 4);

      mesh::Utils::sha256((uint8_t *)&connections[i].expected_ack, 4, data, 9, identityByIndex(0).pub_key,
                          PUB_KEY_SIZE);

      uint8_t secret[PUB_KEY_SIZE];
      mesh::LocalIdentity local_id = identityByIndex(0);
      getContactSharedSecret(local_id, *contact, secret);
      auto pkt = createDatagramFromIdentity(PAYLOAD_TYPE_REQ, local_id, contact->id, secret, data, 9);
      if (pkt) {
        sendDirect(pkt, contact->out_path, contact->out_path_len);
      }
      connections[i].next_ping = futureMillis(connections[i].keep_alive_millis);
    }
  }
}

void MyMesh::resetPathTo(ContactInfo &recipient) {
  recipient.out_path_len = OUT_PATH_UNKNOWN;
}

static ContactInfo *table;

static int cmp_adv_timestamp(const void *a, const void *b) {
  int a_idx = *((int *)a);
  int b_idx = *((int *)b);
  if (table[b_idx].last_advert_timestamp > table[a_idx].last_advert_timestamp) return 1;
  if (table[b_idx].last_advert_timestamp < table[a_idx].last_advert_timestamp) return -1;
  return 0;
}

void MyMesh::scanRecentContacts(int last_n, ContactVisitor *visitor) {
  for (int i = 0; i < num_contacts; i++) {
    sort_array[i] = i;
  }
  table = contacts;
  qsort(sort_array, num_contacts, sizeof(sort_array[0]), cmp_adv_timestamp);

  if (last_n == 0) {
    last_n = num_contacts;
  } else if (last_n > num_contacts) {
    last_n = num_contacts;
  }

  for (int i = 0; i < last_n; i++) {
    visitor->onContactVisit(contacts[sort_array[i]]);
  }
}

ContactInfo *MyMesh::searchContactsByPrefix(const char *name_prefix) {
  int len = strlen(name_prefix);
  for (int i = 0; i < num_contacts; i++) {
    auto c = &contacts[i];
    if (memcmp(c->name, name_prefix, len) == 0) return c;
  }
  return NULL;
}

ContactInfo *MyMesh::lookupContactByPubKey(const uint8_t *pub_key, int prefix_len) {
  for (int i = 0; i < num_contacts; i++) {
    auto c = &contacts[i];
    if (memcmp(c->id.pub_key, pub_key, prefix_len) == 0) return c;
  }
  return NULL;
}

ContactInfo *MyMesh::ensureContactForIdentity(const mesh::Identity &id, uint8_t type, const char *name) {
  ContactInfo *contact = lookupContactByPubKey(id.pub_key, PUB_KEY_SIZE);
  if (contact != NULL) {
    return contact;
  }

  contact = allocateContactSlot();
  if (contact == NULL) {
    MESH_DEBUG_PRINTLN("ensureContactForIdentity: unable to allocate contact slot");
    return NULL;
  }

  memset(contact, 0, sizeof(ContactInfo));
  contact->id = id;
  contact->out_path_len = OUT_PATH_UNKNOWN;
  contact->type = type;
  StrHelper::strncpy(contact->name, name, sizeof(contact->name));
  contact->sync_since = 0;
  contact->shared_secret_valid = false;
  contact->lastmod = getRTCClock()->getCurrentTime();

  dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
  return contact;
}

bool MyMesh::addContact(const ContactInfo &contact) {
  ContactInfo *dest = allocateContactSlot();
  if (dest) {
    *dest = contact;
    dest->shared_secret_valid = false;
    return true;
  }
  return false;
}

bool MyMesh::removeContact(ContactInfo &contact) {
  int idx = 0;
  while (idx < num_contacts && !contacts[idx].id.matches(contact.id)) {
    idx++;
  }
  if (idx >= num_contacts) return false;

  num_contacts--;
  while (idx < num_contacts) {
    contacts[idx] = contacts[idx + 1];
    idx++;
  }
  return true;
}

bool MyMesh::getContactByIdx(uint32_t idx, ContactInfo &contact) {
  if (idx >= num_contacts) return false;
  contact = contacts[idx];
  return true;
}

ContactsIterator MyMesh::startContactsIterator() {
  return ContactsIterator();
}

bool ContactsIterator::hasNext(const MyMesh *mesh, ContactInfo &dest) {
  if (next_idx >= mesh->getNumContacts()) return false;
  dest = mesh->contacts[next_idx++];
  return true;
}

#ifdef MAX_GROUP_CHANNELS
#include <base64.hpp>

ChannelDetails *MyMesh::addChannel(const char *name, const char *psk_base64) {
  if (num_channels < MAX_GROUP_CHANNELS) {
    auto dest = &channels[num_channels];

    memset(dest->channel.secret, 0, sizeof(dest->channel.secret));
    int len = decode_base64((unsigned char *)psk_base64, strlen(psk_base64), dest->channel.secret);
    if (len == 32 || len == 16) {
      mesh::Utils::sha256(dest->channel.hash, sizeof(dest->channel.hash), dest->channel.secret, len);
      StrHelper::strncpy(dest->name, name, sizeof(dest->name));
      num_channels++;
      return dest;
    }
  }
  return NULL;
}

bool MyMesh::getChannel(int idx, ChannelDetails &dest) {
  if (idx >= 0 && idx < MAX_GROUP_CHANNELS) {
    dest = channels[idx];
    return true;
  }
  return false;
}

bool MyMesh::setChannel(int idx, const ChannelDetails &src) {
  static uint8_t zeroes[] = { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };

  if (idx >= 0 && idx < MAX_GROUP_CHANNELS) {
    channels[idx] = src;
    if (memcmp(&src.channel.secret[16], zeroes, 16) == 0) {
      mesh::Utils::sha256(channels[idx].channel.hash, sizeof(channels[idx].channel.hash), src.channel.secret,
                          16);
    } else {
      mesh::Utils::sha256(channels[idx].channel.hash, sizeof(channels[idx].channel.hash), src.channel.secret,
                          32);
    }
    return true;
  }
  return false;
}

int MyMesh::findChannelIdx(const mesh::GroupChannel &ch) {
  for (int i = 0; i < MAX_GROUP_CHANNELS; i++) {
    if (memcmp(ch.secret, channels[i].channel.secret, sizeof(ch.secret)) == 0) return i;
  }
  return -1;
}
#else
ChannelDetails *MyMesh::addChannel(const char *name, const char *psk_base64) {
  return NULL;
}
bool MyMesh::getChannel(int idx, ChannelDetails &dest) {
  return false;
}
bool MyMesh::setChannel(int idx, const ChannelDetails &src) {
  return false;
}
int MyMesh::findChannelIdx(const mesh::GroupChannel &ch) {
  return -1;
}
#endif

void MyMesh::writeOKFrame() {
  uint8_t buf[1];
  buf[0] = RESP_CODE_OK;
  _serial->writeFrame(buf, 1);
}
void MyMesh::writeErrFrame(uint8_t err_code) {
  uint8_t buf[2];
  buf[0] = RESP_CODE_ERR;
  buf[1] = err_code;
  _serial->writeFrame(buf, 2);
}

void MyMesh::writeDisabledFrame() {
  uint8_t buf[1];
  buf[0] = RESP_CODE_DISABLED;
  _serial->writeFrame(buf, 1);
}

void MyMesh::writeContactRespFrame(uint8_t code, const ContactInfo &contact) {
  int i = 0;
  out_frame[i++] = code;
  memcpy(&out_frame[i], contact.id.pub_key, PUB_KEY_SIZE);
  i += PUB_KEY_SIZE;
  out_frame[i++] = contact.type;
  out_frame[i++] = contact.flags;
  out_frame[i++] = contact.out_path_len;
  memcpy(&out_frame[i], contact.out_path, MAX_PATH_SIZE);
  i += MAX_PATH_SIZE;
  StrHelper::strzcpy((char *)&out_frame[i], contact.name, 32);
  i += 32;
  memcpy(&out_frame[i], &contact.last_advert_timestamp, 4);
  i += 4;
  memcpy(&out_frame[i], &contact.gps_lat, 4);
  i += 4;
  memcpy(&out_frame[i], &contact.gps_lon, 4);
  i += 4;
  memcpy(&out_frame[i], &contact.lastmod, 4);
  i += 4;
  _serial->writeFrame(out_frame, i);
}

void MyMesh::updateContactFromFrame(ContactInfo &contact, uint32_t &last_mod, const uint8_t *frame, int len) {
  int i = 0;
  uint8_t code = frame[i++]; // eg. CMD_ADD_UPDATE_CONTACT
  memcpy(contact.id.pub_key, &frame[i], PUB_KEY_SIZE);
  i += PUB_KEY_SIZE;
  contact.type = frame[i++];
  contact.flags = frame[i++];
  contact.out_path_len = frame[i++];
  memcpy(contact.out_path, &frame[i], MAX_PATH_SIZE);
  i += MAX_PATH_SIZE;
  memcpy(contact.name, &frame[i], 32);
  i += 32;
  memcpy(&contact.last_advert_timestamp, &frame[i], 4);
  i += 4;
  if (len >= i + 8) { // optional fields
    memcpy(&contact.gps_lat, &frame[i], 4);
    i += 4;
    memcpy(&contact.gps_lon, &frame[i], 4);
    i += 4;
    if (len >= i + 4) {
      memcpy(&last_mod, &frame[i], 4);
    }
  }
}

bool MyMesh::Frame::isChannelMsg() const {
  return buf[0] == RESP_CODE_CHANNEL_MSG_RECV || buf[0] == RESP_CODE_CHANNEL_MSG_RECV_V3;
}

void MyMesh::addToOfflineQueue(const uint8_t frame[], int len) {
  if (offline_queue_len >= OFFLINE_QUEUE_SIZE) {
    MESH_DEBUG_PRINTLN("WARN: offline_queue is full!");
    int pos = 0;
    while (pos < offline_queue_len) {
      if (offline_queue[pos].isChannelMsg()) {
        for (int i = pos; i < offline_queue_len - 1; i++) { // delete oldest channel msg from queue
          offline_queue[i] = offline_queue[i + 1];
        }
        MESH_DEBUG_PRINTLN("INFO: removed oldest channel message from queue.");
        offline_queue[offline_queue_len - 1].len = len;
        memcpy(offline_queue[offline_queue_len - 1].buf, frame, len);
        return;
      }
      pos++;
    }
    MESH_DEBUG_PRINTLN("INFO: no channel messages to remove from queue.");
  } else {
    offline_queue[offline_queue_len].len = len;
    memcpy(offline_queue[offline_queue_len].buf, frame, len);
    offline_queue_len++;
  }
}

int MyMesh::getFromOfflineQueue(uint8_t frame[]) {
  if (offline_queue_len > 0) {         // check offline queue
    size_t len = offline_queue[0].len; // take from top of queue
    memcpy(frame, offline_queue[0].buf, len);

    offline_queue_len--;
    for (int i = 0; i < offline_queue_len; i++) { // delete top item from queue
      offline_queue[i] = offline_queue[i + 1];
    }
    return len;
  }
  return 0; // queue is empty
}

float MyMesh::getAirtimeBudgetFactor() const {
  return _prefs.airtime_factor;
}

int MyMesh::getInterferenceThreshold() const {
  return 0; // disabled for now, until currentRSSI() problem is resolved
}

int MyMesh::calcRxDelay(float score, uint32_t air_time) const {
  if (_prefs.rx_delay_base <= 0.0f) return 0;
  return (int)((pow(_prefs.rx_delay_base, 0.85f - score) - 1.0) * air_time);
}

uint32_t MyMesh::getRetransmitDelay(const mesh::Packet *packet) {
  uint32_t t = (_radio->getEstAirtimeFor(packet->getPathByteLen() + packet->payload_len + 2) * 0.5f);
  return getRNG()->nextInt(0, 5 * t + 1);
}
uint32_t MyMesh::getDirectRetransmitDelay(const mesh::Packet *packet) {
  uint32_t t = (_radio->getEstAirtimeFor(packet->getPathByteLen() + packet->payload_len + 2) * 0.2f);
  return getRNG()->nextInt(0, 5 * t + 1);
}

uint8_t MyMesh::getExtraAckTransmitCount() const {
  return _prefs.multi_acks;
}

void MyMesh::logRxRaw(float snr, float rssi, const uint8_t raw[], int len) {
  if (_serial->isConnected() && len + 3 <= MAX_FRAME_SIZE) {
    int i = 0;
    out_frame[i++] = PUSH_CODE_LOG_RX_DATA;
    out_frame[i++] = (int8_t)(snr * 4);
    out_frame[i++] = (int8_t)(rssi);
    memcpy(&out_frame[i], raw, len);
    i += len;

    _serial->writeFrame(out_frame, i);
  }
}

bool MyMesh::isAutoAddEnabled() const {
  return (_prefs.manual_add_contacts & 1) == 0;
}

bool MyMesh::shouldAutoAddContactType(uint8_t contact_type) const {
  if ((_prefs.manual_add_contacts & 1) == 0) {
    return true;
  }

  uint8_t type_bit = 0;
  switch (contact_type) {
  case ADV_TYPE_CHAT:
    type_bit = AUTO_ADD_CHAT;
    break;
  case ADV_TYPE_REPEATER:
    type_bit = AUTO_ADD_REPEATER;
    break;
  case ADV_TYPE_ROOM:
    type_bit = AUTO_ADD_ROOM_SERVER;
    break;
  case ADV_TYPE_SENSOR:
    type_bit = AUTO_ADD_SENSOR;
    break;
  default:
    return false; // Unknown type, don't auto-add
  }

  return (_prefs.autoadd_config & type_bit) != 0;
}

bool MyMesh::shouldOverwriteWhenFull() const {
  return (_prefs.autoadd_config & AUTO_ADD_OVERWRITE_OLDEST) != 0;
}

uint8_t MyMesh::getAutoAddMaxHops() const {
  return _prefs.autoadd_max_hops;
}

void MyMesh::onContactOverwrite(const uint8_t *pub_key) {
  _store->deleteBlobByKey(pub_key, PUB_KEY_SIZE); // delete from storage
  if (_serial->isConnected()) {
    out_frame[0] = PUSH_CODE_CONTACT_DELETED;
    memcpy(&out_frame[1], pub_key, PUB_KEY_SIZE);
    _serial->writeFrame(out_frame, 1 + PUB_KEY_SIZE);
  }
}

void MyMesh::onContactsFull() {
  if (_serial->isConnected()) {
    out_frame[0] = PUSH_CODE_CONTACTS_FULL;
    _serial->writeFrame(out_frame, 1);
  }
}

void MyMesh::onDiscoveredContact(ContactInfo &contact, bool is_new, uint8_t path_len, const uint8_t *path) {
  if (_serial && _serial->isConnected()) {
    if (is_new) {
      writeContactRespFrame(PUSH_CODE_NEW_ADVERT, contact);
    } else {
      out_frame[0] = PUSH_CODE_ADVERT;
      memcpy(&out_frame[1], contact.id.pub_key, PUB_KEY_SIZE);
      _serial->writeFrame(out_frame, 1 + PUB_KEY_SIZE);
    }
  } else {
#ifdef DISPLAY_CLASS
    if (_ui) _ui->notify(UIEventType::newContactMessage);
#endif
  }

  // add inbound-path to mem cache
  if (path && mesh::Packet::isValidPathLen(path_len)) { // check path is valid
    AdvertPath *p = advert_paths;
    uint32_t oldest = 0xFFFFFFFF;
    for (int i = 0; i < ADVERT_PATH_TABLE_SIZE; i++) { // check if already in table, otherwise evict oldest
      if (memcmp(advert_paths[i].pubkey_prefix, contact.id.pub_key, sizeof(AdvertPath::pubkey_prefix)) == 0) {
        p = &advert_paths[i]; // found
        break;
      }
      if (advert_paths[i].recv_timestamp < oldest) {
        oldest = advert_paths[i].recv_timestamp;
        p = &advert_paths[i];
      }
    }

    memcpy(p->pubkey_prefix, contact.id.pub_key, sizeof(p->pubkey_prefix));
    strcpy(p->name, contact.name);
    p->recv_timestamp = getRTCClock()->getCurrentTime();
    p->path_len = mesh::Packet::copyPath(p->path, path, path_len);
  }

  if (!is_new)
    dirty_contacts_expiry = futureMillis(
        LAZY_CONTACTS_WRITE_DELAY); // only schedule lazy write for contacts that are in contacts[]
}

static int sort_by_recent(const void *a, const void *b) {
  return ((AdvertPath *)b)->recv_timestamp - ((AdvertPath *)a)->recv_timestamp;
}

int MyMesh::getRecentlyHeard(AdvertPath dest[], int max_num) {
  if (max_num > ADVERT_PATH_TABLE_SIZE) max_num = ADVERT_PATH_TABLE_SIZE;
  qsort(advert_paths, ADVERT_PATH_TABLE_SIZE, sizeof(advert_paths[0]), sort_by_recent);

  for (int i = 0; i < max_num; i++) {
    dest[i] = advert_paths[i];
  }
  return max_num;
}

void MyMesh::onContactPathUpdated(const ContactInfo &contact) {
  out_frame[0] = PUSH_CODE_PATH_UPDATED;
  memcpy(&out_frame[1], contact.id.pub_key, PUB_KEY_SIZE);
  _serial->writeFrame(out_frame, 1 + PUB_KEY_SIZE); // NOTE: app may not be connected

  dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
}

ContactInfo *MyMesh::processAck(const uint8_t *data) {
  // see if matches any in a table
  for (int i = 0; i < EXPECTED_ACK_TABLE_SIZE; i++) {
    if (memcmp(data, &expected_ack_table[i].ack, 4) == 0) { // got an ACK from recipient
      out_frame[0] = PUSH_CODE_SEND_CONFIRMED;
      memcpy(&out_frame[1], data, 4);
      uint32_t trip_time = _ms->getMillis() - expected_ack_table[i].msg_sent;
      memcpy(&out_frame[5], &trip_time, 4);
      _serial->writeFrame(out_frame, 9);

      // NOTE: the same ACK can be received multiple times!
      expected_ack_table[i].ack = 0; // clear expected hash, now that we have received ACK
      return expected_ack_table[i].contact;
    }
  }
  return checkConnectionsAck(data);
}

void MyMesh::onPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                            uint8_t *data, size_t len) {
  auto peer = matching_peers[sender_idx];
  int i = peer.identity_idx;
  int c = peer.contact_idx;
  int a = peer.acl_idx;
  MESH_DEBUG_PRINTLN("onPeerDataRecv: Received data from peer; id=%d, contact_idx=%d, acl_idx=%d", i, c, a);
  if (c != -1) { // Client
    return clientOnPeerDataRecv(packet, type, sender_idx, secret, data, len);
  } else { // Sensor
    return sensorOnPeerDataRecv(packet, type, sender_idx, secret, data, len);
  }
}

bool MyMesh::handleIncomingMsg(ClientInfo &from, uint32_t timestamp, uint8_t *data, uint8_t flags,
                               size_t len) {
  MESH_DEBUG_PRINTLN("handleIncomingMsg: unhandled msg from ");
#ifdef MESH_DEBUG
  mesh::Utils::printHex(Serial, from.id.pub_key, PUB_KEY_SIZE);
  Serial.printf(": %s\n", data);
#endif
  return false;
}

uint8_t MyMesh::handleRequest(uint8_t perms, uint32_t sender_timestamp, uint8_t req_type, uint8_t *payload,
                              size_t payload_len) {
  memcpy(reply_data, &sender_timestamp,
         4); // reflect sender_timestamp back in response packet (kind of like a 'tag')

  MESH_DEBUG_PRINTLN("MyMesh::handleRequest: req_type=%d, perms=0x%02X", req_type, perms);
  if (req_type == REQ_TYPE_GET_TELEMETRY_DATA) { // allow all
    MESH_DEBUG_PRINTLN("MyMesh::handleRequest: REQ_TYPE_GET_TELEMETRY_DATA");
    uint8_t perm_mask =
        ~(payload[0]); // NEW: first reserved byte (of 4), is now inverse mask to apply to permissions

    telemetry.reset();
    telemetry.addVoltage(TELEM_CHANNEL_SELF, (float)board.getBattMilliVolts() / 1000.0f);
    // query other sensors -- target specific
    sensors.querySensors(0xFF & perm_mask, telemetry); // allow all telemetry permissions for admin or guest
    // TODO: let requester know permissions they have:  telemetry.addPresence(TELEM_CHANNEL_SELF, perms);

    uint8_t tlen = telemetry.getSize();
    memcpy(&reply_data[4], telemetry.getBuffer(), tlen);
    return 4 + tlen; // reply_len
  }
  if (req_type == REQ_TYPE_GET_ACCESS_LIST && (perms & PERM_ACL_ROLE_MASK) == PERM_ACL_ADMIN) {
    MESH_DEBUG_PRINTLN("MyMesh::handleRequest: REQ_TYPE_GET_ACCESS_LIST");
    uint8_t res1 = payload[0]; // reserved for future  (extra query params)
    uint8_t res2 = payload[1];
    if (res1 == 0 && res2 == 0) {
      uint8_t ofs = 4;
      for (int i = 0; i < _acl.getNumClients() && ofs + 7 <= sizeof(reply_data) - 4; i++) {
        auto c = _acl.getClientByIdx(i);
        if (c->permissions == 0) continue; // skip deleted entries
        memcpy(&reply_data[ofs], c->id.pub_key, 6);
        ofs += 6; // just 6-byte pub_key prefix
        reply_data[ofs++] = c->permissions;
      }
      return ofs;
    }
  }
  return 0; // unknown command
}

void MyMesh::sensorOnPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                                  uint8_t *data, size_t len) {
  MESH_DEBUG_PRINTLN("sensorOnPeerDataRecv: type=%d, sender_idx=%d, len=%d", type, sender_idx, (int)len);
  auto peer = matching_peers[sender_idx];
  int a = peer.acl_idx;
  const mesh::LocalIdentity recv_identity = getLocalIdentity(peer.identity_idx)->id;
  if (a < 0) {
    MESH_DEBUG_PRINTLN("sensorOnPeerDataRecv: sender not in ACL, ignoring");
    return;
  }
  ClientInfo *from = _acl.getClientByIdx(a);
  if (type == PAYLOAD_TYPE_REQ) { // request (from a known contact)
    MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: type == PAYLOAD_TYPE_REQ");
    uint32_t timestamp;
    memcpy(&timestamp, data, 4);

    if (timestamp > from->last_timestamp) { // prevent replay attacks
      uint8_t reply_len =
          handleRequest(from->isAdmin() ? 0xFF : from->permissions, timestamp, data[4], &data[5], len - 5);
      MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: handleRequest returned reply_len=%d; isAdmin=%d",
                         reply_len, from->isAdmin());
      if (reply_len == 0) return; // invalid command

      from->last_timestamp = timestamp;
      from->last_activity = getRTCClock()->getCurrentTime();

      if (packet->isRouteFlood()) {
        // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the response
        mesh::Packet *path =
            createPathReturnFromIdentity(recv_identity, from->id, secret, packet->path, packet->path_len,
                                         PAYLOAD_TYPE_RESPONSE, reply_data, reply_len);
        if (path) sendFlood(path, SERVER_RESPONSE_DELAY, packet->getPathHashSize());
      } else {
        mesh::Packet *reply = createDatagramFromIdentity(PAYLOAD_TYPE_RESPONSE, recv_identity, from->id,
                                                         secret, reply_data, reply_len);
        if (reply) {
          if (from->out_path_len != OUT_PATH_UNKNOWN) { // we have an out_path, so send DIRECT
            sendDirect(reply, from->out_path, from->out_path_len, SERVER_RESPONSE_DELAY);
          } else {
            sendFlood(reply, SERVER_RESPONSE_DELAY, packet->getPathHashSize());
          }
        }
      }
    } else {
      MESH_DEBUG_PRINTLN("onPeerDataRecv: possible replay attack detected");
    }
  } else if (type == PAYLOAD_TYPE_TXT_MSG && len > 5 && from->isAdmin()) { // a CLI command
    MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: type == PAYLOAD_TYPE_TXT_MSG");
    uint32_t sender_timestamp;
    memcpy(&sender_timestamp, data, 4); // timestamp (by sender's RTC clock - which could be wrong)
    uint8_t flags = (data[4] >> 2);     // message attempt number, and other flags

    if (sender_timestamp > from->last_timestamp) { // prevent replay attacks
      if (flags == TXT_TYPE_PLAIN) {
        MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: flags == TXT_TYPE_PLAIN");
        bool handled = handleIncomingMsg(*from, sender_timestamp, &data[5], flags, len - 5);
        if (handled) {       // if msg was handled then send an ack
          uint32_t ack_hash; // calc truncated hash of the message timestamp + text + sender pub_key, to prove
                             // to sender that we got it
          mesh::Utils::sha256((uint8_t *)&ack_hash, 4, data, 5 + strlen((char *)&data[5]), from->id.pub_key,
                              PUB_KEY_SIZE);
          MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: sending ACK send_type=%s",
                             packet->isRouteFlood() ? "flood" : "direct");
          if (packet->isRouteFlood()) {
            // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the ACK
            mesh::Packet *path =
                createPathReturnFromIdentity(recv_identity, from->id, secret, packet->path, packet->path_len,
                                             PAYLOAD_TYPE_ACK, (uint8_t *)&ack_hash, 4);
            if (path) sendFlood(path, TXT_ACK_DELAY, packet->getPathHashSize());
          } else {
            sendAckTo(*from, ack_hash, packet->getPathHashSize());
          }
        }
      } else if (flags == TXT_TYPE_CLI_DATA) {
        MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: flags == TXT_TYPE_CLI_DATA");
        from->last_timestamp = sender_timestamp;
        from->last_activity = getRTCClock()->getCurrentTime();

        // len can be > original length, but 'text' will be padded with zeroes
        data[len] = 0; // need to make a C string again, with null terminator

        uint8_t temp[166];
        char *command = (char *)&data[5];
        char *reply = (char *)&temp[5];
        _cli.handleCommand(sender_timestamp, command, reply);

        int text_len = strlen(reply);
        if (text_len > 0) {
          uint32_t timestamp = getRTCClock()->getCurrentTimeUnique();
          if (timestamp == sender_timestamp) {
            // WORKAROUND: the two timestamps need to be different, in the CLI view
            timestamp++;
          }
          memcpy(temp, &timestamp, 4); // mostly an extra blob to help make packet_hash unique
          temp[4] = (TXT_TYPE_CLI_DATA << 2);

          mesh::Packet *reply = createDatagramFromIdentity(PAYLOAD_TYPE_TXT_MSG, recv_identity, from->id,
                                                           secret, temp, 5 + text_len);
          if (reply) {
            MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: sending CLI reply send_type=%s",
                               (from->out_path_len == OUT_PATH_UNKNOWN) ? "flood" : "direct");
            if (from->out_path_len == OUT_PATH_UNKNOWN) {
              sendFlood(reply, CLI_REPLY_DELAY_MILLIS, packet->getPathHashSize());
            } else {
              MESH_DEBUG_PRINTLN("MyMesh::sensorOnPeerDataRecv: sending CLI reply DIRECT, out_path_len=%d",
                                 from->out_path_len);
              for (int i = 0; i < from->out_path_len; i++) {
                MESH_DEBUG_PRINTLN("  out_path[%d] = %02x", i, from->out_path[i]);
              }
              sendDirect(reply, from->out_path, from->out_path_len, CLI_REPLY_DELAY_MILLIS);
            }
          }
        }
      } else {
        MESH_DEBUG_PRINTLN("onPeerDataRecv: unsupported text type received: flags=%02x", (uint32_t)flags);
      }
    } else {
      MESH_DEBUG_PRINTLN("onPeerDataRecv: possible replay attack detected");
    }
  }
}

void MyMesh::clientOnPeerDataRecv(mesh::Packet *packet, uint8_t type, int sender_idx, const uint8_t *secret,
                                  uint8_t *data, size_t len) {
  MESH_DEBUG_PRINTLN("clientOnPeerDataRecv: type=%d, sender_idx=%d, len=%d", type, sender_idx, (int)len);
  auto peer = matching_peers[sender_idx];
  int c = peer.contact_idx;
  int id_idx = peer.identity_idx;
  mesh::LocalIdentity recv_identity = getLocalIdentity(id_idx)->id;

  ContactInfo &from = contacts[c];

  if (type == PAYLOAD_TYPE_TXT_MSG && len > 5) {
    MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: type == PAYLOAD_TYPE_TXT_MSG");
    uint32_t timestamp;
    memcpy(&timestamp, data, 4);  // timestamp (by sender's RTC clock - which could be wrong)
    uint8_t flags = data[4] >> 2; // message attempt number, and other flags

    // len can be > original length, but 'text' will be padded with zeroes
    data[len] = 0; // need to make a C string again, with null terminator

    if (flags == TXT_TYPE_PLAIN) {
      MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: flags == TXT_TYPE_PLAIN");
      from.lastmod = getRTCClock()->getCurrentTime();                 // update last heard time
      onMessageRecv(from, packet, timestamp, (const char *)&data[5]); // let UI know

      uint32_t ack_hash; // calc truncated hash of the message timestamp + text + sender pub_key, to prove to
                         // sender that we got it
      mesh::Utils::sha256((uint8_t *)&ack_hash, 4, data, 5 + strlen((char *)&data[5]), from.id.pub_key,
                          PUB_KEY_SIZE);

      if (packet->isRouteFlood()) {
        // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the ACK
        mesh::Packet *path =
            createPathReturnFromIdentity(recv_identity, from.id, secret, packet->path, packet->path_len,
                                         PAYLOAD_TYPE_ACK, (uint8_t *)&ack_hash, 4);
        if (path) sendFloodScoped(from, path, TXT_ACK_DELAY);
      } else {
        sendAckTo(from, ack_hash, packet->getPathHashSize());
      }
    } else if (flags == TXT_TYPE_CLI_DATA) {
      MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: flags == TXT_TYPE_CLI_DATA");
      onCommandDataRecv(from, packet, timestamp, (const char *)&data[5]); // let UI know
      // NOTE: no ack expected for CLI_DATA replies
      if (packet->isRouteFlood()) {
        // let this sender know path TO here, so they can use sendDirect() (NOTE: no ACK as extra)
        mesh::Packet *path = createPathReturnFromIdentity(recv_identity, from.id, secret, packet->path,
                                                          packet->path_len, 0, NULL, 0);
        if (path) sendFloodScoped(from, path);
      }
    } else if (flags == TXT_TYPE_SIGNED_PLAIN) {
      MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: type == TXT_TYPE_SIGNED_PLAIN");
      if (timestamp > from.sync_since) { // make sure 'sync_since' is up-to-date
        from.sync_since = timestamp;
      }
      from.lastmod = getRTCClock()->getCurrentTime(); // update last heard time
      onSignedMessageRecv(from, packet, timestamp, &data[5], (const char *)&data[9]); // let UI know

      uint32_t ack_hash; // calc truncated hash of the message timestamp + text + OUR pub_key, to prove to
                         // sender that we got it
      mesh::Utils::sha256((uint8_t *)&ack_hash, 4, data, 9 + strlen((char *)&data[9]), recv_identity.pub_key,
                          PUB_KEY_SIZE);

      if (packet->isRouteFlood()) {
        // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the ACK
        mesh::Packet *path =
            createPathReturnFromIdentity(recv_identity, from.id, secret, packet->path, packet->path_len,
                                         PAYLOAD_TYPE_ACK, (uint8_t *)&ack_hash, 4);
        if (path) sendFloodScoped(from, path, TXT_ACK_DELAY);
      } else {
        sendAckTo(from, ack_hash, packet->getPathHashSize());
      }
    } else {
      MESH_DEBUG_PRINTLN("onPeerDataRecv: unsupported message type: %u", (uint32_t)flags);
    }
  } else if (type == PAYLOAD_TYPE_REQ && len > 4) {
    MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: type == PAYLOAD_TYPE_REQ");
    uint32_t sender_timestamp;
    memcpy(&sender_timestamp, data, 4);
    uint8_t reply_len = onContactRequest(from, sender_timestamp, &data[4], len - 4, temp_buf);
    if (reply_len > 0) {
      if (packet->isRouteFlood()) {
        // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the response
        mesh::Packet *path =
            createPathReturnFromIdentity(recv_identity, from.id, secret, packet->path, packet->path_len,
                                         PAYLOAD_TYPE_RESPONSE, temp_buf, reply_len);
        if (path) sendFloodScoped(from, path, SERVER_RESPONSE_DELAY);
      } else {
        mesh::Packet *reply = createDatagramFromIdentity(PAYLOAD_TYPE_RESPONSE, recv_identity, from.id,
                                                         secret, temp_buf, reply_len);
        if (reply) {
          if (from.out_path_len != OUT_PATH_UNKNOWN) { // we have an out_path, so send DIRECT
            sendDirect(reply, from.out_path, from.out_path_len, SERVER_RESPONSE_DELAY);
          } else {
            sendFloodScoped(from, reply, SERVER_RESPONSE_DELAY);
          }
        }
      }
    }
  } else if (type == PAYLOAD_TYPE_RESPONSE && len > 0) {
    MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: type == PAYLOAD_TYPE_RESPONSE");
    onContactResponse(from, data, len);
    if (packet->isRouteFlood() && from.out_path_len != OUT_PATH_UNKNOWN) {
      MESH_DEBUG_PRINTLN("MyMesh::clientOnPeerDataRecv: packet is flood && out_path_len=%d",
                         from.out_path_len);
      // we have direct path, but other node is still sending flood response, so maybe they didn't receive
      // reciprocal path properly(?)
      for (int i = 0; i < from.out_path_len; i++) {
        MESH_DEBUG_PRINTLN("  out_path[%d] = %02x", i, from.out_path[i]);
      }
      handleReturnPathRetry(from, packet->path, packet->path_len);
    }
  }
}

void MyMesh::queueMessage(const ContactInfo &from, uint8_t txt_type, mesh::Packet *pkt,
                          uint32_t sender_timestamp, const uint8_t *extra, int extra_len, const char *text) {
  MESH_DEBUG_PRINTLN("queueMessage");
  int i = 0;
  if (app_target_ver >= 3) {
    out_frame[i++] = RESP_CODE_CONTACT_MSG_RECV_V3;
    out_frame[i++] = (int8_t)(pkt->getSNR() * 4);
    out_frame[i++] = 0; // reserved1
    out_frame[i++] = 0; // reserved2
  } else {
    out_frame[i++] = RESP_CODE_CONTACT_MSG_RECV;
  }
  memcpy(&out_frame[i], from.id.pub_key, 6);
  i += 6; // just 6-byte prefix
  uint8_t path_len = out_frame[i++] = pkt->isRouteFlood() ? pkt->path_len : 0xFF;
  out_frame[i++] = txt_type;
  memcpy(&out_frame[i], &sender_timestamp, 4);
  i += 4;
  if (extra_len > 0) {
    memcpy(&out_frame[i], extra, extra_len);
    i += extra_len;
  }
  int tlen = strlen(text); // TODO: UTF-8 ??
  if (i + tlen > MAX_FRAME_SIZE) {
    tlen = MAX_FRAME_SIZE - i;
  }
  memcpy(&out_frame[i], text, tlen);
  i += tlen;
  addToOfflineQueue(out_frame, i);

  if (_serial->isConnected()) {
    uint8_t frame[1];
    frame[0] = PUSH_CODE_MSG_WAITING; // send push 'tickle'
    _serial->writeFrame(frame, 1);
  }

#ifdef DISPLAY_CLASS
  // we only want to show text messages on display, not cli data
  bool should_display = txt_type == TXT_TYPE_PLAIN || txt_type == TXT_TYPE_SIGNED_PLAIN;
  if (should_display && _ui) {
    _ui->newMsg(path_len, from.name, text, offline_queue_len);
    if (!_serial->isConnected()) {
      _ui->notify(UIEventType::contactMessage);
    }
  }
#endif
}

bool MyMesh::filterRecvFloodPacket(mesh::Packet *packet) {
  // REVISIT: try to determine which Region (from transport_codes[1]) that Sender is indicating for
  // replies/responses
  //    if unknown, fallback to finding Region from transport_codes[0], the 'scope' used by Sender
  return false;
}

bool MyMesh::allowPacketForward(const mesh::Packet *packet) {
  return _prefs.client_repeat != 0;
}

bool MyMesh::routePacketInternallyIfLocal(mesh::Packet *packet) {
  if (packet == NULL) {
    return false;
  }
  // TRACE payloads should always go over radio.
  if (packet->getPayloadType() == PAYLOAD_TYPE_TRACE) {
    return false;
  }
  if (isPacketForLocalIdentity(packet)) {
    if (queueInternalPacket(packet)) {
      MESH_DEBUG_PRINTLN("MyMesh::routePacketInternallyIfLocal(): packet is for local recipient, queued for "
                         "internal routing");
      getTables()->clear(packet);
      return true;
    }
    MESH_DEBUG_PRINTLN("MyMesh::routePacketInternallyIfLocal(): local loopback queue full");
  }
  return false;
}

void MyMesh::sendFloodScoped(const ContactInfo &recipient, mesh::Packet *pkt, uint32_t delay_millis) {
  // TODO: dynamic send_scope, depending on recipient and current 'home' Region
  if (send_scope.isNull()) {
    sendFlood(pkt, delay_millis, _prefs.path_hash_mode + 1);
  } else {
    uint16_t codes[2];
    codes[0] = send_scope.calcTransportCode(pkt);
    codes[1] = 0; // REVISIT: set to 'home' Region, for sender/return region?
    sendFlood(pkt, codes, delay_millis, _prefs.path_hash_mode + 1);
  }
}
void MyMesh::sendFloodScoped(const mesh::GroupChannel &channel, mesh::Packet *pkt, uint32_t delay_millis) {
  // TODO: have per-channel send_scope
  if (send_scope.isNull()) {
    sendFlood(pkt, delay_millis, _prefs.path_hash_mode + 1);
  } else {
    uint16_t codes[2];
    codes[0] = send_scope.calcTransportCode(pkt);
    codes[1] = 0; // REVISIT: set to 'home' Region, for sender/return region?
    sendFlood(pkt, codes, delay_millis, _prefs.path_hash_mode + 1);
  }
}

void MyMesh::onMessageRecv(const ContactInfo &from, mesh::Packet *pkt, uint32_t sender_timestamp,
                           const char *text) {
  markConnectionActive(from); // in case this is from a server, and we have a connection
  queueMessage(from, TXT_TYPE_PLAIN, pkt, sender_timestamp, NULL, 0, text);
}

// client
void MyMesh::onCommandDataRecv(const ContactInfo &from, mesh::Packet *pkt, uint32_t sender_timestamp,
                               const char *text) {
  markConnectionActive(from); // in case this is from a server, and we have a connection
  queueMessage(from, TXT_TYPE_CLI_DATA, pkt, sender_timestamp, NULL, 0, text);
}

void MyMesh::onSignedMessageRecv(const ContactInfo &from, mesh::Packet *pkt, uint32_t sender_timestamp,
                                 const uint8_t *sender_prefix, const char *text) {
  markConnectionActive(from);
  // from.sync_since change needs to be persisted
  dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
  queueMessage(from, TXT_TYPE_SIGNED_PLAIN, pkt, sender_timestamp, sender_prefix, 4, text);
}

void MyMesh::onChannelMessageRecv(const mesh::GroupChannel &channel, mesh::Packet *pkt, uint32_t timestamp,
                                  const char *text) {
  int i = 0;
  if (app_target_ver >= 3) {
    out_frame[i++] = RESP_CODE_CHANNEL_MSG_RECV_V3;
    out_frame[i++] = (int8_t)(pkt->getSNR() * 4);
    out_frame[i++] = 0; // reserved1
    out_frame[i++] = 0; // reserved2
  } else {
    out_frame[i++] = RESP_CODE_CHANNEL_MSG_RECV;
  }

  uint8_t channel_idx = findChannelIdx(channel);
  out_frame[i++] = channel_idx;
  uint8_t path_len = out_frame[i++] = pkt->isRouteFlood() ? pkt->path_len : 0xFF;

  out_frame[i++] = TXT_TYPE_PLAIN;
  memcpy(&out_frame[i], &timestamp, 4);
  i += 4;
  int tlen = strlen(text); // TODO: UTF-8 ??
  if (i + tlen > MAX_FRAME_SIZE) {
    tlen = MAX_FRAME_SIZE - i;
  }
  memcpy(&out_frame[i], text, tlen);
  i += tlen;
  addToOfflineQueue(out_frame, i);

  if (_serial->isConnected()) {
    uint8_t frame[1];
    frame[0] = PUSH_CODE_MSG_WAITING; // send push 'tickle'
    _serial->writeFrame(frame, 1);
  } else {
#ifdef DISPLAY_CLASS
    if (_ui) _ui->notify(UIEventType::channelMessage);
#endif
  }
#ifdef DISPLAY_CLASS
  // Get the channel name from the channel index
  const char *channel_name = "Unknown";
  ChannelDetails channel_details;
  if (getChannel(channel_idx, channel_details)) {
    channel_name = channel_details.name;
  }
  if (_ui) _ui->newMsg(path_len, channel_name, text, offline_queue_len);
#endif
}

// only client
uint8_t MyMesh::onContactRequest(const ContactInfo &contact, uint32_t sender_timestamp, const uint8_t *data,
                                 uint8_t len, uint8_t *reply) {
  if (data[0] == REQ_TYPE_GET_TELEMETRY_DATA) {
    uint8_t permissions = 0;
    uint8_t cp = contact.flags >> 1; // LSB used as 'favourite' bit (so only use upper bits)

    if (_prefs.telemetry_mode_base == TELEM_MODE_ALLOW_ALL) {
      permissions = TELEM_PERM_BASE;
    } else if (_prefs.telemetry_mode_base == TELEM_MODE_ALLOW_FLAGS) {
      permissions = cp & TELEM_PERM_BASE;
    }

    if (_prefs.telemetry_mode_loc == TELEM_MODE_ALLOW_ALL) {
      permissions |= TELEM_PERM_LOCATION;
    } else if (_prefs.telemetry_mode_loc == TELEM_MODE_ALLOW_FLAGS) {
      permissions |= cp & TELEM_PERM_LOCATION;
    }

    if (_prefs.telemetry_mode_env == TELEM_MODE_ALLOW_ALL) {
      permissions |= TELEM_PERM_ENVIRONMENT;
    } else if (_prefs.telemetry_mode_env == TELEM_MODE_ALLOW_FLAGS) {
      permissions |= cp & TELEM_PERM_ENVIRONMENT;
    }

    uint8_t perm_mask =
        ~(data[1]); // NEW: first reserved byte (of 4), is now inverse mask to apply to permissions
    permissions &= perm_mask;

    if (permissions & TELEM_PERM_BASE) { // only respond if base permission bit is set
      telemetry.reset();
      telemetry.addVoltage(TELEM_CHANNEL_SELF, (float)board.getBattMilliVolts() / 1000.0f);
      // query other sensors -- target specific
      sensors.querySensors(permissions, telemetry);

      memcpy(reply, &sender_timestamp,
             4); // reflect sender_timestamp back in response packet (kind of like a 'tag')

      uint8_t tlen = telemetry.getSize();
      memcpy(&reply[4], telemetry.getBuffer(), tlen);
      return 4 + tlen;
    }
  }
  return 0; // unknown
}

void MyMesh::onContactResponse(const ContactInfo &contact, const uint8_t *data, uint8_t len) {
  uint32_t tag;
  memcpy(&tag, data, 4);

  if (pending_login && memcmp(&pending_login, contact.id.pub_key, 4) == 0) { // check for login response
    // yes, is response to pending sendLogin()
    pending_login = 0;

    int i = 0;
    if (memcmp(&data[4], "OK", 2) == 0) { // legacy Repeater login OK response
      out_frame[i++] = PUSH_CODE_LOGIN_SUCCESS;
      out_frame[i++] = 0; // legacy: is_admin = false
      memcpy(&out_frame[i], contact.id.pub_key, 6);
      i += 6;                                     // pub_key_prefix
    } else if (data[4] == RESP_SERVER_LOGIN_OK) { // new login response
      uint16_t keep_alive_secs = ((uint16_t)data[5]) * 16;
      if (keep_alive_secs > 0) {
        startConnection(contact, keep_alive_secs);
      }
      out_frame[i++] = PUSH_CODE_LOGIN_SUCCESS;
      out_frame[i++] = data[6]; // permissions (eg. is_admin)
      memcpy(&out_frame[i], contact.id.pub_key, 6);
      i += 6; // pub_key_prefix
      memcpy(&out_frame[i], &tag, 4);
      i += 4;                    // NEW: include server timestamp
      out_frame[i++] = data[7];  // NEW (v7): ACL permissions
      out_frame[i++] = data[12]; // FIRMWARE_VER_LEVEL
    } else {
      out_frame[i++] = PUSH_CODE_LOGIN_FAIL;
      out_frame[i++] = 0; // reserved
      memcpy(&out_frame[i], contact.id.pub_key, 6);
      i += 6; // pub_key_prefix
    }
    _serial->writeFrame(out_frame, i);
  } else if (len > 4 && // check for status response
             pending_status &&
             memcmp(&pending_status, contact.id.pub_key, 4) == 0 // legacy matching scheme
                                                                 // FUTURE: tag == pending_status
  ) {
    pending_status = 0;

    int i = 0;
    out_frame[i++] = PUSH_CODE_STATUS_RESPONSE;
    out_frame[i++] = 0; // reserved
    memcpy(&out_frame[i], contact.id.pub_key, 6);
    i += 6; // pub_key_prefix
    memcpy(&out_frame[i], &data[4], len - 4);
    i += (len - 4);
    _serial->writeFrame(out_frame, i);
  } else if (len > 4 && tag == pending_telemetry) { // check for matching response tag
    pending_telemetry = 0;

    int i = 0;
    out_frame[i++] = PUSH_CODE_TELEMETRY_RESPONSE;
    out_frame[i++] = 0; // reserved
    memcpy(&out_frame[i], contact.id.pub_key, 6);
    i += 6; // pub_key_prefix
    memcpy(&out_frame[i], &data[4], len - 4);
    i += (len - 4);
    _serial->writeFrame(out_frame, i);
  } else if (len > 4 && tag == pending_req) { // check for matching response tag
    pending_req = 0;

    int i = 0;
    out_frame[i++] = PUSH_CODE_BINARY_RESPONSE;
    out_frame[i++] = 0;             // reserved
    memcpy(&out_frame[i], &tag, 4); // app needs to match this to RESP_CODE_SENT.tag
    i += 4;
    memcpy(&out_frame[i], &data[4], len - 4);
    i += (len - 4);
    _serial->writeFrame(out_frame, i);
  }
}

// client only
bool MyMesh::onContactPathRecv(ContactInfo &contact, uint8_t *in_path, uint8_t in_path_len, uint8_t *out_path,
                               uint8_t out_path_len, uint8_t extra_type, uint8_t *extra, uint8_t extra_len) {
  if (extra_type == PAYLOAD_TYPE_RESPONSE && extra_len > 4) {
    MESH_DEBUG_PRINTLN("onContactPathRecv(): PAYLOAD_TYPE_RESPONSE received, extra_len=%d", extra_len);
    uint32_t tag;
    memcpy(&tag, extra, 4);

    if (tag == pending_discovery) { // check for matching response tag)
      pending_discovery = 0;

      if (!mesh::Packet::isValidPathLen(in_path_len) || !mesh::Packet::isValidPathLen(out_path_len)) {
        MESH_DEBUG_PRINTLN("onContactPathRecv, invalid path sizes: %d, %d", in_path_len, out_path_len);
      } else {
        int i = 0;
        out_frame[i++] = PUSH_CODE_PATH_DISCOVERY_RESPONSE;
        out_frame[i++] = 0; // reserved
        memcpy(&out_frame[i], contact.id.pub_key, 6);
        i += 6; // pub_key_prefix
        out_frame[i++] = out_path_len;
        i += mesh::Packet::writePath(&out_frame[i], out_path, out_path_len);
        out_frame[i++] = in_path_len;
        i += mesh::Packet::writePath(&out_frame[i], in_path, in_path_len);
        // NOTE: telemetry data in 'extra' is discarded at present

        _serial->writeFrame(out_frame, i);
      }
      return false; // DON'T send reciprocal path!
    }
  }
  // from BaseChatMesh::onContactPathRecv

  contact.out_path_len = mesh::Packet::copyPath(contact.out_path, out_path, out_path_len);
  contact.lastmod = getRTCClock()->getCurrentTime();
  onContactPathUpdated(contact);

  if (extra_type == PAYLOAD_TYPE_ACK && extra_len >= 4) {
    if (processAck(extra) != NULL) {
      txt_send_timeout = 0;
    }
  } else if (extra_type == PAYLOAD_TYPE_RESPONSE && extra_len > 0) {
    onContactResponse(contact, extra, extra_len);
  }
  return true;
}

void MyMesh::onControlDataRecv(mesh::Packet *packet) {
  if (packet->payload_len + 4 > sizeof(out_frame)) {
    MESH_DEBUG_PRINTLN("onControlDataRecv(), payload_len too long: %d", packet->payload_len);
    return;
  }
  int i = 0;
  out_frame[i++] = PUSH_CODE_CONTROL_DATA;
  out_frame[i++] = (int8_t)(_radio->getLastSNR() * 4);
  out_frame[i++] = (int8_t)(_radio->getLastRSSI());
  out_frame[i++] = packet->path_len;
  memcpy(&out_frame[i], packet->payload, packet->payload_len);
  i += packet->payload_len;

  if (_serial->isConnected()) {
    _serial->writeFrame(out_frame, i);
  } else {
    MESH_DEBUG_PRINTLN("onControlDataRecv(), data received while app offline");
  }
}

void MyMesh::onRawDataRecv(mesh::Packet *packet) {
  if (packet->payload_len + 4 > sizeof(out_frame)) {
    MESH_DEBUG_PRINTLN("onRawDataRecv(), payload_len too long: %d", packet->payload_len);
    return;
  }
  int i = 0;
  out_frame[i++] = PUSH_CODE_RAW_DATA;
  out_frame[i++] = (int8_t)(_radio->getLastSNR() * 4);
  out_frame[i++] = (int8_t)(_radio->getLastRSSI());
  out_frame[i++] = 0xFF; // reserved (possibly path_len in future)
  memcpy(&out_frame[i], packet->payload, packet->payload_len);
  i += packet->payload_len;

  if (_serial->isConnected()) {
    _serial->writeFrame(out_frame, i);
  } else {
    MESH_DEBUG_PRINTLN("onRawDataRecv(), data received while app offline");
  }
}

void MyMesh::onTraceRecv(mesh::Packet *packet, uint32_t tag, uint32_t auth_code, uint8_t flags,
                         const uint8_t *path_snrs, const uint8_t *path_hashes, uint8_t path_len) {
  uint8_t path_sz = flags & 0x03; // NEW v1.11+
  if (12 + path_len + (path_len >> path_sz) + 1 > sizeof(out_frame)) {
    MESH_DEBUG_PRINTLN("onTraceRecv(), path_len is too long: %d", (uint32_t)path_len);
    return;
  }
  int i = 0;
  out_frame[i++] = PUSH_CODE_TRACE_DATA;
  out_frame[i++] = 0; // reserved
  out_frame[i++] = path_len;
  out_frame[i++] = flags;
  memcpy(&out_frame[i], &tag, 4);
  i += 4;
  memcpy(&out_frame[i], &auth_code, 4);
  i += 4;
  memcpy(&out_frame[i], path_hashes, path_len);
  i += path_len;

  memcpy(&out_frame[i], path_snrs, path_len >> path_sz);
  i += path_len >> path_sz;
  out_frame[i++] = (int8_t)(packet->getSNR() * 4); // extra/final SNR (to this node)

  if (_serial->isConnected()) {
    _serial->writeFrame(out_frame, i);
  } else {
    MESH_DEBUG_PRINTLN("onTraceRecv(), data received while app offline");
  }
}

uint32_t MyMesh::calcFloodTimeoutMillisFor(uint32_t pkt_airtime_millis) const {
  return SEND_TIMEOUT_BASE_MILLIS + (FLOOD_SEND_TIMEOUT_FACTOR * pkt_airtime_millis);
}
uint32_t MyMesh::calcDirectTimeoutMillisFor(uint32_t pkt_airtime_millis, uint8_t path_len) const {
  uint8_t path_hash_count = path_len & 63;
  return SEND_TIMEOUT_BASE_MILLIS +
         ((pkt_airtime_millis * DIRECT_SEND_PERHOP_FACTOR + DIRECT_SEND_PERHOP_EXTRA_MILLIS) *
          (path_hash_count + 1));
}

void MyMesh::onSendTimeout() {}

MyMesh::MyMesh(mesh::Radio &radio, mesh::RNG &rng, mesh::RTCClock &rtc, SimpleMeshTables &tables,
               DataStore &store, AbstractUITask *ui)
    : mesh::Mesh(radio, *new ArduinoMillis(), rng, rtc, *new LocalAwarePacketManager(16, this), tables),
      _store(&store), _serial(NULL), _ui(ui), telemetry(MAX_PACKET_PAYLOAD - 4), _sensor_prefs(), _acl(),
      _callbacks(*this, &_sensor_prefs), _cli(board, rtc, sensors, _acl, &_sensor_prefs, &_callbacks) {
  num_contacts = 0;
  txt_send_timeout = 0;
  loopback_queue_head = 0;
  loopback_queue_len = 0;
  memset(loopback_queue, 0, sizeof(loopback_queue));
  memset(connections, 0, sizeof(connections));
#ifdef MAX_GROUP_CHANNELS
  memset(channels, 0, sizeof(channels));
  num_channels = 0;
#endif
  num_local_identities = 0;
  sensor_identity_idx = 0;

  _iter_started = false;
  _cli_rescue = false;
  offline_queue_len = 0;
  app_target_ver = 0;
  clearPendingReqs();
  next_ack_idx = 0;
  sign_data = NULL;
  dirty_contacts_expiry = 0;
  memset(advert_paths, 0, sizeof(advert_paths));
  memset(send_scope.key, 0, sizeof(send_scope.key));

  // defaults
  memset(&_prefs, 0, sizeof(_prefs));
  _prefs.airtime_factor = 1.0;
  strcpy(_prefs.node_name, "NONAME");
  _prefs.freq = LORA_FREQ;
  _prefs.sf = LORA_SF;
  _prefs.bw = LORA_BW;
  _prefs.cr = LORA_CR;
  _prefs.tx_power_dbm = LORA_TX_POWER;
  _prefs.gps_enabled = 0;  // GPS disabled by default
  _prefs.gps_interval = 0; // No automatic GPS updates by default
  //_prefs.rx_delay_base = 10.0f;  enable once new algo fixed
#if defined(USE_SX1262) || defined(USE_SX1268)
#ifdef SX126X_RX_BOOSTED_GAIN
  _prefs.rx_boosted_gain = SX126X_RX_BOOSTED_GAIN;
#else
  _prefs.rx_boosted_gain = 1; // enabled by default
#endif
#endif

  // remote defaults
  memset(&_sensor_prefs, 0, sizeof(_sensor_prefs));
  _sensor_prefs.airtime_factor = 1.0;
  _sensor_prefs.rx_delay_base = 0.0f;          // turn off by default, was 10.0;
  _sensor_prefs.tx_delay_factor = 0.5f;        // was 0.25f
  _sensor_prefs.direct_tx_delay_factor = 0.2f; // was zero
  StrHelper::strncpy(_sensor_prefs.node_name, ADVERT_NAME, sizeof(_sensor_prefs.node_name));
  _sensor_prefs.node_lat = ADVERT_LAT;
  _sensor_prefs.node_lon = ADVERT_LON;
  StrHelper::strncpy(_sensor_prefs.password, ADMIN_PASSWORD, sizeof(_sensor_prefs.password));
  _sensor_prefs.freq = LORA_FREQ;
  _sensor_prefs.sf = LORA_SF;
  _sensor_prefs.bw = LORA_BW;
  _sensor_prefs.cr = LORA_CR;
  _sensor_prefs.tx_power_dbm = LORA_TX_POWER;
  _sensor_prefs.advert_interval = 1;       // default to 2 minutes for NEW installs
  _sensor_prefs.flood_advert_interval = 0; // disabled
  _sensor_prefs.disable_fwd = true;
  _sensor_prefs.flood_max = 64;
  _sensor_prefs.interference_threshold = 0; // disabled

  // GPS defaults
  _sensor_prefs.gps_enabled = 0;
  _sensor_prefs.gps_interval = 0;
  _sensor_prefs.advert_loc_policy = ADVERT_LOC_PREFS;
}

void MyMesh::begin(bool has_display) {
  mesh::Mesh::begin();

  mesh::LocalIdentity main_identity;
  if (!_store->loadMainIdentity(main_identity)) {
    main_identity = radio_new_identity(); // create new random identity
    int count = 0;
    while (count < 10 &&
           (main_identity.pub_key[0] == 0x00 || main_identity.pub_key[0] == 0xFF)) { // reserved id hashes
      main_identity = radio_new_identity();
      count++;
    }
    _store->saveMainIdentity(main_identity);
  }
  addLocalIdentity(main_identity, ADV_TYPE_CHAT, PREFS_KIND_MAIN);

  mesh::LocalIdentity sensor_identity;
  if (!_store->loadSensorIdentity(sensor_identity)) {
    sensor_identity = radio_new_identity(); // create new random identity for 'sensor' role
    int count = 0;
    while (count < 10 &&
           (sensor_identity.pub_key[0] == 0x00 || sensor_identity.pub_key[0] == 0xFF)) { // reserved id hashes
      sensor_identity = radio_new_identity();
      count++;
    }
    _store->saveSensorIdentity(sensor_identity);
  }
  int added_sensor_idx = addLocalIdentity(sensor_identity, ADV_TYPE_SENSOR, PREFS_KIND_SENSOR);
  sensor_identity_idx = added_sensor_idx >= 0 ? (uint8_t)added_sensor_idx : 0;

// if name is provided as a build flag, use that as default node name instead
#ifdef ADVERT_NAME
  strcpy(_prefs.node_name, ADVERT_NAME);
#else
  // use hex of first 4 bytes of identity public key as default node name
  char pub_key_hex[10];
  mesh::Utils::toHex(pub_key_hex, getMainIdentity().pub_key, 4);
  strcpy(_prefs.node_name, pub_key_hex);
#endif

// if name is provided as a build flag, use that as default node name instead
#ifdef SENSOR_ADVERT_NAME
  strcpy(_sensor_prefs.node_name, SENSOR_ADVERT_NAME);
#else
  // use hex of first 4 bytes of identity public key as default node name
  char sensor_pub_key_hex[10];
  mesh::Utils::toHex(sensor_pub_key_hex, getSensorIdentity().pub_key, 4);
  strcpy(_sensor_prefs.node_name, sensor_pub_key_hex);
#endif

  // load persisted prefs
  _store->loadPrefs(_prefs, _cli, sensors.node_lat, sensors.node_lon);

  // sanitise bad pref values
  _prefs.rx_delay_base = constrain(_prefs.rx_delay_base, 0, 20.0f);
  _prefs.airtime_factor = constrain(_prefs.airtime_factor, 0, 9.0f);
  _prefs.freq = constrain(_prefs.freq, 400.0f, 2500.0f);
  _prefs.bw = constrain(_prefs.bw, 7.8f, 500.0f);
  _prefs.sf = constrain(_prefs.sf, 5, 12);
  _prefs.cr = constrain(_prefs.cr, 5, 8);
  _prefs.tx_power_dbm = constrain(_prefs.tx_power_dbm, -9, MAX_LORA_TX_POWER);
  _prefs.gps_enabled = constrain(_prefs.gps_enabled, 0, 1);       // Ensure boolean 0 or 1
  _prefs.gps_interval = constrain(_prefs.gps_interval, 0, 86400); // Max 24 hours

#ifdef BLE_PIN_CODE // 123456 by default
  if (_prefs.ble_pin == 0) {
#ifdef DISPLAY_CLASS
    if (has_display && BLE_PIN_CODE == 123456) {
      StdRNG rng;
      _active_ble_pin = rng.nextInt(100000, 999999); // random pin each session
    } else {
      _active_ble_pin = BLE_PIN_CODE; // otherwise static pin
    }
#else
    _active_ble_pin = BLE_PIN_CODE; // otherwise static pin
#endif
  } else {
    _active_ble_pin = _prefs.ble_pin;
  }
#else
  _active_ble_pin = 0;
#endif

  resetContacts();
  _store->loadContacts(this);
  bootstrapRTCfromContacts();
  addChannel("Public", PUBLIC_GROUP_PSK); // pre-configure Andy's public channel
  _store->loadChannels(this);

  radio_set_params(_prefs.freq, _prefs.bw, _prefs.sf, _prefs.cr);
  radio_set_tx_power(_prefs.tx_power_dbm);
  radio_driver.setRxBoostedGainMode(_prefs.rx_boosted_gain);
  MESH_DEBUG_PRINTLN("RX Boosted Gain Mode: %s",
                     radio_driver.getRxBoostedGainMode() ? "Enabled" : "Disabled");

  // send sensor advert to loopback for client
  sendSensorAdvertToSelf();

  _acl.load(_store->getPrimaryFS(), local_identities[0].id);
}

void MyMesh::sendSensorAdvertToSelf() {
  MESH_DEBUG_PRINTLN("MyMesh::sendSensorAdvertToSelf(); start");
  mesh::Packet *pkt;
  if (_sensor_prefs.advert_loc_policy == ADVERT_LOC_NONE) {
    pkt = createSensorAdvert(_sensor_prefs.node_name);
  } else {
    pkt = createSensorAdvert(_sensor_prefs.node_name, sensors.node_lat, sensors.node_lon);
  }
  onRecvPacket(pkt);
  releasePacket(pkt);
  MESH_DEBUG_PRINTLN("MyMesh::sendSensorAdvertToSelf(); end");
}

const char *MyMesh::getNodeName() {
  return _prefs.node_name;
}
ClientNodePrefs *MyMesh::getNodePrefs() {
  return &_prefs;
}
uint32_t MyMesh::getBLEPin() {
  return _active_ble_pin;
}

struct FreqRange {
  uint32_t lower_freq, upper_freq;
};

static FreqRange repeat_freq_ranges[] = { { 433000, 433000 }, { 869000, 869000 }, { 918000, 918000 } };

bool MyMesh::isValidClientRepeatFreq(uint32_t f) const {
  for (int i = 0; i < sizeof(repeat_freq_ranges) / sizeof(repeat_freq_ranges[0]); i++) {
    auto r = &repeat_freq_ranges[i];
    if (f >= r->lower_freq && f <= r->upper_freq) return true;
  }
  return false;
}

void MyMesh::startInterface(BaseSerialInterface &serial) {
  _serial = &serial;
  serial.enable();
}

void MyMesh::handleCmdFrame(size_t len) {
  if (cmd_frame[0] == CMD_DEVICE_QEURY && len >= 2) { // sent when app establishes connection
    app_target_ver = cmd_frame[1];                    // which version of protocol does app understand

    int i = 0;
    out_frame[i++] = RESP_CODE_DEVICE_INFO;
    out_frame[i++] = FIRMWARE_VER_CODE;
    out_frame[i++] = MAX_CONTACTS / 2;   // v3+
    out_frame[i++] = MAX_GROUP_CHANNELS; // v3+
    memcpy(&out_frame[i], &_prefs.ble_pin, 4);
    i += 4;
    memset(&out_frame[i], 0, 12);
    strcpy((char *)&out_frame[i], FIRMWARE_BUILD_DATE);
    i += 12;
    StrHelper::strzcpy((char *)&out_frame[i], board.getManufacturerName(), 40);
    i += 40;
    StrHelper::strzcpy((char *)&out_frame[i], FIRMWARE_VERSION, 20);
    i += 20;
    out_frame[i++] = _prefs.client_repeat;  // v9+
    out_frame[i++] = _prefs.path_hash_mode; // v10+
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_APP_START &&
             len >= 8) { // sent when app establishes connection, respond with node ID
    //  cmd_frame[1..7]  reserved future
    char *app_name = (char *)&cmd_frame[8];
    cmd_frame[len] = 0; // make app_name null terminated
    MESH_DEBUG_PRINTLN("App %s connected", app_name);

    _iter_started = false; // stop any left-over ContactsIterator
    int i = 0;
    out_frame[i++] = RESP_CODE_SELF_INFO;
    out_frame[i++] = ADV_TYPE_CHAT; // what this node Advert identifies as (maybe node's pronouns too?? :-)
    out_frame[i++] = _prefs.tx_power_dbm;
    out_frame[i++] = MAX_LORA_TX_POWER;
    memcpy(&out_frame[i], getMainIdentity().pub_key, PUB_KEY_SIZE);
    i += PUB_KEY_SIZE;

    int32_t lat, lon;
    lat = (sensors.node_lat * 1000000.0);
    lon = (sensors.node_lon * 1000000.0);
    memcpy(&out_frame[i], &lat, 4);
    i += 4;
    memcpy(&out_frame[i], &lon, 4);
    i += 4;
    out_frame[i++] = _prefs.multi_acks; // new v7+
    out_frame[i++] = _prefs.advert_loc_policy;
    out_frame[i++] = (_prefs.telemetry_mode_env << 4) | (_prefs.telemetry_mode_loc << 2) |
                     (_prefs.telemetry_mode_base); // v5+
    out_frame[i++] = _prefs.manual_add_contacts;

    uint32_t freq = _prefs.freq * 1000;
    memcpy(&out_frame[i], &freq, 4);
    i += 4;
    uint32_t bw = _prefs.bw * 1000;
    memcpy(&out_frame[i], &bw, 4);
    i += 4;
    out_frame[i++] = _prefs.sf;
    out_frame[i++] = _prefs.cr;

    int tlen = strlen(_prefs.node_name); // revisit: UTF_8 ??
    memcpy(&out_frame[i], _prefs.node_name, tlen);
    i += tlen;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_SEND_TXT_MSG && len >= 14) {
    int i = 1;
    uint8_t txt_type = cmd_frame[i++];
    uint8_t attempt = cmd_frame[i++];
    uint32_t msg_timestamp;
    memcpy(&msg_timestamp, &cmd_frame[i], 4);
    i += 4;
    uint8_t *pub_key_prefix = &cmd_frame[i];
    i += 6;
    ContactInfo *recipient = lookupContactByPubKey(pub_key_prefix, 6);
    if (recipient && (txt_type == TXT_TYPE_PLAIN || txt_type == TXT_TYPE_CLI_DATA)) {
      char *text = (char *)&cmd_frame[i];
      int tlen = len - i;
      uint32_t est_timeout;
      text[tlen] = 0; // ensure null
      int result;
      uint32_t expected_ack;
      if (txt_type == TXT_TYPE_CLI_DATA) {
        msg_timestamp = getRTCClock()->getCurrentTimeUnique(); // Use node's RTC instead of app timestamp to
                                                               // avoid tripping replay protection
        result = sendCommandData(*recipient, msg_timestamp, attempt, text, est_timeout);
        expected_ack = 0; // no Ack expected
      } else {
        result = sendMessage(*recipient, msg_timestamp, attempt, text, expected_ack, est_timeout);
      }
      // TODO: add expected ACK to table
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        if (expected_ack) {
          expected_ack_table[next_ack_idx].msg_sent = _ms->getMillis(); // add to circular table
          expected_ack_table[next_ack_idx].ack = expected_ack;
          expected_ack_table[next_ack_idx].contact = recipient;
          next_ack_idx = (next_ack_idx + 1) % EXPECTED_ACK_TABLE_SIZE;
        }

        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &expected_ack, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(recipient == NULL
                        ? ERR_CODE_NOT_FOUND
                        : ERR_CODE_UNSUPPORTED_CMD); // unknown recipient, or unsuported TXT_TYPE_*
    }
  } else if (cmd_frame[0] == CMD_SEND_CHANNEL_TXT_MSG) { // send GroupChannel msg
    int i = 1;
    uint8_t txt_type = cmd_frame[i++]; // should be TXT_TYPE_PLAIN
    uint8_t channel_idx = cmd_frame[i++];
    uint32_t msg_timestamp;
    memcpy(&msg_timestamp, &cmd_frame[i], 4);
    i += 4;
    const char *text = (char *)&cmd_frame[i];

    if (txt_type != TXT_TYPE_PLAIN) {
      writeErrFrame(ERR_CODE_UNSUPPORTED_CMD);
    } else {
      ChannelDetails channel;
      bool success = getChannel(channel_idx, channel);
      if (success && sendGroupMessage(msg_timestamp, channel.channel, _prefs.node_name, text, len - i)) {
        writeOKFrame();
      } else {
        writeErrFrame(ERR_CODE_NOT_FOUND); // bad channel_idx
      }
    }
  } else if (cmd_frame[0] == CMD_GET_CONTACTS) { // get Contact list
    if (_iter_started) {
      writeErrFrame(ERR_CODE_BAD_STATE); // iterator is currently busy
    } else {
      if (len >= 5) { // has optional 'since' param
        memcpy(&_iter_filter_since, &cmd_frame[1], 4);
      } else {
        _iter_filter_since = 0;
      }

      uint8_t reply[5];
      reply[0] = RESP_CODE_CONTACTS_START;
      uint32_t count = getNumContacts(); // total, NOT filtered count
      memcpy(&reply[1], &count, 4);
      _serial->writeFrame(reply, 5);

      // start iterator
      _iter = startContactsIterator();
      _iter_started = true;
      _most_recent_lastmod = 0;
    }
  } else if (cmd_frame[0] == CMD_SET_ADVERT_NAME && len >= 2) {
    int nlen = len - 1;
    if (nlen > sizeof(_prefs.node_name) - 1) nlen = sizeof(_prefs.node_name) - 1; // max len
    memcpy(_prefs.node_name, &cmd_frame[1], nlen);
    _prefs.node_name[nlen] = 0; // null terminator
    savePrefs();
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_SET_ADVERT_LATLON && len >= 9) {
    int32_t lat, lon, alt = 0;
    memcpy(&lat, &cmd_frame[1], 4);
    memcpy(&lon, &cmd_frame[5], 4);
    if (len >= 13) {
      memcpy(&alt, &cmd_frame[9], 4); // for FUTURE support
    }
    if (lat <= 90 * 1E6 && lat >= -90 * 1E6 && lon <= 180 * 1E6 && lon >= -180 * 1E6) {
      sensors.node_lat = ((double)lat) / 1000000.0;
      sensors.node_lon = ((double)lon) / 1000000.0;
      savePrefs();
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG); // invalid geo coordinate
    }
  } else if (cmd_frame[0] == CMD_GET_DEVICE_TIME) {
    uint8_t reply[5];
    reply[0] = RESP_CODE_CURR_TIME;
    uint32_t now = getRTCClock()->getCurrentTime();
    memcpy(&reply[1], &now, 4);
    _serial->writeFrame(reply, 5);
  } else if (cmd_frame[0] == CMD_SET_DEVICE_TIME && len >= 5) {
    uint32_t secs;
    memcpy(&secs, &cmd_frame[1], 4);
    uint32_t curr = getRTCClock()->getCurrentTime();
    if (secs >= curr) {
      getRTCClock()->setCurrentTime(secs);
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    }
  } else if (cmd_frame[0] == CMD_SEND_SELF_ADVERT) {
    mesh::Packet *pkt;
    if (_prefs.advert_loc_policy == ADVERT_LOC_NONE) {
      pkt = createSelfAdvert(0, _prefs.node_name);
    } else {
      pkt = createSelfAdvert(0, _prefs.node_name, sensors.node_lat, sensors.node_lon);
    }
    if (pkt) {
      if (len >= 2 && cmd_frame[1] == 1) { // optional param (1 = flood, 0 = zero hop)
        unsigned long delay_millis = 0;
        sendFlood(pkt, delay_millis, _prefs.path_hash_mode + 1);
      } else {
        sendZeroHop(pkt);
      }
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    }
  } else if (cmd_frame[0] == CMD_RESET_PATH && len >= 1 + 32) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient) {
      recipient->out_path_len = OUT_PATH_UNKNOWN;
      // recipient->lastmod = ??   shouldn't be needed, app already has this version of contact
      dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // unknown contact
    }
  } else if (cmd_frame[0] == CMD_ADD_UPDATE_CONTACT && len >= 1 + 32 + 2 + 1) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    uint32_t last_mod = getRTCClock()->getCurrentTime(); // fallback value if not present in cmd_frame
    if (recipient) {
      updateContactFromFrame(*recipient, last_mod, cmd_frame, len);
      recipient->lastmod = last_mod;
      dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
      writeOKFrame();
    } else {
      ContactInfo contact;
      updateContactFromFrame(contact, last_mod, cmd_frame, len);
      contact.lastmod = last_mod;
      contact.sync_since = 0;
      if (addContact(contact)) {
        dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
        writeOKFrame();
      } else {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      }
    }
  } else if (cmd_frame[0] == CMD_REMOVE_CONTACT) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient && removeContact(*recipient)) {
      _store->deleteBlobByKey(pub_key, PUB_KEY_SIZE);
      dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // not found, or unable to remove
    }
  } else if (cmd_frame[0] == CMD_SHARE_CONTACT) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient) {
      if (shareContactZeroHop(*recipient)) {
        writeOKFrame();
      } else {
        writeErrFrame(ERR_CODE_TABLE_FULL); // unable to send
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND);
    }
  } else if (cmd_frame[0] == CMD_GET_CONTACT_BY_KEY) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *contact = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    MESH_DEBUG_PRINTLN("MyMesh::handleCmdFrame: CMD_GET_CONTACT_BY_KEY contact %s",
                       contact ? contact->name : "unknown");
    if (contact) {
      writeContactRespFrame(RESP_CODE_CONTACT, *contact);
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // not found
    }
  } else if (cmd_frame[0] == CMD_EXPORT_CONTACT) {
    if (len < 1 + PUB_KEY_SIZE) {
      // export SELF
      mesh::Packet *pkt;
      if (_prefs.advert_loc_policy == ADVERT_LOC_NONE) {
        pkt = createSelfAdvert(0, _prefs.node_name);
      } else {
        pkt = createSelfAdvert(0, _prefs.node_name, sensors.node_lat, sensors.node_lon);
      }
      if (pkt) {
        pkt->header |= ROUTE_TYPE_FLOOD; // would normally be sent in this mode

        out_frame[0] = RESP_CODE_EXPORT_CONTACT;
        uint8_t out_len = pkt->writeTo(&out_frame[1]);
        releasePacket(pkt); // undo the obtainNewPacket()
        _serial->writeFrame(out_frame, out_len + 1);
      } else {
        writeErrFrame(ERR_CODE_TABLE_FULL); // Error
      }
    } else {
      uint8_t *pub_key = &cmd_frame[1];
      ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
      uint8_t out_len;
      if (recipient && (out_len = exportContact(*recipient, &out_frame[1])) > 0) {
        out_frame[0] = RESP_CODE_EXPORT_CONTACT;
        _serial->writeFrame(out_frame, out_len + 1);
      } else {
        writeErrFrame(ERR_CODE_NOT_FOUND); // not found
      }
    }
  } else if (cmd_frame[0] == CMD_IMPORT_CONTACT && len > 2 + 32 + 64) {
    if (importContact(&cmd_frame[1], len - 1)) {
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    }
  } else if (cmd_frame[0] == CMD_SYNC_NEXT_MESSAGE) {
    int out_len;
    if ((out_len = getFromOfflineQueue(out_frame)) > 0) {
      _serial->writeFrame(out_frame, out_len);
#ifdef DISPLAY_CLASS
      if (_ui) _ui->msgRead(offline_queue_len);
#endif
    } else {
      out_frame[0] = RESP_CODE_NO_MORE_MESSAGES;
      _serial->writeFrame(out_frame, 1);
    }
  } else if (cmd_frame[0] == CMD_SET_RADIO_PARAMS) {
    int i = 1;
    uint32_t freq;
    memcpy(&freq, &cmd_frame[i], 4);
    i += 4;
    uint32_t bw;
    memcpy(&bw, &cmd_frame[i], 4);
    i += 4;
    uint8_t sf = cmd_frame[i++];
    uint8_t cr = cmd_frame[i++];
    uint8_t repeat = 0; // default - false
    if (len > i) {
      repeat = cmd_frame[i++]; // FIRMWARE_VER_CODE  9+
    }

    if (repeat && !isValidClientRepeatFreq(freq)) {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    } else if (freq >= 300000 && freq <= 2500000 && sf >= 5 && sf <= 12 && cr >= 5 && cr <= 8 && bw >= 7000 &&
               bw <= 500000) {
      _prefs.sf = sf;
      _prefs.cr = cr;
      _prefs.freq = (float)freq / 1000.0;
      _prefs.bw = (float)bw / 1000.0;
      _prefs.client_repeat = repeat;
      savePrefs();

      radio_set_params(_prefs.freq, _prefs.bw, _prefs.sf, _prefs.cr);
      MESH_DEBUG_PRINTLN("OK: CMD_SET_RADIO_PARAMS: f=%d, bw=%d, sf=%d, cr=%d", freq, bw, (uint32_t)sf,
                         (uint32_t)cr);

      writeOKFrame();
    } else {
      MESH_DEBUG_PRINTLN("Error: CMD_SET_RADIO_PARAMS: f=%d, bw=%d, sf=%d, cr=%d", freq, bw, (uint32_t)sf,
                         (uint32_t)cr);
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    }
  } else if (cmd_frame[0] == CMD_SET_RADIO_TX_POWER) {
    int8_t power = (int8_t)cmd_frame[1];
    if (power < -9 || power > MAX_LORA_TX_POWER) {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    } else {
      _prefs.tx_power_dbm = power;
      savePrefs();
      radio_set_tx_power(_prefs.tx_power_dbm);
      writeOKFrame();
    }
  } else if (cmd_frame[0] == CMD_SET_TUNING_PARAMS) {
    int i = 1;
    uint32_t rx, af;
    memcpy(&rx, &cmd_frame[i], 4);
    i += 4;
    memcpy(&af, &cmd_frame[i], 4);
    i += 4;
    _prefs.rx_delay_base = ((float)rx) / 1000.0f;
    _prefs.airtime_factor = ((float)af) / 1000.0f;
    savePrefs();
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_GET_TUNING_PARAMS) {
    uint32_t rx = _prefs.rx_delay_base * 1000, af = _prefs.airtime_factor * 1000;
    int i = 0;
    out_frame[i++] = RESP_CODE_TUNING_PARAMS;
    memcpy(&out_frame[i], &rx, 4);
    i += 4;
    memcpy(&out_frame[i], &af, 4);
    i += 4;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_SET_OTHER_PARAMS) {
    _prefs.manual_add_contacts = cmd_frame[1];
    if (len >= 3) {
      _prefs.telemetry_mode_base = cmd_frame[2] & 0x03; // v5+
      _prefs.telemetry_mode_loc = (cmd_frame[2] >> 2) & 0x03;
      _prefs.telemetry_mode_env = (cmd_frame[2] >> 4) & 0x03;

      if (len >= 4) {
        _prefs.advert_loc_policy = cmd_frame[3];
        if (len >= 5) {
          _prefs.multi_acks = cmd_frame[4];
        }
      }
    }
    savePrefs();
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_SET_PATH_HASH_MODE && cmd_frame[1] == 0 && len >= 3) {
    if (cmd_frame[2] >= 3) {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    } else {
      _prefs.path_hash_mode = cmd_frame[2];
      savePrefs();
      writeOKFrame();
    }
  } else if (cmd_frame[0] == CMD_REBOOT && memcmp(&cmd_frame[1], "reboot", 6) == 0) {
    if (dirty_contacts_expiry) { // is there are pending dirty contacts write needed?
      saveContacts();
    }
    board.reboot();
  } else if (cmd_frame[0] == CMD_GET_BATT_AND_STORAGE) {
    uint8_t reply[11];
    int i = 0;
    reply[i++] = RESP_CODE_BATT_AND_STORAGE;
    uint16_t battery_millivolts = board.getBattMilliVolts();
    uint32_t used = _store->getStorageUsedKb();
    uint32_t total = _store->getStorageTotalKb();
    memcpy(&reply[i], &battery_millivolts, 2);
    i += 2;
    memcpy(&reply[i], &used, 4);
    i += 4;
    memcpy(&reply[i], &total, 4);
    i += 4;
    _serial->writeFrame(reply, i);
  } else if (cmd_frame[0] == CMD_EXPORT_PRIVATE_KEY) {
#if ENABLE_PRIVATE_KEY_EXPORT
    uint8_t reply[65];
    reply[0] = RESP_CODE_PRIVATE_KEY;
    getMainIdentity().writeTo(&reply[1], 64);
    _serial->writeFrame(reply, 65);
#else
    writeDisabledFrame();
#endif
  } else if (cmd_frame[0] == CMD_IMPORT_PRIVATE_KEY && len >= 65) {
#if ENABLE_PRIVATE_KEY_IMPORT
    if (!mesh::LocalIdentity::validatePrivateKey(&cmd_frame[1])) {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG); // invalid key
    } else {
      mesh::LocalIdentity identity;
      identity.readFrom(&cmd_frame[1], 64);
      if (_store->saveMainIdentity(identity)) {
        local_identities[0].id = identity;
        self_id = identity;
        writeOKFrame();
        // re-load contacts, to invalidate ecdh shared_secrets
        resetContacts();
        _store->loadContacts(this);
      } else {
        writeErrFrame(ERR_CODE_FILE_IO_ERROR);
      }
    }
#else
    writeDisabledFrame();
#endif
  } else if (cmd_frame[0] == CMD_SEND_RAW_DATA && len >= 6) {
    int i = 1;
    int8_t path_len = cmd_frame[i++];
    if (path_len >= 0 && i + path_len + 4 <= len) { // minimum 4 byte payload
      uint8_t *path = &cmd_frame[i];
      i += path_len;
      auto pkt = createRawData(&cmd_frame[i], len - i);
      if (pkt) {
        sendDirect(pkt, path, path_len);
        writeOKFrame();
      } else {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      }
    } else {
      writeErrFrame(ERR_CODE_UNSUPPORTED_CMD); // flood, not supported (yet)
    }
  } else if (cmd_frame[0] == CMD_SEND_LOGIN && len >= 1 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    char *password = (char *)&cmd_frame[1 + PUB_KEY_SIZE];
    cmd_frame[len] = 0; // ensure null terminator in password
    MESH_DEBUG_PRINTLN("SEND_LOGIN: recipient=%s, password=%s", recipient ? recipient->name : "unknown",
                       password);
    if (recipient) {
      uint32_t est_timeout;
      int result = sendLogin(*recipient, password, est_timeout);
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        clearPendingReqs();
        memcpy(&pending_login, recipient->id.pub_key, 4); // match this to onContactResponse()
        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &pending_login, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // contact not found
    }
  } else if (cmd_frame[0] == CMD_SEND_ANON_REQ && len > 1 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    uint8_t *data = &cmd_frame[1 + PUB_KEY_SIZE];
    if (recipient) {
      uint32_t tag, est_timeout;
      int result = sendAnonReq(*recipient, data, len - (1 + PUB_KEY_SIZE), tag, est_timeout);
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        clearPendingReqs();
        pending_req = tag; // match this to onContactResponse()
        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &tag, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // contact not found
    }
  } else if (cmd_frame[0] == CMD_SEND_STATUS_REQ && len >= 1 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient) {
      uint32_t tag, est_timeout;
      int result = sendRequest(*recipient, REQ_TYPE_GET_STATUS, tag, est_timeout);
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        clearPendingReqs();
        // FUTURE:  pending_status = tag;  // match this in onContactResponse()
        memcpy(&pending_status, recipient->id.pub_key, 4); // legacy matching scheme
        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &tag, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // contact not found
    }
  } else if (cmd_frame[0] == CMD_SEND_PATH_DISCOVERY_REQ && cmd_frame[1] == 0 && len >= 2 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[2];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient) {
      MESH_DEBUG_PRINTLN("MyMesh::handleCmdFrame: CMD_SEND_PATH_DISCOVERY_REQ for contact %s",
                         recipient->name);
      uint32_t tag, est_timeout;
      // 'Path Discovery' is just a special case of flood + Telemetry req
      uint8_t req_data[9];
      req_data[0] = REQ_TYPE_GET_TELEMETRY_DATA;
      req_data[1] = ~(TELEM_PERM_BASE);    // NEW: inverse permissions mask (ie. we only want BASE telemetry)
      memset(&req_data[2], 0, 3);          // reserved
      getRNG()->random(&req_data[5], 4);   // random blob to help make packet-hash unique
      auto save = recipient->out_path_len; // temporarily force sendRequest() to flood
      recipient->out_path_len = OUT_PATH_UNKNOWN;
      int result = sendRequest(*recipient, req_data, sizeof(req_data), tag, est_timeout);
      recipient->out_path_len = save;
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        clearPendingReqs();
        pending_discovery = tag; // match this in onContactResponse()
        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &tag, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // contact not found
    }
  } else if (cmd_frame[0] == CMD_SEND_TELEMETRY_REQ &&
             len >= 4 + PUB_KEY_SIZE) { // can deprecate, in favour of CMD_SEND_BINARY_REQ
    uint8_t *pub_key = &cmd_frame[4];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient) {
      uint32_t tag, est_timeout;
      int result = sendRequest(*recipient, REQ_TYPE_GET_TELEMETRY_DATA, tag, est_timeout);
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        clearPendingReqs();
        pending_telemetry = tag; // match this in onContactResponse()
        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &tag, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // contact not found
    }
  } else if (cmd_frame[0] == CMD_SEND_TELEMETRY_REQ && len == 4) { // 'self' telemetry request
    telemetry.reset();
    telemetry.addVoltage(TELEM_CHANNEL_SELF, (float)board.getBattMilliVolts() / 1000.0f);
    // query other sensors -- target specific
    sensors.querySensors(0xFF, telemetry);

    int i = 0;
    out_frame[i++] = PUSH_CODE_TELEMETRY_RESPONSE;
    out_frame[i++] = 0; // reserved
    memcpy(&out_frame[i], getMainIdentity().pub_key, 6);
    i += 6; // pub_key_prefix
    uint8_t tlen = telemetry.getSize();
    memcpy(&out_frame[i], telemetry.getBuffer(), tlen);
    i += tlen;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_SEND_BINARY_REQ && len >= 2 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[1];
    ContactInfo *recipient = lookupContactByPubKey(pub_key, PUB_KEY_SIZE);
    if (recipient) {
      uint8_t *req_data = &cmd_frame[1 + PUB_KEY_SIZE];
      uint32_t tag, est_timeout;
      int result = sendRequest(*recipient, req_data, len - (1 + PUB_KEY_SIZE), tag, est_timeout);
      if (result == MSG_SEND_FAILED) {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      } else {
        clearPendingReqs();
        pending_req = tag; // match this in onContactResponse()
        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = (result == MSG_SEND_SENT_FLOOD) ? 1 : 0;
        memcpy(&out_frame[2], &tag, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      }
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // contact not found
    }
  } else if (cmd_frame[0] == CMD_HAS_CONNECTION && len >= 1 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[1];
    if (hasConnectionTo(pub_key)) {
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND);
    }
  } else if (cmd_frame[0] == CMD_LOGOUT && len >= 1 + PUB_KEY_SIZE) {
    uint8_t *pub_key = &cmd_frame[1];
    stopConnection(pub_key);
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_GET_CHANNEL && len >= 2) {
    uint8_t channel_idx = cmd_frame[1];
    ChannelDetails channel;
    if (getChannel(channel_idx, channel)) {
      int i = 0;
      out_frame[i++] = RESP_CODE_CHANNEL_INFO;
      out_frame[i++] = channel_idx;
      strcpy((char *)&out_frame[i], channel.name);
      i += 32;
      memcpy(&out_frame[i], channel.channel.secret, 16);
      i += 16; // NOTE: only 128-bit supported
      _serial->writeFrame(out_frame, i);
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND);
    }
  } else if (cmd_frame[0] == CMD_SET_CHANNEL && len >= 2 + 32 + 32) {
    writeErrFrame(ERR_CODE_UNSUPPORTED_CMD); // not supported (yet)
  } else if (cmd_frame[0] == CMD_SET_CHANNEL && len >= 2 + 32 + 16) {
    uint8_t channel_idx = cmd_frame[1];
    ChannelDetails channel;
    StrHelper::strncpy(channel.name, (char *)&cmd_frame[2], 32);
    memset(channel.channel.secret, 0, sizeof(channel.channel.secret));
    memcpy(channel.channel.secret, &cmd_frame[2 + 32], 16); // NOTE: only 128-bit supported
    if (setChannel(channel_idx, channel)) {
      saveChannels();
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND); // bad channel_idx
    }
  } else if (cmd_frame[0] == CMD_SIGN_START) {
    out_frame[0] = RESP_CODE_SIGN_START;
    out_frame[1] = 0; // reserved
    uint32_t len = MAX_SIGN_DATA_LEN;
    memcpy(&out_frame[2], &len, 4);
    _serial->writeFrame(out_frame, 6);

    if (sign_data) {
      free(sign_data);
    }
    sign_data = (uint8_t *)malloc(MAX_SIGN_DATA_LEN);
    sign_data_len = 0;
  } else if (cmd_frame[0] == CMD_SIGN_DATA && len > 1) {
    if (sign_data == NULL || sign_data_len + (len - 1) > MAX_SIGN_DATA_LEN) {
      writeErrFrame(sign_data == NULL ? ERR_CODE_BAD_STATE : ERR_CODE_TABLE_FULL); // error: too long
    } else {
      memcpy(&sign_data[sign_data_len], &cmd_frame[1], len - 1);
      sign_data_len += (len - 1);
      writeOKFrame();
    }
  } else if (cmd_frame[0] == CMD_SIGN_FINISH) {
    if (sign_data) {
      getMainIdentity().sign(&out_frame[1], sign_data, sign_data_len);

      free(sign_data); // don't need sign_data now
      sign_data = NULL;

      out_frame[0] = RESP_CODE_SIGNATURE;
      _serial->writeFrame(out_frame, 1 + SIGNATURE_SIZE);
    } else {
      writeErrFrame(ERR_CODE_BAD_STATE);
    }
  } else if (cmd_frame[0] == CMD_SEND_TRACE_PATH && len > 10 && len - 10 < MAX_PACKET_PAYLOAD - 5) {
    uint8_t path_len = len - 10;
    uint8_t flags = cmd_frame[9];
    uint8_t path_sz = flags & 0x03; // NEW v1.11+
    if ((path_len >> path_sz) > MAX_PATH_SIZE ||
        (path_len % (1 << path_sz)) != 0) { // make sure is multiple of path_sz
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    } else {
      uint32_t tag, auth;
      memcpy(&tag, &cmd_frame[1], 4);
      memcpy(&auth, &cmd_frame[5], 4);
      auto pkt = createTrace(tag, auth, flags);
      if (pkt) {
        sendDirect(pkt, &cmd_frame[10], path_len);

        uint32_t t = _radio->getEstAirtimeFor(pkt->payload_len + pkt->path_len + 2);
        uint32_t est_timeout = calcDirectTimeoutMillisFor(t, path_len >> path_sz);

        out_frame[0] = RESP_CODE_SENT;
        out_frame[1] = 0;
        memcpy(&out_frame[2], &tag, 4);
        memcpy(&out_frame[6], &est_timeout, 4);
        _serial->writeFrame(out_frame, 10);
      } else {
        writeErrFrame(ERR_CODE_TABLE_FULL);
      }
    }
  } else if (cmd_frame[0] == CMD_SET_DEVICE_PIN && len >= 5) {

    // get pin from command frame
    uint32_t pin;
    memcpy(&pin, &cmd_frame[1], 4);

    // ensure pin is zero, or a valid 6 digit pin
    if (pin == 0 || (pin >= 100000 && pin <= 999999)) {
      _prefs.ble_pin = pin;
      savePrefs();
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    }
  } else if (cmd_frame[0] == CMD_GET_CUSTOM_VARS) {
    out_frame[0] = RESP_CODE_CUSTOM_VARS;
    char *dp = (char *)&out_frame[1];
    for (int i = 0; i < sensors.getNumSettings() && dp - (char *)&out_frame[1] < 140; i++) {
      if (i > 0) {
        *dp++ = ',';
      }
      strcpy(dp, sensors.getSettingName(i));
      dp = strchr(dp, 0);
      *dp++ = ':';
      strcpy(dp, sensors.getSettingValue(i));
      dp = strchr(dp, 0);
    }
    _serial->writeFrame(out_frame, dp - (char *)out_frame);
  } else if (cmd_frame[0] == CMD_SET_CUSTOM_VAR && len >= 4) {
    cmd_frame[len] = 0;
    char *sp = (char *)&cmd_frame[1];
    char *np = strchr(sp, ':'); // look for separator char
    if (np) {
      *np++ = 0; // modify 'cmd_frame', replace ':' with null
      bool success = sensors.setSettingValue(sp, np);
      if (success) {
#if ENV_INCLUDE_GPS == 1
        // Update node preferences for GPS settings
        if (strcmp(sp, "gps") == 0) {
          _prefs.gps_enabled = (np[0] == '1') ? 1 : 0;
          savePrefs();
        } else if (strcmp(sp, "gps_interval") == 0) {
          uint32_t interval_seconds = atoi(np);
          _prefs.gps_interval = constrain(interval_seconds, 0, 86400);
          savePrefs();
        }
#endif
        writeOKFrame();
      } else {
        writeErrFrame(ERR_CODE_ILLEGAL_ARG);
      }
    } else {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG);
    }
  } else if (cmd_frame[0] == CMD_GET_ADVERT_PATH && len >= PUB_KEY_SIZE + 2) {
    // FUTURE use:  uint8_t reserved = cmd_frame[1];
    uint8_t *pub_key = &cmd_frame[2];
    AdvertPath *found = NULL;
    for (int i = 0; i < ADVERT_PATH_TABLE_SIZE; i++) {
      auto p = &advert_paths[i];
      if (memcmp(p->pubkey_prefix, pub_key, sizeof(p->pubkey_prefix)) == 0) {
        found = p;
        break;
      }
    }
    if (found) {
      int i = 0;
      out_frame[i++] = RESP_CODE_ADVERT_PATH;
      memcpy(&out_frame[i], &found->recv_timestamp, 4);
      i += 4;
      out_frame[i++] = found->path_len;
      i += mesh::Packet::writePath(&out_frame[i], found->path, found->path_len);
      _serial->writeFrame(out_frame, i);
    } else {
      writeErrFrame(ERR_CODE_NOT_FOUND);
    }
  } else if (cmd_frame[0] == CMD_GET_STATS && len >= 2) {
    uint8_t stats_type = cmd_frame[1];
    if (stats_type == STATS_TYPE_CORE) {
      int i = 0;
      out_frame[i++] = RESP_CODE_STATS;
      out_frame[i++] = STATS_TYPE_CORE;
      uint16_t battery_mv = board.getBattMilliVolts();
      uint32_t uptime_secs = _ms->getMillis() / 1000;
      uint8_t queue_len = (uint8_t)_mgr->getOutboundTotal();
      memcpy(&out_frame[i], &battery_mv, 2);
      i += 2;
      memcpy(&out_frame[i], &uptime_secs, 4);
      i += 4;
      memcpy(&out_frame[i], &_err_flags, 2);
      i += 2;
      out_frame[i++] = queue_len;
      _serial->writeFrame(out_frame, i);
    } else if (stats_type == STATS_TYPE_RADIO) {
      int i = 0;
      out_frame[i++] = RESP_CODE_STATS;
      out_frame[i++] = STATS_TYPE_RADIO;
      int16_t noise_floor = (int16_t)_radio->getNoiseFloor();
      int8_t last_rssi = (int8_t)radio_driver.getLastRSSI();
      int8_t last_snr = (int8_t)(radio_driver.getLastSNR() * 4); // scaled by 4 for 0.25 dB precision
      uint32_t tx_air_secs = getTotalAirTime() / 1000;
      uint32_t rx_air_secs = getReceiveAirTime() / 1000;
      memcpy(&out_frame[i], &noise_floor, 2);
      i += 2;
      out_frame[i++] = last_rssi;
      out_frame[i++] = last_snr;
      memcpy(&out_frame[i], &tx_air_secs, 4);
      i += 4;
      memcpy(&out_frame[i], &rx_air_secs, 4);
      i += 4;
      _serial->writeFrame(out_frame, i);
    } else if (stats_type == STATS_TYPE_PACKETS) {
      int i = 0;
      out_frame[i++] = RESP_CODE_STATS;
      out_frame[i++] = STATS_TYPE_PACKETS;
      uint32_t recv = radio_driver.getPacketsRecv();
      uint32_t sent = radio_driver.getPacketsSent();
      uint32_t n_sent_flood = getNumSentFlood();
      uint32_t n_sent_direct = getNumSentDirect();
      uint32_t n_recv_flood = getNumRecvFlood();
      uint32_t n_recv_direct = getNumRecvDirect();
      uint32_t n_recv_errors = radio_driver.getPacketsRecvErrors();
      memcpy(&out_frame[i], &recv, 4);
      i += 4;
      memcpy(&out_frame[i], &sent, 4);
      i += 4;
      memcpy(&out_frame[i], &n_sent_flood, 4);
      i += 4;
      memcpy(&out_frame[i], &n_sent_direct, 4);
      i += 4;
      memcpy(&out_frame[i], &n_recv_flood, 4);
      i += 4;
      memcpy(&out_frame[i], &n_recv_direct, 4);
      i += 4;
      memcpy(&out_frame[i], &n_recv_errors, 4);
      i += 4;
      _serial->writeFrame(out_frame, i);
    } else {
      writeErrFrame(ERR_CODE_ILLEGAL_ARG); // invalid stats sub-type
    }
  } else if (cmd_frame[0] == CMD_FACTORY_RESET && memcmp(&cmd_frame[1], "reset", 5) == 0) {
    if (_serial) {
      MESH_DEBUG_PRINTLN("Factory reset: disabling serial interface to prevent reconnects (BLE/WiFi)");
      _serial->disable(); // Phone app disconnects before we can send OK frame so it's safe here
    }
    bool success = _store->formatFileSystem();
    if (success) {
      writeOKFrame();
      delay(1000);
      board.reboot(); // doesn't return
    } else {
      writeErrFrame(ERR_CODE_FILE_IO_ERROR);
    }
  } else if (cmd_frame[0] == CMD_SET_FLOOD_SCOPE && len >= 2 && cmd_frame[1] == 0) {
    if (len >= 2 + 16) {
      memcpy(send_scope.key, &cmd_frame[2], sizeof(send_scope.key)); // set curr scope TransportKey
    } else {
      memset(send_scope.key, 0, sizeof(send_scope.key)); // set scope to null
    }
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_SEND_CONTROL_DATA && len >= 2 && (cmd_frame[1] & 0x80) != 0) {
    auto resp = createControlData(&cmd_frame[1], len - 1);
    if (resp) {
      sendZeroHop(resp);
      writeOKFrame();
    } else {
      writeErrFrame(ERR_CODE_TABLE_FULL);
    }
  } else if (cmd_frame[0] == CMD_SET_AUTOADD_CONFIG) {
    _prefs.autoadd_config = cmd_frame[1];
    if (len >= 3) {
      _prefs.autoadd_max_hops = min(cmd_frame[2], (uint8_t)64);
    }
    savePrefs();
    writeOKFrame();
  } else if (cmd_frame[0] == CMD_GET_AUTOADD_CONFIG) {
    int i = 0;
    out_frame[i++] = RESP_CODE_AUTOADD_CONFIG;
    out_frame[i++] = _prefs.autoadd_config;
    out_frame[i++] = _prefs.autoadd_max_hops;
    _serial->writeFrame(out_frame, i);
  } else if (cmd_frame[0] == CMD_GET_ALLOWED_REPEAT_FREQ) {
    int i = 0;
    out_frame[i++] = RESP_ALLOWED_REPEAT_FREQ;
    for (int k = 0;
         k < sizeof(repeat_freq_ranges) / sizeof(repeat_freq_ranges[0]) && i + 8 < sizeof(out_frame); k++) {
      auto r = &repeat_freq_ranges[k];
      memcpy(&out_frame[i], &r->lower_freq, 4);
      i += 4;
      memcpy(&out_frame[i], &r->upper_freq, 4);
      i += 4;
    }
    _serial->writeFrame(out_frame, i);
  } else {
    writeErrFrame(ERR_CODE_UNSUPPORTED_CMD);
    MESH_DEBUG_PRINTLN("ERROR: unknown command: %02X", cmd_frame[0]);
  }
}

void MyMesh::enterCLIRescue() {
  _cli_rescue = true;
  cli_command[0] = 0;
  Serial.println("========= CLI Rescue =========");
}

void MyMesh::checkCLIRescueCmd() {
  int len = strlen(cli_command);
  while (Serial.available() && len < sizeof(cli_command) - 1) {
    char c = Serial.read();
    if (c != '\n') {
      cli_command[len++] = c;
      cli_command[len] = 0;
    }
    Serial.print(c); // echo
  }
  if (len == sizeof(cli_command) - 1) { // command buffer full
    cli_command[sizeof(cli_command) - 1] = '\r';
  }

  if (len > 0 && cli_command[len - 1] == '\r') { // received complete line
    cli_command[len - 1] = 0;                    // replace newline with C string null terminator

    if (memcmp(cli_command, "set ", 4) == 0) {
      const char *config = &cli_command[4];
      if (memcmp(config, "pin ", 4) == 0) {
        _prefs.ble_pin = atoi(&config[4]);
        savePrefs();
        Serial.printf("  > pin is now %06d\n", _prefs.ble_pin);
      } else {
        Serial.printf("  Error: unknown config: %s\n", config);
      }
    } else if (strcmp(cli_command, "rebuild") == 0) {
      bool success = _store->formatFileSystem();
      if (success) {
        _store->saveMainIdentity(getMainIdentity());
        _store->saveSensorIdentity(getSensorIdentity());
        savePrefs();
        saveContacts();
        saveChannels();
        Serial.println("  > erase and rebuild done");
      } else {
        Serial.println("  Error: erase failed");
      }
    } else if (strcmp(cli_command, "erase") == 0) {
      bool success = _store->formatFileSystem();
      if (success) {
        Serial.println("  > erase done");
      } else {
        Serial.println("  Error: erase failed");
      }
    } else if (memcmp(cli_command, "ls", 2) == 0) {

      // get path from command e.g: "ls /adafruit"
      const char *path = &cli_command[3];

      bool is_fs2 = false;
      if (memcmp(path, "UserData/", 9) == 0) {
        path += 8; // skip "UserData"
      } else if (memcmp(path, "ExtraFS/", 8) == 0) {
        path += 7; // skip "ExtraFS"
        is_fs2 = true;
      }
      Serial.printf("Listing files in %s\n", path);

      // log each file and directory
      File root = _store->openRead(path);
      if (is_fs2 == false) {
        if (root) {
          File file = root.openNextFile();
          while (file) {
            if (file.isDirectory()) {
              Serial.printf("[dir]  UserData%s/%s\n", path, file.name());
            } else {
              Serial.printf("[file] UserData%s/%s (%d bytes)\n", path, file.name(), file.size());
            }
            // move to next file
            file = root.openNextFile();
          }
          root.close();
        }
      }

      if (is_fs2 == true || strlen(path) == 0 || strcmp(path, "/") == 0) {
        if (_store->getSecondaryFS() != nullptr) {
          File root2 = _store->openRead(_store->getSecondaryFS(), path);
          File file = root2.openNextFile();
          while (file) {
            if (file.isDirectory()) {
              Serial.printf("[dir]  ExtraFS%s/%s\n", path, file.name());
            } else {
              Serial.printf("[file] ExtraFS%s/%s (%d bytes)\n", path, file.name(), file.size());
            }
            // move to next file
            file = root2.openNextFile();
          }
          root2.close();
        }
      }
    } else if (memcmp(cli_command, "cat", 3) == 0) {

      // get path from command e.g: "cat /contacts3"
      const char *path = &cli_command[4];

      bool is_fs2 = false;
      if (memcmp(path, "UserData/", 9) == 0) {
        path += 8; // skip "UserData"
      } else if (memcmp(path, "ExtraFS/", 8) == 0) {
        path += 7; // skip "ExtraFS"
        is_fs2 = true;
      } else {
        Serial.println("Invalid path provided, must start with UserData/ or ExtraFS/");
        cli_command[0] = 0;
        return;
      }

      // log file content as hex
      File file = _store->openRead(path);
      if (is_fs2 == true) {
        file = _store->openRead(_store->getSecondaryFS(), path);
      }
      if (file) {

        // get file content
        int file_size = file.available();
        uint8_t buffer[file_size];
        file.read(buffer, file_size);

        // print hex
        mesh::Utils::printHex(Serial, buffer, file_size);
        Serial.print("\n");

        file.close();
      }

    } else if (memcmp(cli_command, "rm ", 3) == 0) {
      // get path from command e.g: "rm /adv_blobs"
      const char *path = &cli_command[3];
      MESH_DEBUG_PRINTLN("Removing file: %s", path);
      // ensure path is not empty, or root dir
      if (!path || strlen(path) == 0 || strcmp(path, "/") == 0) {
        Serial.println("Invalid path provided");
      } else {
        bool is_fs2 = false;
        if (memcmp(path, "UserData/", 9) == 0) {
          path += 8; // skip "UserData"
        } else if (memcmp(path, "ExtraFS/", 8) == 0) {
          path += 7; // skip "ExtraFS"
          is_fs2 = true;
        }

        // remove file
        bool removed;
        if (is_fs2) {
          MESH_DEBUG_PRINTLN("Removing file from ExtraFS: %s", path);
          removed = _store->removeFile(_store->getSecondaryFS(), path);
        } else {
          MESH_DEBUG_PRINTLN("Removing file from UserData: %s", path);
          removed = _store->removeFile(path);
        }
        if (removed) {
          Serial.println("File removed");
        } else {
          Serial.println("Failed to remove file");
        }
      }

    } else if (strcmp(cli_command, "reboot") == 0) {
      board.reboot(); // doesn't return
    } else {
      Serial.println("  Error: unknown command");
    }

    cli_command[0] = 0; // reset command buffer
  }
}

void MyMesh::checkSerialInterface() {
  size_t len = _serial->checkRecvFrame(cmd_frame);
  if (len > 0) {
    handleCmdFrame(len);
  } else if (_iter_started              // check if our ContactsIterator is 'running'
             && !_serial->isWriteBusy() // don't spam the Serial Interface too quickly!
  ) {
    ContactInfo contact;
    if (_iter.hasNext(this, contact)) {
      if (contact.lastmod > _iter_filter_since) { // apply the 'since' filter
        writeContactRespFrame(RESP_CODE_CONTACT, contact);
        if (contact.lastmod > _most_recent_lastmod) {
          _most_recent_lastmod = contact.lastmod; // save for the RESP_CODE_END_OF_CONTACTS frame
        }
      }
    } else { // EOF
      out_frame[0] = RESP_CODE_END_OF_CONTACTS;
      memcpy(&out_frame[1], &_most_recent_lastmod,
             4); // include the most recent lastmod, so app can update their 'since'
      _serial->writeFrame(out_frame, 5);
      _iter_started = false;
    }
    //} else if (!_serial->isWriteBusy()) {
    //  checkConnections();    // TODO - deprecate the 'Connections' stuff
  }
}

uint8_t MyMesh::handleLoginReq(const mesh::Identity &sender, const uint8_t *secret, uint32_t sender_timestamp,
                               const uint8_t *data, bool is_flood) {
  ClientInfo *client;
  if (data[0] == 0) { // blank password, just check if sender is in ACL
    client = _acl.getClient(sender.pub_key, PUB_KEY_SIZE);
    if (client == NULL) {
#if MESH_DEBUG
      MESH_DEBUG_PRINTLN("Login, sender not in ACL");
#endif
      return 0;
    }
    // ensureContactForIdentity(sender, ADV_TYPE_CHAT, "Client");
  } else {
    if (strcmp((char *)data, _sensor_prefs.password) != 0) { // check for valid admin password
#if MESH_DEBUG
      MESH_DEBUG_PRINTLN("Invalid password: %s", data);
#endif
      return 0;
    }

    client = _acl.putClient(sender, PERM_ACL_READ_WRITE); // add to contacts (if not already known)
    if (sender_timestamp <= client->last_timestamp) {
      MESH_DEBUG_PRINTLN("Possible login replay attack!");
      return 0; // FATAL: client table is full -OR- replay attack
    }

    MESH_DEBUG_PRINTLN("Login success!");
    client->last_timestamp = sender_timestamp;
    client->last_activity = getRTCClock()->getCurrentTime();
    client->permissions |= PERM_ACL_ADMIN;
    memcpy(client->shared_secret, secret, PUB_KEY_SIZE);

    // ensureContactForIdentity(sender, ADV_TYPE_CHAT, "Client");

    dirty_contacts_expiry = futureMillis(LAZY_CONTACTS_WRITE_DELAY);
  }

  if (is_flood) {
    client->out_path_len = OUT_PATH_UNKNOWN; // need to rediscover out_path
  }

  uint32_t now = getRTCClock()->getCurrentTimeUnique();
  memcpy(reply_data, &now, 4); // response packets always prefixed with timestamp
  reply_data[4] = RESP_SERVER_LOGIN_OK;
  reply_data[5] = 0;
  reply_data[6] = client->isAdmin() ? 1 : 0;
  reply_data[7] = client->permissions;
  getRNG()->random(&reply_data[8], 4); // random blob to help packet-hash uniqueness
  reply_data[12] = FIRMWARE_VER_CODE;

  return 13; // reply length
}

void MyMesh::onAnonDataRecv(mesh::Packet *packet, const uint8_t *secret, const mesh::Identity &sender,
                            uint8_t *data, size_t len) {
  if (packet->getPayloadType() == PAYLOAD_TYPE_ANON_REQ) { // received an initial request by a possible admin
                                                           // client (unknown at this stage)
    uint32_t timestamp;
    memcpy(&timestamp, data, 4);

    data[len] = 0; // ensure null terminator
    MESH_DEBUG_PRINTLN("onAnonDataRecv: PAYLOAD_TYPE_ANON_REQ, timestamp: %u, data: %d", timestamp, &data[4]);
    uint8_t reply_len;
    if (data[4] == 0 || data[4] >= ' ') { // is password, ie. a login request
      reply_len = handleLoginReq(sender, secret, timestamp, &data[4], packet->isRouteFlood());
      //} else if (data[4] == ANON_REQ_TYPE_*) {   // future type codes
      // TODO
    } else {
      reply_len = 0; // unknown request type
    }

    if (reply_len == 0) return; // invalid request

    const mesh::LocalIdentity *recv_identity = NULL;
    if (packet->payload_len > 0) {
      uint8_t dest_hash = packet->payload[0];
      for (uint8_t id_idx = 0; id_idx < num_local_identities; id_idx++) {
        if (local_identities[id_idx].id.isHashMatch(&dest_hash)) {
          recv_identity = &local_identities[id_idx].id;
          break;
        }
      }
    }
    if (recv_identity == NULL) {
      MESH_DEBUG_PRINTLN("onAnonDataRecv: Unknown destination identity");
      return;
    }
    MESH_DEBUG_PRINTLN("onAnonDataRecv: request packet is %s", packet->isRouteFlood() ? "flood" : "direct");

    if (packet->isRouteFlood()) {
      MESH_DEBUG_PRINTLN("onAnonDataRecv: creating path return packet; path_len: %d", packet->path_len);
      for (int i = 0; i < packet->path_len; i++) {
        MESH_DEBUG_PRINTLN("  path[%d]: %02X", i, packet->path[i]);
      }
      // let this sender know path TO here, so they can use sendDirect(), and ALSO encode the response
      mesh::Packet *path =
          createPathReturnFromIdentity(*recv_identity, sender, secret, packet->path, packet->path_len,
                                       PAYLOAD_TYPE_RESPONSE, reply_data, reply_len);
      if (path) {
        sendFlood(path, SERVER_RESPONSE_DELAY, packet->getPathHashSize());
      }
    } else {
      mesh::Packet *reply = createDatagramFromIdentity(PAYLOAD_TYPE_RESPONSE, *recv_identity, sender, secret,
                                                       reply_data, reply_len);
      if (reply) {
        sendFlood(reply, SERVER_RESPONSE_DELAY, packet->getPathHashSize());
      }
    }
  }
}

void MyMesh::loop() {
  mesh::Mesh::loop();

  if (txt_send_timeout && millisHasNowPassed(txt_send_timeout)) {
    onSendTimeout();
    txt_send_timeout = 0;
  }

  // Process queued local loopback packets out-of-band to avoid deep recursive recv stack.
  for (int n = 0; n < LOOPBACK_QUEUE_SIZE && loopback_queue_len > 0; n++) {
    mesh::Packet *pkt = loopback_queue[loopback_queue_head];
    loopback_queue[loopback_queue_head] = NULL;
    loopback_queue_head = (loopback_queue_head + 1) % LOOPBACK_QUEUE_SIZE;
    loopback_queue_len--;

    if (pkt) {
      MESH_DEBUG_PRINTLN("\nMyMesh::loop(): start");
      onRecvPacket(pkt);
      MESH_DEBUG_PRINTLN("MyMesh::loop(): end\n");
      releasePacket(pkt);
    }
  }

  if (_cli_rescue) {
    checkCLIRescueCmd();
  } else {
    checkSerialInterface();
  }

  // is there are pending dirty contacts write needed?
  if (dirty_contacts_expiry && millisHasNowPassed(dirty_contacts_expiry)) {
    saveContacts();
    _acl.save(_store->getPrimaryFS());
    dirty_contacts_expiry = 0;
  }

#ifdef DISPLAY_CLASS
  if (_ui) _ui->setHasConnection(_serial->isConnected());
#endif
}

bool MyMesh::advert() {
  mesh::Packet *pkt;
  if (_prefs.advert_loc_policy == ADVERT_LOC_NONE) {
    pkt = createSelfAdvert(0, _prefs.node_name);
  } else {
    pkt = createSelfAdvert(0, _prefs.node_name, sensors.node_lat, sensors.node_lon);
  }
  if (pkt) {
    sendZeroHop(pkt);
    return true;
  } else {
    return false;
  }
}
