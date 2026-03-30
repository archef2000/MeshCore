#include "RemoteCliCallbacks.h"

#include "MyMesh.h"

#include <Arduino.h>
#include <cstdio>

void RemoteCliCallbacks::savePrefs() {
  _mesh->savePrefs();
}

bool RemoteCliCallbacks::formatFileSystem() {
  return _mesh->_store->formatFileSystem();
}

void RemoteCliCallbacks::sendSelfAdvertisement(int delay_millis, bool flood) {
  mesh::Packet *pkt;
  if (_sensor_prefs->advert_loc_policy == ADVERT_LOC_NONE) {
    pkt = _mesh->createSensorAdvert(_sensor_prefs->node_name);
  } else {
    pkt = _mesh->createSensorAdvert(_sensor_prefs->node_name, sensors.node_lat, sensors.node_lon);
  }

  if (!pkt) {
    return;
  }

  if (flood) {
    _mesh->sendFlood(pkt, delay_millis, _mesh->_prefs.path_hash_mode + 1);
  } else {
    _mesh->sendZeroHop(pkt, delay_millis);
  }
}

void RemoteCliCallbacks::updateAdvertTimer() {}

void RemoteCliCallbacks::updateFloodAdvertTimer() {}

void RemoteCliCallbacks::setTxPower(int8_t power_dbm) {
  _sensor_prefs->tx_power_dbm = constrain(power_dbm, (int8_t)-9, (int8_t)MAX_LORA_TX_POWER);
  _mesh->_prefs.tx_power_dbm = _sensor_prefs->tx_power_dbm;
  radio_set_tx_power(_sensor_prefs->tx_power_dbm);
}

void RemoteCliCallbacks::formatStatsReply(char *reply) {
  const int contacts = _mesh->getNumContacts();
  const int queue_len = _mesh->offline_queue_len;
  sprintf(reply, "contacts:%d queued:%d", contacts, queue_len);
}

void RemoteCliCallbacks::formatRadioStatsReply(char *reply) {
  int16_t noise_floor = (int16_t)_mesh->_radio->getNoiseFloor();
  int8_t last_rssi = (int8_t)radio_driver.getLastRSSI();
  int8_t last_snr = (int8_t)(radio_driver.getLastSNR() * 4);
  sprintf(reply, "noise:%d rssi:%d snr4:%d", noise_floor, last_rssi, last_snr);
}

void RemoteCliCallbacks::formatPacketStatsReply(char *reply) {
  uint32_t recv = radio_driver.getPacketsRecv();
  uint32_t sent = radio_driver.getPacketsSent();
  uint32_t errs = radio_driver.getPacketsRecvErrors();
  sprintf(reply, "recv:%lu sent:%lu errs:%lu", recv, sent, errs);
}

mesh::LocalIdentity &RemoteCliCallbacks::getSelfId() {
  return _mesh->getMainIdentity();
}

void RemoteCliCallbacks::saveIdentity(const mesh::LocalIdentity &new_id) {
  _mesh->getMainIdentity() = new_id;
  _mesh->self_id = new_id;
  _mesh->_store->saveMainIdentity(_mesh->getMainIdentity());
}

void RemoteCliCallbacks::applyTempRadioParams(float freq, float bw, uint8_t sf, uint8_t cr,
                                              int timeout_mins) {
  (void)timeout_mins;
  radio_set_params(freq, bw, sf, cr);
}
