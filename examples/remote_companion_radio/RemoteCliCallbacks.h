
#pragma once

#include <Mesh.h>
#include <cstring>
#include <helpers/CommonCLI.h>

#define FIRMWARE_ROLE "sensor"

#ifndef FIRMWARE_BUILD_DATE
#define FIRMWARE_BUILD_DATE "20 Mar 2026"
#endif

#ifndef FIRMWARE_VERSION
#define FIRMWARE_VERSION "v1.14.1"
#endif

class MyMesh;

class RemoteCliCallbacks : public CommonCLICallbacks {
public:
  RemoteCliCallbacks(MyMesh &mesh, NodePrefs *_sensor_prefs) : _sensor_prefs(_sensor_prefs), _mesh(&mesh) {}

  // CommonCLI callbacks
  const char *getFirmwareVer() override { return FIRMWARE_VERSION; }
  const char *getBuildDate() override { return FIRMWARE_BUILD_DATE; }
  const char *getRole() override { return FIRMWARE_ROLE; }
  const char *getNodeName() { return _sensor_prefs->node_name; }
  NodePrefs *getNodePrefs() { return _sensor_prefs; }
  void savePrefs() override;
  bool formatFileSystem() override;
  void sendSelfAdvertisement(int delay_millis, bool flood) override;
  void updateAdvertTimer() override;
  void updateFloodAdvertTimer() override;
  void setLoggingOn(bool enable) override {}
  void eraseLogFile() override {}
  void dumpLogFile() override {}
  void setTxPower(int8_t power_dbm) override;
  void formatNeighborsReply(char *reply) override { strcpy(reply, "not supported"); }
  void formatStatsReply(char *reply) override;
  void formatRadioStatsReply(char *reply) override;
  void formatPacketStatsReply(char *reply) override;
  mesh::LocalIdentity &getSelfId() override;
  void saveIdentity(const mesh::LocalIdentity &new_id) override;
  void clearStats() override {}
  void applyTempRadioParams(float freq, float bw, uint8_t sf, uint8_t cr, int timeout_mins) override;

protected:
  NodePrefs *_sensor_prefs;
  MyMesh *_mesh;
};
