# statewalk

Registry-driven state machine framework for realtime session software (calls, SMS, HTTP, WiFi
data sessions — anything that lives, times out, and must leave a record).

- One supervisor + child machines per session cell, driven serially (the ESL invariant).
- Object-pooled machines, mandatory per-state timeouts, quota admission (`rebindQuotaKeys` for
  anonymous-at-birth sessions), persistence + rehydration (JDBC/in-memory), channels for wire I/O.
- `v2.session`: the generic `SessionSupervisor` base — ADMITTING → ADMITTED → ACTIVE →
  TEARING_DOWN → SUCCEEDED/FAILED with abstract hooks; exactly ONE SDR per session from BOTH
  terminals, embedding the full transition history of the whole cell.

Maven: `com.telcobright:statewalk-v2:1.0-SNAPSHOT` (coordinates kept from the routesphere era;
a rename is planned for v3). Java 21. Build: `mvn test` (122 tests).

History: extracted 2026-09-01 from `routesphere/statewalk-v2` with full git history
(28 commits). First production consumers: routesphere call/SMS v2 supervision, BTCL Carrier
WiFi (wifi-sphere F37 "a session is a call").
