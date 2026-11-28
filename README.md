# quantara-core
Shared package: env config + topic/group prefixing, Kafka/in-memory bus, Ed25519 command signing, message validators. MIT.
Install in other repos: `quantara-core @ git+https://github.com/<org>/quantara-core@v0.1.0`
Env: TOPIC_PREFIX, KAFKA_BOOTSTRAP, KAFKA_SECURITY_PROTOCOL, KAFKA_SASL_MECHANISM, KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD.

## Topic budget (cluster limit: 10 topics)
Services use 20 *logical* topics (`topics.TOPICS`), but the bus maps them onto **7 physical Kafka topics** (`topics.PHYSICAL`):
`raw.telemetry`, `raw.edge`, `valid.telemetry`, `valid.edge`, `vss.enriched`, `cloud.events`, `edge.in` (all prefixed with `TOPIC_PREFIX`). 3 are spare.
Each message is tagged with its logical name (`_lt`) on publish; consumers filter on it and strip the tag, so service code is unchanged.
