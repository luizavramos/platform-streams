# platform-streams

Base comum dos apps Kafka Streams do motor de fraude (customer-service e os demais que não usam Spring).

- `StreamsAppBootstrap`: exactly-once v2, static membership (`group.instance.id`), métricas do Kafka Streams no Micrometer, encerramento limpo.
- `MiniHttpServer`: servidor HTTP do JDK com `/actuator/health`, `/actuator/health/liveness`, `/actuator/health/readiness` e `/actuator/prometheus`.
- `StreamsSettings`: configuração lida do ambiente (`KAFKA_BOOTSTRAP_SERVERS`, `HOSTNAME`, `HTTP_PORT`, `STATE_DIR`, `REPLICATION_FACTOR`, `STANDBY_REPLICAS`).

Build: `./mvnw verify` (o teste de integração usa Testcontainers).
