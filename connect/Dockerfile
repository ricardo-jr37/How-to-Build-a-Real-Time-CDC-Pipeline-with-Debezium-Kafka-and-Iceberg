# ---------------------------------------------------------------------------
# Kafka Connect worker carrying both halves of the pipeline:
#   * Debezium PostgreSQL  -> reads the WAL, produces CDC events
#   * Apache Iceberg sink  -> commits those events into Iceberg tables
#
# Stage 1 assembles the Iceberg plugin. Apache publishes the connector to Maven
# Central but not as a ready-made "runtime" bundle, so we resolve it here.
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-17 AS iceberg-plugin

WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:copy-dependencies \
      -DoutputDirectory=/build/plugin \
      -DincludeScope=runtime


FROM confluentinc/cp-kafka-connect:7.9.9

ARG DEBEZIUM_VERSION=3.1.1.Final

USER root

# Debezium ships a self-contained plugin tarball on Maven Central.
RUN mkdir -p /usr/share/confluent-hub-components/debezium-connector-postgres \
    && curl -fsSL \
       "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz" \
       | tar -xz -C /usr/share/confluent-hub-components/debezium-connector-postgres --strip-components=1

COPY --from=iceberg-plugin /build/plugin \
     /usr/share/confluent-hub-components/iceberg-kafka-connect/lib

RUN chown -R appuser:appuser /usr/share/confluent-hub-components

USER appuser
