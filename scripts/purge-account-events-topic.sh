#!/usr/bin/env bash
# Purge all messages from the account-events Kafka topic by deleting and
# recreating the topic. Use this when the topic may contain bad/legacy
# messages (e.g. wrong Protobuf schema) so connector-admin only sees new
# messages from account-service.
#
# Requires: Docker, and the Compose dev-services Kafka container running
#   (container name: pipeline-kafka).
#
# Usage: ./scripts/purge-account-events-topic.sh

set -e

TOPIC="${ACCOUNT_EVENTS_TOPIC:-account-events}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-pipeline-kafka}"
# Inside the container we use the PLAINTEXT listener
BOOTSTRAP="localhost:9092"

if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
  echo "Error: Kafka container '${KAFKA_CONTAINER}' is not running."
  echo "Start Compose dev services first (e.g. run connector-admin or another service that uses them)."
  exit 1
fi

echo "Purging topic: ${TOPIC} (container: ${KAFKA_CONTAINER})"

docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --delete \
  --topic "$TOPIC" \
  --if-exists 2>/dev/null || true

echo "Topic deleted (or did not exist). Recreating..."

docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" \
  --create \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1

echo "Done. Topic '${TOPIC}' is empty and ready for new messages."
