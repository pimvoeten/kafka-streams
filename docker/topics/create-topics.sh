echo "Waiting for Kafka to come online..."

cub kafka-ready -b kafka_1:9091 1 20

# create the users topic
kafka-topics \
  --bootstrap-server kafka_1:9091 \
  --topic vessel-visits \
  --replication-factor 1 \
  --partitions 30 \
  --config cleanup.policy=compact \
  --config delete.retention.ms=600000 \
  --create

# create the posts topic
kafka-topics \
  --bootstrap-server kafka_1:9091 \
  --topic bills-of-lading \
  --replication-factor 1 \
  --partitions 30 \
  --config cleanup.policy=compact \
  --config delete.retention.ms=600000 \
  --create

# create the results topic
kafka-topics \
  --bootstrap-server kafka_1:9091 \
  --topic matched.results \
  --replication-factor 1 \
  --partitions 30 \
  --config cleanup.policy=compact \
  --config delete.retention.ms=600000 \
  --create

sleep infinity