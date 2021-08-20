echo "Waiting for Kafka to come online..."

cub kafka-ready -b kafka:9092 1 20

# create the users topic
kafka-topics \
  --bootstrap-server kafka:9092 \
  --topic vessel-visits \
  --replication-factor 1 \
  --partitions 3 \
  --create

# create the posts topic
kafka-topics \
  --bootstrap-server kafka:9092 \
  --topic bills-of-lading \
  --replication-factor 1 \
  --partitions 3 \
  --create

sleep infinity