docker build -t kafka-streams:latest .
#docker-compose up -d --remove-orphans --force-recreate
docker-compose up -d --remove-orphans --scale streams_app=5
docker-compose logs -f streams_app