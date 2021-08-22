docker build -t kafka-streams:latest .
docker-compose up -d --remove-orphans --force-recreate
docker-compose logs -f streams_app_1 streams_app_2 streams_app_3 streams_app_4 streams_app_5