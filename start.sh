docker build -t kafka-streams-app:latest ./app
docker build -t kafka-streams-simulators:latest ./simulators
docker-compose up -d --remove-orphans --scale kafka-streams-simulators=2
docker-compose logs -f streams_app
#docker-compose logs -f