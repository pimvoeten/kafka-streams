docker build -t kafka-streams-app:latest ./app
docker build -t kafka-streams-simulators:latest ./simulators
docker-compose up -d --remove-orphans --scale kafka-streams-simulators=2 --scale streams-app=3
#docker-compose logs -f kafka-streams-simulators
docker-compose logs -f streams-app