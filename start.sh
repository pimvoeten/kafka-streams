docker build -t kafka-streams-app:latest ./app
docker build -t kafka-streams-generators:latest ./generators
docker-compose up -d --remove-orphans --scale kafka-streams-generators=0 --scale streams-app=0 #--force-recreate
docker-compose logs -f streams-app | grep -v -E 'RestTemplate'
#docker-compose logs -f streams-app | grep -i -E 'ERROR|Buffer contains|Putting|Removing BillOfLading'