docker stop zookeeper-server
docker stop kafka-server
docker stop run-app
docker rm run-app
docker rm kafka-producer
docker rm zookeeper-server
docker rm kafka-server
docker network rm my-network
