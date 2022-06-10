docker build --network=host -t hw8-write .
docker run --name kafka-producer --network my-network --rm hw8-write

