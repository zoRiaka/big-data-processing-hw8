docker build --network=host -t hw8-read .
docker run --name kafka-consumer --network my-network --rm hw8-read

