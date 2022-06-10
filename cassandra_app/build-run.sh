docker build --network=host -t my-app .
docker run --name run-app --network my-network --rm my-app

