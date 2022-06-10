# big-data-processing-hw8
Eighth Homework for the UCU Big Data Processing course.

## Usage:

In order to run kafka writer execute:

```
bash run-cluster.sh
bash create-topic.sh
bash kafka_write/build-run.sh
```
In order to run kafka consumer and cassandra writer execute:
```
bash kafka_cassandra/run-cassandra-node.sh
bash kafka_cassandra/keyspace-tables.sh
bash kafka_cassandra/build-run.sh
```
In order to run app execute:

```
bash cassandra_app/build-run.sh
```

To shutdown all the containers and cluster run all shutdown scripts.

## NOTE:

Example of queries:

http://172.29.0.5:5000/queries?query=1&uuid=C17222024


http://172.29.0.5:5000/queries?query=2&uuid=C1735554279


http://172.29.0.5:5000/queries?query=3&uuid=C721395199&start=2022-01-02&end=2022-07-01

Make sure you have the dataset csv named as a dataset.csv stored in kafka_write directory
