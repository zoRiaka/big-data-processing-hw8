FROM python:3.9-slim

RUN pip install --upgrade pip

WORKDIR /kafka_cassandra
COPY requirements.txt .
RUN pip install cassandra-driver
RUN python3 -m pip install -r ./requirements.txt

COPY ./write-to-cassandra.py .

ENTRYPOINT ["python", "-u", "write-to-cassandra.py"]
