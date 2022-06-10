FROM python:3.9-slim

RUN pip install --upgrade pip

WORKDIR /cassandra_app
COPY requirements.txt .
RUN pip install cassandra-driver
RUN python3 -m pip install -r ./requirements.txt

COPY ./app.py .
COPY write_to_cassandra.py .

ENTRYPOINT ["python", "-u", "app.py"]