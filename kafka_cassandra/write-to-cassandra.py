import pandas as pd
from datetime import datetime
import gzip
import shutil
from kafka import KafkaProducer, KafkaConsumer
import csv
from json import loads

import numpy as np


class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_tables(self):
        query1 = "INSERT INTO transactions(name_orig, tran_type, amount, is_fraud, transaction_date) VALUES (?, ?, ?, ?, ?)"
        prepared1 = self.session.prepare(query1)

        for msg in consumer:
            msg = loads(msg.value.replace("'", "\""))
            keys = ['nameOrig', 'type', 'amount', 'isFraud', 'transaction_date']
            new_msg = {key1: msg[key1] for key1 in keys}
            self.session.execute(prepared1, (
                str(new_msg['nameOrig']), str(new_msg['type']), float(new_msg['amount']), int(new_msg['isFraud']),
                datetime.strptime(new_msg['transaction_date'], '%d/%m/%Y')))

    def query_1(self, uuid):
        query = "select * from transactions where name_orig = '%s' and is_fraud=1;" % uuid
        rows = self.session.execute(query)
        return list(rows)

    def query_2(self, uuid):
        query = "select * from transactions where name_orig = '%s';" % uuid
        rows = self.session.execute(query)
        rows = sorted(rows, key=lambda item: item[2], reverse=True)
        return list(rows)[0:2]

    def query_3(self, uuid, start_date, end_date):
        query = "select sum(amount) from transactions where name_orig = '%s' and tran_type='CASH_IN' and transaction_date > '%s' and transaction_date < '%s' ALLOW FILTERING;" % (
            uuid,
            start_date, end_date)
        rows = self.session.execute(query)
        return list(rows)


if __name__ == '__main__':
    host = 'node1'
    port = 9042
    keyspace = 'my_keyspace'
    consumer = KafkaConsumer('my-topic', bootstrap_servers='kafka-server:9092', api_version=(2, 0, 2),
                             value_deserializer=lambda x: loads(x.decode('utf-8')))

    client = CassandraClient(host, port, keyspace)
    client.connect()
    client.insert_tables()
    #client.query_1('C684758020')
    #client.query_2('C684758020')
    #client.query_3('C541051042', '2021-01-02', '2022-08-01')
    client.close()
