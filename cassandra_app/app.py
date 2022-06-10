from flask import Flask, request
from write_to_cassandra import CassandraClient
import json

# create the Flask app
app = Flask(__name__)


@app.route('/queries', methods=['GET'])
def query_example():
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    args = request.args
    if args.get("query") == '1':
        return json.dumps(client.query_1(args.get("uuid")), default=str, sort_keys=True, indent=4)
    elif args.get("query") == '2':
        return json.dumps(client.query_2(args.get("uuid")), default=str)
    elif args.get("query") == '3':
        return json.dumps(float(client.query_3(args.get("uuid"), args.get("start"), args.get("end"))[0][0]), default=str)
    else:
        exit(-2)


if __name__ == '__main__':
    host = 'node1'
    port = 9042
    keyspace = 'my_keyspace'

    client = CassandraClient(host, port, keyspace)
    client.connect()
    app.run(host='0.0.0.0', port=5000)
