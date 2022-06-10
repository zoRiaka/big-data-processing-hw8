docker exec -it node1 cqlsh -e "CREATE KEYSPACE my_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1 };
USE my_keyspace;
CREATE TABLE transactions (name_orig text, tran_type text, amount float, is_fraud int, transaction_date date, PRIMARY KEY (name_orig, is_fraud, transaction_date, amount));
EXIT;"
