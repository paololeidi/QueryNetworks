# QueryNetworks

docker compose up


docker exec -it querynetworks-postgres-1 /bin/bash

psql -h risingwave -p 4566 -d dev -U root

CREATE SOURCE stressStream (timestamp timestamp, id int, status varchar, stressLevel int)
WITH ( connector = 'kafka', topic = 'stress', properties.bootstrap.server = 'broker:9092', scan.startup.mode = 'latest' ) FORMAT PLAIN ENCODE CSV ( without_header = 'true', delimiter = ',' );




KAFKA
docker exec -it broker /bin/bash
kafka-console-consumer --bootstrap-server localhost:19092 --topic name

ksql cli
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088