# QueryNetworks

docker compose up  /  docker-compose up

KAFKA

docker exec -it broker2 /bin/bash

kafka-topics --list --bootstrap-server localhost:19092

kafka-console-consumer --bootstrap-server localhost:19092 --topic name

kafka-topics --create --bootstrap-server localhost:19092 --replication-factor 1 --partitions 1 --topic name

ksql cli
docker exec -it ksqldb-cli2 ksql http://ksqldb-server:8088

RISINGWAVE

docker exec -it querynetworks-postgres-1 /bin/bash

psql -h risingwave -p 4566 -d dev -U root

CREATE SOURCE stressStream (timestamp timestamp, id int, status varchar, stressLevel int)
WITH (
connector = 'kafka',
topic = 'stress',
properties.bootstrap.server = 'broker:9092',
scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (
without_header = 'true',
delimiter = ','
);

CREATE SOURCE weightStream (timestamp timestamp, id int, weight double)
WITH (
connector = 'kafka',
topic = 'weight',
properties.bootstrap.server = 'broker:9092',
scan.startup.mode = 'latest'
) FORMAT PLAIN ENCODE CSV (
without_header = 'true',
delimiter = ','
);

CREATE MATERIALIZED VIEW query4result as
dev-> select window_start, window_end, id, max(stressLevel) as max_stress
FROM TUMBLE (stressStream, timestamp, INTERVAL '10 SECONDS')
GROUP BY window_start, window_end, id;

create sink query4sink from query4result
with (
connector='kafka',
properties.bootstrap.server='broker:9092',
topic='output')
format plain encode JSON (force_append_only='true');

