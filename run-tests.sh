#!/bin/bash
docker run --name hive -d -p 10000:10000 -p 10001:10001 -p 10002:10002 nagasuga/docker-hive /bin/bash -c 'cd /usr/local/hive && ./bin/hiveserver2'
sleep 20
docker cp HiveSupplyCollectorTests/tests/data_maps.txt hive:/data_maps.txt
docker exec -i hive /bin/bash -c 'cd /usr/local/hive && ./bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p anonymous' < HiveSupplyCollectorTests/tests/data.sql

export HIVE_HOST=localhost
export HIVE_PORT=10000
export HIVE_USER=hive
export HIVE_PASS=anonymous
export HIVE_DB=default

dotnet build
dotnet test

docker stop hive
docker rm hive
