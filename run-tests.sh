#!/bin/bash
docker run --name hive -d -p 10000:10000 -p 10001:10001 -p 10002:10002 nagasuga/docker-hive /bin/bash -c 'cd /usr/local/hive && ./bin/hiveserver2'
docker cp HiveSupplyCollectorTests/tests/data_maps.txt hive:/data_maps.txt
docker exec -i hive /bin/bash -c 'cd /usr/local/hive && ./bin/beeline -u jdbc:hive2://localhost:10000/default -n hive -p anonymous' < HiveSupplyCollectorTests/tests/data.sql

mkdir HiveSupplyCollectorTests/Properties
echo { > HiveSupplyCollectorTests/Properties/launchSettings.json
echo   \"profiles\": { >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo     \"HiveSupplyCollectorTests\": { >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo       \"commandName\": \"Project\", >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo       \"environmentVariables\": { >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo         \"HIVE_HOST\": \"localhost\", >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo         \"HIVE_PORT\": \"10000\", >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo         \"HIVE_USER\": \"hive\", >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo         \"HIVE_PASS\": \"anonymous\", >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo         \"HIVE_DB\": \"default\" >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo       } >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo     } >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo   } >> HiveSupplyCollectorTests/Properties/launchSettings.json
echo } >> HiveSupplyCollectorTests/Properties/launchSettings.json

dotnet build
dotnet test

docker stop hive
docker rm hive
