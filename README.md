# HiveSupplyCollector
A supply collector designed to connect to Hive

## Building
Run `dotnet build`

## Testing
- First, build docker image
```
cd docker && sudo docker build -t s2shape/hive . 
```
- Then, run `./run-tests.sh`
