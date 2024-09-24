# Build container

```shell
docker build -t spark3io-sbt:latest .
````

# Run container

```shell
docker run -it --rm \
  -v $(pwd):/app \
  --network xdbc-net \
  -p 4040:4040 \
  -p 18080:18080 \
  spark3io-sbt:latest
```

# Generate native header files

`sbt javah`

# Package
`sbt package`

# Run JDBC

```shell
sbt package && /spark/bin/spark-submit  \
 --class "example.ReadPGJDBC"   \
 --master "local"  \
 --conf spark.eventLog.enabled=true  \
 --num-executors 1 \
 --executor-cores 8 \
 --executor-memory 16G   \
 --conf spark.memory.storageFraction=0.8 \
 --conf spark.driver.memory=16g \
 --conf spark.executor.extraJavaOptions="-XX:+UseG1GC"   \
 /app/target/spark3io-1.0.jar

```

# Run XDBC

```shell
sbt package && /spark/bin/spark-submit  \
 --class "example.ReadPGXDBC"   \
 --master "local"  \
 --conf spark.eventLog.enabled=true  \
 --num-executors 1 \
 --executor-cores 8 \
 --executor-memory 16G   \
 --conf spark.memory.storageFraction=0.8 \
 --conf spark.driver.memory=16g \
 --conf spark.executor.extraJavaOptions="-XX:+UseG1GC"   \
 /app/target/spark3io-1.0.jar

```