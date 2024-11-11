#!/bin/bash

# Initialize variables with defaults or empty strings
tableToRead=""
buffer_size=""
bufferpool_size=""
rcv_par=""
decomp_par=""
write_par=""
iformat=""
transfer_id=""

# Parse named parameters
for arg in "$@"; do
  case $arg in
    tableName=*)
      tableToRead="${arg#*=}"
      ;;
    buffer_size=*)
      buffer_size="${arg#*=}"
      ;;
    bufferpool_size=*)
      bufferpool_size="${arg#*=}"
      ;;
    rcv_par=*)
      rcv_par="${arg#*=}"
      ;;
    decomp_par=*)
      decomp_par="${arg#*=}"
      ;;
    write_par=*)
      write_par="${arg#*=}"
      ;;
    iformat=*)
      iformat="${arg#*=}"
      ;;
    transfer_id=*)
      transfer_id="${arg#*=}"
      ;;
    server_host=*)
      server_host="${arg#*=}"
          ;;
    *)
      echo "Unknown option: $arg"
      exit 1
      ;;
  esac
done

# Ensure all required parameters are provided
if [ -z "$tableToRead" ] || [ -z "$buffer_size" ] || [ -z "$bufferpool_size" ] || [ -z "$rcv_par" ] || [ -z "$decomp_par" ] || [ -z "$write_par" ] || [ -z "$iformat" ] || [ -z "$transfer_id" ] || [ -z "$server_host" ]; then
  echo "Error: Missing one or more required parameters."
  echo "Usage: $0 tableName=<string> buffer_size=<size> bufferpool_size=<size> rcv_par=<int> decomp_par=<int> write_par=<int> iformat=<int> transfer_id=<int> server_host=<string>"
  exit 1
fi

#TODO: introduce parameter for build
# Package the project with sbt
#echo "Packaging the project with sbt..."
#sbt package && \

# Run the Spark submit command
#echo "Running spark-submit with table: $tableToRead"
spark-submit \
  --class "example.ReadPGXDBC" \
  --master "local" \
  --conf spark.eventLog.enabled=true \
  --num-executors 1 \
  --executor-cores 8 \
  --executor-memory 16G \
  --conf spark.memory.storageFraction=0.8 \
  --conf spark.driver.memory=16g \
  --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
  /app/target/spark3io-1.0.jar "$tableToRead" "$buffer_size" "$bufferpool_size" "$rcv_par" "$decomp_par" "$write_par" "$iformat" "$transfer_id" "$server_host"
