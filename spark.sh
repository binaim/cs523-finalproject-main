# build the jar file
mvn package
# copy our dataset inside the spark docker container
docker cp final-project-1.0-SNAPSHOT.jar spark:/app.jar
# Run spark submit inside the docker container
docker exec -it spark spark-submit --master=local --conf spark.sql.shuffle.partitions=1 --class com.cs523.KafkaStream --packages "org.apache.hbase:hbase-client:2.4.17,org.apache.hbase:hbase-common:2.4.17,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.2" /app.jar