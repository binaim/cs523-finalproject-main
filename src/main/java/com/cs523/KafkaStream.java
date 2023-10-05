package com.cs523;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.IteratorUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaStream {
    public static String KAFKA_TOPIC = "electronic-store";

    public static void main(String[] args) throws InterruptedException, IOException {
        TableUtils tableUtils = new TableUtils();
        tableUtils.createTable();

        SparkConf sparkConf = new SparkConf().setAppName("JavaNetworkWordCount");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(2));

        ssc.sparkContext().setLogLevel("ERROR");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "kafka:29092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        List<String> topics = Arrays.asList(KAFKA_TOPIC);

        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        ObjectMapper mapper = new ObjectMapper();

        JavaDStream<JsonNode> datas = stream.map(record -> record.value().toString())
                .map((String line) -> mapper.readTree(line)).cache();

        // Event type count
        JavaPairDStream<String, Integer> eventTypeCounts = datas
                .mapToPair((JsonNode actualObj) -> {
                    return new Tuple2<String, Integer>(actualObj.get("event_type").asText(), 1);
                }).reduceByKey((x, y) -> x + y);

        // Category view count
        JavaPairDStream<String, Integer> categoryViewCounts = datas
                .filter(obj -> obj.get("event_type").asText().equals("view"))
                .map((JsonNode obj) -> obj.get("category_code").asText(null))
                .filter(c -> c != null && !c.isEmpty())
                .mapToPair(c -> new Tuple2<String, Integer>(c.split("\\.")[0], 1))
                .reduceByKey((x, y) -> x + y);

        categoryViewCounts.print();

        eventTypeCounts.print();
        eventTypeCounts.foreachRDD((rdd, time) -> {

            rdd.foreachPartition((events) -> {
                List<Tuple2<String, Integer>> result = IteratorUtils.toList(events);
                if (result.size() == 0)
                    return;
                // Output to kafa-analytics
                new KafkaWriter().writeEvents(result);

                // write to HBase
                new HBaseWriter().writeEvents(result, time.toString());
            });
        });

        ssc.start();
        ssc.awaitTermination();
    }

}
