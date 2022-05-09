package com.example.kafkaAmitTest;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.concurrent.Future;
import java.util.stream.IntStream;

public class kafkaBasicStringRecordProducer {

    private static final String TOPIC_NAME = "amit-test";

    private KafkaProducer<String, String> producer;

    public kafkaBasicStringRecordProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        producer = new KafkaProducer<>(props);
    }

    public void sendMessages(int numMessages) {
        IntStream.range(0, numMessages).forEach(n -> {
                    Future<RecordMetadata> sentRecord = producer.send(new ProducerRecord<String, String>(TOPIC_NAME, Integer.toString(n), Integer.toString(n)));
                    try {
                        RecordMetadata recordMetadata = sentRecord.get();
                        System.out.println("Producing a record: " + recordMetadata.toString());
                    } catch (Exception e) {
                        System.out.println("Error sending record to kafka");
                    }
                }
        );
    }

    public void sendMessages(String MesasgeKeyVal) {
        Future<RecordMetadata> sentRecord = producer.send(new ProducerRecord<String, String>(TOPIC_NAME, MesasgeKeyVal, MesasgeKeyVal));
        try {
            RecordMetadata recordMetadata = sentRecord.get();
            System.out.println("Producing a record: " + recordMetadata.toString());
        } catch (Exception e) {
            System.out.println("Error sending record to kafka");
        }
    }
}
