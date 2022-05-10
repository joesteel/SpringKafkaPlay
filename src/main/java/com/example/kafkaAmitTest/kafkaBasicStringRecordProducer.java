package com.example.kafkaAmitTest;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
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
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC_NAME, Integer.toString(n), Integer.toString(n));
                    Future<RecordMetadata> sentRecord = producer.send(record,
                            new Callback() {
                                @Override
                                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                    if(e != null) {
                                        e.printStackTrace();
                                    } else {
                                        System.out.println("The offset of the record we just sent is: " + recordMetadata.toString());
                                    }
                                }
                            }
                    );
                }
        );
    }

    public void sendMessages(String MesasgeKeyVal) {
        ProducerRecord record = new ProducerRecord<String, String>(TOPIC_NAME, MesasgeKeyVal, MesasgeKeyVal);
        Future<RecordMetadata> sentRecord = producer.send(record,
        new Callback() {
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if(e != null) {
                    e.printStackTrace();
                } else {
                    System.out.println("The offset of the record we just sent is: " + metadata.toString());
                }
            }
        });
    }
}
