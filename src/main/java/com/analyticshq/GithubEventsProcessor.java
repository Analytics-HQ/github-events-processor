package com.analyticshq;

import com.analyticshq.models.GithubEvent;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GithubEventsProcessor {
    // private static final String KAFKA_BOOTSTRAP_SERVER = "kfk-github-kafka-bootstrap.env-g0vgp2.svc.dev.ahq:9092";
    // private static final String KAFKA_INPUT_TOPIC = "kfk-t-github-sink";
    // private static final String KAFKA_OUTPUT_TOPIC = "kfk-t-github-events";
    // private static final String KAFKA_USERNAME = "kfk-u-github-ccravens";
    // private static final String KAFKA_PASSWORD = "vRDFslHaRImo1Xz8WqkOeQa8qmHtXP79";
    
    private static final String KAFKA_BOOTSTRAP_SERVER = System.getenv("KAFKA_BOOTSTRAP_SERVER");
    private static final String KAFKA_INPUT_TOPIC = System.getenv("KAFKA_INPUT_TOPIC");
    private static final String KAFKA_OUTPUT_TOPIC = System.getenv("KAFKA_OUTPUT_TOPIC");
    private static final String KAFKA_USERNAME = System.getenv("KAFKA_USERNAME");
    private static final String KAFKA_PASSWORD = System.getenv("KAFKA_PASSWORD");

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static void main(String[] args) {
        if (KAFKA_BOOTSTRAP_SERVER == null || 
            KAFKA_INPUT_TOPIC == null || 
            KAFKA_OUTPUT_TOPIC == null || 
            KAFKA_USERNAME == null || 
            KAFKA_PASSWORD == null) 
        {
            System.err.println("❌ Environment variables KAFKA_BOOTSTRAP_SERVER, KAFKA_INPUT_TOPIC, KAFKA_OUTPUT_TOPIC, KAFKA_USERNAME and KAFKA_PASSWORD must be set.");
            System.exit(1);
        }
        
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "github-events-processor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put("security.protocol", "SASL_PLAINTEXT");
        consumerProps.put("sasl.mechanism", "SCRAM-SHA-512");
        consumerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + KAFKA_USERNAME + "\" " +
                        "password=\"" + KAFKA_PASSWORD + "\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(KAFKA_INPUT_TOPIC));

        // ✅ Configure Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put("security.protocol", "SASL_PLAINTEXT");
        producerProps.put("sasl.mechanism", "SCRAM-SHA-512");
        producerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + KAFKA_USERNAME + "\" " +
                        "password=\"" + KAFKA_PASSWORD + "\";");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println("✅ Listening for messages on topic: " + KAFKA_INPUT_TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // ✅ Parse the full event JSON
                    GithubEvent event = objectMapper.readValue(record.value(), GithubEvent.class);

                    // ✅ Publish to the output topic
                    producer.send(new ProducerRecord<>(KAFKA_OUTPUT_TOPIC, objectMapper.writeValueAsString(event)),
                            (metadata, exception) -> {
                                if (exception != null) {
                                    System.err.println("❌ Failed to publish event: " + exception.getMessage());
                                } else {
                                    System.out.println("✅ Published event to " + metadata.topic() + " partition " + metadata.partition());
                                }
                            });

                } catch (Exception e) {
                    System.err.println("❌ Failed to process event: " + e.getMessage());
                }
            }
        }
    }
}
