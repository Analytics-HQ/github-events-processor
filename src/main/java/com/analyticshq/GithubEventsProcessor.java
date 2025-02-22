package com.analyticshq;

import com.analyticshq.models.GithubEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GithubEventsProcessor {
    private static final String INPUT_TOPIC = "kfk-t-github-sink";
    private static final String OUTPUT_TOPIC = "kfk-t-github-events";
    private static final String BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-g0vgp2.svc.dev.ahq:9092";
    private static final String KAFKA_USERNAME = "kfk-u-github-ccravens";
    private static final String KAFKA_PASSWORD = "pF9Jq16H5JrhrDUNyCLUk3OZjDZJVvFf";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "github-events-processor");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ✅ Add SASL/SCRAM Authentication for Consumer
        consumerProps.put("security.protocol", "SASL_PLAINTEXT");
        consumerProps.put("sasl.mechanism", "SCRAM-SHA-512");
        consumerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + KAFKA_USERNAME + "\" " +
                        "password=\"" + KAFKA_PASSWORD + "\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(INPUT_TOPIC));

        // ✅ Configure Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // ✅ Add SASL/SCRAM Authentication for Producer
        producerProps.put("security.protocol", "SASL_PLAINTEXT");
        producerProps.put("sasl.mechanism", "SCRAM-SHA-512");
        producerProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                        "username=\"" + KAFKA_USERNAME + "\" " +
                        "password=\"" + KAFKA_PASSWORD + "\";");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        System.out.println("✅ Listening for messages on topic: " + INPUT_TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    // Parse the full event JSON
                    GithubEvent event = objectMapper.readValue(record.value(), GithubEvent.class);

                    // Serialize only the top-level attributes
                    String simplifiedEventJson = objectMapper.writeValueAsString(event);
                    System.out.println("📥 Processed Event: " + simplifiedEventJson);

                    // Publish to the output topic
                    producer.send(new ProducerRecord<>(OUTPUT_TOPIC, simplifiedEventJson),
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
