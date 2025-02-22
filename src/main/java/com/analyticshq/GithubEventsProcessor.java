package com.analyticshq;

public class GithubEventsProcessor 
{
    private static final String BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-g0vgp2.svc.dev.ahq:9092";
    private static final String SINK_TOPIC = "kfk-t-github-sink";
    private static final String GROUP_ID = "github-events-consumer";

    public static void main(String[] args) {
        // Configure Consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from beginning if no offset exists

        // Create Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(SINK_TOPIC));

        System.out.println("✅ Subscribed to topic: " + SINK_TOPIC);

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Poll messages
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("📥 Received Message: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
