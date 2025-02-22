## requirements.txt
```
sseclient-py==1.8.0
requests==2.32.3
kafka-python==2.0.3
```

## github_events.py
```python
import sseclient
import requests
import json
import sys
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# Constants
SSE_URL = "http://github-firehose.libraries.io/events"
KAFKA_BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-g0vgp2.svc.dev.ahq:9092"
KAFKA_TOPIC = "kfk-t-github-sink"
KAFKA_USERNAME = "kfk-u-github-ccravens"
KAFKA_PASSWORD = "pF9Jq16H5JrhrDUNyCLUk3OZjDZJVvFf"

BATCH_SIZE = 100  # Adjust batch size as needed

def fetch_github_events():
    """Fetches GitHub SSE events and sends them in batches to Kafka."""
    logging.info("🔄 Initializing Kafka Producer...")
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        logging.info("✅ Kafka Producer initialized successfully.")
    except KafkaError as e:
        logging.error(f"❌ Kafka Producer initialization failed: {e}")
        sys.exit(1)

    event_batch = []

    try:
        logging.info(f"🔄 Connecting to GitHub Firehose SSE at {SSE_URL}...")
        response = requests.get(SSE_URL, stream=True, timeout=300, headers={"Accept": "text/event-stream"})

        logging.info(f"🔍 Received HTTP Status Code: {response.status_code}")
        if response.status_code != 200:
            logging.error(f"❌ Connection failed with status {response.status_code}")
            sys.exit(1)

        logging.info("📡 SSE Connection Established. Listening for events...")
        client = sseclient.SSEClient(response)

        event_count = 0
        batch_count = 0

        for event in client.events():
            try:
                if event.data:
                    event_batch.append(json.loads(event.data))
                    event_count += 1

                    if len(event_batch) >= BATCH_SIZE:
                        logging.info(f"🚀 Sending batch {batch_count + 1} to Kafka ({len(event_batch)} events)...")
                        
                        for record in event_batch:
                            try:
                                future = producer.send(KAFKA_TOPIC, record)
                                future.add_callback(lambda metadata: logging.info(f"✅ Message sent to {metadata.topic} partition {metadata.partition}"))
                                future.add_errback(lambda exc: logging.error(f"❌ Message failed to send: {exc}"))
                            except KafkaError as ke:
                                logging.error(f"❌ Kafka send error: {ke}")

                        producer.flush()
                        logging.info(f"✅ Published {len(event_batch)} events to Kafka (Batch {batch_count + 1}).")
                        batch_count += 1
                        event_batch = []  # Clear batch

                    if event_count % 10 == 0:
                        logging.debug(f"📊 Processed {event_count} events so far...")

            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ JSON Decode Error: {e}")

    except requests.exceptions.ChunkedEncodingError:
        logging.warning("⚠️ SSE Connection closed normally (Chunked Encoding Error). Exiting gracefully.")
        sys.exit(0)

    except requests.exceptions.RequestException as e:
        logging.error(f"❌ SSE Connection failed: {e}")
        sys.exit(1)

    except Exception as e:
        logging.error(f"⚠️ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    fetch_github_events()

```

## Kafka User Spec
```yaml
spec:
  authentication:
    password:
      valueFrom:
        secretKeyRef:
          key: password
          name: kfk-u-github-ccravens
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          name: github-events-processor
          patternType: literal
          type: group
        operations:
          - Read
          - Describe
      - resource:
          name: kfk-t-github-sink
          patternType: literal
          type: topic
        operations:
          # - Create
          # - Describe
          - Read
          - Write
      - resource:
          name: kfk-t-github-events
          patternType: literal
          type: topic
        operations:
          - Write
```