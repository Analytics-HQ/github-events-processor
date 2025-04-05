import json
import requests
import sseclient
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError
from schema_validator.validator import SchemaValidator

# === Constants ===
SSE_URL = "http://github-firehose.libraries.io/events"

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-45y9fr.svc.dev.ahq:9092"
KAFKA_TOPIC = "kfk-t-github-events"
KAFKA_USERNAME = "kfk-u-github-ccravens"
KAFKA_PASSWORD = "pSfP5SXY1WO1MWX2kgkIFi0lLNgOM1l5"

# Registry & Auth Config
REGISTRY_URL = "http://acr-apicurio-registry-api.env-45y9fr.svc.dev.ahq:8080/apis/registry/v3"
KEYCLOAK_URL = "https://idp.dev.analyticshq.com/realms/env-45y9fr/protocol/openid-connect/token"
CLIENT_ID = "acr-apicurio-registry-api"
CLIENT_SECRET = "LPyybQKduIjv6XTww1m8KNlYrL9RlOVP"
SCHEMA_GROUP = "com.analyticshq.github"
SCHEMA_ID = "events"
SCHEMA_VERSION = "1.0.0"

# Batch size
BATCH_SIZE = 100

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fetch_github_events():
    # Initialize Schema Validator
    validator = SchemaValidator(REGISTRY_URL, KEYCLOAK_URL, CLIENT_ID, CLIENT_SECRET)

    # Initialize Kafka Producer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: validator.encode_avro_with_magic(SCHEMA_GROUP, SCHEMA_ID, SCHEMA_VERSION, v),
        )
        logging.info("✅ Kafka producer initialized.")
    except KafkaError as e:
        logging.error(f"❌ Kafka initialization failed: {e}")
        return

    # Connect to GitHub SSE
    logging.info("🔄 Connecting to GitHub Firehose SSE...")
    response = requests.get(SSE_URL, stream=True, timeout=300, headers={"Accept": "text/event-stream"})

    if response.status_code != 200:
        logging.error(f"❌ SSE connection failed: {response.status_code}")
        return

    client = sseclient.SSEClient(response)
    event_batch = []
    total_published = 0

    try:
        schema = validator.load_schema(SCHEMA_GROUP, SCHEMA_ID, SCHEMA_VERSION)
        for event in client.events():
            if not event.data:
                continue

            try:
                json_event = json.loads(event.data)

                validated_event = {
                    "id": json_event.get("id"),
                    "type": json_event.get("type"),
                    "created_at": json_event.get("created_at"),
                    "public": json_event.get("public", False),
                    "actor_name": json_event.get("actor", {}).get("login"),
                    "repo_name": json_event.get("repo", {}).get("name"),
                    "ts": json_event.get("created_at"),
                }

                if validator.validate_avro(schema, validated_event):
                    logging.info(f"✅ Validated: {validated_event}")
                    event_batch.append(validated_event)

                    if len(event_batch) >= BATCH_SIZE:
                        for record in event_batch:
                            try:
                                future = producer.send(KAFKA_TOPIC, value=record)
                                future.add_callback(lambda r: logging.info(f"✅ Sent to Kafka: {record['id']}"))
                                future.add_errback(lambda e: logging.error(f"❌ Kafka send error: {e}"))
                                # pass
                            except Exception as send_error:
                                logging.error(f"❌ Failed to serialize/send: {send_error}")

                        producer.flush()
                        total_published += len(event_batch)
                        logging.info(f"📦 {len(event_batch)} events sent (Total validated: {total_published})")
                        event_batch = []

            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ JSON decode error: {e}")
    finally:
        logging.info("🛑 Exiting and closing producer...")
        producer.close()

if __name__ == "__main__":
    fetch_github_events()