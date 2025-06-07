import os
import json
import requests
import sseclient
import logging
import pandas as pd

from kafka import KafkaProducer
from kafka.errors import KafkaError
from validator.schema_validator import SchemaValidator
# from validator.data_validator import DataValidator

# === Constants ===
SSE_URL = "http://github-firehose.libraries.io/events"

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-1czw4v.svc.army.kubezt:9092"
KAFKA_USERNAME = "kfk-u-github-ccravens"
KAFKA_PASSWORD = "PJoM0WiV4NWvbJdUwq1KSYmZpM6vQTFc"

KAFKA_EVENTS_TOPIC = "kfk-t-github-events"
KAFKA_NOTIFICATIONS_TOPIC = "kfk-t-github-notifications"

# Registry & Auth Config
REGISTRY_URL = "http://acr-apicurio-registry-api.env-1czw4v.svc.army.kubezt:8080/apis/registry/v3"
KEYCLOAK_URL = "https://idp.army.kubezt.com/realms/env-1czw4v/protocol/openid-connect/token"
CLIENT_ID = "acr-apicurio-registry-api"
CLIENT_SECRET = "qB3hpuRQnIMEkybMh3g042qQnQ65Ntjn"

GITHUB_EVENTS_SCHEMA_GROUP = "com.analyticshq.github"
GITHUB_EVENTS_SCHEMA_ID = "event"
GITHUB_EVENTS_SCHEMA_VERSION = "1.0.0"

USER_MESSAGE_SCHEMA_GROUP = "com.analyticshq.github"
USER_MESSAGE_SCHEMA_ID = "user"
USER_MESSAGE_SCHEMA_VERSION = "1.0.0"

DATA_VALIDATION_RULES_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_validation_rules.json")

BATCH_SIZE = 100

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fetch_github_events():
    schema_validator = SchemaValidator(REGISTRY_URL, KEYCLOAK_URL, CLIENT_ID, CLIENT_SECRET)
    
    # data_validator = DataValidator()
    # data_validator.load_rules_from_file(DATA_VALIDATION_RULES_PATH)

    try:
        producer_avro = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: schema_validator.encode_avro(
                GITHUB_EVENTS_SCHEMA_GROUP,
                GITHUB_EVENTS_SCHEMA_ID,
                GITHUB_EVENTS_SCHEMA_VERSION,
                v
            ),
        )

        producer_json = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: schema_validator.encode_json_schema(
                USER_MESSAGE_SCHEMA_GROUP,
                USER_MESSAGE_SCHEMA_ID,
                USER_MESSAGE_SCHEMA_VERSION,
                v
            ),
        )

        logging.info("✅ Kafka producers initialized.")
    except KafkaError as e:
        logging.error(f"❌ Kafka initialization failed: {e}")
        return

    logging.info("🔄 Connecting to GitHub Firehose SSE...")
    response = requests.get(SSE_URL, stream=True, timeout=300, headers={"Accept": "text/event-stream"})

    if response.status_code != 200:
        logging.error(f"❌ SSE connection failed: {response.status_code}")
        return

    client = sseclient.SSEClient(response)
    schema_avro = schema_validator.load_schema(GITHUB_EVENTS_SCHEMA_GROUP, GITHUB_EVENTS_SCHEMA_ID, GITHUB_EVENTS_SCHEMA_VERSION)
    schema_json = schema_validator.load_schema(USER_MESSAGE_SCHEMA_GROUP, USER_MESSAGE_SCHEMA_ID, USER_MESSAGE_SCHEMA_VERSION)

    event_batch = []

    try:
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

                if not schema_validator.validate_avro(schema_avro, validated_event):
                    continue

                event_batch.append(validated_event)

                if len(event_batch) >= BATCH_SIZE:
                    # data_validator.add_data(event_batch)

                    # if not data_validator.validate():
                    #     notification = {
                    #         "title": "GitHub Events Validation Failed",
                    #         "message": "Great Expectations checks failed. Data dropped."
                    #     }

                    #     if schema_validator.validate_json_schema(schema_json, notification):
                    #         producer_json.send(KAFKA_NOTIFICATIONS_TOPIC, value=notification)
                    #         producer_json.flush()
                    #         logging.warning("⚠️ GE validation failed. Sent notification.")
                    #     else:
                    #         logging.error("❌ Notification failed JSON schema validation.")

                    #     event_batch.clear()
                    #     continue

                    # All checks passed, send to Kafka
                    for record in event_batch:
                        try:
                            logging.debug(f"📤 Attempting to send record to Kafka: {json.dumps(record)}")
                            producer_avro.send(KAFKA_EVENTS_TOPIC, value=record)
                        except Exception as send_error:
                            logging.error(f"❌ Kafka send failed: {send_error}\nRecord: {json.dumps(record)}")

                    producer_avro.flush()
                    logging.info(f"📦 Sent {len(event_batch)} GitHub events to Kafka.")
                    event_batch.clear()

            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ JSON decode error: {e}")

    finally:
        logging.info("🛑 Shutting down.")
        producer_avro.close()
        producer_json.close()

if __name__ == "__main__":
    fetch_github_events()
