import os
import json
import requests
import sseclient
import logging
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from validator.schema_validator import SchemaValidator
from validator.data_validator import DataValidator, DataSourceType

# === Constants ===
SSE_URL = "http://github-firehose.libraries.io/events"

# Kafka Config
KAFKA_BOOTSTRAP_SERVERS = "kfk-github-kafka-bootstrap.env-bcizkp.svc.army.kubezt:9092"
KAFKA_USERNAME = "kfk-u-github-ccravens"
KAFKA_PASSWORD = "YZlyyngqdk1SQF4pQuTmmh8v5VFP9pAH"

KAFKA_EVENTS_TOPIC = "kfk-t-github-events"
KAFKA_USERS_TOPIC = "kfk-t-github-users"

# Registry & Auth Config
REGISTRY_URL = "http://acr-apicurio-registry-api.env-bcizkp.svc.army.kubezt:8080/apis/registry/v3"
KEYCLOAK_URL = "https://idp.army.kubezt.com/realms/env-bcizkp/protocol/openid-connect/token"
CLIENT_ID = "acr-apicurio-registry-api"
CLIENT_SECRET = "tIINNmuAvWrBQa6fuPHUqcEzkgFeTe1S"

GITHUB_EVENTS_SCHEMA_GROUP = "com.analyticshq.github"
GITHUB_EVENTS_SCHEMA_ID = "event"
GITHUB_EVENTS_SCHEMA_VERSION = "1.0.0"

USER_MESSAGE_SCHEMA_GROUP = "com.analyticshq.github"
USER_MESSAGE_SCHEMA_ID = "user"
USER_MESSAGE_SCHEMA_VERSION = "1.0.0"

BATCH_SIZE = 100

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fetch_github_events():
    schema_validator = SchemaValidator(REGISTRY_URL, KEYCLOAK_URL, CLIENT_ID, CLIENT_SECRET)

    # Initialize Great Expectations validator
    gx_validator = DataValidator(project_root_dir=".")
    gx_validator.add_datasource(
        type=DataSourceType.PANDAS,
        name="github_events",
        execution_engine={},
        data_connectors={}
    )
    gx_validator.add_dataframe_asset("github_events_batch", pd.DataFrame())
    gx_validator.add_batch_definition("github_events_batch_def")

    try:
        producer_events = KafkaProducer(
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

        producer_users = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            value_serializer=lambda v: schema_validator.encode_avro(
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

    response = requests.get(SSE_URL, stream=True, timeout=300, headers={"Accept": "text/event-stream"})
    if response.status_code != 200:
        logging.error(f"❌ SSE connection failed: {response.status_code}")
        return

    client = sseclient.SSEClient(response)

    schema_event_avro = schema_validator.load_schema(
        GITHUB_EVENTS_SCHEMA_GROUP, GITHUB_EVENTS_SCHEMA_ID, GITHUB_EVENTS_SCHEMA_VERSION
    )
    schema_user_avro = schema_validator.load_schema(
        USER_MESSAGE_SCHEMA_GROUP, USER_MESSAGE_SCHEMA_ID, USER_MESSAGE_SCHEMA_VERSION
    )

    event_batch = []
    seen_user_ids = set()

    try:
        for event in client.events():
            if not event.data:
                continue

            try:
                json_event = json.loads(event.data)

                # Validate raw JSON using Great Expectations
                validation_result = gx_validator.validate(pd.DataFrame([json_event]))
                if not validation_result["success"]:
                    logging.warning("⚠️ GE validation failed for event. Skipping.")
                    continue

                actor = json_event.get("actor", {})
                actor_id = actor.get("id")

                validated_event = {
                    "id": str(json_event.get("id")),
                    "type": json_event.get("type"),
                    "created_at": json_event.get("created_at"),
                    "public": json_event.get("public", False),
                    "actor_name": actor.get("login"),
                    "repo_name": json_event.get("repo", {}).get("name"),
                    "ts": json_event.get("created_at"),
                }

                validated_user = {
                    "id": str(actor_id),
                    "login": actor.get("login"),
                    "display_login": actor.get("display_login"),
                    "gravatar_id": actor.get("gravatar_id"),
                    "url": actor.get("url"),
                    "avatar_url": actor.get("avatar_url"),
                }

                if actor_id and actor_id not in seen_user_ids:
                    if schema_validator.validate_avro(schema_user_avro, validated_user):
                        producer_users.send(KAFKA_USERS_TOPIC, value=validated_user)
                        seen_user_ids.add(actor_id)
                        logging.debug(f"👤 Sent new user to Kafka: {validated_user}")
                    else:
                        logging.warning(f"⚠️ Invalid user schema: {validated_user}")

                if schema_validator.validate_avro(schema_event_avro, validated_event):
                    event_batch.append(validated_event)
                else:
                    logging.warning(f"⚠️ Invalid event schema: {validated_event}")

                if len(event_batch) >= BATCH_SIZE:
                    for record in event_batch:
                        try:
                            logging.debug(f"📤 Sending event to Kafka: {json.dumps(record)}")
                            producer_events.send(KAFKA_EVENTS_TOPIC, value=record)
                        except Exception as send_error:
                            logging.error(f"❌ Kafka send failed: {send_error}\nRecord: {json.dumps(record)}")

                    producer_events.flush()
                    logging.info(f"📦 Sent {len(event_batch)} GitHub events to Kafka.")
                    event_batch.clear()

            except json.JSONDecodeError as e:
                logging.warning(f"⚠️ JSON decode error: {e}")

    except requests.exceptions.ChunkedEncodingError:
        logging.info("✅ Finished pulling GitHub events (SSE connection closed normally).")

    except Exception as e:
        logging.error(f"❌ Unexpected error: {e}")

    finally:
        logging.info("🛑 Shutting down.")
        producer_events.close()
        producer_users.close()

if __name__ == "__main__":
    fetch_github_events()
