from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 7),
    "retries": 0,
}

dag = DAG(
    "github_firehose_avro_pipeline",
    default_args=default_args,
    schedule_interval=timedelta(minutes=1),
    catchup=False,
)

def run_pipeline():
    import os
    import json
    import requests
    import sseclient
    import logging
    import struct
    import binascii
    from io import BytesIO

    from kafka import KafkaProducer
    from kafka.errors import KafkaError
    import fastavro
    from fastavro import schemaless_writer
    from apicurioregistrysdk.client.registry_client import RegistryClient
    from kiota_abstractions.authentication.base_bearer_token_authentication_provider import (
        BaseBearerTokenAuthenticationProvider,
    )
    from kiota_abstractions.authentication.access_token_provider import AccessTokenProvider
    from kiota_http.httpx_request_adapter import HttpxRequestAdapter

    MAGIC_BYTE = b"\x00"

    class KeycloakAccessTokenProvider(AccessTokenProvider):
        def __init__(self, keycloak_url, client_id, client_secret):
            self.keycloak_url = keycloak_url
            self.client_id = client_id
            self.client_secret = client_secret
            self.access_token = None

        def get_authorization_token(self, *scopes):
            if self.access_token:
                return self.access_token
            resp = requests.post(self.keycloak_url, data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }, headers={"Content-Type": "application/x-www-form-urlencoded"})
            resp.raise_for_status()
            self.access_token = resp.json()["access_token"]
            return self.access_token

        def get_allowed_hosts_validator(self):
            return lambda: True

    class SchemaValidator:
        def __init__(self, registry_url, keycloak_url, client_id, client_secret):
            self.registry_url = registry_url.rstrip("/")
            self.auth_provider = KeycloakAccessTokenProvider(keycloak_url, client_id, client_secret)
            self.schema_cache = {}
            self.global_id_cache = {}
            self.parsed_schema_cache = {}
            self.request_adapter = HttpxRequestAdapter(BaseBearerTokenAuthenticationProvider(self.auth_provider))
            self.request_adapter.base_url = self.registry_url
            self.client = RegistryClient(self.request_adapter)

        def _get_token_headers(self):
            return {
                "Authorization": f"Bearer {self.auth_provider.get_authorization_token()}",
                "Accept": "application/json",
            }

        def load_schema(self, group, artifact_id, version):
            key = f"{group}:{artifact_id}:{version}"
            if key not in self.schema_cache:
                url = f"{self.registry_url}/groups/{group}/artifacts/{artifact_id}/versions/{version}/content"
                resp = requests.get(url, headers=self._get_token_headers())
                resp.raise_for_status()
                self.schema_cache[key] = resp.json()
            return self.schema_cache[key]

        def get_global_id(self, group, artifact_id, version):
            key = f"{group}:{artifact_id}:{version}"
            if key not in self.global_id_cache:
                url = f"{self.registry_url}/groups/{group}/artifacts/{artifact_id}/versions/{version}"
                resp = requests.get(url, headers=self._get_token_headers())
                resp.raise_for_status()
                self.global_id_cache[key] = resp.json()["globalId"]
            return self.global_id_cache[key]

        def encode_avro(self, group, artifact, version, payload):
            cache_key = f"{group}:{artifact}:{version}"
            if cache_key not in self.parsed_schema_cache:
                raw_schema = self.load_schema(group, artifact, version)
                parsed = fastavro.parse_schema(raw_schema)
                self.parsed_schema_cache[cache_key] = parsed
            else:
                parsed = self.parsed_schema_cache[cache_key]
            global_id = self.get_global_id(group, artifact, version)
            out = BytesIO()
            schemaless_writer(out, parsed, payload)
            return MAGIC_BYTE + struct.pack(">I", global_id) + out.getvalue()

        def validate_avro(self, schema, payload):
            try:
                parsed = fastavro.parse_schema(schema)
                buf = BytesIO()
                schemaless_writer(buf, parsed, payload)
                return True
            except Exception:
                return False

    # === Static Config ===
    SSE_URL = "http://github-firehose.libraries.io/events"
    KAFKA_BOOTSTRAP = "kfk-github-kafka-bootstrap.env-bcizkp.svc.army.kubezt:9092"
    KAFKA_USERNAME = "kfk-u-github-ccravens"
    KAFKA_PASSWORD = "YZlyyngqdk1SQF4pQuTmmh8v5VFP9pAH"
    TOPIC_EVENTS = "kfk-t-github-events"
    TOPIC_USERS = "kfk-t-github-users"
    REGISTRY_URL = "http://acr-apicurio-registry-api.env-bcizkp.svc.army.kubezt:8080/apis/registry/v3"
    KEYCLOAK_URL = "https://idp.army.kubezt.com/realms/env-bcizkp/protocol/openid-connect/token"
    CLIENT_ID = "acr-apicurio-registry-api"
    CLIENT_SECRET = "tIINNmuAvWrBQa6fuPHUqcEzkgFeTe1S"
    BATCH_SIZE = 100

    schema_validator = SchemaValidator(REGISTRY_URL, KEYCLOAK_URL, CLIENT_ID, CLIENT_SECRET)

    producer_events = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda v: schema_validator.encode_avro(
            "com.analyticshq.github", "event", "1.0.0", v),
    )

    producer_users = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        value_serializer=lambda v: schema_validator.encode_avro(
            "com.analyticshq.github", "user", "1.0.0", v),
    )

    event_schema = schema_validator.load_schema("com.analyticshq.github", "event", "1.0.0")
    user_schema = schema_validator.load_schema("com.analyticshq.github", "user", "1.0.0")

    seen_users = set()
    batch = []

    resp = requests.get(SSE_URL, stream=True, headers={"Accept": "text/event-stream"})
    client = sseclient.SSEClient(resp)

    for msg in client.events():
        if not msg.data:
            continue
        try:
            raw = json.loads(msg.data)
            actor = raw.get("actor", {})
            actor_id = actor.get("id")

            user = {
                "id": str(actor_id),
                "login": actor.get("login"),
                "display_login": actor.get("display_login"),
                "gravatar_id": actor.get("gravatar_id"),
                "url": actor.get("url"),
                "avatar_url": actor.get("avatar_url"),
            }
            event = {
                "id": str(raw.get("id")),
                "type": raw.get("type"),
                "created_at": raw.get("created_at"),
                "public": raw.get("public"),
                "actor_name": actor.get("login"),
                "repo_name": raw.get("repo", {}).get("name"),
                "ts": raw.get("created_at"),
            }

            if actor_id and actor_id not in seen_users and schema_validator.validate_avro(user_schema, user):
                producer_users.send(TOPIC_USERS, value=user)
                seen_users.add(actor_id)

            if schema_validator.validate_avro(event_schema, event):
                batch.append(event)

            if len(batch) >= BATCH_SIZE:
                for e in batch:
                    producer_events.send(TOPIC_EVENTS, value=e)
                producer_events.flush()
                batch.clear()

        except json.JSONDecodeError:
            continue

    producer_users.close()
    producer_events.close()

fetch_task = PythonVirtualenvOperator(
    task_id="fetch_firehose_to_kafka_avro",
    python_callable=run_pipeline,
    requirements=[
        "requests==2.32.3",
        "sseclient-py==1.8.0",
        "kafka-python==2.1.5",
        "apicurioregistryclient==0.6.2",
        "apicurioregistrysdk==3.0.6",
        "microsoft-kiota-abstractions==1.9.3",
        "microsoft-kiota-http==1.9.3",
        "fastavro==1.10.0",
    ],
    system_site_packages=False,
    dag=dag,
)

fetch_task