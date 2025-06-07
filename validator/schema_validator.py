import asyncio
import json
import requests
import struct
import logging
import binascii

from io import BytesIO
from typing import Dict, Any, Callable, List

from kiota_abstractions.authentication.access_token_provider import AccessTokenProvider
from kiota_abstractions.authentication.base_bearer_token_authentication_provider import (
    BaseBearerTokenAuthenticationProvider,
)
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from apicurioregistrysdk.client.registry_client import RegistryClient

import fastavro
from fastavro import schemaless_writer, parse_schema
from avro.io import BinaryEncoder, DatumWriter

from jsonschema import validate as jsonschema_validate, ValidationError as JSONValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

MAGIC_BYTE = b'\x00'

class KeycloakAccessTokenProvider(AccessTokenProvider):
    def __init__(self, keycloak_url: str, client_id: str, client_secret: str, grant_type: str = "client_credentials"):
        self.keycloak_url = keycloak_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.grant_type = grant_type
        self.access_token = None

    def get_authorization_token(self, *scopes) -> str:
        if self.access_token:
            return self.access_token
            
        response = requests.post(self.keycloak_url, data={
            "grant_type": self.grant_type,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }, headers={
            "Content-Type": 
            "application/x-www-form-urlencoded"
        }
        )
        response.raise_for_status()
        self.access_token = response.json()["access_token"]
        return self.access_token

    def get_allowed_hosts_validator(self) -> Callable[[], bool]:
        return lambda: True

class SchemaValidator:
    def __init__(self, registry_url: str, keycloak_url: str, client_id: str, client_secret: str):
        self.registry_url = registry_url.rstrip("/")
        self.auth_provider = KeycloakAccessTokenProvider(keycloak_url, client_id, client_secret)
        self.schema_cache: Dict[str, Any] = {}
        self.global_id_cache: Dict[str, int] = {}
        self.parsed_schema_cache: Dict[str, Any] = {}

        self.request_adapter = HttpxRequestAdapter(BaseBearerTokenAuthenticationProvider(self.auth_provider))
        self.request_adapter.base_url = self.registry_url
        self.client = RegistryClient(self.request_adapter)

    def _get_token_headers(self) -> Dict[str, str]:
        access_token = self.auth_provider.get_authorization_token()
        return {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def _get_cached_schema(self, group: str, artifact_id: str, version: str):
        cache_key = f"{group}:{artifact_id}:{version}"

        if cache_key not in self.parsed_schema_cache:
            raw_schema = self.load_schema(group, artifact_id, version)
            parsed_schema = fastavro.parse_schema(raw_schema)
            self.parsed_schema_cache[cache_key] = parsed_schema
        else:
            parsed_schema = self.parsed_schema_cache[cache_key]

        global_id = self.get_global_id(group, artifact_id, version)
        return parsed_schema, global_id

    def load_schema(self, group: str, artifact_id: str, version: str) -> Dict[str, Any]:
        cache_key = f"{group}:{artifact_id}:{version}"
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]

        url = f"{self.registry_url}/groups/{group}/artifacts/{artifact_id}/versions/{version}/content"
        response = requests.get(url, headers=self._get_token_headers())
        response.raise_for_status()
        schema = response.json()
        self.schema_cache[cache_key] = schema
        logging.info(f"✅ Loaded schema {artifact_id} v{version}")
        return schema

    def get_global_id(self, group: str, artifact_id: str, version: str) -> int:
        cache_key = f"{group}:{artifact_id}:{version}"
        if cache_key in self.global_id_cache:
            return self.global_id_cache[cache_key]

        url = f"{self.registry_url}/groups/{group}/artifacts/{artifact_id}/versions/{version}"
        response = requests.get(url, headers=self._get_token_headers())
        response.raise_for_status()
        global_id = response.json()["globalId"]
        self.global_id_cache[cache_key] = global_id
        logging.info(f"🌍 Retrieved globalId {global_id} for {artifact_id} v{version}")
        return global_id

    def encode_avro(self, group_id, artifact_id, version, payload):
        parsed_schema, global_id = self._get_cached_schema(group_id, artifact_id, version)

        logging.debug(f"📘 Avro schema used: {json.dumps(parsed_schema)}")
        logging.debug(f"📤 Payload before encoding: {json.dumps(payload)}")

        out = BytesIO()
        try:
            schemaless_writer(out, parsed_schema, payload)
        except Exception as e:
            logging.error(f"❌ Avro schemaless_writer error: {e}")
            raise

        message = MAGIC_BYTE + struct.pack(">I", global_id) + out.getvalue()

        logging.info(f"🧪 Avro encoded preview: {binascii.hexlify(message[:20])}")
        return message

    def validate_avro(self, schema: Dict[str, Any], event: Dict[str, Any]) -> bool:
        try:
            parsed_schema = fastavro.parse_schema(schema)
            buf = BytesIO()
            schemaless_writer(buf, parsed_schema, event)
            return True
        except Exception as e:
            logging.error(f"❌ Invalid Avro record:\n{json.dumps(event, indent=2)}\nSchema:\n{json.dumps(schema, indent=2)}\nReason: {e}")
            return False

    def encode_json_schema(self, group_id, artifact_id, version, payload: Dict[str, Any]) -> bytes:
        schema = self.load_schema(group_id, artifact_id, version)
        global_id = self.get_global_id(group_id, artifact_id, version)

        json_bytes = json.dumps(payload).encode("utf-8")
        message = MAGIC_BYTE + struct.pack(">I", global_id) + json_bytes

        logging.debug(f"🔎 JSON encoded bytes preview: {binascii.hexlify(message[:20])}")
        return message

    def validate_json_schema(self, schema: Dict[str, Any], payload: Dict[str, Any]) -> bool:
        try:
            jsonschema_validate(instance=payload, schema=schema)
            return True
        except JSONValidationError as e:
            logging.error(f"❌ Invalid JSON record: {json.dumps(payload)}\nReason: {e.message}")
            return False
