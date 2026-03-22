"""
Confluent Schema Registry Client
=================================

Production client for the Confluent Schema Registry supporting:
- Schema registration (Avro and JSON Schema)
- Schema retrieval by subject and version
- Compatibility checks before registration
- Schema evolution management
- Caching for repeated lookups

Usage:
    from schema_registry import SchemaRegistryClient

    client = SchemaRegistryClient("http://localhost:8081")
    schema_id = client.register_schema("user-events-value", avro_schema)
    schema = client.get_latest_schema("user-events-value")
"""

import json
import logging
import sys
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("schema_registry")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


class CompatibilityLevel(str, Enum):
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class SchemaRegistryClient:
    """Client for the Confluent Schema Registry REST API."""

    def __init__(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = url.rstrip("/")
        self._auth = auth
        self._timeout = timeout
        self._headers = {
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        }
        # Cache: subject -> {version -> (schema_id, schema_str)}
        self._cache: Dict[str, Dict[int, Tuple[int, str]]] = {}
        logger.info("SchemaRegistryClient initialised: url=%s", self._url)

    # ----- helpers --------------------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self._url}{path}"
        response = requests.request(
            method,
            url,
            headers=self._headers,
            auth=self._auth,
            timeout=self._timeout,
            **kwargs,
        )
        if response.status_code >= 400:
            logger.error(
                "Schema Registry %s %s returned %d: %s",
                method.upper(),
                path,
                response.status_code,
                response.text,
            )
        response.raise_for_status()
        return response

    # ----- Subjects -------------------------------------------------------

    def list_subjects(self) -> List[str]:
        """List all registered subjects."""
        response = self._request("GET", "/subjects")
        subjects = response.json()
        logger.info("Found %d subjects", len(subjects))
        return subjects

    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """Delete a subject (soft or permanent)."""
        params = {"permanent": "true"} if permanent else {}
        response = self._request("DELETE", f"/subjects/{subject}", params=params)
        return response.json()

    # ----- Registration ---------------------------------------------------

    def register_schema(
        self,
        subject: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
        references: Optional[List[Dict[str, str]]] = None,
    ) -> int:
        """Register a new schema under the given subject.

        Returns the schema ID assigned by the registry.
        """
        payload: Dict[str, Any] = {
            "schema": schema_str,
            "schemaType": schema_type.value,
        }
        if references:
            payload["references"] = references

        response = self._request(
            "POST", f"/subjects/{subject}/versions", json=payload
        )
        schema_id = response.json()["id"]
        logger.info(
            "Registered schema: subject=%s type=%s id=%d",
            subject,
            schema_type.value,
            schema_id,
        )
        return schema_id

    # ----- Retrieval ------------------------------------------------------

    def get_schema_by_id(self, schema_id: int) -> str:
        """Retrieve a schema by its global ID."""
        response = self._request("GET", f"/schemas/ids/{schema_id}")
        return response.json()["schema"]

    def get_latest_schema(self, subject: str) -> Dict[str, Any]:
        """Get the latest version of a schema for a subject.

        Returns a dict with keys: subject, version, id, schema, schemaType.
        """
        response = self._request("GET", f"/subjects/{subject}/versions/latest")
        data = response.json()

        # Cache
        version = data["version"]
        self._cache.setdefault(subject, {})[version] = (
            data["id"],
            data["schema"],
        )

        logger.info(
            "Retrieved latest schema: subject=%s version=%d id=%d",
            subject,
            version,
            data["id"],
        )
        return data

    def get_schema_by_version(self, subject: str, version: int) -> Dict[str, Any]:
        """Get a specific version of a schema for a subject."""
        # Check cache first
        if subject in self._cache and version in self._cache[subject]:
            schema_id, schema_str = self._cache[subject][version]
            return {
                "subject": subject,
                "version": version,
                "id": schema_id,
                "schema": schema_str,
            }

        response = self._request(
            "GET", f"/subjects/{subject}/versions/{version}"
        )
        data = response.json()

        self._cache.setdefault(subject, {})[version] = (
            data["id"],
            data["schema"],
        )
        return data

    def get_all_versions(self, subject: str) -> List[int]:
        """List all version numbers for a subject."""
        response = self._request("GET", f"/subjects/{subject}/versions")
        return response.json()

    # ----- Compatibility --------------------------------------------------

    def check_compatibility(
        self,
        subject: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
        version: str = "latest",
    ) -> bool:
        """Check if a schema is compatible with a specific version.

        Returns True if compatible, False otherwise.
        """
        payload = {
            "schema": schema_str,
            "schemaType": schema_type.value,
        }
        try:
            response = self._request(
                "POST",
                f"/compatibility/subjects/{subject}/versions/{version}",
                json=payload,
            )
            is_compatible = response.json().get("is_compatible", False)
            logger.info(
                "Compatibility check: subject=%s version=%s compatible=%s",
                subject,
                version,
                is_compatible,
            )
            return is_compatible
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 404:
                logger.info(
                    "No existing schema for subject=%s — compatibility check N/A",
                    subject,
                )
                return True
            raise

    def get_compatibility_level(self, subject: Optional[str] = None) -> str:
        """Get the compatibility level (global or per-subject)."""
        path = f"/config/{subject}" if subject else "/config"
        response = self._request("GET", path)
        return response.json().get("compatibilityLevel", "UNKNOWN")

    def set_compatibility_level(
        self,
        level: CompatibilityLevel,
        subject: Optional[str] = None,
    ) -> str:
        """Set the compatibility level (global or per-subject)."""
        path = f"/config/{subject}" if subject else "/config"
        payload = {"compatibility": level.value}
        response = self._request("PUT", path, json=payload)
        result = response.json().get("compatibility", "UNKNOWN")
        logger.info(
            "Set compatibility: subject=%s level=%s",
            subject or "GLOBAL",
            result,
        )
        return result

    # ----- Convenience: register with compatibility pre-check -------------

    def safe_register(
        self,
        subject: str,
        schema_str: str,
        schema_type: SchemaType = SchemaType.AVRO,
    ) -> int:
        """Check compatibility before registering; raise on incompatibility."""
        if not self.check_compatibility(subject, schema_str, schema_type):
            raise ValueError(
                f"Schema is not compatible with subject '{subject}'. "
                "Update the schema or change the compatibility level."
            )
        return self.register_schema(subject, schema_str, schema_type)


# ---------------------------------------------------------------------------
# Example schemas
# ---------------------------------------------------------------------------
USER_EVENT_AVRO_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "UserEvent",
        "namespace": "com.example.events",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "user_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "event_timestamp", "type": "string"},
            {
                "name": "event_properties",
                "type": ["null", {"type": "map", "values": "string"}],
                "default": None,
            },
            {"name": "session_id", "type": ["null", "string"], "default": None},
            {"name": "platform", "type": ["null", "string"], "default": None},
            {"name": "app_version", "type": ["null", "string"], "default": None},
        ],
    }
)

USER_EVENT_JSON_SCHEMA = json.dumps(
    {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "UserEvent",
        "type": "object",
        "required": ["event_id", "user_id", "event_type", "event_timestamp"],
        "properties": {
            "event_id": {"type": "string"},
            "user_id": {"type": "string"},
            "event_type": {
                "type": "string",
                "enum": [
                    "page_view",
                    "click",
                    "purchase",
                    "signup",
                    "logout",
                    "search",
                    "add_to_cart",
                    "remove_from_cart",
                    "checkout",
                ],
            },
            "event_timestamp": {"type": "string", "format": "date-time"},
            "event_properties": {"type": "object"},
            "session_id": {"type": ["string", "null"]},
            "platform": {"type": ["string", "null"]},
            "app_version": {"type": ["string", "null"]},
        },
    }
)
