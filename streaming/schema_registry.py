"""
Schema Registry Client

Manages schema registration, evolution validation, versioning, and caching
for Avro and JSON schemas. Wraps the Confluent Schema Registry REST API.

Features:
- Register new schemas (Avro and JSON)
- Schema evolution validation (BACKWARD, FORWARD, FULL, NONE)
- Thread-safe local schema cache
- Version management (get specific or latest)
- Compatibility checking before registration

Usage:
    from schema_registry import SchemaRegistryClient, SchemaType

    client = SchemaRegistryClient("http://schema-registry:8081")
    schema_id = client.register_schema("events-value", avro_schema, SchemaType.AVRO)
    is_compatible = client.check_compatibility("events-value", new_schema)
"""

import json
import logging
import threading
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------
class SchemaType(str, Enum):
    AVRO = "AVRO"
    JSON = "JSON"
    PROTOBUF = "PROTOBUF"


class CompatibilityLevel(str, Enum):
    NONE = "NONE"
    BACKWARD = "BACKWARD"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD = "FORWARD"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL = "FULL"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"


class SchemaRegistryError(Exception):
    """Base exception for Schema Registry operations."""

    def __init__(self, message: str, error_code: Optional[int] = None, status_code: Optional[int] = None):
        super().__init__(message)
        self.error_code = error_code
        self.status_code = status_code


class SchemaNotFoundError(SchemaRegistryError):
    pass


class IncompatibleSchemaError(SchemaRegistryError):
    pass


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------
class SchemaCache:
    """Thread-safe in-memory schema cache."""

    def __init__(self):
        self._by_id: Dict[int, Dict[str, Any]] = {}
        self._by_subject_version: Dict[Tuple[str, int], Dict[str, Any]] = {}
        self._latest_by_subject: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def get_by_id(self, schema_id: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._by_id.get(schema_id)

    def get_by_subject_version(self, subject: str, version: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._by_subject_version.get((subject, version))

    def get_latest(self, subject: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._latest_by_subject.get(subject)

    def put(self, schema_id: int, subject: str, version: int, schema: Dict[str, Any]) -> None:
        with self._lock:
            entry = {
                "schema_id": schema_id,
                "subject": subject,
                "version": version,
                "schema": schema,
            }
            self._by_id[schema_id] = entry
            self._by_subject_version[(subject, version)] = entry
            # Update latest if this version is newer
            current_latest = self._latest_by_subject.get(subject)
            if current_latest is None or version > current_latest["version"]:
                self._latest_by_subject[subject] = entry

    def invalidate(self, subject: Optional[str] = None) -> None:
        with self._lock:
            if subject is None:
                self._by_id.clear()
                self._by_subject_version.clear()
                self._latest_by_subject.clear()
            else:
                self._latest_by_subject.pop(subject, None)
                keys_to_remove = [k for k in self._by_subject_version if k[0] == subject]
                for key in keys_to_remove:
                    entry = self._by_subject_version.pop(key, None)
                    if entry:
                        self._by_id.pop(entry["schema_id"], None)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class SchemaRegistryClient:
    """
    Client for the Confluent Schema Registry.

    Provides schema registration, retrieval, compatibility checking,
    and version management with local caching.
    """

    def __init__(
        self,
        url: str,
        auth: Optional[Tuple[str, str]] = None,
        timeout: int = 30,
        max_retries: int = 3,
        cache_enabled: bool = True,
    ):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._cache = SchemaCache() if cache_enabled else None

        self._session = requests.Session()
        if auth:
            self._session.auth = auth

        retry = Retry(total=max_retries, backoff_factor=0.5, status_forcelist=[500, 502, 503])
        adapter = HTTPAdapter(max_retries=retry)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        self._session.headers.update({
            "Content-Type": "application/vnd.schemaregistry.v1+json",
            "Accept": "application/vnd.schemaregistry.v1+json",
        })

        logger.info("SchemaRegistryClient initialized: url=%s", self.url)

    def _request(self, method: str, path: str, body: Optional[dict] = None) -> Dict[str, Any]:
        """Make an HTTP request to the Schema Registry."""
        url = f"{self.url}{path}"
        try:
            response = self._session.request(
                method, url, json=body, timeout=self.timeout,
            )
        except requests.exceptions.RequestException as exc:
            raise SchemaRegistryError(f"Request failed: {exc}") from exc

        if response.status_code == 404:
            data = response.json() if response.content else {}
            raise SchemaNotFoundError(
                data.get("message", "Not found"),
                error_code=data.get("error_code"),
                status_code=404,
            )

        if response.status_code == 409:
            data = response.json() if response.content else {}
            raise IncompatibleSchemaError(
                data.get("message", "Schema is incompatible"),
                error_code=data.get("error_code"),
                status_code=409,
            )

        if response.status_code >= 400:
            data = response.json() if response.content else {}
            raise SchemaRegistryError(
                data.get("message", f"HTTP {response.status_code}"),
                error_code=data.get("error_code"),
                status_code=response.status_code,
            )

        return response.json() if response.content else {}

    # ---- Subjects --------------------------------------------------------

    def list_subjects(self) -> List[str]:
        """List all registered subjects."""
        return self._request("GET", "/subjects")

    def delete_subject(self, subject: str, permanent: bool = False) -> List[int]:
        """Delete a subject and all its versions."""
        path = f"/subjects/{subject}"
        if permanent:
            path += "?permanent=true"
        result = self._request("DELETE", path)
        if self._cache:
            self._cache.invalidate(subject)
        return result

    # ---- Registration ----------------------------------------------------

    def register_schema(
        self,
        subject: str,
        schema: str,
        schema_type: SchemaType = SchemaType.AVRO,
        references: Optional[List[Dict[str, str]]] = None,
    ) -> int:
        """
        Register a schema under a subject.

        Returns the schema ID. If the schema already exists, returns the existing ID.
        """
        body = {
            "schema": schema,
            "schemaType": schema_type.value,
        }
        if references:
            body["references"] = references

        result = self._request("POST", f"/subjects/{subject}/versions", body)
        schema_id = result["id"]

        logger.info("Registered schema for '%s': id=%d, type=%s", subject, schema_id, schema_type.value)

        if self._cache:
            self._cache.invalidate(subject)

        return schema_id

    # ---- Retrieval -------------------------------------------------------

    def get_schema_by_id(self, schema_id: int) -> Dict[str, Any]:
        """Get a schema by its global ID."""
        if self._cache:
            cached = self._cache.get_by_id(schema_id)
            if cached:
                return cached["schema"]

        result = self._request("GET", f"/schemas/ids/{schema_id}")
        return result

    def get_schema(self, subject: str, version: str = "latest") -> Dict[str, Any]:
        """
        Get a specific version of a schema for a subject.

        Parameters
        ----------
        subject : str
            Subject name (e.g., "events-value").
        version : str or int
            Schema version number or "latest".
        """
        if self._cache and version == "latest":
            cached = self._cache.get_latest(subject)
            if cached:
                return cached

        result = self._request("GET", f"/subjects/{subject}/versions/{version}")

        if self._cache:
            self._cache.put(
                schema_id=result["id"],
                subject=subject,
                version=result["version"],
                schema=result,
            )

        return result

    def get_versions(self, subject: str) -> List[int]:
        """Get all version numbers for a subject."""
        return self._request("GET", f"/subjects/{subject}/versions")

    def get_latest_version(self, subject: str) -> Dict[str, Any]:
        """Get the latest schema version for a subject."""
        return self.get_schema(subject, "latest")

    # ---- Compatibility ---------------------------------------------------

    def check_compatibility(
        self,
        subject: str,
        schema: str,
        schema_type: SchemaType = SchemaType.AVRO,
        version: str = "latest",
        verbose: bool = True,
    ) -> bool:
        """
        Check if a schema is compatible with the specified version.

        Returns True if compatible, False otherwise.
        """
        body = {
            "schema": schema,
            "schemaType": schema_type.value,
        }

        path = f"/compatibility/subjects/{subject}/versions/{version}"
        if verbose:
            path += "?verbose=true"

        try:
            result = self._request("POST", path, body)
            is_compatible = result.get("is_compatible", False)

            if not is_compatible and verbose:
                messages = result.get("messages", [])
                logger.warning(
                    "Schema incompatible for '%s': %s", subject, messages,
                )

            return is_compatible

        except SchemaNotFoundError:
            # No existing schema -- always compatible
            return True

    def get_compatibility_level(self, subject: Optional[str] = None) -> str:
        """Get the compatibility level for a subject or the global default."""
        if subject:
            path = f"/config/{subject}"
        else:
            path = "/config"

        result = self._request("GET", path)
        return result.get("compatibilityLevel", "NONE")

    def set_compatibility_level(
        self,
        level: CompatibilityLevel,
        subject: Optional[str] = None,
    ) -> str:
        """
        Set the compatibility level for a subject or globally.

        Parameters
        ----------
        level : CompatibilityLevel
            NONE, BACKWARD, FORWARD, FULL, or their TRANSITIVE variants.
        subject : str, optional
            If None, sets the global default.
        """
        if subject:
            path = f"/config/{subject}"
        else:
            path = "/config"

        body = {"compatibility": level.value}
        result = self._request("PUT", path, body)

        logger.info(
            "Set compatibility level: subject=%s, level=%s",
            subject or "GLOBAL", level.value,
        )

        return result.get("compatibility", level.value)

    # ---- Convenience Methods ---------------------------------------------

    def register_avro_schema(self, subject: str, schema_dict: Dict[str, Any]) -> int:
        """Register an Avro schema from a Python dictionary."""
        schema_str = json.dumps(schema_dict)
        return self.register_schema(subject, schema_str, SchemaType.AVRO)

    def register_json_schema(self, subject: str, schema_dict: Dict[str, Any]) -> int:
        """Register a JSON schema from a Python dictionary."""
        schema_str = json.dumps(schema_dict)
        return self.register_schema(subject, schema_str, SchemaType.JSON)

    def evolve_schema(
        self,
        subject: str,
        new_schema: str,
        schema_type: SchemaType = SchemaType.AVRO,
        compatibility_override: Optional[CompatibilityLevel] = None,
    ) -> Optional[int]:
        """
        Evolve a schema: check compatibility, then register if compatible.

        Optionally override compatibility level for this operation.

        Returns the new schema ID if registered, None if incompatible.
        """
        original_level = None

        try:
            if compatibility_override:
                original_level = self.get_compatibility_level(subject)
                self.set_compatibility_level(compatibility_override, subject)

            if self.check_compatibility(subject, new_schema, schema_type):
                schema_id = self.register_schema(subject, new_schema, schema_type)
                logger.info("Schema evolved for '%s': new id=%d", subject, schema_id)
                return schema_id
            else:
                logger.warning("Schema evolution rejected for '%s': incompatible.", subject)
                return None

        finally:
            if original_level and compatibility_override:
                self.set_compatibility_level(CompatibilityLevel(original_level), subject)

    def close(self) -> None:
        """Close the HTTP session."""
        self._session.close()


# ---------------------------------------------------------------------------
# Predefined Schemas
# ---------------------------------------------------------------------------
EVENTS_AVRO_SCHEMA = {
    "type": "record",
    "name": "Event",
    "namespace": "com.dataengineering.events",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": {"type": "enum", "name": "EventType", "symbols": [
            "page_view", "click", "scroll", "purchase", "signup", "login", "logout",
        ]}},
        {"name": "user_id", "type": ["null", "string"], "default": None},
        {"name": "session_id", "type": ["null", "string"], "default": None},
        {"name": "timestamp", "type": "string"},
        {"name": "page_url", "type": ["null", "string"], "default": None},
        {"name": "referrer_url", "type": ["null", "string"], "default": None},
        {"name": "device_type", "type": ["null", "string"], "default": None},
        {"name": "os", "type": ["null", "string"], "default": None},
        {"name": "browser", "type": ["null", "string"], "default": None},
        {"name": "country", "type": ["null", "string"], "default": None},
        {"name": "city", "type": ["null", "string"], "default": None},
        {"name": "duration_ms", "type": ["null", "long"], "default": None},
        {"name": "revenue", "type": ["null", "double"], "default": None},
        {"name": "is_conversion", "type": ["null", "boolean"], "default": None},
    ],
}
