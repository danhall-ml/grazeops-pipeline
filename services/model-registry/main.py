#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


def utc_now() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns
    -------
    str
        Current time formatted as ``YYYY-mm-ddTHH:MM:SSZ``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_registry(index_path: Path) -> dict[str, Any]:
    """Load model registry index from disk with backward compatibility.

    Parameters
    ----------
    index_path : Path
        Registry index JSON path.

    Returns
    -------
    dict[str, Any]
        Normalized registry payload with ``models`` and ``history`` keys.
    """
    empty = {"models": {}, "history": []}
    if not index_path.exists():
        return empty
    try:
        payload = json.loads(index_path.read_text(encoding="utf-8"))
    except Exception:
        return empty

    if not isinstance(payload, dict):
        return empty

    models = payload.get("models")
    history = payload.get("history")

    if isinstance(models, dict) and isinstance(history, list):
        return {
            "models": {str(k): v for k, v in models.items() if isinstance(v, dict)},
            "history": [x for x in history if isinstance(x, dict)],
        }

    # Backward compatibility with old format: top-level {version_id: record}
    old_models = {str(k): v for k, v in payload.items() if isinstance(v, dict)}
    old_history = sorted(old_models.values(), key=lambda x: str(x.get("updated_at") or ""))
    return {"models": old_models, "history": old_history}


def save_registry(index_path: Path, payload: dict[str, Any]) -> None:
    """Persist registry payload atomically.

    Parameters
    ----------
    index_path : Path
        Registry index JSON path.
    payload : dict[str, Any]
        Registry payload to save.
    """
    models = payload.get("models")
    history = payload.get("history")
    safe_payload = {
        "models": {} if not isinstance(models, dict) else models,
        "history": [] if not isinstance(history, list) else history,
    }
    tmp = index_path.with_suffix(index_path.suffix + ".tmp")
    tmp.write_text(json.dumps(safe_payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, index_path)


def sorted_latest_models(models: dict[str, Any]) -> list[dict[str, Any]]:
    """Sort latest model records for deterministic API output.

    Parameters
    ----------
    models : dict[str, Any]
        Mapping of version id to model record.

    Returns
    -------
    list[dict[str, Any]]
        Sorted latest model records.
    """
    items = [x for x in models.values() if isinstance(x, dict)]
    return sorted(
        items,
        key=lambda x: (
            str(x.get("version_id") or ""),
            str(x.get("registered_at") or x.get("updated_at") or ""),
        ),
    )


def sorted_history(history: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Sort model registration history records.

    Parameters
    ----------
    history : list[dict[str, Any]]
        Registry history entries.

    Returns
    -------
    list[dict[str, Any]]
        Sorted history entries.
    """
    return sorted(history, key=lambda x: str(x.get("registered_at") or x.get("updated_at") or ""))


def make_registration_id(version_id: str, config_version: str) -> str:
    """Create unique registration id for model history events.

    Parameters
    ----------
    version_id : str
        Model version identifier.
    config_version : str
        Configuration version identifier.

    Returns
    -------
    str
        Registration identifier.
    """
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{version_id}:{config_version}:{stamp}"


def make_handler(registry_dir: Path):
    """Create HTTP handler class for model registry service.

    Parameters
    ----------
    registry_dir : Path
        Directory that stores registry index file.

    Returns
    -------
    type[BaseHTTPRequestHandler]
        Request handler class for model registry routes.
    """
    index_path = registry_dir / "models.json"
    registry_dir.mkdir(parents=True, exist_ok=True)

    class RegistryHandler(BaseHTTPRequestHandler):
        """HTTP handler for model-registry routes."""

        server_version = "GrazeOpsModelRegistry/0.2"

        def _send(self, code: int, payload: dict[str, Any]) -> None:
            """Send JSON response with status code."""
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json_object(self) -> dict[str, Any]:
            """Read and validate JSON object body from request."""
            length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except Exception as exc:
                raise ValueError(f"invalid JSON body: {exc}") from exc
            if not isinstance(payload, dict):
                raise ValueError("JSON body must be an object")
            return payload

        def do_GET(self) -> None:  # noqa: N802
            """Handle model-registry GET endpoints."""
            parsed = urlparse(self.path)
            if parsed.path == "/health":
                self._send(
                    200,
                    {
                        "status": "ok",
                        "service": "model-registry",
                        "time": utc_now(),
                    },
                )
                return

            if parsed.path == "/models":
                registry = load_registry(index_path)
                items = sorted_latest_models(registry["models"])
                self._send(200, {"count": len(items), "models": items})
                return

            if parsed.path == "/models/history":
                query = parse_qs(parsed.query)
                version_filter = str((query.get("version_id") or [""])[0]).strip()
                history = sorted_history(load_registry(index_path)["history"])
                if version_filter:
                    history = [x for x in history if str(x.get("version_id") or "") == version_filter]
                self._send(200, {"count": len(history), "history": history})
                return

            self._send(404, {"error": f"unknown route: {parsed.path}"})

        def do_POST(self) -> None:  # noqa: N802
            """Handle model-registry POST endpoints."""
            parsed = urlparse(self.path)
            if parsed.path == "/models/register":
                try:
                    payload = self._read_json_object()
                except ValueError as exc:
                    self._send(400, {"error": str(exc)})
                    return

                version_id = str(payload.get("version_id") or "").strip()
                if not version_id:
                    self._send(400, {"error": "version_id is required"})
                    return

                parameters = payload.get("parameters")
                if not isinstance(parameters, dict):
                    parameters = {}

                config_version = str(payload.get("config_version") or "default")
                registered_at = utc_now()
                record = {
                    "version_id": version_id,
                    "config_version": config_version,
                    "parameters": parameters,
                    "registration_id": make_registration_id(version_id, config_version),
                    "registered_at": registered_at,
                    "updated_at": registered_at,
                }
                description = str(payload.get("description") or "").strip()
                if description:
                    record["description"] = description

                registry = load_registry(index_path)
                models = registry["models"]
                history = registry["history"]
                models[version_id] = record
                history.append(record)
                save_registry(index_path, registry)
                self._send(200, {"status": "registered", "model": record})
                return

            self._send(404, {"error": f"unknown route: {parsed.path}"})

        def log_message(self, fmt: str, *args: Any) -> None:
            """Emit structured handler log line."""
            print(f"[{utc_now()}] model-registry: " + (fmt % args))

    return RegistryHandler


def main() -> None:
    """Start model registry HTTP service."""
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    registry_dir = Path(os.getenv("REGISTRY_DIR", "/registry-data"))
    handler = make_handler(registry_dir)
    server = HTTPServer((host, port), handler)
    print(f"[{utc_now()}] model-registry: listening on {host}:{port} dir={registry_dir}")
    server.serve_forever()


if __name__ == "__main__":
    main()
