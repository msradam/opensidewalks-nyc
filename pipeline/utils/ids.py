"""Deterministic feature ID generation.

IDs are SHA-256 hashes of (feature_type, WKT geometry, source_id) truncated to
16 hex characters. Short enough to be readable, long enough to be collision-free
for a city-scale dataset.  Reruns with the same input produce identical IDs.
"""

import hashlib


def feature_id(geometry_wkt: str, feature_type: str, source_id: str) -> str:
    """Return a stable 16-char hex ID for a feature."""
    payload = f"{feature_type}|{source_id}|{geometry_wkt}"
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def node_id(lon: float, lat: float) -> str:
    """Return a stable node ID from coordinates rounded to 7 decimal places (~1 cm).

    Deliberately source-independent: two edges from different sources that share
    a geographic endpoint produce the same node ID and are topologically connected.
    Source provenance belongs in ext:source properties, not in the ID.
    """
    key = f"node|{lon:.7f}|{lat:.7f}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def edge_id(u_lon: float, u_lat: float, v_lon: float, v_lat: float,
            feature_type: str, source_id: str) -> str:
    """Return a stable edge ID from its endpoint coordinates."""
    key = f"edge|{feature_type}|{source_id}|{u_lon:.7f},{u_lat:.7f}|{v_lon:.7f},{v_lat:.7f}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]
