"""Read LiDAR returns around query points from an Entwine Point Tile (EPT) store.

An EPT store is an octree of LAZ files over plain HTTPS. Each node holds one
return per voxel of its cube (span voxels a side), so a node at depth 9 of the
NYC 2017 store (cube 47.5 km) has a return about every 0.7 m: enough to find
the surface a path lies on, at a few hundred kilobytes a node. Only the nodes
that contain a query point are fetched, and every file is cached on disk.
"""

import io
import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import requests
from scipy.spatial import cKDTree


class EptCloud:
    def __init__(self, url: str, cache_dir: Path, depth: int = 9):
        self.url = url.rstrip("/")
        self.cache = Path(cache_dir)
        self.cache.mkdir(parents=True, exist_ok=True)
        self.depth = depth
        meta = json.loads(self._get("ept.json"))
        self.bounds = meta["bounds"]
        self.conforming = meta["boundsConforming"]
        self.srs = meta["srs"]
        self.size = (self.bounds[3] - self.bounds[0]) / 2 ** depth
        self._hier: dict[str, dict] = {}

    def _get(self, rel: str) -> bytes:
        path = self.cache / rel
        if path.exists():
            return path.read_bytes()
        for attempt in range(5):
            try:
                resp = requests.get(f"{self.url}/{rel}", timeout=120)
                resp.raise_for_status()
                break
            except requests.RequestException:
                # Object stores drop a connection now and then under many
                # parallel reads.
                if attempt == 4:
                    raise
                time.sleep(2 ** attempt)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".part")
        tmp.write_bytes(resp.content)
        tmp.rename(path)
        return resp.content

    def _hierarchy(self, key: str) -> dict:
        if key not in self._hier:
            self._hier[key] = json.loads(self._get(f"ept-hierarchy/{key}.json"))
        return self._hier[key]

    def _existing(self, d: int, x: int, y: int, z: int) -> str | None:
        """The node d-x-y-z if the store has it, else its deepest ancestor
        that exists (sparse places, open water, stop above the asked depth)."""
        chain = [(d - k, x >> k, y >> k, z >> k) for k in range(d, -1, -1)]
        hier, found = self._hierarchy("0-0-0-0"), None
        for node in chain:
            key = "-".join(map(str, node))
            n = hier.get(key)
            if n is None:
                break
            if n == -1:
                hier = self._hierarchy(key)
                n = hier.get(key, 0)
            if n > 0:
                found = key
        return found

    def _points(self, key: str) -> np.ndarray:
        """(n, 4) x, y, z, classification of one node."""
        import laspy
        las = laspy.read(io.BytesIO(self._get(f"ept-data/{key}.laz")))
        return np.c_[las.x, las.y, las.z, np.asarray(las.classification)]

    def returns_near(self, xy: np.ndarray, radius: float,
                     z_range: tuple[float, float] = (-60.0, 200.0),
                     workers: int = 8) -> list[np.ndarray]:
        """For each query point, the returns within radius as (n, 2) rows of
        height and classification."""
        xy = np.asarray(xy, dtype=float)
        x0, y0, z0 = self.bounds[:3]
        s = self.size
        zs = range(int((z_range[0] - z0) // s), int((z_range[1] - z0) // s) + 1)

        # The cells a query disc touches: its own and any neighbour in reach.
        need: dict[tuple[int, int], list[int]] = {}
        for k, (px, py) in enumerate(xy):
            for cx in {int((px - radius - x0) // s), int((px + radius - x0) // s)}:
                for cy in {int((py - radius - y0) // s), int((py + radius - y0) // s)}:
                    need.setdefault((cx, cy), []).append(k)

        keys = {c: sorted({k for z in zs if (k := self._existing(self.depth, *c, z))})
                for c in need}
        with ThreadPoolExecutor(workers) as pool:
            list(pool.map(lambda k: self._get(f"ept-data/{k}.laz"),
                          sorted({k for ks in keys.values() for k in ks})))

        out: list[list[np.ndarray]] = [[] for _ in xy]
        for cell, queries in need.items():
            if not keys[cell]:
                continue
            pts = np.vstack([self._points(k) for k in keys[cell]])
            # An ancestor node covers more than this cell; keep the cell's part
            # so a return is not counted once per neighbouring cell.
            inside = ((pts[:, 0] >= x0 + cell[0] * s) & (pts[:, 0] < x0 + (cell[0] + 1) * s)
                      & (pts[:, 1] >= y0 + cell[1] * s) & (pts[:, 1] < y0 + (cell[1] + 1) * s))
            pts = pts[inside]
            if len(pts) == 0:
                continue
            tree = cKDTree(pts[:, :2])
            for k, hits in zip(queries, tree.query_ball_point(xy[queries], radius)):
                out[k].append(pts[hits, 2:])
        return [np.vstack(z) if z else np.empty((0, 2)) for z in out]
