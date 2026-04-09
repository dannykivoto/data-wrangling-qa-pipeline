from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def file_sha256(path: str | Path) -> str:
    digest = sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_csv_rows(path: str | Path) -> int:
    with Path(path).open("r", encoding="utf-8") as handle:
        row_count = sum(1 for _ in handle)
    return max(row_count - 1, 0)


def build_source_file_manifest(path: str | Path, root: str | Path | None = None) -> dict[str, object]:
    file_path = Path(path)
    relative_path = file_path.relative_to(root).as_posix() if root else file_path.as_posix()
    return {
        "file_name": file_path.name,
        "relative_path": relative_path,
        "size_bytes": int(file_path.stat().st_size),
        "row_count": count_csv_rows(file_path),
        "sha256": file_sha256(file_path),
    }


def build_source_file_manifests(paths: list[Path], root: str | Path | None = None) -> list[dict[str, object]]:
    return [build_source_file_manifest(path, root=root) for path in paths]
