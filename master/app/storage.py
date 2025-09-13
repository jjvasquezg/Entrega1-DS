import os
import pathlib
import shutil
from urllib.parse import urlparse
from typing import Optional, List
import httpx

def ensure_dir(p: str):
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)

def job_paths(shared_dir: str, job_id: str):
    base = os.path.join(shared_dir, "jobs", job_id)
    return {
        "base": base,
        "script": os.path.join(base, "script.py"),
        "input_dir": os.path.join(base, "input"),
        "input_file": os.path.join(base, "input", "input.data"),
        "chunks": os.path.join(base, "chunks"),
        "shuffle": os.path.join(base, "shuffle"),
        "out": os.path.join(base, "out"),
        "tmp": os.path.join(base, "tmp"),
        "result": os.path.join(base, "out", "result.txt"),
    }

async def download_to_file(url: str, dst_path: str, shared_dir: str):
    parsed = urlparse(url)
    # http(s)
    if parsed.scheme in ("http", "https"):
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.get(url)
            r.raise_for_status()
            ensure_dir(os.path.dirname(dst_path))
            with open(dst_path, "wb") as f:
                f.write(r.content)
        return dst_path

    normalized_path: Optional[str] = None
    if parsed.scheme in ("file", "nfs"):
        normalized_path = parsed.path
    elif parsed.scheme == "" and url.startswith("/"):
        normalized_path = url
    if normalized_path is None:
        raise ValueError(f"URL no soportada: {url}")

    normalized_path = os.path.abspath(normalized_path)
    shared_dir_abs = os.path.abspath(shared_dir)
    if not normalized_path.startswith(shared_dir_abs):
        raise PermissionError(f"La ruta {normalized_path} debe vivir bajo {shared_dir_abs}")

    ensure_dir(os.path.dirname(dst_path))
    shutil.copyfile(normalized_path, dst_path)
    return dst_path

def file_size(path: str) -> int:
    return os.path.getsize(path)

def partition_input_by_lines(input_file: str, chunks_dir: str, partitions: int) -> List[str]:
    """Partición simple round-robin por líneas."""
    ensure_dir(chunks_dir)
    writers = [open(os.path.join(chunks_dir, f"chunk-{i:04d}"), "w", encoding="utf-8") for i in range(partitions)]
    files = [w.name for w in writers]
    try:
        with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
            for idx, line in enumerate(f):
                writers[idx % partitions].write(line)
    finally:
        for w in writers:
            w.close()
    return files

def concat_files(files: List[str], dest_path: str):
    ensure_dir(os.path.dirname(dest_path))
    with open(dest_path, "w", encoding="utf-8") as out:
        for fp in files:
            with open(fp, "r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    out.write(line)
