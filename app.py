import asyncio
import json
import mimetypes
import shutil
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Optional

import aiofiles
from fastapi import BackgroundTasks, FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).parent / "config.json"

DEFAULT_CONFIG = {
    "mc_path": "mc",
    "default_alias": "",
    "request_timeout": 30,
    "upload_timeout": 300,
}

_config: dict = {}


def load_config() -> dict:
    global _config
    if CONFIG_PATH.exists():
        try:
            _config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            _config = dict(DEFAULT_CONFIG)
    else:
        _config = dict(DEFAULT_CONFIG)
    # fill missing keys with defaults
    for k, v in DEFAULT_CONFIG.items():
        _config.setdefault(k, v)
    return _config


def save_config(data: dict) -> None:
    global _config
    _config.update(data)
    CONFIG_PATH.write_text(json.dumps(_config, indent=2), encoding="utf-8")


load_config()

# ---------------------------------------------------------------------------
# mc helpers
# ---------------------------------------------------------------------------


class MCError(Exception):
    pass


def mc_path(alias: str, bucket: str = "", prefix: str = "") -> str:
    parts = [alias.rstrip("/")]
    if bucket:
        parts.append(bucket.strip("/"))
    if prefix:
        parts.append(prefix.lstrip("/"))
    return "/".join(parts)


async def run_mc(args: list[str], timeout: int | None = None) -> list[dict]:
    if timeout is None:
        timeout = _config.get("request_timeout", 30)
    mc_bin = _config.get("mc_path", "mc")

    # On Windows, resolve mc/mc.exe via shutil.which so subprocess can find it
    resolved = shutil.which(mc_bin)
    if resolved is None and sys.platform == "win32" and not mc_bin.endswith(".exe"):
        resolved = shutil.which(mc_bin + ".exe")
    if resolved:
        mc_bin = resolved

    try:
        proc = await asyncio.create_subprocess_exec(
            mc_bin, "--json", *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except FileNotFoundError:
        raise MCError(f"mc not found at '{mc_bin}'. Check Settings.")
    except asyncio.TimeoutError:
        proc.kill()
        raise MCError(f"mc command timed out after {timeout}s.")

    results = []
    for line in stdout.decode("utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        results.append(obj)

    if proc.returncode != 0 and not results:
        raise MCError(stderr.decode("utf-8", errors="replace").strip() or "mc command failed")

    # surface error objects from NDJSON
    errors = [r for r in results if r.get("status") == "error"]
    if errors and proc.returncode != 0:
        raise MCError(_error_message(errors[0]))

    return results


def _error_message(obj: dict) -> str:
    """Build a readable message from an mc JSON error, including its cause."""
    err = obj.get("error")
    if isinstance(err, str):
        return err
    if not isinstance(err, dict):
        return "Unknown mc error"
    msg = err.get("message") or "Unknown mc error"
    cause = err.get("cause")
    cause_msg = cause.get("message") if isinstance(cause, dict) else cause
    if cause_msg and cause_msg not in msg:
        msg = f"{msg} {cause_msg}"
    return msg


def mc_error_response(e: MCError):
    msg = str(e)
    lower = msg.lower()
    if "access denied" in lower:
        status = 403
    elif "does not exist" in lower or "not found" in lower:
        status = 404
    else:
        status = 500
    raise HTTPException(status_code=status, detail=msg)


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(title="MinIO Manager")


@app.middleware("http")
async def no_cache_frontend(request, call_next):
    # Make the browser revalidate the UI on every load so edits show up immediately
    response = await call_next(request)
    if not request.url.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-cache"
    return response


# ---------------------------------------------------------------------------
# Config endpoints
# ---------------------------------------------------------------------------

@app.get("/api/config")
async def get_config():
    return _config


@app.post("/api/config")
async def post_config(data: dict):
    save_config(data)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Alias endpoints
# ---------------------------------------------------------------------------

@app.get("/api/aliases")
async def list_aliases():
    try:
        results = await run_mc(["alias", "list"])
        # mc alias list --json returns objects with "alias", "URL", "accessKey"
        return results
    except MCError as e:
        mc_error_response(e)


@app.post("/api/aliases")
async def create_alias(data: dict):
    name = data.get("name", "").strip()
    url = data.get("url", "").strip()
    access_key = data.get("accessKey", "").strip()
    secret_key = data.get("secretKey", "").strip()
    if not all([name, url, access_key, secret_key]):
        raise HTTPException(status_code=400, detail="name, url, accessKey, secretKey required")
    try:
        await run_mc(["alias", "set", name, url, access_key, secret_key])
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


@app.delete("/api/aliases/{name}")
async def delete_alias(name: str):
    try:
        await run_mc(["alias", "remove", name])
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


# ---------------------------------------------------------------------------
# Bucket endpoints
# ---------------------------------------------------------------------------

@app.get("/api/buckets")
async def list_buckets(alias: str):
    try:
        results = await run_mc(["ls", f"{alias}/"])
        buckets = [r for r in results if r.get("status") == "success"]
        return buckets
    except MCError as e:
        mc_error_response(e)


@app.post("/api/buckets")
async def create_bucket(data: dict):
    alias = data.get("alias", "").strip()
    name = data.get("name", "").strip()
    if not alias or not name:
        raise HTTPException(status_code=400, detail="alias and name required")
    try:
        await run_mc(["mb", mc_path(alias, name)])
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


@app.post("/api/buckets/rename")
async def rename_bucket(data: dict):
    """S3 has no rename: create the new bucket, mirror objects, copy the
    anonymous policy, and optionally remove the old bucket."""
    alias = data.get("alias", "").strip()
    old = data.get("name", "").strip().strip("/")
    new = data.get("newName", "").strip().strip("/")
    delete_old = bool(data.get("deleteOld", False))
    if not alias or not old or not new:
        raise HTTPException(status_code=400, detail="alias, name and newName required")
    if old == new:
        raise HTTPException(status_code=400, detail="New name is the same as the old one")

    src, dst = mc_path(alias, old), mc_path(alias, new)
    timeout = _config.get("upload_timeout", 300)
    try:
        await run_mc(["mb", dst])
        await run_mc(["mirror", src, dst], timeout=timeout)

        policy_copied = False
        try:
            res = await run_mc(["anonymous", "get-json", src])
            policy = res[0].get("anonymous") if res else None
            if policy and policy.get("Statement"):
                text = json.dumps(policy).replace(
                    f"arn:aws:s3:::{old}/", f"arn:aws:s3:::{new}/"
                ).replace(f'"arn:aws:s3:::{old}"', f'"arn:aws:s3:::{new}"')
                tmp = Path(tempfile.gettempdir()) / f"minio_policy_{uuid.uuid4().hex}.json"
                try:
                    tmp.write_text(text, encoding="utf-8")
                    await run_mc(["anonymous", "set-json", str(tmp), dst])
                finally:
                    tmp.unlink(missing_ok=True)
                policy_copied = True
        except MCError:
            pass  # bucket had no policy or we can't read it; data copy still succeeded

        if delete_old:
            await run_mc(["rb", "--force", src], timeout=timeout)
        return {"status": "ok", "policyCopied": policy_copied, "oldDeleted": delete_old}
    except MCError as e:
        mc_error_response(e)


@app.delete("/api/buckets/{name}")
async def delete_bucket(name: str, alias: str, force: bool = False):
    args = ["rb"]
    if force:
        args.append("--force")
    args.append(mc_path(alias, name))
    try:
        await run_mc(args)
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


# ---------------------------------------------------------------------------
# Object endpoints
# ---------------------------------------------------------------------------

@app.get("/api/objects")
async def list_objects(alias: str, bucket: str, prefix: str = ""):
    path = mc_path(alias, bucket, prefix)
    try:
        results = await run_mc(["ls", path])
        objects = [r for r in results if r.get("status") == "success"]
        return objects
    except MCError as e:
        mc_error_response(e)


@app.get("/api/stat")
async def stat_object(alias: str, bucket: str, key: str):
    path = mc_path(alias, bucket, key)
    try:
        results = await run_mc(["stat", path])
        return results[0] if results else {}
    except MCError:
        # stat is best-effort; return empty so the detail panel still works
        return {}


@app.delete("/api/objects")
async def delete_object(data: dict):
    alias = data.get("alias", "")
    bucket = data.get("bucket", "")
    key = data.get("key", "")
    recursive = data.get("recursive", False)
    if not all([alias, bucket, key]):
        raise HTTPException(status_code=400, detail="alias, bucket, key required")
    args = ["rm"]
    if recursive:
        args += ["--recursive", "--force"]
    args.append(mc_path(alias, bucket, key))
    try:
        await run_mc(args)
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


async def _copy_or_move(cmd: str, data: dict):
    src = data.get("src", "")
    dst = data.get("dst", "")
    if not src or not dst:
        raise HTTPException(status_code=400, detail="src and dst required")
    args = [cmd]
    if src.endswith("/"):  # folders need --recursive
        args.append("--recursive")
    args += [src, dst]
    try:
        await run_mc(args, timeout=_config.get("upload_timeout", 300))
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


@app.post("/api/copy")
async def copy_object(data: dict):
    return await _copy_or_move("cp", data)


@app.post("/api/move")
async def move_object(data: dict):
    return await _copy_or_move("mv", data)


# ---------------------------------------------------------------------------
# Upload / Download
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def upload(
    alias: str = Form(...),
    bucket: str = Form(...),
    prefix: str = Form(""),
    file: UploadFile = File(...),
):
    filename = Path(file.filename).name  # sanitize
    tmp_path = Path(tempfile.gettempdir()) / f"minio_up_{uuid.uuid4().hex}_{filename}"
    try:
        async with aiofiles.open(tmp_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                await f.write(chunk)
        dest = mc_path(alias, bucket, prefix + filename)
        timeout = _config.get("upload_timeout", 300)
        await run_mc(["cp", str(tmp_path), dest], timeout=timeout)
        return {"status": "ok", "key": prefix + filename}
    except MCError as e:
        mc_error_response(e)
    finally:
        tmp_path.unlink(missing_ok=True)


@app.get("/api/download")
async def download(alias: str, bucket: str, key: str, background_tasks: BackgroundTasks):
    filename = key.split("/")[-1]
    tmp_path = Path(tempfile.gettempdir()) / f"minio_dl_{uuid.uuid4().hex}_{filename}"
    try:
        timeout = _config.get("upload_timeout", 300)
        await run_mc(["cp", mc_path(alias, bucket, key), str(tmp_path)], timeout=timeout)
    except MCError as e:
        mc_error_response(e)

    async def file_generator():
        async with aiofiles.open(tmp_path, "rb") as f:
            while chunk := await f.read(65536):
                yield chunk

    background_tasks.add_task(lambda: tmp_path.unlink(missing_ok=True))
    return StreamingResponse(
        file_generator(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/preview")
async def preview(alias: str, bucket: str, key: str, background_tasks: BackgroundTasks):
    filename = key.split("/")[-1]
    tmp_path = Path(tempfile.gettempdir()) / f"minio_pv_{uuid.uuid4().hex}_{filename}"
    try:
        timeout = _config.get("upload_timeout", 300)
        await run_mc(["cp", mc_path(alias, bucket, key), str(tmp_path)], timeout=timeout)
    except MCError as e:
        mc_error_response(e)

    ext = Path(filename).suffix.lower()
    TEXT_EXTS = {".txt", ".md", ".log", ".csv", ".json", ".yaml", ".yml",
                 ".toml", ".xml", ".html", ".htm", ".js", ".ts", ".py",
                 ".go", ".rs", ".sh", ".bat", ".css", ".ini", ".cfg", ".env"}
    if ext in TEXT_EXTS:
        max_bytes = 2 * 1024 * 1024
        with open(tmp_path, "rb") as fh:
            content = fh.read(max_bytes + 1)
        tmp_path.unlink(missing_ok=True)
        if len(content) > max_bytes:
            content = content[:max_bytes] + b"\n\n... [truncated: file larger than 2 MB, download to see all]"
        mime = mimetypes.guess_type(filename)[0] or "text/plain"
        return Response(content=content, media_type=mime)

    MIME_MAP = {
        ".webp": "image/webp", ".avif": "image/avif", ".gif": "image/gif",
        ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".svg": "image/svg+xml", ".bmp": "image/bmp", ".ico": "image/x-icon",
        ".mp4": "video/mp4", ".webm": "video/webm", ".mov": "video/quicktime",
        ".avi": "video/x-msvideo", ".mkv": "video/x-matroska",
        ".mp3": "audio/mpeg", ".wav": "audio/wav", ".ogg": "audio/ogg",
        ".flac": "audio/flac", ".aac": "audio/aac", ".m4a": "audio/mp4",
        ".pdf": "application/pdf",
    }
    mime = MIME_MAP.get(ext) or mimetypes.guess_type(filename)[0] or "application/octet-stream"

    async def file_generator():
        async with aiofiles.open(tmp_path, "rb") as f:
            while chunk := await f.read(65536):
                yield chunk

    background_tasks.add_task(lambda: tmp_path.unlink(missing_ok=True))
    return StreamingResponse(
        file_generator(),
        media_type=mime,
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Policy endpoints
# ---------------------------------------------------------------------------

POLICY_LEVELS = ("none", "download", "upload", "public")


@app.get("/api/policy")
async def get_policy(alias: str, bucket: str, prefix: str = "", detail: bool = False):
    """Return the anonymous access level for a bucket (or folder prefix).

    With detail=true, also return the full bucket policy JSON and the list of
    per-prefix rules so the UI can show exactly what is in effect."""
    path = mc_path(alias, bucket, prefix)
    try:
        results = await run_mc(["anonymous", "get", path])
        info = results[0] if results else {}
        out = {"permission": info.get("permission", "none")}
        if detail:
            out["policy"] = info.get("anonymous") or None
            try:
                rules = await run_mc(["anonymous", "list", mc_path(alias, bucket)])
                out["rules"] = [r for r in rules if "resource" in r]
            except MCError:
                out["rules"] = []
        return out
    except MCError as e:
        mc_error_response(e)


@app.post("/api/policy")
async def set_policy(data: dict):
    alias = data.get("alias", "")
    bucket = data.get("bucket", "")
    prefix = data.get("prefix", "")     # optional folder prefix, e.g. "uploads/"
    level = data.get("level", "")       # none | download | upload | public
    custom_json = data.get("customJson", "")

    if not alias or not bucket:
        raise HTTPException(status_code=400, detail="alias and bucket required")
    if prefix and custom_json:
        raise HTTPException(
            status_code=400,
            detail="Custom JSON replaces the whole bucket policy; set it at bucket level, not on a folder",
        )

    path = mc_path(alias, bucket, prefix)
    try:
        if custom_json:
            try:
                parsed = json.loads(custom_json)
            except json.JSONDecodeError as je:
                raise HTTPException(status_code=400, detail=f"Invalid JSON: {je}")
            if not isinstance(parsed, dict) or "Statement" not in parsed:
                raise HTTPException(status_code=400, detail="Policy JSON must contain a Statement list")
            tmp_path = Path(tempfile.gettempdir()) / f"minio_policy_{uuid.uuid4().hex}.json"
            try:
                tmp_path.write_text(custom_json, encoding="utf-8")
                await run_mc(["anonymous", "set-json", str(tmp_path), path])
            finally:
                tmp_path.unlink(missing_ok=True)
        else:
            if level not in POLICY_LEVELS:
                raise HTTPException(status_code=400, detail="Invalid policy level")
            await run_mc(["anonymous", "set", level, path])
        return {"status": "ok"}
    except MCError as e:
        mc_error_response(e)


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/api/search")
async def search(alias: str, bucket: str, pattern: str, prefix: str = ""):
    path = mc_path(alias, bucket, prefix)
    # mc find returns keys as "alias/bucket/path"; strip to a bucket-relative key
    strip = mc_path(alias, bucket) + "/"
    try:
        results = await run_mc(["find", path, "--name", pattern])
        out = []
        for r in results:
            key = r.get("key", "")
            if r.get("status") != "success" or not key:
                continue
            if key.startswith(strip):
                key = key[len(strip):]
            out.append({**r, "key": key})
        return out
    except MCError as e:
        mc_error_response(e)


# ---------------------------------------------------------------------------
# Serve frontend
# ---------------------------------------------------------------------------

static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)
app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
