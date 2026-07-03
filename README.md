# MinIO Manager

A lightweight local web UI for managing MinIO / S3-compatible storage, built on top of the [MinIO Client (`mc`)](https://min.io/docs/minio/linux/reference/minio-mc.html). FastAPI backend, single-page HTML/JS frontend.

## Features

- Manage `mc` aliases (add/remove connections)
- Browse, create, and delete buckets
- Browse, upload, download, copy, move, and delete objects
- Inline preview for text, images, video, audio, and PDF files
- Set bucket anonymous access policy (none/download/upload/public) or custom JSON policy
- Search objects by name pattern

## Requirements

- Python 3.10+
- [MinIO Client (`mc`)](https://min.io/docs/minio/linux/reference/minio-mc.html) installed and available on your system

## Setup

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

Edit `config.json` if needed:

```json
{
  "mc_path": "mc",          # path to the mc executable, or just "mc" if it's on PATH
  "default_alias": "",
  "request_timeout": 30,
  "upload_timeout": 300
}
```

## Running

```bash
uvicorn app:app --host 127.0.0.1 --port 8765
```

Or on Windows, just run `run.bat`, which activates the venv, starts the server, and opens the UI at `http://127.0.0.1:8765`.

## Notes

- Credentials entered in the UI are passed straight to `mc alias set` and stored by `mc` itself (typically in `~/.mc/config.json`), not by this app.
