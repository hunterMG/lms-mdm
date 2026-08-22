# lms-mdm — LM Studio Model Download Manager

A zero-dependency, multi-threaded model download manager that fetches Hugging Face models directly into LM Studio's local model directory.

## Background

LM Studio downloads models over a **single HTTP connection**. On unstable networks this means:

- Slow throughput (no parallelism, no CDN range exploitation)
- Frequent connection drops that stall or kill the download
- Orphaned `downloading_*.part` files that waste gigabytes when a job is cancelled

Since LM Studio's model files are plain HTTPS objects served by Hugging Face — which supports byte-range requests (`206 Partial Content`) — the transfer layer doesn't need LM Studio at all. `lms-mdm` reimplements the download path with segmented parallel requests, writes results into `~/.lmstudio/models/<publisher>/<repo>/`, and lets LM Studio auto-detect the finished model.

## Features

- **Segmented multi-threaded downloads** — each file is split into chunks fetched concurrently over independent connections (`--workers`, default 8; chunk size `--segment-mb`, default 32 MiB)
- **Resumable** — completed segments are tracked in a `<file>.mdm-state.json` next to the partial data (`<file>.mdm-part`); interrupting (Ctrl-C, crash, network loss) never loses progress, and re-running resumes exactly where it stopped
- **Per-segment retry** — failed segments retry up to 10 times with exponential backoff and jitter; one bad segment doesn't abort the whole file
- **Integrity verification** — large files carry a sha256 in HF's LFS metadata; every downloaded file is verified before being moved into place
- **Mirror endpoint support** — `--endpoint` / `$HF_ENDPOINT` swaps the API/download base URL (e.g. `https://hf-mirror.com`)
- **Automatic capability probing** — hosts without byte-range support are detected and bypassed: segmented downloads transparently switch to the canonical Hugging Face host, falling back to single-stream as a last resort (see Limitations)
- ♻️💾 **LM Studio partial seeding** — existing `downloading_*.part` files left behind by LM Studio are adopted as a starting offset instead of being thrown away
- **Idempotent** — already-complete files are detected by size and skipped, so re-running only fetches what's missing
- **Inspection modes** — `--list` shows remote files vs local state; `--dry-run` previews the plan
- **Extras** — include/exclude glob filters, optional transport-level proxy (`--proxy`), access token for gated repos (`--token` / `$HF_TOKEN`)
- **Zero dependencies** — Python 3.9+ standard library only

## Usage

```bash
python3 lms_mdm.py <source> [options]
```

`<source>` accepts any of:

| Form | Example |
|---|---|
| Repo id | `lmstudio-community/Qwen3.8-27B-MLX-8bit` |
| Hugging Face URL | `https://huggingface.co/publisher/repo` |
| Direct file URL | `https://huggingface.co/pub/repo/resolve/main/model.gguf` |
| LM Studio proxy link | `https://search.lmstudio.ai/v1/hf-proxy/pub/repo/resolve/main/file` |

### Common examples

```bash
# Finish an interrupted model (skips complete shards, seeds LM Studio partials)
python3 lms_mdm.py lmstudio-community/Qwen3.8-27B-MLX-8bit

# Check what's missing before committing
python3 lms_mdm.py lmstudio-community/Qwen3.8-27B-MLX-8bit --list
python3 lms_mdm.py lmstudio-community/Qwen3.8-27B-MLX-8bit --dry-run

# Aggressive parallelism through a specific mirror
python3 lms_mdm.py publisher/repo --endpoint https://hf-mirror.com --workers 16

# Only grab one file / one quantization pattern
python3 lms_mdm.py publisher/repo --include '*Q4_K_M*.gguf'

# Route traffic through a transport proxy
python3 lms_mdm.py publisher/repo --proxy http://127.0.0.1:7890
```

### Options

| Option | Default | Description |
|---|---|---|
| `--revision` | `main` | Branch/tag/commit of the repo |
| `--models-dir` | `~/.lmstudio/models` | Destination root |
| `--endpoint` | `$HF_ENDPOINT` or `https://huggingface.co` | Mirror/base URL for API + downloads |
| `--workers` | `8` | Parallel connections per file |
| `--segment-mb` | `32` | Segment size in MiB |
| `--proxy` | – | Transport-level HTTP(S) proxy (falls back to `$HTTPS_PROXY`) |
| `--token` | `$HF_TOKEN` | Access token for gated repos |
| `--include` / `--exclude` | – | Glob patterns, repeatable |
| `--no-resume` | off | Ignore saved segment state |
| `--no-seed` | off | Don't adopt LM Studio `downloading_*.part` files |
| `--list` | – | Show files + local status, then exit |
| `--dry-run` | – | Plan only, download nothing |

### Workflow tips

- ⚠️🛑 **Pause/cancel the download in LM Studio first** so two writers don't touch the same files.
- After completion, restart LM Studio (or rescan) and the model appears in My Models.
- Progress lines go to stderr; Ctrl-C exits cleanly with progress saved.
- **Tip:** you may want to set the domain suffix `hf.co` (the CDN domain of `huggingface.co`) for **direct connection** if you're using proxy software — otherwise multi-gigabyte model downloads will consume a lot of proxy traffic.

## Limitations

- **No Windows support** — chunked writing uses `os.pwrite` (macOS/Linux).
- **Mirrors without Range support** (e.g. `hf-mirror.com`, verified returning `200` full-body responses) cannot serve segmented or resumable transfers. When detected, `lms-mdm` switches those files to canonical `huggingface.co`; if that host is unreachable, it degrades to single-stream mode — no parallelism, and no mid-file resume.
- **Single-stream fallback** restarts the file from scratch if interrupted (the server gives no way to seek).
- **Sequential file processing** — workers are applied within a file; multiple files download one after another rather than concurrently.
- **Existing-file checks are size-only** — files already on disk are skipped by size match; their content isn't re-hashed (only freshly downloaded data gets sha256 verification).
- **Gated repos** require `--token`/`$HF_TOKEN`.
- **No rate limiting** — aggressive worker counts may trip CDN throttling; backoff handles transient 429s, but lower `--workers` if a host complains.
