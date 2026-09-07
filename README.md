# lms-mdm — LM Studio Model Download Manager

A zero-dependency, multi-threaded model download manager that fetches Hugging Face models directly into LM Studio's local model directory.

Tested on LM Studio >= `0.4.21+2`.

## Background

LM Studio's built-in downloader has several limitations:

- Downloads tend to disconnect on unstable networks, without automatic retries.
- Downloads use at most **3 threads**, which can leave available bandwidth underutilized.
- Download settings offer limited flexibility for customization.

Since LM Studio's model files are plain HTTPS objects served by Hugging Face — which supports byte-range requests (`206 Partial Content`) — the transfer layer doesn't need LM Studio at all. `lms-mdm` reimplements the download path with segmented parallel requests, writes results into `~/.lmstudio/models/<publisher>/<repo>/`, and lets LM Studio auto-detect the finished model.

## Features

- **Segmented multi-threaded downloads** — each file is split into chunks fetched concurrently over independent connections (`--workers`, default 8; chunk size `--segment-mb`, default 32 MiB)
- **Resumable** — completed segments are tracked in a `<file>.mdm-state.json` next to the partial data (`<file>.mdm-part`); interrupting (Ctrl-C, crash, network loss) never loses progress, and re-running resumes exactly where it stopped
- **Per-segment retry** — failed segments retry up to 10 times with exponential backoff and jitter; one bad segment doesn't abort the whole file
- **Integrity verification** — large files carry a sha256 in HF's LFS metadata; every downloaded file is verified before being moved into place
- **Mirror endpoint support** — `--endpoint` / `$HF_ENDPOINT` swaps the API/download base URL (e.g. `https://hf-mirror.com`)
- **Automatic capability probing** — hosts without byte-range support are detected and bypassed: segmented downloads transparently switch to the canonical Hugging Face host, falling back to single-stream as a last resort (see Limitations)
- ♻️💾 **LM Studio partial seeding** — existing `downloading_*.part` files left behind by LM Studio are adopted as a starting offset instead of being thrown away
- 🩹 **Stale download-job cleanup** — after downloading, optionally quits LM Studio and marks its leftover job records for this model as completed, so the model is indexed immediately instead of showing a phantom progress bar (`--fix-lms-jobs`; see [Stale download jobs](#-stale-download-jobs-when-a-finished-model-still-shows-downloading))
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
| --- | --- |
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
| --- | --- | --- |
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
| `--fix-lms-jobs` | off | Post-download: quit LM Studio and mark its stale job records completed so the model indexes (see [Stale download jobs](#-stale-download-jobs-when-a-finished-model-still-shows-downloading)) |
| `--list` | – | Show files + local status, then exit |
| `--dry-run` | – | Plan only, download nothing |

### Workflow tips

- ⚠️🛑 **Pause the download in LM Studio first** so two writers don't touch the same files — and **never click ✕/remove in LM Studio's Downloads panel** on a folder you manage externally: LM Studio will **delete the tracked model files themselves**. If you must, back the folder up first.
- After a successful run, `lms-mdm` offers to **clean up LM Studio's stale download-job records** for this repo (confirm the prompt, or pass `--fix-lms-jobs`). This is what makes LM Studio index the new model instead of showing it as "downloading".
- Progress lines go to stderr; Ctrl-C exits cleanly with progress saved.
- **Tip:** you may want to set the domain suffix `hf.co` (the CDN domain of `huggingface.co`) for **direct connection** if you're using proxy software — otherwise multi-gigabyte model downloads will consume a lot of proxy traffic.

## 🩹 Stale download jobs: when a finished model still shows "downloading"

If LM Studio ever *started* downloading the same model — even briefly — it keeps
a job record under `~/.lmstudio/.internal/`. Any live, paused, or
resumable-failed record pointing at the model folder **blocks indexing**:
`lms ls` won't list the model, and the app shows a phantom progress bar stuck
at some percentage.

### Automatic fix

At the end of a successful run (terminal attached), `lms-mdm` prompts:

```bash
Clean up stale LM Studio download jobs for this model now? (quits LM Studio; makes it index the new model) [y/N]
```

Confirming (or passing `--fix-lms-jobs`) will:

1. Quit LM Studio gracefully. If only the menu-bar agent survives, you're asked to quit it from its own menu-bar icon (recommended — killing it risks corrupting LM Studio's databases) or explicitly opt into forced termination; non-interactive runs abort safely instead
2. Back up both registries to `~/.lmstudio/.internal/<name>_YYYY-MM-DD_HH_MM_SS.json`
3. In `download-jobs-info.json`: mark every task whose `targetPath` falls under this repo's folder as `status:"completed"`, `progress:100`, `downloadedSizeBytes:=totalSizeBytes`, and flip its wrapper `"jobState"` to `{"type":"completed","completedTimestamp":…}`
4. In `single-downloads-info.json`: move those records from `downloadsMap` to `endedDownloadsMap` with `{"type":"ended","endReason":"completed"}`
5. Remove leftover `downloading_*.part` files whose final file already exists

Then relaunch LM Studio — verify with `lms ls`.

### Manual recipe

> [!WARNING]
> Edit these files **only while LM Studio is fully quit** (menu-bar icon gone) — otherwise it rewrites your edits on exit. And never use the Downloads panel's remove/cancel on externally-managed folders; it deletes model files.

1. Quit LM Studio completely; make timestamped backups of both JSON files
2. **`download-jobs-info.json`** — find job(s) whose tasks' `targetPath` fall under `~/.lmstudio/models/<publisher>/<repo>/`; for each matching task set:
   ```json
   "status": "completed", "progress": 100,
   "downloadedSizeBytes": <totalSizeBytes>, "errorMessage": null
   ```
   and on the wrapper object: `"jobState": {"type": "completed", "completedTimestamp": <epoch-ms>}`
3. **`single-downloads-info.json`** — move each matching `[id, record]` pair from `downloadsMap` to `endedDownloadsMap` after setting:
   ```json
   "status": {"type": "ended", "endReason": "completed"},
   "downloadedSizeBytes": <totalSizeBytes>
   ```
4. Delete any stray `downloading_<name>.part` whose final file is complete
5. Relaunch LM Studio; confirm with `lms ls`

> [!NOTE]
> Merely *deleting* the records also unblocks indexing, but the UI keeps showing an unfinished job. Marking them completed — as above — resolves both.

## Limitations

- **No Windows support** — chunked writing uses `os.pwrite` (macOS/Linux).
- **Mirrors without Range support** (e.g. `hf-mirror.com`, verified returning `200` full-body responses) cannot serve segmented or resumable transfers. When detected, `lms-mdm` switches those files to canonical `huggingface.co`; if that host is unreachable, it degrades to single-stream mode — no parallelism, and no mid-file resume.
- **Single-stream fallback** restarts the file from scratch if interrupted (the server gives no way to seek).
- **Sequential file processing** — workers are applied within a file; multiple files download one after another rather than concurrently.
- **Existing-file checks are size-only** — files already on disk are skipped by size match; their content isn't re-hashed (only freshly downloaded data gets sha256 verification).
- **Gated repos** require `--token`/`$HF_TOKEN`.
- **No rate limiting** — aggressive worker counts may trip CDN throttling; backoff handles transient 429s, but lower `--workers` if a host complains.

## Development checks

Install the development tools and enable the Git pre-commit hook after cloning:

```bash
python3 -m pip install pre-commit ruff
pre-commit install
```

Each commit checks staged Python files for syntax errors and runs `ruff check`.
The checks use `python3` and `ruff` from your PATH and block the commit on failure.
They do not modify files. Downloads require no additional runtime dependencies.

Run the checks manually across all tracked Python files:

```bash
pre-commit run --all-files
```

## License

This project is licensed under the GNU Affero General Public License v3.0.
See [LICENSE](LICENSE) for the complete license text.
