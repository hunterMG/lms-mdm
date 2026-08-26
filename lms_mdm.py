#!/usr/bin/env python3
"""lms-mdm: multi-threaded model download manager for LM Studio.

Downloads models from Hugging Face (or a mirror such as hf-mirror.com)
directly into LM Studio's models directory
(~/.lmstudio/models/<publisher>/<repo>/) using segmented parallel range
requests, resumable downloads, per-chunk retries, and sha256 integrity
verification from HF's LFS metadata.

Requires: Python 3.9+ (stdlib only).
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import os
import queue
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

DEFAULT_ENDPOINT = os.environ.get("HF_ENDPOINT", "https://huggingface.co").rstrip("/")
CANONICAL_ENDPOINT = "https://huggingface.co"
DEFAULT_MODELS_DIR = os.path.expanduser("~/.lmstudio/models")
INTERNAL_DIR = os.path.expanduser("~/.lmstudio/.internal")
STATE_SUFFIX = ".mdm-state.json"
PART_SUFFIX = ".mdm-part"
LMS_PART_PREFIX = "downloading_"

CHUNK_READ = 512 * 1024
CONNECT_TIMEOUT = 15
READ_TIMEOUT = 60
MAX_ATTEMPTS = 10
LMS_SETTLE_SECS = 1.0


def human(n: float) -> str:
    for unit in ("B", "KiB", "MiB"):
        if abs(n) < 1024:
            return f"{int(n)} B" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} GiB"


def fmt_eta(seconds: float) -> str:
    if seconds <= 0 or seconds != seconds:
        return "--:--"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def parse_source(source: str) -> tuple[str, str | None]:
    """Return (repo_id, single_file_or_None) from a repo id or URL."""
    src = source.strip().split("?")[0].rstrip("/")
    m = re.match(r"https?://[^/]+/(?:v1/hf-proxy/)?([^/]+/[^/]+)/resolve/[^/]+/(.+)$", src)
    if m:
        return m.group(1), m.group(2)
    m = re.match(r"https?://[^/]+/(?:api/models/)?([^/]+/[^/]+)/(?:tree/[^/]+/?)?$", src)
    if m:
        return m.group(1), None
    if len(src.split("/")) != 2 or not src.split("/")[0]:
        raise SystemExit(f"error: cannot parse source: {source!r}\n"
                         f"expected 'publisher/name' or a Hugging Face URL")
    return src, None


class HttpClient:
    def __init__(self, proxy: str | None = None, token: str | None = None):
        handlers = []
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        self.opener = urllib.request.build_opener(*handlers)
        self.token = token

    def headers(self, extra: dict | None = None) -> dict:
        h = {"User-Agent": "lms-mdm/0.1"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if extra:
            h.update(extra)
        return h

    def get_json(self, url: str):
        req = urllib.request.Request(url, headers=self.headers())
        with self.opener.open(req, timeout=CONNECT_TIMEOUT) as r:
            return json.loads(r.read().decode())

    def open_range(self, url: str, start: int, end: int):
        hdrs = self.headers({"Range": f"bytes={start}-{end}"})
        req = urllib.request.Request(url, headers=hdrs)
        return self.opener.open(req, timeout=READ_TIMEOUT)

    def open_plain(self, url: str):
        req = urllib.request.Request(url, headers=self.headers())
        return self.opener.open(req, timeout=READ_TIMEOUT)

    def probe_range(self, url: str) -> str | None:
        try:
            r = self.open_range(url, 0, 1023)
            try:
                return str(r.status if hasattr(r, "status") else r.getcode())
            finally:
                r.close()
        except (urllib.error.URLError, OSError):
            return None


def fetch_tree(client: HttpClient, endpoint: str, repo: str, revision: str) -> list[dict]:
    url = f"{endpoint}/api/models/{repo}/tree/{revision}?recursive=true"
    return client.get_json(url)


def select_files(tree: list[dict], include: list[str], exclude: list[str]) -> list[dict]:
    out = []
    for e in tree:
        if e.get("type") != "file":
            continue
        path = e["path"]
        if path == ".gitattributes":
            continue
        if include and not any(fnmatch.fnmatch(path, p) for p in include):
            continue
        if any(fnmatch.fnmatch(path, p) for p in exclude):
            continue
        out.append(e)
    return out


class Progress:
    def __init__(self, total: int):
        self.lock = threading.Lock()
        self.done = 0
        self.inflight = 0
        self.preexisting = 0
        self.total = total
        self.label = ""
        self.last_line_len = 0
        self.stop = False

    def tick(self):
        with self.lock:
            cur, tot, pre, label = self.done, self.total, self.preexisting, self.label
            inflight = self.inflight
        pct = (cur + pre + inflight) / tot * 100 if tot else 100.0
        line = f"\r{label}  {pct:6.2f}% overall  ({human(cur + pre + inflight)} / {human(tot)})"
        sys.stderr.write(line + (" " * max(0, self.last_line_len - len(line))))
        sys.stderr.flush()
        self.last_line_len = len(line)

    def note(self, msg: str):
        with self.lock:
            self.clear_line()
            print(msg, file=sys.stderr)
            self.last_line_len = 0

    def clear_line(self):
        sys.stderr.write("\r" + " " * self.last_line_len + "\r")
        self.last_line_len = 0


def tick_loop(prog: Progress):
    while not prog.stop:
        prog.tick()
        time.sleep(0.4)


def load_state(state_path: str, size: int, seg_size: int) -> list[int] | None:
    try:
        with open(state_path) as f:
            s = json.load(f)
        if s.get("size") == size and s.get("segment_size") == seg_size:
            return sorted(set(s["done"]))
    except (OSError, KeyError, ValueError, TypeError):
        pass
    return None


def save_state(state_path: str, url: str, size: int, seg_size: int,
               sha256: str | None, done: list[int]):
    tmp = state_path + ".tmp"
    payload = {"version": 1, "url": url, "size": size, "segment_size": seg_size,
               "sha256": sha256, "done": done}
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, state_path)


def download_one_segment(client: HttpClient, url: str, fd: int, start: int, end: int,
                         prog: Progress | None = None) -> int:
    """Download [start, end] inclusive into fd at offset start. Returns bytes written."""
    resp = client.open_range(url, start, end)
    added_inflight = 0
    try:
        status = resp.status if hasattr(resp, "status") else resp.getcode()
        if status != 206:
            raise RuntimeError(f"expected 206 partial content, got {status}; "
                               f"host may not honor byte ranges")
        expected = end - start + 1
        written = 0
        pos = start
        while written < expected:
            data = resp.read(min(CHUNK_READ, expected - written))
            if not data:
                break
            os.pwrite(fd, data, pos)
            pos += len(data)
            written += len(data)
            if prog:
                with prog.lock:
                    prog.inflight += len(data)
                added_inflight += len(data)
        if written != expected:
            raise RuntimeError(f"short read: got {written} of {expected} bytes")
        return written
    except BaseException:
        if prog and added_inflight:
            with prog.lock:
                prog.inflight -= added_inflight
        raise
    finally:
        resp.close()


def resolve_download_host(client: HttpClient, repo: str, name: str, revision: str,
                          preferred: str, prog: Progress) -> tuple[str, str]:
    tail = f"{repo}/resolve/{revision}/{name}"

    def probe(base: str) -> str | None:
        return client.probe_range(f"{base}/{tail}")

    st = probe(preferred)
    if st == "206":
        return preferred, "segmented"
    if preferred.rstrip("/") != CANONICAL_ENDPOINT:
        if probe(CANONICAL_ENDPOINT) == "206":
            prog.note(f"  ~ {preferred} does not honor byte ranges "
                      f"(got {st or 'connection failed'}); using "
                      f"{CANONICAL_ENDPOINT} for segmented download of {name}")
            return CANONICAL_ENDPOINT, "segmented"
    prog.note(f"  ~ no range-capable host found ({preferred}: "
              f"{st or 'unreachable'}); single-stream fallback for {name}")
    return preferred, "single"


def finalize_part(part: str, final: str, state_path: str, name: str, size: int,
                  sha256: str | None, prog: Progress) -> bool:
    if not verify_file(part, size, sha256):
        for p in (part, state_path):
            if os.path.exists(p):
                os.remove(p)
        prog.clear_line()
        print(f"  ! {name} failed integrity check; deleted, re-run to retry",
              file=sys.stderr)
        return False
    os.replace(part, final)
    if os.path.exists(state_path):
        os.remove(state_path)
    prog.clear_line()
    detail = human(size) + (", sha256 ok" if sha256 else "")
    print(f"  + {name}  ({detail})")
    return True


def download_single_stream(client: HttpClient, url: str, part: str, final: str,
                           state_path: str, name: str, size: int, sha256: str | None,
                           prog: Progress) -> bool:
    fd = os.open(part, os.O_WRONLY | os.O_TRUNC)
    try:
        resp = client.open_plain(url)
        with resp:
            pos = 0
            while True:
                data = resp.read(CHUNK_READ)
                if not data:
                    break
                os.pwrite(fd, data, pos)
                pos += len(data)
                with prog.lock:
                    prog.inflight += len(data)
    except KeyboardInterrupt:
        raise
    except Exception as e:
        prog.note(f"  ! {name} single-stream failed: "
                  f"{type(e).__name__}: {e}; re-run to retry")
        return False
    finally:
        os.close(fd)
        with prog.lock:
            prog.preexisting += prog.inflight
            prog.inflight = 0
    return finalize_part(part, final, state_path, name, size, sha256, prog)


def download_file(client: HttpClient, repo: str, entry: dict, dest_dir: str, args,
                  prog: Progress) -> bool:
    name = entry["path"]
    size = entry["size"]
    sha256 = (entry.get("lfs") or {}).get("oid", "").removeprefix("sha256:") or None
    final = os.path.join(dest_dir, name)
    part = final + PART_SUFFIX
    state_path = final + STATE_SUFFIX
    seg_size = args.segment_mb * 1024 * 1024

    if os.path.exists(final) and os.path.getsize(final) == size:
        prog.clear_line()
        print(f"  = {name}  (already complete)")
        with prog.lock:
            prog.preexisting += size
        return True

    base, mode = resolve_download_host(client, repo, name, args.revision,
                                       args.endpoint, prog)
    url = f"{base}/{repo}/resolve/{args.revision}/{name}"

    if mode == "single":
        for p in (part, state_path):
            if os.path.exists(p):
                os.remove(p)
        fd0 = os.open(part, os.O_RDWR | os.O_CREAT, 0o644)
        os.ftruncate(fd0, size)
        os.close(fd0)
        with prog.lock:
            prog.label = name
        return download_single_stream(client, url, part, final, state_path,
                                      name, size, sha256, prog)

    n_segments = max(1, (size + seg_size - 1) // seg_size)
    ranges = [(i * seg_size, min(size, (i + 1) * seg_size) - 1) for i in range(n_segments)]

    done = None if args.no_resume else load_state(state_path, size, seg_size)
    resumed_from_state = done is not None
    seeded_bytes = 0

    if done is None:
        done = []
        if not args.no_seed and not os.path.exists(part):
            lms_part = os.path.join(dest_dir, LMS_PART_PREFIX + name + ".part")
            if os.path.exists(lms_part):
                lms_len = os.path.getsize(lms_part)
                if 0 < lms_len <= size:
                    os.replace(lms_part, part)
                    if lms_len < size:
                        with open(part, "ab") as f:
                            f.truncate(size)
                    done = list(range(lms_len // seg_size))
                    seeded_bytes = min(lms_len // seg_size * seg_size, size)
                prog.clear_line()
                print(f"  ~ seeding {name} from LM Studio partial "
                      f"({human(min(lms_len, size))} pre-downloaded)")

    if not os.path.exists(part):
        fd = os.open(part, os.O_RDWR | os.O_CREAT, 0o644)
        os.ftruncate(fd, size)
        os.close(fd)

    todo = [(i, ranges[i]) for i in range(n_segments) if i not in set(done)]

    with prog.lock:
        prog.preexisting += seeded_bytes + sum(
            ranges[i][1] - ranges[i][0] + 1 for i in done)
        prog.label = name

    if resumed_from_state and done:
        prog.clear_line()
        print(f"  ~ resuming {name}: {len(done)}/{n_segments} segments already done")

    fd = os.open(part, os.O_WRONLY)
    errors: list[str] = []
    stop = threading.Event()
    work: queue.Queue = queue.Queue()
    for item in todo:
        work.put(item)
    state_lock = threading.Lock()

    def worker():
        while not stop.is_set():
            try:
                idx, (start, end) = work.get_nowait()
            except queue.Empty:
                return
            delay = 1.0
            for attempt in range(1, MAX_ATTEMPTS + 1):
                if stop.is_set():
                    return
                try:
                    nbytes = download_one_segment(client, url, fd, start, end, prog)
                    with prog.lock:
                        prog.done += nbytes
                        prog.inflight -= nbytes
                    with state_lock:
                        done.append(idx)
                        save_state(state_path, url, size, seg_size, sha256,
                                   sorted(set(done)))
                    break
                except Exception as e:
                    if attempt == MAX_ATTEMPTS:
                        errors.append(f"segment {idx} [{start}-{end}] "
                                      f"after {MAX_ATTEMPTS} attempts: {e}")
                        stop.set()
                        return
                    prog.note(f"  retry seg {idx} attempt {attempt}/{MAX_ATTEMPTS - 1}: "
                              f"{type(e).__name__}: {e} -> backoff {delay:.1f}s")
                    time.sleep(delay * (0.5 + random.random()))
                    delay = min(delay * 2, 30)

    threads = [threading.Thread(target=worker, daemon=True)
               for _ in range(min(args.workers, max(1, len(todo))))]
    for t in threads:
        t.start()

    ticker = threading.Thread(target=tick_loop, args=(prog,), daemon=True)
    ticker.start()

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        stop.set()
        raise
    finally:
        prog.stop = True
        os.close(fd)

    if errors:
        prog.clear_line()
        print(f"  ! {name} failed: {errors[0]}", file=sys.stderr)
        print("    progress saved; re-run to resume", file=sys.stderr)
        return False

    return finalize_part(part, final, state_path, name, size, sha256, prog)


def verify_file(path: str, size: int, sha256: str | None) -> bool:
    if os.path.getsize(path) != size:
        return False
    if not sha256:
        return True
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest() == sha256


def prompt_yes_no(question: str, default: bool = False) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    try:
        ans = input(f"{question} {suffix} ").strip().lower()
    except EOFError:
        return False
    if not ans:
        return default
    return ans in ("y", "yes")


def lms_running() -> bool:
    try:
        r = subprocess.run(["pgrep", "-f", "LM Studio.app"],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return r.returncode == 0
    except OSError:
        return False


def _wait_lms_gone(seconds: float) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if not lms_running():
            return True
        time.sleep(0.4)
    return not lms_running()


def quit_lm_studio(timeout: float = 10.0) -> bool:
    """Gracefully ask LM Studio to quit (same as the user choosing Quit).

    Never sends signals: LM Studio may be mid-write to its job registries,
    and killing it risks corrupting them."""
    if sys.platform == "darwin":
        try:
            subprocess.run(["osascript", "-e", 'quit app "LM Studio"'],
                           check=False, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
        except OSError:
            pass
    return _wait_lms_gone(timeout)


def force_kill_lms() -> bool:
    """Last resort, only on explicit user consent: SIGTERM then SIGKILL."""
    try:
        subprocess.run(["pkill", "-TERM", "-f", "LM Studio.app"],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    except OSError:
        pass
    if _wait_lms_gone(8.0):
        return True
    print("  · still alive; sending SIGKILL")
    try:
        subprocess.run(["pkill", "-9", "-f", "LM Studio.app"],
                       check=False, stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
    except OSError:
        pass
    return _wait_lms_gone(5.0)


def _settle_after_exit() -> bool:
    """After the last LM Studio process disappears, give it a moment so any
    final disk flush completes, then re-verify it stayed down."""
    time.sleep(LMS_SETTLE_SECS)
    return not lms_running()


def _atomic_write_json(path: str, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _backup_file(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    base, ext = os.path.splitext(path)
    dst = f"{base}_{time.strftime('%Y-%m-%d_%H_%M_%S')}{ext}"
    shutil.copy2(path, dst)
    return dst


def _iter_downloads(job: dict):
    for t in job.get("tasks") or []:
        d = t.get("download")
        if isinstance(d, dict):
            yield d


def cleanup_lms_job_records(dest_dir: str,
                            internal_dir: str = INTERNAL_DIR) -> tuple[int, int, int, int]:
    """Mark LM Studio's download-job records for dest_dir as completed so the
    app indexes the folder instead of showing a phantom download.

    Must only run while LM Studio is not running.
    Returns (tasks, wrappers, moved, removed_parts)."""
    prefix = dest_dir.rstrip(os.sep) + os.sep
    if lms_running():
        raise RuntimeError("LM Studio is still running; refusing to edit "
                           "its job registries")
    jobs_path = os.path.join(internal_dir, "download-jobs-info.json")
    sd_path = os.path.join(internal_dir, "single-downloads-info.json")
    if not os.path.exists(jobs_path) and not os.path.exists(sd_path):
        raise FileNotFoundError(f"no LM Studio job registries in {internal_dir}")

    for p in (jobs_path, sd_path):
        b = _backup_file(p)
        if b:
            print(f"  ~ backup: {os.path.basename(b)}")

    now_ms = int(time.time() * 1000)
    tasks_done = wrappers_done = 0

    if os.path.exists(jobs_path):
        data = json.load(open(jobs_path))
        jobs = data.get("jobs") if isinstance(data, dict) else data
        for job in jobs if isinstance(jobs, list) else []:
            if not isinstance(job, dict):
                continue
            ds = [d for d in _iter_downloads(job)
                  if str(d.get("targetPath", "")).startswith(prefix)]
            if not ds:
                continue
            for d in ds:
                total = d.get("totalSizeBytes")
                if total:
                    d["downloadedSizeBytes"] = total
                d["progress"] = 100
                d["status"] = "completed"
                d["errorMessage"] = None
                if "autoRetryNextTimestamp" in d:
                    d["autoRetryNextTimestamp"] = None
                tasks_done += 1
            js = job.get("jobState")
            if not (isinstance(js, dict) and js.get("type") in ("completed", "ended")):
                job["jobState"] = {"type": "completed",
                                   "completedTimestamp": now_ms}
                wrappers_done += 1
        if tasks_done or wrappers_done:
            _atomic_write_json(jobs_path, data)

    moved = 0
    if os.path.exists(sd_path):
        sd = json.load(open(sd_path))
        dm = sd.get("downloadsMap") or []
        ed = sd.get("endedDownloadsMap")
        if ed is None:
            ed = []
            sd["endedDownloadsMap"] = ed
        keep = []
        for pair in dm:
            v = pair[1] if isinstance(pair, list) and len(pair) == 2 else None
            if isinstance(v, dict) and str(v.get("targetPath", "")).startswith(prefix):
                v["status"] = {"type": "ended", "endReason": "completed"}
                if v.get("totalSizeBytes"):
                    v["downloadedSizeBytes"] = v["totalSizeBytes"]
                ed.append([pair[0], v])
                moved += 1
            else:
                keep.append(pair)
        if moved:
            sd["downloadsMap"] = keep
            _atomic_write_json(sd_path, sd)

    removed_parts = 0
    if os.path.isdir(dest_dir):
        for name in os.listdir(dest_dir):
            if name.startswith(LMS_PART_PREFIX) and name.endswith(".part"):
                final_name = name[len(LMS_PART_PREFIX):-len(".part")]
                if os.path.exists(os.path.join(dest_dir, final_name)):
                    os.remove(os.path.join(dest_dir, name))
                    removed_parts += 1

    return tasks_done, wrappers_done, moved, removed_parts


def run_job_cleanup(dest_dir: str, confirmed: bool = False,
                    internal_dir: str = INTERNAL_DIR):
    if not confirmed:
        print("\nLM Studio must be closed while its download-job records are edited.")
        if not prompt_yes_no("Quit LM Studio and clean up stale job records now?", True):
            print("skipped: re-run later with --fix-lms-jobs (with LM Studio closed)")
            return
    if lms_running():
        print("quitting LM Studio...")
        if not quit_lm_studio():
            print("  · the LM Studio menu-bar agent is still running "
                  "(the window already closed).")
            print("    Recommended: click its menu-bar icon and choose Quit — "
                  "killing it risks corrupting LM Studio's databases.")
            if not sys.stdin.isatty():
                print("  ! non-interactive session; cleanup aborted. Close LM "
                      "Studio fully (menu-bar icon too) and re-run.")
                return
            choice = input("    [q] I quit it myself   [f] force-terminate it   "
                           "[a] abort: ").strip().lower()
            if choice == "f":
                print("forcing termination...")
                if not force_kill_lms():
                    print("  ! could not terminate LM Studio; cleanup aborted")
                    return
            elif choice == "q":
                print("waiting for LM Studio to exit (up to 3 minutes)...")
                if not _wait_lms_gone(180.0):
                    print("  ! still running after 3 min; cleanup aborted. "
                          "Re-run when fully closed.")
                    return
            else:
                print("cleanup aborted")
                return
    if not _settle_after_exit():
        print("  ! LM Studio started again unexpectedly; cleanup aborted")
        return
    try:
        tasks, wrappers, moved, parts = cleanup_lms_job_records(dest_dir,
                                                                internal_dir)
    except FileNotFoundError as e:
        print(f"  ! {e}")
        return
    except RuntimeError as e:
        print(f"  ! {e}")
        return
    print(f"  ✓ marked {tasks} task(s)/{wrappers} job(s) completed, "
          f"moved {moved} record(s), removed {parts} stray .part file(s)")
    print("done. relaunch LM Studio; the model should appear in My Models.")
    print("Final step: Open the model card again in LM Studio and click the 'Complete Download' button if it is present.")


def main(argv=None):
    sys.stdout.reconfigure(line_buffering=True)
    ap = argparse.ArgumentParser(
        prog="lms-mdm",
        description="Multi-threaded, resumable model downloader for LM Studio "
                    "(downloads straight into ~/.lmstudio/models).")
    ap.add_argument("source",
                    help="publisher/name, HF/mirror URL, or direct file URL "
                         "(LM Studio hf-proxy links also accepted)")
    ap.add_argument("--revision", default="main")
    ap.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT,
                    help="base URL of the model host; use a mirror to speed up or "
                         "unblock downloads, e.g. --endpoint https://hf-mirror.com "
                         "(default $HF_ENDPOINT or https://huggingface.co)")
    ap.add_argument("--workers", type=int, default=8,
                    help="parallel connections per file (default 8)")
    ap.add_argument("--segment-mb", type=int, default=32,
                    help="segment size in MiB (default 32)")
    ap.add_argument("--proxy", default=None,
                    help="optional transport-level HTTP(S) proxy, e.g. "
                         "http://127.0.0.1:7890 (falls back to HTTPS_PROXY env)")
    ap.add_argument("--token", default=os.environ.get("HF_TOKEN"),
                    help="Hugging Face token for gated repos (default $HF_TOKEN)")
    ap.add_argument("--include", action="append", default=[],
                    help="glob pattern(s) of files to download")
    ap.add_argument("--exclude", action="append", default=[],
                    help="glob pattern(s) of files to skip")
    ap.add_argument("--no-resume", action="store_true",
                    help="ignore saved segment state")
    ap.add_argument("--no-seed", action="store_true",
                    help="do not adopt LM Studio 'downloading_*.part' files")
    ap.add_argument("--fix-lms-jobs", action="store_true",
                    help="after downloading, quit LM Studio and mark its stale "
                         "download-job records for this model as completed so "
                         "the model is indexed (with a terminal attached, an "
                         "interactive prompt is shown by default)")
    ap.add_argument("--list", action="store_true",
                    help="show remote files and local status, then exit")
    ap.add_argument("--dry-run", action="store_true",
                    help="plan only, download nothing")
    args = ap.parse_args(argv)

    if args.workers < 1 or args.segment_mb < 1:
        ap.error("--workers and --segment-mb must be >= 1")

    repo, single_file = parse_source(args.source)
    client = HttpClient(proxy=args.proxy, token=args.token)

    tree = None
    errors = []
    for ep in dict.fromkeys([args.endpoint, CANONICAL_ENDPOINT]):
        for attempt in range(3):
            try:
                tree = fetch_tree(client, ep, repo, args.revision)
                break
            except urllib.error.HTTPError as e:
                errors.append(f"{ep}: HTTP {e.code} for {repo}@{args.revision}")
                break
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                errors.append(f"{ep}: {type(e).__name__}: {e}")
                time.sleep(1.5 * (attempt + 1))
        if tree is not None:
            break
    if tree is None:
        raise SystemExit("error: could not fetch repo listing:\n  "
                         + "\n  ".join(errors))

    files = select_files(tree, args.include, args.exclude)
    if single_file:
        matches = [e for e in files if e["path"] == single_file]
        if not matches:
            raise SystemExit(f"error: {single_file} not found in {repo}")
        files = matches

    dest_dir = os.path.join(args.models_dir, *repo.split("/"))
    total = sum(e["size"] for e in files)

    print(f"repo:     {repo}@{args.revision}")
    print(f"endpoint: {args.endpoint}")
    print(f"files:    {len(files)}  ({human(total)})")
    print(f"dest:     {dest_dir}")
    if args.proxy:
        print(f"proxy:    {args.proxy}")

    if args.list:
        for e in files:
            final = os.path.join(dest_dir, e["path"])
            lms_part = os.path.join(dest_dir, LMS_PART_PREFIX + e["path"] + ".part")
            if os.path.exists(final) and os.path.getsize(final) == e["size"]:
                status = "complete"
            elif os.path.exists(lms_part):
                status = f"lms-partial {human(os.path.getsize(lms_part))}"
            elif os.path.exists(final + PART_SUFFIX) or os.path.exists(final + STATE_SUFFIX):
                status = "partial"
            else:
                status = "missing"
            print(f"  {human(e['size']):>12}  {status:<22} {e['path']}")
        return 0

    if args.dry_run:
        print("(dry run: nothing downloaded)")
        return 0

    os.makedirs(dest_dir, exist_ok=True)
    prog = Progress(total)
    failed = []
    started = time.monotonic()

    for e in files:
        try:
            if not download_file(client, repo, e, dest_dir, args, prog):
                failed.append(e["path"])
        except KeyboardInterrupt:
            prog.clear_line()
            print("\ninterrupted: progress saved, re-run to resume", file=sys.stderr)
            return 130

    elapsed = time.monotonic() - started
    prog.tick()
    sys.stderr.write("\n")
    if failed:
        print(f"finished with {len(failed)} failed file(s):")
        for f in failed:
            print(f"  - {f}")
        return 1
    speed = f" ({human(prog.done / max(elapsed, 0.001))}/s avg)" \
        if prog.done and elapsed > 0.5 else ""
    print(f"all {len(files)} file(s) ready in {fmt_eta(elapsed)}{speed}")

    if args.fix_lms_jobs:
        run_job_cleanup(dest_dir, confirmed=True)
    elif not args.list and not args.dry_run and sys.stdin.isatty():
        if prompt_yes_no(
                "\nClean up stale LM Studio download jobs for this model now? "
                "(quits LM Studio; makes it index the new model)"):
            run_job_cleanup(dest_dir, confirmed=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
