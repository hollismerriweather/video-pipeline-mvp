"""Concurrent, resumable Seedance video pipeline with per-shot durable state."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

BASE_URL = "https://argolink.io/v1"
USER_AGENT = "video-pipeline-mvp/1.0"
MODELS = {
    "seedance-2.5": {"resolutions": {"480p": 0.0874, "720p": 0.196, "1080p": 0.483}, "min": 4, "max": 30},
    "seedance-2.0": {"resolutions": {"480p": 0.053, "720p": 0.11, "1080p": 0.28, "4k": 0.58}, "min": 4, "max": 15},
}
STATUSES = {"pending", "done", "failed", "expired"}


class PipelineError(RuntimeError):
    pass


def headers(key: str) -> Dict[str, str]:
    if not key:
        raise PipelineError("VIDEO_API_KEY is required")
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json", "User-Agent": USER_AGENT}


def validate_plan(plan: Dict[str, Any]) -> List[Dict[str, Any]]:
    shots = plan.get("shots") if isinstance(plan, dict) else None
    if not isinstance(shots, list) or not shots:
        raise ValueError("plan.shots must be a non-empty list")
    seen = set()
    for shot in shots:
        if not isinstance(shot, dict) or not isinstance(shot.get("id"), str) or not shot["id"]:
            raise ValueError("every shot needs a stable id")
        if shot["id"] in seen:
            raise ValueError(f"duplicate shot id: {shot['id']}")
        seen.add(shot["id"])
        model = shot.get("model")
        if model not in MODELS:
            raise ValueError(f"unsupported model: {model}")
        duration = shot.get("duration", 5)
        if isinstance(duration, bool) or not isinstance(duration, int) or not MODELS[model]["min"] <= duration <= MODELS[model]["max"]:
            raise ValueError(f"{shot['id']} duration is outside the {model} contract")
        resolution = shot.get("resolution", "720p")
        if resolution not in MODELS[model]["resolutions"]:
            raise ValueError(f"{shot['id']} resolution is not published for {model}")
        if not isinstance(shot.get("prompt"), str) or not shot["prompt"].strip():
            raise ValueError(f"{shot['id']} prompt is required")
        ratio = shot.get("aspect_ratio", "16:9")
        if ratio not in {"21:9", "16:9", "4:3", "1:1", "3:4", "9:16"}:
            raise ValueError(f"{shot['id']} aspect_ratio is unsupported")
    return shots


def estimate(plan: Dict[str, Any]) -> float:
    return round(sum(MODELS[s["model"]]["resolutions"][s.get("resolution", "720p")] * s.get("duration", 5) for s in validate_plan(plan)), 4)


def _https(value: str) -> str:
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        raise ValueError("media URLs must be HTTPS without credentials")
    return value


def _atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, prefix=".json-", delete=False) as handle:
        temp = Path(handle.name)
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, path)


class HTTP:
    def __init__(self, base: str, key: str, opener: Optional[Callable[..., Any]] = None):
        self.base = base.rstrip("/")
        self.key = key
        self._open = opener or urllib.request.urlopen
        parsed = urllib.parse.urlparse(self.base)
        if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in {"127.0.0.1", "localhost"}):
            raise ValueError("base URL must use HTTPS except localhost tests")

    def request(self, method: str, path: str, body: Optional[Dict[str, Any]] = None, *, binary: bool = False) -> Any:
        data = json.dumps(body, ensure_ascii=False).encode() if body is not None else None
        request_headers = headers(self.key)
        if data is None:
            request_headers.pop("Content-Type")
        request = urllib.request.Request(self.base + path, data=data, method=method, headers=request_headers)
        try:
            response = self._open(request, timeout=90)
            try:
                raw = response.read()
            finally:
                response.close()
        except urllib.error.HTTPError as error:
            detail = error.read(4096).decode("utf-8", errors="replace").replace(self.key, "[redacted]")
            raise urllib.error.HTTPError(error.url, error.code, detail, error.headers, error.fp) from None
        if binary:
            return raw
        try:
            result = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise PipelineError(f"{method} {path} returned invalid JSON") from error
        if not isinstance(result, dict):
            raise PipelineError(f"{method} {path} returned a non-object")
        return result


def _retry_after(error: urllib.error.HTTPError, attempt: int) -> float:
    value = error.headers.get("Retry-After") if error.headers else None
    try:
        return min(60.0, max(0.0, float(value))) if value else min(60.0, 2.0 ** attempt)
    except ValueError:
        return min(60.0, 2.0 ** attempt)


def run_shot(
    base: str,
    key: str,
    shot: Dict[str, Any],
    *,
    state_dir: Path,
    output_dir: Path,
    poll_seconds: float = 10.0,
    deadline: float = 3600.0,
    opener: Optional[Callable[..., Any]] = None,
    sleep: Callable[[float], None] = time.sleep,
    _submit_attempt: int = 0,
) -> Dict[str, Any]:
    if poll_seconds <= 0 or not math.isfinite(poll_seconds) or deadline <= 0 or not math.isfinite(deadline):
        raise ValueError("poll_seconds and deadline must be positive finite numbers")
    state_path = state_dir / f"{shot['id']}.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {"id": shot["id"], "body": {
        "model": shot["model"], "prompt": shot["prompt"], "duration": shot.get("duration", 5),
        "resolution": shot.get("resolution", "720p"), "aspect_ratio": shot.get("aspect_ratio", "16:9"),
        "generate_audio": shot.get("generate_audio", True),
    }}
    body = state["body"]
    fingerprint = hashlib.sha256(json.dumps(body, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    if state.get("fingerprint") and state["fingerprint"] != fingerprint:
        raise PipelineError(f"{shot['id']} plan changed after a request was recorded")
    state["fingerprint"] = fingerprint
    api = HTTP(base, key, opener)
    if state.get("status") == "done" and state.get("output") and Path(state["output"]).is_file():
        return state
    if state.get("request_id") and state.get("status") in {"failed", "expired", "unknown"}:
        raise PipelineError(f"{shot['id']} is terminal/unknown; inspect its state before creating a new run")
    if not state.get("request_id"):
        request = urllib.request.Request(base.rstrip("/") + "/videos/generations", data=json.dumps(body).encode(),
                                          method="POST", headers=headers(key))
        try:
            response = (opener or urllib.request.urlopen)(request, timeout=90)
            try:
                result = json.loads(response.read().decode("utf-8"))
            finally:
                response.close()
        except urllib.error.HTTPError as error:
            if error.code in {429, 503} and _submit_attempt < 4:
                sleep(_retry_after(error, 0))
                return run_shot(base, key, shot, state_dir=state_dir, output_dir=output_dir,
                                poll_seconds=poll_seconds, deadline=deadline, opener=opener, sleep=sleep,
                                _submit_attempt=_submit_attempt + 1)
            raise PipelineError(f"{shot['id']} submit HTTP {error.code}") from None
        except (TimeoutError, urllib.error.URLError) as error:
            state["status"] = "unknown"
            _atomic(state_path, state)
            raise PipelineError(f"{shot['id']} submit outcome is unknown; state saved") from error
        request_id = result.get("request_id")
        if not isinstance(request_id, str) or not request_id.replace("-", "").replace("_", "").isalnum():
            raise PipelineError(f"{shot['id']} response has no valid request_id")
        state.update({"request_id": request_id, "status": "pending"})
        _atomic(state_path, state)
    started = time.monotonic()
    while True:
        try:
            provider = api.request("GET", f"/videos/{state['request_id']}")
        except urllib.error.HTTPError as error:
            if error.code in {429, 503}:
                sleep(_retry_after(error, 0))
                continue
            raise
        status = provider.get("status")
        if status not in STATUSES:
            state["status"] = "unknown"
            _atomic(state_path, state)
            raise PipelineError(f"{shot['id']} returned unknown status {status!r}")
        state.update({"status": status, "provider": provider})
        _atomic(state_path, state)
        if status == "done":
            break
        if status in {"failed", "expired"}:
            return state
        if time.monotonic() + poll_seconds > started + deadline:
            state["status"] = "resume"
            _atomic(state_path, state)
            return state
        sleep(poll_seconds)
    target = output_dir / f"{shot['id']}.mp4"
    if target.exists():
        raise FileExistsError(str(target))
    raw = api.request("GET", f"/videos/{state['request_id']}/content", binary=True)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=target.parent, prefix=".video-", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(raw)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        os.link(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    state.update({"output": str(target), "sha256": hashlib.sha256(raw).hexdigest()})
    _atomic(state_path, state)
    return state


def assemble(states: List[Dict[str, Any]], output: Path) -> None:
    import subprocess
    clips = [Path(state["output"]) for state in states]
    if not clips or any(not path.is_file() for path in clips):
        raise PipelineError("all shots must be done before assembly")
    if output.exists():
        raise FileExistsError(str(output))
    with tempfile.NamedTemporaryFile("w", suffix=".ffconcat", delete=False, encoding="utf-8") as handle:
        manifest = Path(handle.name)
        handle.write("ffconcat version 1.0\n")
        for path in clips:
            handle.write("file '" + str(path.resolve()).replace("'", "'\\\\''") + "'\n")
    try:
        subprocess.run(["ffmpeg", "-nostdin", "-y", "-f", "concat", "-safe", "0", "-i", str(manifest),
                        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(output)], check=True)
    finally:
        manifest.unlink(missing_ok=True)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="bounded concurrent video pipeline")
    parser.add_argument("plan", type=Path)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--state-dir", type=Path, default=Path(".pipeline-state"))
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--manifest", type=Path, default=Path("pipeline-manifest.json"))
    parser.add_argument("--poll-seconds", type=float, default=10)
    parser.add_argument("--deadline", type=float, default=3600)
    parser.add_argument("--assemble", type=Path)
    args = parser.parse_args(argv)
    if not 1 <= args.workers <= 8:
        parser.error("workers must be 1..8")
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    shots = validate_plan(plan)
    key = os.environ.get("VIDEO_API_KEY")
    base = os.environ.get("VIDEO_API_BASE_URL", BASE_URL)
    args.state_dir.mkdir(parents=True, exist_ok=True)
    results = {}
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(run_shot, base, key, shot, state_dir=args.state_dir, output_dir=args.output_dir,
                               poll_seconds=args.poll_seconds, deadline=args.deadline): shot["id"] for shot in shots}
        for future in as_completed(futures):
            results[futures[future]] = future.result()
            _atomic(args.manifest, {"estimate_usd": estimate(plan), "shots": [results.get(s["id"], {"id": s["id"], "status": "running"}) for s in shots]})
    ordered = [results[s["id"]] for s in shots]
    _atomic(args.manifest, {"estimate_usd": estimate(plan), "shots": ordered})
    if args.assemble:
        if any(row.get("status") != "done" for row in ordered):
            raise PipelineError("cannot assemble until every shot is done")
        assemble(ordered, args.assemble)
    print(json.dumps({"estimate_usd": estimate(plan), "shots": ordered}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
