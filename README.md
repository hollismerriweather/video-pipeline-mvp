# Concurrent resumable video pipeline

A small Python pipeline for a reviewed shot JSON file: validate each model contract, submit shots with a bounded worker pool, persist a request ID before polling, download completed MP4 files, and optionally assemble them with ffmpeg. Each shot owns its state file, so an interrupted worker can resume without re-submitting an accepted job.

[Open the Seedance 2.5 model page](https://argolink.io/en/models/seedance-2.5) for current limits and pricing. This repository is independent of the model provider. Last checked: 2026-10-07.

## Install

The runner uses only Python's standard library. Install ffmpeg if you want the optional assembly step.

~~~sh
git clone https://github.com/YOUR_ACCOUNT/video-pipeline-mvp
cd video-pipeline-mvp
python3 -m venv .venv
. .venv/bin/activate
ffmpeg -version
export VIDEO_API_KEY='from-a-secret-manager'
~~~

The default base URL is https://argolink.io/v1. For an HTTPS-compatible local test service set VIDEO_API_BASE_URL. Production keys and manifests must stay outside a public commit.

## Plan format

Each shot has a unique ID, a model ID that exists in the live catalog, a prompt, integer duration, resolution and fixed aspect ratio. This example contains two five-second Seedance 2.5 shots:

~~~json
{
  "shots": [
    {
      "id": "opening",
      "model": "seedance-2.5",
      "prompt": "A courier enters a quiet station at dawn.",
      "duration": 5,
      "resolution": "720p",
      "aspect_ratio": "16:9"
    },
    {
      "id": "clue",
      "model": "seedance-2.5",
      "prompt": "The courier reads a blue envelope beside a window.",
      "duration": 5,
      "resolution": "720p",
      "aspect_ratio": "16:9"
    }
  ]
}
~~~

Seedance 2.5 accepts 4–30 seconds and 480p, 720p or 1080p. Seedance 2.0 accepts 4–15 seconds and its own 480p, 720p, 1080p and 4k tiers. The validator deliberately keeps those contracts separate. Add HTTPS reference URLs only after uploading local media through the provider's media-ticket flow; this MVP does not silently upload a filesystem path.

## Run with a bounded pool

~~~sh
python -m video_pipeline.pipeline examples/plan.json \
  --workers 2 \
  --state-dir .pipeline-state \
  --output-dir output \
  --manifest pipeline-manifest.json
~~~

The worker setting is 1–8. The account may have a different limit; pick a cap that leaves capacity for other workloads. The orchestrator submits shots concurrently, writes the aggregate manifest after every completed future, and preserves input order in the final manifest.

For each shot, the first operation is POST /v1/videos/generations. The returned request_id is written to state-dir/shot-id.json before the first status request. The worker then reads GET /v1/videos/{request_id} and accepts only pending, done, failed or expired. It downloads GET /v1/videos/{request_id}/content only after done. A terminal failure is returned to the caller with its provider object. An unknown status is recorded and stops the shot instead of being treated as success.

A deadline returns resume while keeping request_id. Rerun with the same state directory to inspect that ID. A client timeout during POST is recorded as unknown because the server may have accepted it; never replace it with a new request automatically.

## Retry and concurrency rules

A 429 or 503 during the initial POST is retried up to four times, using Retry-After when it is a numeric delay and bounded exponential backoff otherwise. A successful POST is never repeated. A rate-limit or temporary-service response during status reads is retried for the same request. Other HTTP errors preserve a redacted response body. Every request, including a JSON POST, status GET and binary content GET, has the User-Agent video-pipeline-mvp/1.0; the API key is sent only in the Authorization header.

State files are written through a temporary file and atomic replace. Download uses a temporary file, fsync and a hard link, and refuses to replace an existing output. The state stores a body fingerprint; changing a plan after a request has been recorded is an explicit error, so a new run directory is required.

## Cost estimate

The runner prints an output-only estimate before the shot list in its final JSON:

| Model | Effective rates used by this checked example |
| --- | --- |
| Seedance 2.5 | 0.0874, 0.196, 0.483 USD/s at 480p, 720p, 1080p |
| Seedance 2.0 | 0.053, 0.11, 0.28, 0.58 USD/s at 480p, 720p, 1080p, 4k |

The two five-second 720p shots above estimate 1.96 USD. This arithmetic excludes reference-video seconds, account discounts, retries that were rejected before a job, and any replacement an operator creates after review. Failed, rejected and expired jobs are not charged under the model contract. The model page and returned usage object are authoritative.

External model pages can have different limits and formulas. Do not substitute a competitor's price into this table without a dated primary source. The pipeline intentionally does not publish unverified star counts, throughput or provider comparisons.

## Assemble after inspection

When all rows are done, assemble in plan order:

~~~sh
python -m video_pipeline.pipeline examples/plan.json \
  --workers 2 --state-dir .pipeline-state --output-dir output \
  --manifest pipeline-manifest.json --assemble episode.mp4
~~~

The command writes an ffconcat manifest with escaped absolute paths and calls ffmpeg with H.264, yuv420p and AAC output. It refuses to assemble when any shot is pending, failed, expired, resume or unknown, and refuses to overwrite episode.mp4. Inspect each source clip before deleting it; generation success does not prove an editorial requirement was met.

## Python entry point

The same lifecycle is available to an embedding process:

~~~python
import json
from pathlib import Path
from video_pipeline.pipeline import estimate, run_shot, validate_plan

plan = json.loads(Path("examples/plan.json").read_text())
validate_plan(plan)
state = run_shot(
    "https://argolink.io/v1",
    "read-from-a-secret-manager",
    plan["shots"][0],
    state_dir=Path(".pipeline-state"),
    output_dir=Path("output"),
)
print(state["status"], estimate(plan))
~~~

An embedding process should supply its own secret store and avoid putting the key in logs. The callable opener and sleep arguments exist for local tests, not for bypassing HTTPS or changing the request contract.

## Errors and FAQ

**Why limit workers?** Provider capacity and account limits are finite; eight is a hard safety ceiling in the CLI. **How do I resume?** Keep the state directory and rerun the same plan. **What does resume mean?** The local deadline expired while the provider job remains pending; the request ID is still valid. **Can I add automatic retry-all?** No: an accepted generation may be billable and a retry-all button can create duplicates. **Does the pipeline cancel jobs?** No; cancellation is provider-specific. **Can I use another model?** Add its live model-specific limits and rates to the validator before using it.

## Tests

~~~sh
python -m unittest discover -s tests -v
python -m py_compile video_pipeline/pipeline.py
~~~

Tests use a local fake opener. They verify model-specific validation, cost arithmetic, headers, ID persistence before status, unknown-state handling, and non-overwriting downloads. No production endpoint is called and no paid job is created.
