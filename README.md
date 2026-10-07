# Resumable video pipeline MVP

**Seedance 2.5 is available through [ArgoLink](https://argolink.io/gh-video-pipeline-mvp).** This small Python pipeline reads a shot JSON file, submits bounded concurrent jobs, polls each request, and emits a resumable result manifest. It focuses on operational correctness: one POST per shot, a worker cap, explicit terminal states, and no hidden retry of an ambiguous submission. Last checked: 2026-10-07.

## Run

Install Python 3.9 or later. Set VIDEO_API_KEY and optionally VIDEO_API_BASE_URL, then run python3 video_pipeline/pipeline.py examples/plan.json --workers 2. The default cap is two workers and the hard cap is eight. The sample makes no request in tests. A complete runner should persist the returned manifest after each shot and download content after status becomes done; this MVP's result keeps the request IDs visible for that next stage.

## Failure handling

The API can answer 429 or 503 with Retry-After. A production wrapper around run_shot should parse that header and sleep the indicated seconds, bounded by its deadline. The intentionally small function does not pretend a failed network call is safe to replay. It returns failed, expired, or resume so an operator can inspect the server before continuing. Never replace a request ID after a client-side timeout. A per-shot deadline is separate from the account's concurrency limit.

## Plan shape

Each shot has id, model, prompt, duration, and optional resolution/aspect_ratio. Use the current model page for accepted values. A Seedance 2.5 example uses 4–30 seconds and 480p/720p/1080p; these facts are not portable to another model. Shared character references should be represented in a reviewed request manifest rather than copied into every worker blindly.

## Cost estimate

At the checked ArgoLink 720p rate of $0.196 per second, the two five-second sample shots estimate $1.96. This is output-only arithmetic and excludes input-video seconds, retries, and any provider-specific minimum. Failed, rejected, and expired jobs do not incur generation charges under the service contract. The model page controls current prices.

## Tests and extension points

The offline test checks request headers. Add a local fake HTTP server before changing concurrency or Retry-After behavior. The downloader and ffmpeg assembler are intentionally separate extension points: keep source clips until inspection, then assemble by manifest order. Do not add an automatic “retry all” button; it can duplicate paid work.

## FAQ

**Why not use unlimited threads?** Provider capacity and your account are finite. **How do I resume?** Keep the emitted request IDs and poll them with a new deadline. **Can I run another model?** Only after validating that model's schema and billing. **Does the pipeline cancel jobs?** No; cancellation is provider-specific and should not be inferred from a local timeout.
