import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parents[1]))
from video_pipeline.pipeline import PipelineError, estimate, headers, run_shot, validate_plan


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
    def read(self):
        return self.payload
    def close(self):
        pass


class Recorder:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.requests = []
    def __call__(self, request, timeout):
        self.requests.append(request)
        return FakeResponse(self.payloads.pop(0))


class PipelineTests(unittest.TestCase):
    def plan(self):
        return {"shots": [
            {"id": "opening", "model": "seedance-2.5", "prompt": "enter", "duration": 5,
             "resolution": "720p", "aspect_ratio": "16:9"},
            {"id": "clue", "model": "seedance-2.5", "prompt": "read", "duration": 5,
             "resolution": "720p", "aspect_ratio": "16:9"},
        ]}

    def test_validate_and_estimate_use_model_specific_rates(self):
        plan = self.plan()
        self.assertEqual(len(validate_plan(plan)), 2)
        self.assertEqual(estimate(plan), 1.96)
        with self.assertRaises(ValueError):
            validate_plan({"shots": [{"id": "x", "model": "seedance-2.0", "prompt": "x", "duration": 30}]})

    def test_headers_have_neutral_key_and_user_agent(self):
        self.assertEqual(headers("secret")["Authorization"], "Bearer secret")
        self.assertEqual(headers("secret")["User-Agent"], "video-pipeline-mvp/1.0")

    def test_run_shot_persists_id_before_done_and_downloads(self):
        recorder = Recorder([b'{"request_id":"job-1"}', b'{"status":"done"}', b'mp4-bytes'])
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            shot = self.plan()["shots"][0]
            state = run_shot("https://api.test/v1", "secret", shot, state_dir=root / "state",
                             output_dir=root / "output", opener=recorder)
            saved = json.loads((root / "state/opening.json").read_text())
            self.assertEqual(saved["request_id"], "job-1")
            self.assertEqual(state["status"], "done")
            self.assertEqual((root / "output/opening.mp4").read_bytes(), b"mp4-bytes")
            self.assertEqual(recorder.requests[0].get_header("User-agent"), "video-pipeline-mvp/1.0")
            self.assertEqual(recorder.requests[0].get_header("Content-type"), "application/json")

    def test_unknown_status_is_recorded_and_stops(self):
        recorder = Recorder([b'{"request_id":"job-1"}', b'{"status":"running"}'])
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            with self.assertRaises(PipelineError):
                run_shot("https://api.test/v1", "secret", self.plan()["shots"][0],
                         state_dir=root / "state", output_dir=root / "output", opener=recorder)
            self.assertEqual(json.loads((root / "state/opening.json").read_text())["status"], "unknown")

    def test_existing_output_is_never_replaced(self):
        recorder = Recorder([b'{"request_id":"job-1"}', b'{"status":"done"}', b'new'])
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            output = root / "output/opening.mp4"
            output.parent.mkdir()
            output.write_bytes(b"old")
            with self.assertRaises(FileExistsError):
                run_shot("https://api.test/v1", "secret", self.plan()["shots"][0],
                         state_dir=root / "state", output_dir=root / "output", opener=recorder)
            self.assertEqual(output.read_bytes(), b"old")


if __name__ == "__main__":
    unittest.main()
