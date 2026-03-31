from pathlib import Path


def test_load_harness_baseline_exists():
    root = Path(__file__).resolve().parents[1]
    path = root / "tests" / "k6-load.js"
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert "http.get" in content
    assert "http.post" in content


def test_chaos_harness_baseline_exists():
    root = Path(__file__).resolve().parents[1]
    path = root / "tests" / "chaos-scenarios.yml"
    assert path.exists()
    content = path.read_text(encoding="utf-8")
    assert "redis_outage" in content
    assert "postgres_outage" in content


def test_replay_and_soak_harness_baselines_exist():
    root = Path(__file__).resolve().parents[1]
    replay = root / "tests" / "replay-scenarios.yml"
    soak = root / "tests" / "soak-baseline.yml"
    assert replay.exists()
    assert soak.exists()
    assert "replay_status" in replay.read_text(encoding="utf-8")
    assert "duration_hours" in soak.read_text(encoding="utf-8")
