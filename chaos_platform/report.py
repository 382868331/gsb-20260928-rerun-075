"""运行报告持久化：保存种子与完整输入，之后可原样重放。"""

import json
from pathlib import Path


def save_run(result, scenario, path: str | Path) -> Path:
    path = Path(path)
    payload = {
        "scenario": scenario.to_dict(),
        "result": result.to_dict(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def save_adaptive_run(payload: dict, scenario, path: str | Path) -> Path:
    path = Path(path)
    report = {
        "schema": "adaptive_run_v1",
        "scenario": scenario.to_dict(),
        "result": payload,
    }
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_run(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))
