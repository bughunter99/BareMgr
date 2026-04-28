import json
import tempfile
import unittest
from pathlib import Path

from src.config_loader import load_config


class TestConfigLoader(unittest.TestCase):
    def test_load_json(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "cfg.json"
            p.write_text(json.dumps({"node_id": "n1", "weight": 10}), encoding="utf-8")

            cfg = load_config(str(p))

            self.assertEqual(cfg["node_id"], "n1")
            self.assertEqual(cfg["weight"], 10)

    def test_load_yaml(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "cfg.yml"
            p.write_text("node_id: n2\nweight: 20\n", encoding="utf-8")

            cfg = load_config(str(p))

            self.assertEqual(cfg["node_id"], "n2")
            self.assertEqual(cfg["weight"], 20)


if __name__ == "__main__":
    unittest.main()
