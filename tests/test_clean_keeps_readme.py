"""`pipeline clean` wipes data/ and output/ but keeps the tracked data/README.md.

Run: python tests/test_clean_keeps_readme.py
"""
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

import pipeline.__main__ as cli


def test_clean_keeps_data_readme(tmp_path, monkeypatch):
    (tmp_path / "data" / "raw").mkdir(parents=True)
    (tmp_path / "data" / "raw" / "cache.bin").write_text("x")
    (tmp_path / "data" / "README.md").write_text("tracked")
    monkeypatch.setattr(cli, "REPO_ROOT", tmp_path)
    assert CliRunner().invoke(cli.clean_cmd, input="y\n").exit_code == 0
    assert (tmp_path / "data" / "README.md").read_text() == "tracked"
    assert not any((tmp_path / "data" / "raw").iterdir())


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as d, pytest.MonkeyPatch.context() as mp:
        test_clean_keeps_data_readme(Path(d), mp)
        print("ok test_clean_keeps_data_readme")
