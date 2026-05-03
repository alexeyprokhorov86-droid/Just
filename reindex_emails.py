#!/usr/bin/env python3
"""Reindex all emails into source_chunks via Qwen3 (EmailChunker pipeline).

Replaces the legacy e5 embeddings pipeline. Run once after migration or to
rebuild source_chunks for email source_kind.
"""
import os
import pathlib
import subprocess
import sys
from dotenv import load_dotenv

env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=str(env_path))

if __name__ == "__main__":
    script_dir = pathlib.Path(__file__).parent
    cmd = [
        str(script_dir / "venv" / "bin" / "python"),
        "-m", "chunkers.main",
        "--source", "email",
        "--full",
    ]
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(script_dir))
    sys.exit(result.returncode)
