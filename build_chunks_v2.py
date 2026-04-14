#!/usr/bin/env python3
"""
build_chunks_v2.py — точка входа для RAG chunking pipeline.
Использование:
    python build_chunks_v2.py --full                    # полная пересборка
    python build_chunks_v2.py --incremental             # только новое
    python build_chunks_v2.py --source email --dry-run  # только email, без записи
    python build_chunks_v2.py --source km --no-embed    # km-чанки без embedding
"""
from chunkers.main import main

if __name__ == "__main__":
    main()
