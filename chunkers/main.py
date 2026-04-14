"""
Оркестратор build_chunks_v2 pipeline.
Запуск: python -m chunkers.main [--full|--incremental] [--source SOURCE] [--dry-run]
"""
import argparse
import logging
import sys
import time

from chunkers.email_chunker import EmailChunker
from chunkers.onec_chunker import OneCChunker
from chunkers.messenger_chunker import MessengerChunker
from chunkers.km_chunker import KMChunker
from chunkers.attachment_chunker import AttachmentChunker
from chunkers.embedder import Embedder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("build_chunks_v2")

# Mapping source name → chunker class
CHUNKERS = {
    "email": EmailChunker,
    "1c": OneCChunker,
    "km": KMChunker,
    "messenger": MessengerChunker,
    "attachment": AttachmentChunker,
}


def parse_args():
    parser = argparse.ArgumentParser(description="build_chunks_v2 — RAG chunking pipeline")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--full", action="store_true", help="Полная пересборка всех чанков")
    mode.add_argument("--incremental", action="store_true", help="Только новые/изменённые документы")
    parser.add_argument("--source", choices=list(CHUNKERS.keys()),
                        help="Обработать только указанный источник")
    parser.add_argument("--dry-run", action="store_true",
                        help="Показать что будет сделано, не записывать")
    parser.add_argument("--no-embed", action="store_true",
                        help="Пропустить генерацию embeddings")
    parser.add_argument("--batch", type=int, default=0,
                        help="Ограничить количество документов для обработки (0 = все)")
    return parser.parse_args()


def main():
    args = parse_args()
    full = args.full
    dry_run = args.dry_run

    if not full and not args.incremental:
        logger.info("Режим не указан, используется --incremental по умолчанию")

    logger.info(f"=== build_chunks_v2 start === full={full} dry_run={dry_run} source={args.source}")
    t0 = time.time()

    # Select chunkers
    if args.source:
        selected = {args.source: CHUNKERS[args.source]}
    else:
        selected = CHUNKERS

    total_chunks = 0
    for name, chunker_cls in selected.items():
        logger.info(f"--- Processing: {name} ---")
        try:
            chunker = chunker_cls(dry_run=dry_run, batch_limit=args.batch)
        except TypeError:
            chunker = chunker_cls(dry_run=dry_run)
        try:
            chunks = chunker.generate_chunks(full=full)
            logger.info(f"  {name}: generated {len(chunks)} chunks")

            if chunks and not dry_run:
                saved = chunker.save_chunks(chunks)
                total_chunks += saved
            else:
                total_chunks += len(chunks)
        finally:
            chunker.close()

    elapsed = time.time() - t0
    logger.info(f"=== build_chunks_v2 done === {total_chunks} chunks, {elapsed:.1f}s")

    # Embedding (отдельный шаг)
    if not args.no_embed and not dry_run and total_chunks > 0:
        logger.info("Embedding step skipped — model not configured yet (use --no-embed)")
        # TODO: embedder.load_model() + embed + UPDATE source_chunks SET embedding = ...


if __name__ == "__main__":
    main()
